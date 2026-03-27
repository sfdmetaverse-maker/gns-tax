"""Daily AI News Bot — separate Telegram bot for AI news digests.

Uses its own bot token (AI_NEWS_BOT_TOKEN) and subscriber list.
News is fetched from RSS feeds every 6 hours and cached in the DB.
Users get cached news instantly on /start or /ainews.
Daily push to subscribers at 9:30 AM ET.
"""

import logging
import os
import time as _time
import threading
from datetime import datetime, timedelta, timezone

import requests
from flask import Blueprint, request as flask_request, jsonify

import db

logger = logging.getLogger(__name__)

ai_news_bp = Blueprint("ai_news", __name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"

# Supported languages: code -> (display name, Google Translate code)
LANGUAGES = {
    "en": ("English", "en"),
    "zh-TW": ("繁體中文", "zh-TW"),
    "fr": ("Français", "fr"),
    "de": ("Deutsch", "de"),
}

# RSS feeds for AI news (free, no API key needed)
RSS_FEEDS = {
    "TechCrunch AI": "https://techcrunch.com/category/artificial-intelligence/feed/",
    "VentureBeat AI": "https://venturebeat.com/category/ai/feed/",
    "MIT Tech Review": "https://www.technologyreview.com/topic/artificial-intelligence/feed",
    "AI News": "https://www.artificialintelligence-news.com/feed/",
    "arXiv cs.AI": "https://rss.arxiv.org/rss/cs.AI",
    "The Rundown AI": "https://www.therundownai.com/feed",
    "Ars Technica AI": "https://feeds.arstechnica.com/arstechnica/technology-lab",
    "The Verge AI": "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml",
    "Wired AI": "https://www.wired.com/feed/tag/ai/latest/rss",
    "HuggingFace Papers": "https://huggingface.co/papers/rss",
}

# Reddit JSON endpoints (no auth needed, just add .json)
REDDIT_FEEDS = {
    "r/artificial": "https://www.reddit.com/r/artificial/hot.json?limit=10",
    "r/LocalLLaMA": "https://www.reddit.com/r/LocalLLaMA/hot.json?limit=10",
}

# Filter out political / non-tech content
SKIP_KEYWORDS = {
    "trump", "biden", "congress", "senate", "democrat", "republican",
    "election", "legislation", "lawmaker", "white house", "executive order",
    "political", "politician", "partisan", "lobbying", "geopolitics",
    "tariff", "sanction", "immigration", "border wall", "gun control",
    "abortion", "supreme court nomination",
}

# Boost articles about actual AI tech development
TECH_BOOST_KEYWORDS = {
    "model", "benchmark", "llm", "gpt", "claude", "gemini", "llama",
    "open source", "fine-tuning", "training", "inference", "transformer",
    "diffusion", "multimodal", "agent", "rag", "embedding", "api",
    "release", "launch", "update", "paper", "research", "architecture",
    "gpu", "tpu", "cuda", "parameter", "token", "context window",
    "dataset", "hugging face", "pytorch", "tensorflow", "weights",
    "tool use", "function calling", "reasoning", "coding", "vision",
    "robotics", "autonomous", "neural", "deep learning", "machine learning",
    "startup", "funding", "product", "platform", "developer", "sdk",
}


def _bot_token():
    return os.environ.get("AI_NEWS_BOT_TOKEN", "")


def _tg_request(method, **kwargs):
    """Make a request to the AI News Telegram Bot API."""
    token = _bot_token()
    if not token:
        logger.error("AI_NEWS_BOT_TOKEN not set")
        return None
    url = TELEGRAM_API.format(token=token, method=method)
    resp = requests.post(url, json=kwargs, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _tg_send(chat_id, text, token=None):
    """Send a Telegram message, splitting if over 4096 chars."""
    token = token or _bot_token()
    if not token:
        logger.error("AI_NEWS_BOT_TOKEN not set")
        return

    chunks = []
    while len(text) > 4096:
        split_at = text.rfind("\n", 0, 4096)
        if split_at < 100:
            split_at = 4096
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    chunks.append(text)

    for chunk in chunks:
        try:
            url = TELEGRAM_API.format(token=token, method="sendMessage")
            resp = requests.post(url, json={
                "chat_id": chat_id,
                "text": chunk,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error("Failed to send AI news message: %s", e)
            # Retry without markdown if parse fails
            try:
                url = TELEGRAM_API.format(token=token, method="sendMessage")
                requests.post(url, json={
                    "chat_id": chat_id,
                    "text": chunk,
                    "disable_web_page_preview": True,
                }, timeout=30)
            except Exception:
                pass


def _translate_text(text, target_lang):
    """Translate text using Google Translate (free). Returns translated text."""
    if target_lang == "en":
        return text
    try:
        from deep_translator import GoogleTranslator
        # Split into chunks of ~4500 chars (Google Translate limit is ~5000)
        chunks = []
        while len(text) > 4500:
            split_at = text.rfind("\n", 0, 4500)
            if split_at < 100:
                split_at = 4500
            chunks.append(text[:split_at])
            text = text[split_at:]
        chunks.append(text)

        translated_parts = []
        translator = GoogleTranslator(source="en", target=target_lang)
        for chunk in chunks:
            translated_parts.append(translator.translate(chunk))

        return "\n".join(translated_parts)
    except Exception as e:
        logger.error("Translation to %s failed: %s", target_lang, e)
        return text  # Return original on failure


def _get_news_for_lang(news_type, lang):
    """Get cached news, translated if needed. Caches translations too."""
    if lang == "en":
        return db.get_cached_news(news_type)

    # Check for cached translation (skip if empty — cleared on refresh)
    cache_key = f"{news_type}_{lang}"
    cached = db.get_cached_news(cache_key)
    if cached and cached.strip():
        return cached

    # Translate from English and cache
    english = db.get_cached_news(news_type)
    if not english:
        return None

    translated = _translate_text(english, lang)
    if translated and translated != english:
        db.save_cached_news(cache_key, translated)
        logger.info("Cached %s translation (%d chars)", cache_key, len(translated))
    return translated


def _tg_send_inline_keyboard(chat_id, text, keyboard):
    """Send a message with an inline keyboard."""
    token = _bot_token()
    if not token:
        return
    url = TELEGRAM_API.format(token=token, method="sendMessage")
    try:
        requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "reply_markup": {"inline_keyboard": keyboard},
            "parse_mode": "Markdown",
        }, timeout=30)
    except Exception as e:
        logger.error("Failed to send inline keyboard: %s", e)


# ---------------------------------------------------------------------------
# RSS-based news fetching (free, no API credits needed)
# ---------------------------------------------------------------------------

def _parse_rss_date(entry):
    """Extract a datetime from an RSS entry, return None if unparsable."""
    import email.utils
    for field in ("published", "updated"):
        raw = getattr(entry, field, None) or entry.get(field)
        if raw:
            try:
                parsed = email.utils.parsedate_to_datetime(raw)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except Exception:
                pass
    # feedparser's time struct
    for field in ("published_parsed", "updated_parsed"):
        ts = getattr(entry, field, None) or entry.get(field)
        if ts:
            try:
                from calendar import timegm
                return datetime.fromtimestamp(timegm(ts), tz=timezone.utc)
            except Exception:
                pass
    return None


def _fetch_rss_articles():
    """Fetch recent articles from all RSS feeds. Returns list of dicts."""
    try:
        import feedparser
    except ImportError:
        logger.error("feedparser not installed — pip install feedparser")
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    articles = []

    for source_name, url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:15]:
                pub_date = _parse_rss_date(entry)
                if pub_date and pub_date < cutoff:
                    continue
                title = entry.get("title", "").strip()
                link = entry.get("link", "")
                summary = entry.get("summary", "")
                # Clean HTML tags from summary
                import re
                summary = re.sub(r"<[^>]+>", "", summary).strip()
                if len(summary) > 200:
                    summary = summary[:197] + "..."
                if title:
                    articles.append({
                        "title": title,
                        "link": link,
                        "summary": summary,
                        "source": source_name,
                        "date": pub_date,
                    })
        except Exception as e:
            logger.warning("Failed to fetch RSS from %s: %s", source_name, e)

    return articles


def _fetch_reddit_posts():
    """Fetch top posts from AI subreddits."""
    articles = []
    headers = {"User-Agent": "DailyAINewsBot/1.0"}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)

    for source_name, url in REDDIT_FEEDS.items():
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            for child in data.get("data", {}).get("children", []):
                post = child.get("data", {})
                created = datetime.fromtimestamp(post.get("created_utc", 0), tz=timezone.utc)
                if created < cutoff:
                    continue
                title = post.get("title", "").strip()
                link = f"https://reddit.com{post.get('permalink', '')}"
                score = post.get("score", 0)
                if title and score > 10:
                    articles.append({
                        "title": title,
                        "link": link,
                        "summary": f"({score} upvotes)",
                        "source": source_name,
                        "date": created,
                    })
        except Exception as e:
            logger.warning("Failed to fetch Reddit %s: %s", source_name, e)

    return articles


def _is_political(title, summary):
    """Check if article is political rather than AI tech."""
    text = (title + " " + summary).lower()
    return any(kw in text for kw in SKIP_KEYWORDS)


def _tech_score(title, summary):
    """Score how tech/AI-focused an article is. Higher = more relevant."""
    text = (title + " " + summary).lower()
    return sum(1 for kw in TECH_BOOST_KEYWORDS if kw in text)


def _filter_and_deduplicate(articles):
    """Filter out political content, deduplicate, and sort by tech relevance."""
    # Filter out political articles
    filtered = [a for a in articles if not _is_political(a["title"], a.get("summary", ""))]
    logger.info("Filtered %d political articles (kept %d)", len(articles) - len(filtered), len(filtered))

    # Deduplicate
    seen_titles = set()
    unique = []
    for a in filtered:
        norm = a["title"].lower().strip()
        is_dup = False
        for seen in seen_titles:
            words_a = set(norm.split())
            words_b = set(seen.split())
            if len(words_a & words_b) > 0.6 * max(len(words_a), len(words_b), 1):
                is_dup = True
                break
        if not is_dup:
            seen_titles.add(norm)
            a["_tech_score"] = _tech_score(a["title"], a.get("summary", ""))
            unique.append(a)

    # Sort by tech score (higher first), then by date
    unique.sort(key=lambda a: (a.get("_tech_score", 0),
                                a["date"] or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True)
    return unique


def _build_top5_from_articles(articles):
    """Format top 5 articles as Telegram message."""
    today_display = datetime.now().strftime("%B %d, %Y")

    if not articles:
        return None

    # Sort by date (newest first), take top 5
    articles = sorted(articles, key=lambda a: a["date"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    top = articles[:5]

    lines = [f"*Top 5 AI News — {today_display}*\n"]
    for i, a in enumerate(top, 1):
        lines.append(f"{i}. *{a['title']}* — {a['summary']} ({a['source']})")

    lines.append("\nUse /ainews for the full daily digest.")
    return "\n".join(lines)


def _build_digest_from_articles(articles):
    """Format all articles as a comprehensive Telegram digest."""
    today_display = datetime.now().strftime("%B %d, %Y")

    if not articles:
        return None

    # Sort by date (newest first)
    articles = sorted(articles, key=lambda a: a["date"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    # Categorize by source type
    top_stories = []
    research = []
    reddit_posts = []
    other_news = []

    for a in articles:
        src = a["source"]
        if src == "arXiv cs.AI":
            research.append(a)
        elif src.startswith("r/"):
            reddit_posts.append(a)
        elif src in ("TechCrunch AI", "VentureBeat AI", "MIT Tech Review", "The Verge AI"):
            top_stories.append(a)
        else:
            other_news.append(a)

    lines = [f"*Daily AI News — {today_display}*\n"]

    # Top Stories
    if top_stories:
        lines.append("*TOP STORIES*")
        for a in top_stories[:7]:
            link_text = f"[Link]({a['link']})" if a["link"] else ""
            lines.append(f"• *{a['title']}* — {a['summary']} ({a['source']}) {link_text}")
        lines.append("")

    # More News
    if other_news:
        lines.append("*MORE AI NEWS*")
        for a in other_news[:7]:
            link_text = f"[Link]({a['link']})" if a["link"] else ""
            lines.append(f"• *{a['title']}* — {a['summary']} ({a['source']}) {link_text}")
        lines.append("")

    # Research
    if research:
        lines.append("*RESEARCH (arXiv)*")
        for a in research[:5]:
            link_text = f"[Link]({a['link']})" if a["link"] else ""
            lines.append(f"• {a['title']} {link_text}")
        lines.append("")

    # Reddit buzz
    if reddit_posts:
        lines.append("*REDDIT BUZZ*")
        for a in reddit_posts[:5]:
            link_text = f"[Link]({a['link']})" if a["link"] else ""
            lines.append(f"• {a['title']} {a['summary']} {link_text}")
        lines.append("")

    # Sources footer
    lines.append("*RECOMMENDED FOLLOWS*")
    lines.append("Newsletters: The Rundown AI, TLDR AI, Ben's Bites, Import AI")
    lines.append("YouTube: Matt Wolfe, AI Explained, Two Minute Papers")
    lines.append("Podcasts: Latent Space")

    return "\n".join(lines)


def _is_cache_stale(max_age_hours=6):
    """Check if cached news is older than max_age_hours."""
    updated = db.get_cached_news_time("digest")
    if not updated:
        return True
    # Make both timezone-aware for comparison
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - updated
    return age > timedelta(hours=max_age_hours)


def refresh_cached_news():
    """Fetch fresh news from RSS feeds and cache in DB."""
    logger.info("Refreshing cached AI news from RSS feeds...")

    rss_articles = _fetch_rss_articles()
    reddit_articles = _fetch_reddit_posts()
    all_articles = _filter_and_deduplicate(rss_articles + reddit_articles)

    logger.info("Fetched %d articles (%d RSS + %d Reddit, %d after dedup)",
                len(all_articles), len(rss_articles), len(reddit_articles), len(all_articles))

    top5 = _build_top5_from_articles(all_articles)
    if top5:
        db.save_cached_news("top5", top5)
        logger.info("Cached top5 news updated (%d chars)", len(top5))

    digest = _build_digest_from_articles(all_articles)
    if digest:
        db.save_cached_news("digest", digest)
        logger.info("Cached full digest updated (%d chars)", len(digest))

    # Clear old translated caches so they regenerate from fresh English
    for lang_code in LANGUAGES:
        if lang_code != "en":
            for ntype in ("top5", "digest"):
                cache_key = f"{ntype}_{lang_code}"
                try:
                    db.save_cached_news(cache_key, "")
                except Exception:
                    pass

    return top5, digest


def get_cached_top5():
    """Get cached top 5 news. Returns text or None if no cache."""
    return db.get_cached_news("top5")


def get_cached_digest():
    """Get cached full digest. Returns text or None if no cache."""
    return db.get_cached_news("digest")


def send_daily_news(chat_ids=None):
    """Send cached digest to all subscribers in their preferred language."""
    digest = get_cached_digest()
    if not digest:
        refresh_cached_news()
        digest = get_cached_digest()

    if not digest:
        logger.error("Failed to get AI news digest")
        return None

    token = _bot_token()

    if chat_ids is None:
        # Get subscribers with their language preferences
        subs = db.get_subscribers_with_lang()
        for chat_id, lang in subs:
            text = _get_news_for_lang("digest", lang)
            if text:
                _tg_send(chat_id, text, token)
                logger.info("Sent daily AI news to chat_id=%s (lang=%s)", chat_id, lang)
    else:
        for chat_id in chat_ids:
            lang = db.get_subscriber_lang(chat_id)
            text = _get_news_for_lang("digest", lang)
            if text:
                _tg_send(chat_id, text, token)

    return digest


def save_digest_to_file(digest, base_dir="daily-ai-news"):
    """Save the digest as a markdown file organized by date."""
    now = datetime.now()
    dir_path = os.path.join(base_dir, now.strftime("%Y"), now.strftime("%m"))
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, now.strftime("%Y-%m-%d") + ".md")
    with open(file_path, "w") as f:
        f.write(digest)
    logger.info("Saved daily AI news to %s", file_path)
    return file_path


# ---------------------------------------------------------------------------
# Telegram bot webhook handlers
# ---------------------------------------------------------------------------

def _handle_start(chat_id, first_name, username):
    # Auto-subscribe on start
    if not db.is_ai_news_subscriber(str(chat_id)):
        db.add_ai_news_subscriber(str(chat_id), first_name, username)

    _tg_send(chat_id, (
        f"Hi {first_name}! Welcome to *Daily AI News* bot.\n\n"
        "You're now subscribed! You'll get a curated AI digest every day at 9:30 AM ET.\n\n"
        "Commands:\n"
        "/ainews — get today's full digest\n"
        "/language — choose your language\n"
        "/unsubscribe — stop daily news\n"
        "/status — check your subscription\n"
    ))

    # Send cached top 5 instantly, or fetch fresh if stale/empty
    lang = db.get_subscriber_lang(str(chat_id))
    if _is_cache_stale():
        _tg_send(chat_id, "Fetching today's top AI news — one moment...")
        _trigger_refresh_and_send(chat_id, "top5")
    else:
        top5 = _get_news_for_lang("top5", lang)
        if top5:
            _tg_send(chat_id, top5)
        else:
            _tg_send(chat_id, "Fetching today's top AI news — one moment...")
            _trigger_refresh_and_send(chat_id, "top5")


def _handle_subscribe(chat_id, first_name, username):
    already = db.is_ai_news_subscriber(str(chat_id))
    if already:
        _tg_send(chat_id, "You're already subscribed! You'll get the daily digest at 9:30 AM ET.")
        return
    db.add_ai_news_subscriber(str(chat_id), first_name, username)
    _tg_send(chat_id, (
        "Subscribed! You'll receive the daily AI news digest at 9:30 AM ET.\n\n"
        "Use /ainews anytime to get today's digest immediately."
    ))


def _handle_unsubscribe(chat_id):
    db.remove_ai_news_subscriber(str(chat_id))
    _tg_send(chat_id, "Unsubscribed. You won't receive daily digests anymore.\nUse /subscribe to re-join anytime.")


def _handle_status(chat_id):
    is_sub = db.is_ai_news_subscriber(str(chat_id))
    count = db.count_ai_news_subscribers()
    updated = db.get_cached_news_time("top5")
    time_str = updated.strftime("%Y-%m-%d %H:%M ET") if updated else "never"
    lang = db.get_subscriber_lang(str(chat_id))
    lang_name = LANGUAGES.get(lang, ("English",))[0]
    if is_sub:
        _tg_send(chat_id, (
            f"You are subscribed.\n"
            f"Language: {lang_name}\n"
            f"Total subscribers: {count}\n"
            f"Last news update: {time_str}\n"
            f"News refreshes every 6 hours.\n"
            f"Use /language to change language."
        ))
    else:
        _tg_send(chat_id, "You are not subscribed.\nUse /subscribe to start receiving daily AI news.")


def _trigger_refresh_and_send(chat_id, news_type="digest"):
    """Refresh news in background and send to user in their language."""
    lang = db.get_subscriber_lang(str(chat_id))

    def _do_refresh():
        try:
            logger.info("Background refresh triggered for chat_id=%s (lang=%s)", chat_id, lang)
            refresh_cached_news()
            text = _get_news_for_lang(news_type, lang)
            if text:
                _tg_send(chat_id, text)
            else:
                _tg_send(chat_id, "Sorry, couldn't fetch news right now. Try /ainews again shortly.")
        except Exception as e:
            logger.error("Background refresh failed: %s", e)
            _tg_send(chat_id, "Sorry, something went wrong fetching news. Try /ainews again.")
    threading.Thread(target=_do_refresh, daemon=True).start()


def _handle_language(chat_id):
    """Show language selection inline keyboard."""
    keyboard = []
    for code, (name, _) in LANGUAGES.items():
        keyboard.append([{"text": name, "callback_data": f"lang:{code}"}])
    _tg_send_inline_keyboard(chat_id, "Choose your news language:", keyboard)


def _handle_lang_callback(chat_id, lang_code):
    """Set user's language preference from callback."""
    if lang_code not in LANGUAGES:
        return
    db.set_subscriber_lang(str(chat_id), lang_code)
    lang_name = LANGUAGES[lang_code][0]
    _tg_send(chat_id, f"Language set to *{lang_name}*.\nYour next /ainews will be in {lang_name}.")
    # Clear cached translations so they refresh
    logger.info("Language set to %s for chat_id=%s", lang_code, chat_id)


def _handle_ainews(chat_id):
    lang = db.get_subscriber_lang(str(chat_id))
    # If cache is stale (>6h old) or empty, refresh first
    if _is_cache_stale():
        _tg_send(chat_id, "Fetching fresh AI news — one moment...")
        _trigger_refresh_and_send(chat_id, "digest")
    else:
        digest = _get_news_for_lang("digest", lang)
        if digest:
            _tg_send(chat_id, digest)
        else:
            _tg_send(chat_id, "Fetching today's AI news now — one moment...")
            _trigger_refresh_and_send(chat_id, "digest")


@ai_news_bp.route("/webhook/ai-news", methods=["POST"])
def ai_news_webhook():
    """Handle incoming Telegram webhook for the AI News bot."""
    data = flask_request.get_json(silent=True)
    if not data:
        return jsonify({"ok": True}), 200

    # Handle callback queries (inline keyboard button presses)
    callback = data.get("callback_query")
    if callback:
        cb_chat_id = callback["message"]["chat"]["id"]
        cb_data = callback.get("data", "")
        try:
            if cb_data.startswith("lang:"):
                lang_code = cb_data.split(":", 1)[1]
                _handle_lang_callback(cb_chat_id, lang_code)
            # Answer the callback to remove loading state
            token = _bot_token()
            if token:
                url = TELEGRAM_API.format(token=token, method="answerCallbackQuery")
                requests.post(url, json={"callback_query_id": callback["id"]}, timeout=10)
        except Exception as e:
            logger.error("Callback query error: %s", e, exc_info=True)
        return jsonify({"ok": True}), 200

    message = data.get("message")
    if not message:
        return jsonify({"ok": True}), 200

    chat_id = message["chat"]["id"]
    text = message.get("text", "").strip()
    first_name = message.get("from", {}).get("first_name", "")
    username = message.get("from", {}).get("username", "")

    try:
        if text.startswith("/start"):
            _handle_start(chat_id, first_name, username)
        elif text.startswith("/subscribe"):
            _handle_subscribe(chat_id, first_name, username)
        elif text.startswith("/unsubscribe"):
            _handle_unsubscribe(chat_id)
        elif text.startswith("/ainews"):
            _handle_ainews(chat_id)
        elif text.startswith("/language"):
            _handle_language(chat_id)
        elif text.startswith("/status"):
            _handle_status(chat_id)
        else:
            _tg_send(chat_id, (
                "Commands:\n"
                "/subscribe — start receiving daily AI news\n"
                "/unsubscribe — stop daily news\n"
                "/ainews — get today's digest now\n"
                "/language — choose your language\n"
                "/status — check subscription"
            ))
    except Exception as e:
        logger.error("AI News webhook error: %s", e, exc_info=True)
        try:
            _tg_send(chat_id, "Something went wrong. Please try again.")
        except Exception:
            pass

    return jsonify({"ok": True}), 200


@ai_news_bp.route("/setup-ai-news-webhook", methods=["POST"])
def setup_ai_news_webhook():
    """Register the AI News bot Telegram webhook. Call once after deploy."""
    token = _bot_token()
    if not token:
        return jsonify({"error": "AI_NEWS_BOT_TOKEN not set"}), 500
    webhook_url = "https://gleamnsip.fly.dev/webhook/ai-news"
    result = _tg_request("setWebhook", url=webhook_url)
    return jsonify(result)
