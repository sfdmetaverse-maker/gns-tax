"""Daily AI News Bot — separate Telegram bot for AI news digests.

Uses its own bot token (AI_NEWS_BOT_TOKEN) and subscriber list.
News is fetched every 6 hours and cached in the DB.
Users get cached news instantly on /start or /ainews.
Daily push to subscribers at 9:30 AM ET.
"""

import logging
import os
from datetime import datetime

import requests
from flask import Blueprint, request as flask_request, jsonify

import db

logger = logging.getLogger(__name__)

ai_news_bp = Blueprint("ai_news", __name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"

NEWS_SOURCES = [
    "The Rundown AI (therundownai.com)",
    "TLDR AI (tldr.tech/ai)",
    "Ben's Bites (bensbites.co)",
    "Import AI (importai.substack.com)",
    "MIT Technology Review AI",
    "VentureBeat AI",
    "TechCrunch AI",
    "AI News (artificialintelligence-news.com)",
    "Hugging Face trending papers",
    "Reddit r/artificial and r/LocalLLaMA",
    "Matt Wolfe / FutureTools YouTube",
    "AI Explained YouTube",
    "Latent Space podcast",
    "Two Minute Papers YouTube",
    "arXiv cs.AI recent",
]


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


# ---------------------------------------------------------------------------
# News generation + caching
# ---------------------------------------------------------------------------

def _call_claude(prompt, max_tokens=4000):
    """Make a Claude API call. Returns text or None."""
    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed")
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        return None

    client = anthropic.Anthropic(api_key=api_key)
    try:
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=max_tokens,
            tools=[{"type": "web_search_20250305"}],
            messages=[{"role": "user", "content": prompt}],
        )
        # Extract text blocks from response (skip tool use/result blocks)
        text_parts = [block.text for block in msg.content if hasattr(block, "text")]
        return "\n".join(text_parts) if text_parts else None
    except Exception as e:
        logger.error("Claude API call failed: %s", e)
        return None


def _build_top5_prompt():
    today = datetime.now().strftime("%Y-%m-%d")
    today_display = datetime.now().strftime("%B %d, %Y")
    sources_list = "\n".join(f"- {s}" for s in NEWS_SOURCES)
    return f"""Today is {today}. Find the 5 most important AI news stories from the past 24-48 hours.

Search these sources:
{sources_list}

Return EXACTLY this format for Telegram (use *bold* not **bold**):

*Top 5 AI News — {today_display}*

1. *Headline* — one sentence summary (source)
2. *Headline* — one sentence summary (source)
3. *Headline* — one sentence summary (source)
4. *Headline* — one sentence summary (source)
5. *Headline* — one sentence summary (source)

Use /ainews for the full daily digest.

Keep it very concise. Include source name but no URLs."""


def _build_digest_prompt():
    today = datetime.now().strftime("%Y-%m-%d")
    today_display = datetime.now().strftime("%B %d, %Y")
    sources_list = "\n".join(f"- {s}" for s in NEWS_SOURCES)
    return f"""Today is {today}. Research and compile the latest AI news from the past 24-48 hours.

Search these sources and any other reliable AI news sources you can find:
{sources_list}

Create a comprehensive daily AI news digest with these sections:

1. *TOP STORIES* (5-7 most important stories with brief descriptions and source links)
2. *MODEL RELEASES & BENCHMARKS* (any new AI models, updates, or benchmark results)
3. *TOOLS & PRODUCTS* (new AI tools, apps, or product launches)
4. *FUNDING & BUSINESS* (funding rounds, acquisitions, business news)
5. *RESEARCH* (notable papers or breakthroughs)
6. *CREATOR CONTENT* (notable YouTube videos or podcast episodes from AI creators)
7. *HOW THIS BENEFITS YOU* (2-3 bullet points on how today's news could benefit a small business owner who uses AI for automation)

Format as clean Markdown suitable for Telegram (use *bold* not **bold**, use simple formatting).
Include source URLs where possible.
Keep it concise but comprehensive — aim for a 3-5 minute read.
Start with: *Daily AI News — {today_display}*"""


def refresh_cached_news():
    """Fetch fresh news from Claude and store in DB. Called every 6 hours."""
    logger.info("Refreshing cached AI news...")

    top5 = _call_claude(_build_top5_prompt(), max_tokens=800)
    if top5:
        db.save_cached_news("top5", top5)
        logger.info("Cached top5 news updated")

    digest = _call_claude(_build_digest_prompt(), max_tokens=4000)
    if digest:
        db.save_cached_news("digest", digest)
        logger.info("Cached full digest updated")

    return top5, digest


def get_cached_top5():
    """Get cached top 5 news. Returns text or None if no cache."""
    return db.get_cached_news("top5")


def get_cached_digest():
    """Get cached full digest. Returns text or None if no cache."""
    return db.get_cached_news("digest")


def send_daily_news(chat_ids=None):
    """Send cached digest to chat IDs, or all subscribers."""
    digest = get_cached_digest()
    if not digest:
        # No cache — try a fresh fetch
        digest = _call_claude(_build_digest_prompt(), max_tokens=4000)
        if digest:
            db.save_cached_news("digest", digest)

    if not digest:
        logger.error("Failed to get AI news digest")
        return None

    token = _bot_token()

    if chat_ids is None:
        chat_ids = db.get_ai_news_subscribers()

    for chat_id in chat_ids:
        _tg_send(chat_id, digest, token)
        logger.info("Sent daily AI news to chat_id=%s", chat_id)

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
        "/unsubscribe — stop daily news\n"
        "/status — check your subscription\n"
    ))

    # Send cached top 5 instantly
    top5 = get_cached_top5()
    if top5:
        _tg_send(chat_id, top5)
    else:
        _tg_send(chat_id, "News is being prepared. Use /ainews shortly to get the latest digest.")


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
    if is_sub:
        _tg_send(chat_id, (
            f"You are subscribed.\n"
            f"Total subscribers: {count}\n"
            f"Last news update: {time_str}\n"
            f"News refreshes every 6 hours."
        ))
    else:
        _tg_send(chat_id, "You are not subscribed.\nUse /subscribe to start receiving daily AI news.")


def _handle_ainews(chat_id):
    digest = get_cached_digest()
    if digest:
        _tg_send(chat_id, digest)
    else:
        _tg_send(chat_id, "News is being prepared. Please try again in a few minutes.")


@ai_news_bp.route("/webhook/ai-news", methods=["POST"])
def ai_news_webhook():
    """Handle incoming Telegram webhook for the AI News bot."""
    data = flask_request.get_json(silent=True)
    if not data:
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
        elif text.startswith("/status"):
            _handle_status(chat_id)
        else:
            _tg_send(chat_id, (
                "Commands:\n"
                "/subscribe — start receiving daily AI news\n"
                "/unsubscribe — stop daily news\n"
                "/ainews — get today's digest now\n"
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
