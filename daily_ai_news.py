"""Daily AI News — fetch, compile, and send AI news digest via Telegram."""

import json
import logging
import os
import re
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"

# Sources to search (RSS/web endpoints that don't require auth)
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


def _tg_send(chat_id, text, token=None):
    """Send a Telegram message, splitting if over 4096 chars."""
    token = token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return

    # Telegram max message length is 4096
    chunks = []
    while len(text) > 4096:
        # Find a good split point
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
            logger.error("Failed to send Telegram message: %s", e)
            # Retry without markdown if parse fails
            try:
                resp = requests.post(url, json={
                    "chat_id": chat_id,
                    "text": chunk,
                    "disable_web_page_preview": True,
                }, timeout=30)
            except Exception:
                pass


def fetch_ai_news_digest():
    """Use Claude to research and compile today's AI news digest."""
    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed")
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        return None

    today = datetime.now().strftime("%Y-%m-%d")
    today_display = datetime.now().strftime("%B %d, %Y")

    sources_list = "\n".join(f"- {s}" for s in NEWS_SOURCES)

    prompt = f"""Today is {today}. Research and compile the latest AI news from the past 24-48 hours.

Search these sources and any other reliable AI news sources you can find:
{sources_list}

Create a comprehensive daily AI news digest with these sections:

1. **TOP STORIES** (5-7 most important stories with brief descriptions and source links)
2. **MODEL RELEASES & BENCHMARKS** (any new AI models, updates, or benchmark results)
3. **TOOLS & PRODUCTS** (new AI tools, apps, or product launches)
4. **FUNDING & BUSINESS** (funding rounds, acquisitions, business news)
5. **RESEARCH** (notable papers or breakthroughs)
6. **CREATOR CONTENT** (notable YouTube videos or podcast episodes from AI creators)
7. **BENEFIT TO YOU** (2-3 bullet points on how today's news could benefit a small business owner who runs a tax filing app and uses AI for OCR/automation)

Format as clean Markdown suitable for Telegram (use *bold* not **bold**, use simple formatting).
Include source URLs where possible.
Keep it concise but comprehensive — aim for a 3-5 minute read.
Start with: *Daily AI News — {today_display}*"""

    client = anthropic.Anthropic(api_key=api_key)

    try:
        # Use Claude with web search for real-time news
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text
    except Exception as e:
        logger.error("Claude API call failed: %s", e)
        return None


def send_daily_news(chat_ids=None):
    """Fetch AI news and send to specified Telegram chat IDs.

    If chat_ids is None, sends to all linked Telegram users.
    Returns the digest text or None on failure.
    """
    digest = fetch_ai_news_digest()
    if not digest:
        logger.error("Failed to generate AI news digest")
        return None

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")

    if chat_ids is None:
        # Get all users with linked Telegram accounts
        try:
            import db
            chat_ids = db.get_all_telegram_chat_ids()
        except Exception as e:
            logger.error("Failed to get Telegram chat IDs: %s", e)
            return digest

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
