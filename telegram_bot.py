import base64
import hashlib
import io
import json
import logging
import os
import re
import uuid
from pathlib import Path

import requests
from flask import Blueprint, request as flask_request, jsonify

import db

logger = logging.getLogger(__name__)

telegram_bp = Blueprint("telegram", __name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"


def _bot_token():
    return os.environ.get("TELEGRAM_BOT_TOKEN", "")


def _tg_request(method, **kwargs):
    """Make a request to the Telegram Bot API."""
    token = _bot_token()
    url = TELEGRAM_API.format(token=token, method=method)
    resp = requests.post(url, json=kwargs, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _send_message(chat_id, text):
    """Send a text message to a Telegram chat."""
    _tg_request("sendMessage", chat_id=chat_id, text=text, parse_mode="Markdown")


def _download_file(file_id):
    """Download a file from Telegram by file_id. Returns (bytes, file_path)."""
    token = _bot_token()
    file_info = _tg_request("getFile", file_id=file_id)
    file_path = file_info["result"]["file_path"]
    download_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
    resp = requests.get(download_url, timeout=60)
    resp.raise_for_status()
    return resp.content, file_path


def _classify_document(image_bytes, media_type, api_key):
    """Ask Claude to classify an image as receipt, bank_statement, or credit_card_statement.
    Returns one of: 'receipt', 'bank_statement', 'credit_card_statement', 'unknown'."""
    try:
        import anthropic
    except ImportError:
        return "unknown"
    if not api_key:
        return "unknown"

    if "jpeg" in media_type or "jpg" in media_type:
        media_type = "image/jpeg"
    elif "png" in media_type:
        media_type = "image/png"
    else:
        media_type = "image/jpeg"

    b64 = base64.standard_b64encode(image_bytes).decode("utf-8")
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=50,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
                {"type": "text", "text": (
                    "What type of financial document is this? Reply with ONLY one of these exact words:\n"
                    "receipt\nbank_statement\ncredit_card_statement\nunknown"
                )}
            ]
        }]
    )
    try:
        result = msg.content[0].text.strip().lower().replace(" ", "_")
        if result in ("receipt", "bank_statement", "credit_card_statement"):
            return result
        if "bank" in result:
            return "bank_statement"
        if "credit" in result:
            return "credit_card_statement"
        if "receipt" in result:
            return "receipt"
        return "unknown"
    except Exception:
        return "unknown"


def _ocr_statement_image(image_bytes, media_type, api_key, doc_type="bank_statement"):
    """Use Claude Vision to extract transactions from a bank/credit card statement image.
    Returns list of transaction dicts."""
    try:
        import anthropic
    except ImportError:
        return None
    if not api_key:
        return None

    if "jpeg" in media_type or "jpg" in media_type:
        media_type = "image/jpeg"
    elif "png" in media_type:
        media_type = "image/png"
    else:
        media_type = "image/jpeg"

    source_label = "Credit Card" if "credit" in doc_type else "Bank"
    b64 = base64.standard_b64encode(image_bytes).decode("utf-8")
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4000,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
                {"type": "text", "text": (
                    f"This is a {source_label.lower()} statement. Extract ALL transactions.\n"
                    "Return ONLY a valid JSON array:\n"
                    '[{"date": "YYYY-MM-DD", "description": "payee/description", "amount": 0.00, "type": "debit_or_credit"}]\n'
                    "Rules:\n"
                    "- One object per transaction line\n"
                    "- date in YYYY-MM-DD format\n"
                    "- description = payee name or transaction description\n"
                    "- amount = positive number (absolute value)\n"
                    '- type = "debit" for purchases/withdrawals/payments, "credit" for deposits/refunds\n'
                    "- Include ALL transactions visible, even partial ones\n"
                    "- Skip summary lines, balances, and headers"
                )}
            ]
        }]
    )
    try:
        text = msg.content[0].text.strip()
        if "```" in text:
            text = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL).group(1).strip()
        raw = json.loads(text)
        if isinstance(raw, dict):
            raw = [raw]

        txns = []
        for item in raw:
            amt = abs(float(item.get("amount", 0) or 0))
            is_credit = item.get("type", "").lower() == "credit"
            dt = "bank" if doc_type == "bank_statement" else "credit_card"
            txns.append({
                "id": str(uuid.uuid4())[:8],
                "date": item.get("date", ""),
                "description": item.get("description", ""),
                "amount": amt if is_credit else -amt,
                "tax": 0,
                "source": source_label,
                "category": "other",
                "doc_type": dt,
                "reviewed": False,
            })
        return txns if txns else None
    except Exception as e:
        logger.error("Statement OCR parse failed: %s", e)
        return None


def _ocr_receipts(image_bytes, media_type, api_key):
    """Use Claude Vision to extract receipt data. Returns list of parsed dicts."""
    try:
        import anthropic
    except ImportError:
        return None

    if not api_key:
        return None

    # Normalize media type
    if "jpeg" in media_type or "jpg" in media_type:
        media_type = "image/jpeg"
    elif "png" in media_type:
        media_type = "image/png"
    elif "webp" in media_type:
        media_type = "image/webp"
    else:
        media_type = "image/jpeg"

    b64 = base64.standard_b64encode(image_bytes).decode("utf-8")

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1500,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
                {"type": "text", "text": (
                    "This image may contain one or more receipts. Extract data from EVERY receipt visible.\n"
                    "Return ONLY a valid JSON array (even if there is just one receipt):\n"
                    '[{"vendor": "store name", "address": "full store address", "date": "YYYY-MM-DD", '
                    '"items": "brief description of items purchased", '
                    '"subtotal": 0.00, "tax": 0.00, "total": 0.00, '
                    '"card_info": "card type and last 4 digits"}]\n'
                    "Rules:\n"
                    "- One object per receipt in the image\n"
                    "- vendor = store/business name at top of receipt\n"
                    "- address = full street address, city, province as shown\n"
                    "- items = brief summary of what was purchased\n"
                    "- subtotal = amount before tax\n"
                    "- tax = HST, GST, PST, or any sales tax shown\n"
                    "- total = final amount paid\n"
                    "- card_info = e.g. 'Visa *1234' or 'Mastercard *5678' from payment line\n"
                    "- If subtotal is missing, calculate it as total - tax\n"
                    "- Use 0.00 for any amount you cannot find\n"
                    "- Use empty string for any text field you cannot find\n"
                    "- Date must be YYYY-MM-DD format"
                )}
            ]
        }]
    )

    try:
        text = msg.content[0].text.strip()
        if "```" in text:
            text = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL).group(1).strip()
        raw = json.loads(text)
        if isinstance(raw, dict):
            raw = [raw]

        results = []
        for parsed in raw:
            result = {
                "description": parsed.get("items", "") or parsed.get("vendor", ""),
                "store_name": parsed.get("vendor", ""),
                "store_address": parsed.get("address", ""),
                "card_info": parsed.get("card_info", ""),
                "date": parsed.get("date", ""),
                "amount": float(parsed.get("subtotal", 0) or 0),
                "tax": float(parsed.get("tax", 0) or 0),
            }
            if result["amount"] == 0 and parsed.get("total"):
                result["amount"] = round(float(parsed["total"]) - result["tax"], 2)
            results.append(result)
        return results if results else None
    except Exception as e:
        logger.error("Telegram OCR parse failed: %s", e)
        return None


def _handle_ainews(chat_id):
    """Handle the /ainews command — send today's AI news digest."""
    _send_message(chat_id, "Researching today's AI news... This may take a moment.")
    try:
        from daily_ai_news import send_daily_news
        digest = send_daily_news(chat_ids=[str(chat_id)])
        if not digest:
            _send_message(chat_id, "Sorry, couldn't fetch AI news right now. Try again later.")
    except Exception as e:
        logger.error("AI news fetch failed: %s", e)
        _send_message(chat_id, "Something went wrong fetching AI news. Try again later.")


def _handle_start(chat_id):
    """Handle the /start command."""
    _send_message(chat_id, (
        "Welcome to Gleam & Sip Receipt Bot!\n\n"
        "To get started, link your Telegram account:\n"
        "  /link your@email.com\n\n"
        "Once linked, you can send:\n"
        "- Receipt photos (single or multiple receipts in one image)\n"
        "- PDF receipts or bank/credit card statements\n"
        "- CSV bank/credit card exports\n\n"
        "Commands:\n"
        "  /ainews — get today's AI news digest\n\n"
        "I'll process them and add transactions to your account automatically."
    ))


def _handle_link(chat_id, args):
    """Handle the /link <email> command to connect Telegram to a web account."""
    if not args:
        _send_message(chat_id, "Usage: /link your@email.com")
        return

    email = args.strip().lower()
    user = db.get_user_by_email(email)
    if not user:
        _send_message(chat_id, f"No account found for {email}. Please sign up in the web app first.")
        return

    db.update_user_telegram_id(user["id"], str(chat_id))
    _send_message(chat_id, (
        f"Linked to {email}. You can now send receipt photos, "
        "PDFs, or CSV statements and they'll be added to your transactions."
    ))


def _get_user_or_reply(chat_id):
    """Get user by telegram ID, or send a message asking to link."""
    user = db.get_user_by_telegram_id(str(chat_id))
    if not user:
        _send_message(chat_id, (
            "Your Telegram account is not linked yet. "
            "Use /link your@email.com to connect to your Gleam & Sip account."
        ))
        return None
    return user


def _save_receipt_txns(user, chat_id, image_bytes, media_type, save_name, file_hash):
    """OCR an image and save transaction(s). Returns count of receipts added."""
    user_id = user["id"]
    org_id = user.get("org_id")
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    parsed_list = _ocr_receipts(image_bytes, media_type, api_key)
    if not parsed_list:
        parsed_list = [{"description": "Telegram receipt", "date": "", "amount": 0, "tax": 0,
                        "store_name": "", "store_address": "", "card_info": ""}]

    count = 0
    summaries = []
    for parsed in parsed_list:
        from app import auto_categorize
        cat = auto_categorize(parsed.get("description", ""))

        txn = {
            "id": str(uuid.uuid4())[:8],
            "date": parsed["date"],
            "description": parsed.get("description", "") or "Telegram receipt",
            "store_name": parsed.get("store_name", ""),
            "store_address": parsed.get("store_address", ""),
            "card_info": parsed.get("card_info", ""),
            "amount": -abs(round(parsed.get("amount", 0), 2)),
            "tax": round(parsed.get("tax", 0), 2),
            "source": "Telegram",
            "category": cat,
            "doc_type": "receipt",
            "payment_method": parsed.get("payment_method", ""),
            "ocr_file": save_name,
            "file_hash": file_hash,
            "reviewed": False,
        }
        db.save_txn(org_id, user_id, txn)
        store = parsed.get("store_name") or parsed.get("description", "?")
        summaries.append(f"  {store} | ${abs(txn['amount']):.2f} + ${txn['tax']:.2f} tax | {txn['date'] or 'no date'}")
        count += 1

    try:
        from app import find_matches
        find_matches(org_id)
    except Exception as e:
        logger.error("find_matches failed after receipt save: %s", e)

    return count, summaries


def _handle_photo(chat_id, message):
    """Process a photo message: classify, then OCR as receipt or statement."""
    user = _get_user_or_reply(chat_id)
    if not user:
        return

    user_id = user["id"]
    org_id = user.get("org_id")
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    photos = message.get("photo", [])
    if not photos:
        _send_message(chat_id, "Could not read the photo. Please try again.")
        return

    file_id = photos[-1]["file_id"]

    try:
        image_bytes, _ = _download_file(file_id)
    except Exception as e:
        _send_message(chat_id, f"Failed to download image: {e}")
        return

    file_hash = hashlib.sha256(image_bytes).hexdigest()
    if db.check_file_hash_exists(org_id, file_hash):
        _send_message(chat_id, "Skipped: duplicate file (already uploaded).")
        return

    upload_dir = Path(__file__).parent / "uploads" / str(org_id)
    upload_dir.mkdir(parents=True, exist_ok=True)
    save_name = f"{uuid.uuid4().hex[:8]}_telegram.jpg"
    (upload_dir / save_name).write_bytes(image_bytes)

    # Classify the document
    doc_type = _classify_document(image_bytes, "image/jpeg", api_key)
    logger.info("Telegram photo classified as: %s", doc_type)

    if doc_type in ("bank_statement", "credit_card_statement"):
        # Extract statement transactions
        txns = _ocr_statement_image(image_bytes, "image/jpeg", api_key, doc_type)
        if txns and len(txns) > 0:
            for t in txns:
                t["ocr_file"] = save_name
                t["file_hash"] = file_hash
            db.save_txns_bulk(org_id, user_id, txns)
            try:
                from app import find_matches
                find_matches(org_id)
            except Exception as e:
                logger.error("find_matches failed after statement save: %s", e)
            source = "Credit Card" if "credit" in doc_type else "Bank"
            _send_message(chat_id, f"Imported {len(txns)} transactions from {source} statement.\nReview in the web app.")
            return

    # Default: treat as receipt(s)
    count, summaries = _save_receipt_txns(user, chat_id, image_bytes, "image/jpeg", save_name, file_hash)
    if count == 1:
        _send_message(chat_id, f"Added 1 receipt:\n{summaries[0]}\n\nReview in the web app.")
    else:
        lines = "\n".join(summaries)
        _send_message(chat_id, f"Added {count} receipts:\n{lines}\n\nReview in the web app.")


def _handle_document(chat_id, message):
    """Process a document: PDF/image receipts, CSV/PDF bank statements."""
    user = _get_user_or_reply(chat_id)
    if not user:
        return

    user_id = user["id"]
    org_id = user.get("org_id")

    doc = message.get("document", {})
    file_id = doc.get("file_id")
    file_name = doc.get("file_name", "document")
    mime_type = doc.get("mime_type", "")

    if not file_id:
        _send_message(chat_id, "Could not read the document. Please try again.")
        return

    try:
        file_bytes, _ = _download_file(file_id)
    except Exception as e:
        _send_message(chat_id, f"Failed to download document: {e}")
        return

    file_hash = hashlib.sha256(file_bytes).hexdigest()
    ext = Path(file_name).suffix.lower()

    upload_dir = Path(__file__).parent / "uploads" / str(org_id)
    upload_dir.mkdir(parents=True, exist_ok=True)
    save_name = f"{uuid.uuid4().hex[:8]}_telegram{ext}"
    (upload_dir / save_name).write_bytes(file_bytes)

    # CSV — bank/credit card statement
    if ext == ".csv" or "csv" in mime_type:
        try:
            text = file_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = file_bytes.decode("latin-1")

        from app import detect_and_parse_csv
        # Try to detect source from caption or default to "Bank"
        caption = message.get("caption", "").strip()
        source = caption if caption else "Bank"
        new_txns = detect_and_parse_csv(text, source)
        if not new_txns:
            _send_message(chat_id, "No transactions found in CSV. Check the file format.")
            return
        db.save_txns_bulk(org_id, user_id, new_txns)
        try:
            from app import find_matches
            find_matches(org_id)
        except Exception as e:
            logger.error("find_matches failed after CSV import: %s", e)
        _send_message(chat_id, f"Imported {len(new_txns)} transactions from {source} CSV.\nReview in the web app.")
        return

    # PDF — classify then route
    if ext == ".pdf" or "pdf" in mime_type:
        if db.check_file_hash_exists(org_id, file_hash):
            _send_message(chat_id, "Skipped: duplicate file (already uploaded).")
            return

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")

        # Render first page to image for classification
        try:
            import fitz
            from PIL import Image as PILImage
            doc_pdf = fitz.open(stream=file_bytes, filetype="pdf")
            pix = doc_pdf[0].get_pixmap(dpi=200)
            img = PILImage.frombytes("RGB", [pix.width, pix.height], pix.samples)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=85)
            img_bytes = buf.getvalue()
            doc_pdf.close()
        except Exception as e:
            _send_message(chat_id, f"Failed to read PDF: {e}")
            return

        # Classify the document
        doc_type = _classify_document(img_bytes, "image/jpeg", api_key)
        logger.info("Telegram PDF classified as: %s", doc_type)

        if doc_type in ("bank_statement", "credit_card_statement"):
            # Try structured PDF parsing first
            from app import parse_bank_statement_pdf
            source = "Credit Card" if "credit" in doc_type else "Bank"
            dt = "bank" if doc_type == "bank_statement" else "credit_card"
            new_txns = parse_bank_statement_pdf(file_bytes, source)
            if new_txns and len(new_txns) >= 1:
                for t in new_txns:
                    t["doc_type"] = dt
                db.save_txns_bulk(org_id, user_id, new_txns)
                try:
                    from app import find_matches
                    find_matches(org_id)
                except Exception as e:
                    logger.error("find_matches failed after PDF statement save: %s", e)
                _send_message(chat_id, f"Imported {len(new_txns)} transactions from {source} statement.\nReview in the web app.")
                return
            # Fall back to vision-based statement extraction
            txns = _ocr_statement_image(img_bytes, "image/jpeg", api_key, doc_type)
            if txns and len(txns) > 0:
                for t in txns:
                    t["ocr_file"] = save_name
                    t["file_hash"] = file_hash
                db.save_txns_bulk(org_id, user_id, txns)
                try:
                    from app import find_matches
                    find_matches(org_id)
                except Exception as e:
                    logger.error("find_matches failed after PDF OCR statement save: %s", e)
                _send_message(chat_id, f"Imported {len(txns)} transactions from {source} statement.\nReview in the web app.")
                return

        # Receipt or unknown — OCR as receipt
        count, summaries = _save_receipt_txns(user, chat_id, img_bytes, "image/jpeg", save_name, file_hash)
        if count == 1:
            _send_message(chat_id, f"Added 1 receipt from PDF:\n{summaries[0]}\n\nReview in the web app.")
        else:
            lines = "\n".join(summaries)
            _send_message(chat_id, f"Added {count} receipts from PDF:\n{lines}\n\nReview in the web app.")
        return

    # Image sent as document (uncompressed) — classify then route
    if ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff", ".tif"):
        if db.check_file_hash_exists(org_id, file_hash):
            _send_message(chat_id, "Skipped: duplicate file (already uploaded).")
            return

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        media_type = "image/jpeg" if ext in (".jpg", ".jpeg") else f"image/{ext.lstrip('.')}"

        doc_type = _classify_document(file_bytes, media_type, api_key)
        logger.info("Telegram image doc classified as: %s", doc_type)

        if doc_type in ("bank_statement", "credit_card_statement"):
            txns = _ocr_statement_image(file_bytes, media_type, api_key, doc_type)
            if txns and len(txns) > 0:
                for t in txns:
                    t["ocr_file"] = save_name
                    t["file_hash"] = file_hash
                db.save_txns_bulk(org_id, user_id, txns)
                try:
                    from app import find_matches
                    find_matches(org_id)
                except Exception as e:
                    logger.error("find_matches failed after image doc statement save: %s", e)
                source = "Credit Card" if "credit" in doc_type else "Bank"
                _send_message(chat_id, f"Imported {len(txns)} transactions from {source} statement.\nReview in the web app.")
                return

        # Receipt
        count, summaries = _save_receipt_txns(user, chat_id, file_bytes, media_type, save_name, file_hash)
        if count == 1:
            _send_message(chat_id, f"Added 1 receipt:\n{summaries[0]}\n\nReview in the web app.")
        else:
            lines = "\n".join(summaries)
            _send_message(chat_id, f"Added {count} receipts:\n{lines}\n\nReview in the web app.")
        return

    _send_message(chat_id, f"Unsupported file type: {ext}. Send photos, PDFs, or CSV files.")


@telegram_bp.route("/webhook/telegram", methods=["POST"])
def telegram_webhook():
    """Handle incoming Telegram webhook updates."""
    data = flask_request.get_json(silent=True)
    if not data:
        return jsonify({"ok": True}), 200

    message = data.get("message")
    if not message:
        return jsonify({"ok": True}), 200

    chat_id = message["chat"]["id"]
    text = message.get("text", "").strip()

    try:
        if text.startswith("/start"):
            _handle_start(chat_id)
        elif text.startswith("/link"):
            args = text[len("/link"):].strip()
            _handle_link(chat_id, args)
        elif text.startswith("/ainews"):
            _handle_ainews(chat_id)
        elif message.get("photo"):
            _handle_photo(chat_id, message)
        elif message.get("document"):
            _handle_document(chat_id, message)
        else:
            _send_message(chat_id, (
                "Send me a photo of a receipt, a PDF, or a CSV bank statement.\n"
                "Commands: /start, /link your@email.com"
            ))
    except Exception as e:
        logger.error("Telegram webhook error: %s", e, exc_info=True)
        try:
            _send_message(chat_id, f"Something went wrong processing your message. Please try again.")
        except Exception:
            pass

    return jsonify({"ok": True}), 200


@telegram_bp.route("/setup-telegram-webhook", methods=["POST"])
def setup_telegram_webhook():
    """Register the Telegram webhook. Call once after deploy."""
    token = _bot_token()
    if not token:
        return jsonify({"error": "TELEGRAM_BOT_TOKEN not set"}), 500
    webhook_url = "https://gleamnsip.fly.dev/webhook/telegram"
    result = _tg_request("setWebhook", url=webhook_url)
    return jsonify(result)
