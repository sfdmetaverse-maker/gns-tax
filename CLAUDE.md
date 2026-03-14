# Gleam & Sip - Tax Filing App

## Overview
Flask web app for Canadian small business T2 corporate tax and GST/HST return preparation. Deployed on Fly.io at `gleamnsip.fly.dev`.

## Tech Stack
- **Backend:** Python 3.13, Flask, Gunicorn
- **Database:** Fly Managed Postgres (fly-db via pgbouncer)
- **OCR:** Claude Vision (claude-haiku-4-5) primary, pytesseract fallback
- **PDF parsing:** PyMuPDF (fitz) for rendering, Claude Vision for extraction
- **Deployment:** Fly.io (Dockerfile, shared-cpu-1x, 512MB, yyz region)
- **Telegram Bot:** Webhook at `/webhook/telegram`

## Architecture

### Entry Points
- `app.py` — main Flask app, runs on port 8080 (via gunicorn)
- `auth.py` — Flask-Login auth, user registration, login/logout
- `db.py` — all database operations, schema migrations, connection pooling
- `telegram_bot.py` — Telegram bot webhook handler (Blueprint)
- `schema.sql` — initial table definitions (run by `_run_schema()`)

### Key Files
- `templates/index.html` — single-page dashboard with tabbed UI
- `templates/wizard.html` — step-by-step guided workflow
- `gunicorn.conf.py` — gunicorn config (2 workers)
- `fly.toml` — Fly.io deployment config
- `Dockerfile` — Python 3.13-slim + tesseract-ocr

### Data Model
Three-layer financial document system:

1. **Bank entries** (`doc_type='bank'`) — authoritative ledger from chequing statements. Every real cash flow appears here.
2. **Credit card entries** (`doc_type='credit_card'`) — reference layer from Visa/MC statements. Used for matching against receipts. CC payments appear as lump sums in the bank statement.
3. **Receipts** (`doc_type='receipt'`) — detailed evidence with vendor, items, tax breakdown. Matched to CC or bank entries.
4. **POS entries** (`doc_type='pos'`) — from Toast/Square/GoDaddy imports.

### Transaction Matching
- `match_group` — shared ID linking entries that represent the same purchase (receipt + CC line)
- `match_role` — `'primary'` (counts in totals) vs `'supporting'` (evidence only, excluded from aggregation)
- `payment_method` — extracted from receipt OCR (visa, mastercard, amex, debit, cash)
- `statement_period` — e.g. `'2025-01'`, prevents re-importing same month
- `match_suggestions` table — auto-detected pairs pending user review (accept/reject)
- Matching engine: `find_matches(org_id)` in app.py, runs after every import

### Database Schema (transactions table)
Core: `id, user_id, org_id, date, description, store_name, store_address, card_info, amount, tax, source, category`
OCR: `ocr_text, ocr_file, ocr_bbox, file_hash`
Matching: `doc_type, match_group, match_role, payment_method, statement_period`
Status: `reviewed, possible_shared_expense, linked_to`

### Migrations
All migrations are in `db.py:_apply_migrations()`, idempotent (safe to run multiple times). Steps:
1-8: Multi-tenancy (organizations, org_id on all tables)
9: linked_to column
10: ocr_bbox column
11: Indexes
12: doc_type, match_group, match_role, payment_method, statement_period columns + backfill
13: match_suggestions table

## UI Tabs
- **Add** — scan receipts (camera/upload), import statements (bank/CC PDF/CSV, POS CSV), manual entry
- **Pending Review** — unreviewed receipts only (not statements), OCR review with image + edit form
- **Matching** — auto-detected receipt-to-statement pairs, side-by-side view with receipt image + statement entry, accept/reject
- **Bank** — collapsible list grouped by statement PDF, debit/credit columns, "View PDF" link
- **Credit Card** — collapsible list grouped by statement PDF, matched status column, "View PDF" link
- **Receipts** — checked/reviewed receipt entries
- **T2 Summary** — income statement, tax calculation (year-filtered)
- **GST/HST** — HST return summary (year-filtered)
- **Data Quality** — completeness checks, duplicate detection, warnings
- **Settings** — business info, province, fiscal year

Year selector appears inside Bank, Credit Card, Receipts, T2, GST, Data Quality tabs. Changes year via page navigation preserving active tab hash.

## OCR Pipeline
1. **Claude Vision** (primary) — sends image to claude-haiku-4-5 with structured JSON prompt
   - Receipts: returns array (supports multiple receipts per image) with vendor, date, items, subtotal, tax, card_info, payment_method, bbox
   - Statements: returns array of {date, description, amount, type} per page
2. **Tesseract** (fallback) — if Claude Vision fails, uses pytesseract + regex parsing
3. **PDF statements** — text extraction via PyMuPDF first, falls back to Claude Vision page-by-page OCR (`_ocr_statement_pdf()`)

## Telegram Bot
- Webhook: `POST /webhook/telegram`
- Setup: `POST /setup-telegram-webhook`
- Commands: `/start`, `/link your@email.com`
- Accepts: photos (receipts or statements), documents (PDF/CSV/images)
- Auto-classifies documents using `_classify_document()` — receipt vs bank_statement vs credit_card_statement
- Processes accordingly: receipts go through multi-receipt OCR, statements through statement extraction
- Runs `find_matches()` after every save
- Duplicate detection via file_hash

## API Endpoints
- `POST /api/add-txn` — programmatic transaction add (auth required)
- `POST /api/bulk-import` — bulk import with secret key (type: receipt/bank/credit_card/run_matching)
- `POST /webhook/telegram` — Telegram bot webhook

## Environment Variables
- `DATABASE_URL` — Postgres connection string
- `ANTHROPIC_API_KEY` — for Claude Vision OCR
- `TELEGRAM_BOT_TOKEN` — Telegram bot token
- `SECRET_KEY` — Flask session secret
- `BULK_IMPORT_KEY` — secret for bulk import API (default: gleam-bulk-2026)

## Important Conventions
- Expenses are stored as **negative** amounts, income as positive
- `file_hash` (SHA256) used for dedup across all uploads
- `uploads/` directory is a Fly volume mount at `/app/uploads`
- All monetary values stored as NUMERIC in Postgres
- Categories defined in `CATEGORIES` dict in app.py
- Auto-categorization via keyword matching in `auto_categorize()`
- Multi-tenancy via `org_id` on all tables
- `aggregate_txns()` skips entries with `match_role='supporting'` to avoid double-counting

## Deployment
```bash
fly deploy                    # deploy to Fly.io
fly logs -a gleamnsip         # view logs
fly ssh console -a gleamnsip  # SSH into container
```

## Common Operations
- The Flask instance should stay alive; only restart when code changes are made
- Do not kill processes on ports without permission
- RBC PDF statements require Claude Vision OCR (text parser can't handle multi-line format)
- After importing statements, run matching to detect receipt-statement pairs
