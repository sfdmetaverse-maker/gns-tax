import os
import time
import logging
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.pool
import psycopg2.extras

_pool = None
_db_initialized = False
logger = logging.getLogger(__name__)


def get_pool():
    global _pool
    if _pool is None:
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 10, db_url)
    return _pool


@contextmanager
def get_conn():
    global _pool
    try:
        pool = get_pool()
        conn = pool.getconn()
    except psycopg2.OperationalError:
        # Connection pool may be stale; reset and retry once
        _pool = None
        pool = get_pool()
        conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def _run_schema():
    schema_path = Path(__file__).parent / "schema.sql"
    sql = schema_path.read_text()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def _apply_migrations():
    """Apply schema migrations for org-based multi-tenancy.
    Safe to run multiple times — all statements are idempotent."""
    import uuid as _uuid
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1. Create organizations table (handle leftover sequence from failed attempt)
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'organizations'")
            if not cur.fetchone():
                cur.execute("DROP SEQUENCE IF EXISTS organizations_id_seq CASCADE")
                cur.execute("""
                    CREATE TABLE organizations (
                        id SERIAL PRIMARY KEY,
                        slug TEXT UNIQUE NOT NULL,
                        name TEXT NOT NULL,
                        tagline TEXT NOT NULL DEFAULT 'Tax Filing Assistant',
                        logo_url TEXT NOT NULL DEFAULT '',
                        primary_color TEXT NOT NULL DEFAULT '#92702a',
                        invite_code TEXT NOT NULL DEFAULT '',
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """)

            # 2. Add columns to users if missing
            for col, defn in [
                ("org_id", "INTEGER REFERENCES organizations(id)"),
                ("role", "TEXT NOT NULL DEFAULT 'member'"),
                ("is_superadmin", "BOOLEAN NOT NULL DEFAULT FALSE"),
            ]:
                cur.execute("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'users' AND column_name = %s
                """, (col,))
                if not cur.fetchone():
                    cur.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")

            # 3. Add org_id to business_settings if missing
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'business_settings' AND column_name = 'org_id'
            """)
            if not cur.fetchone():
                cur.execute("ALTER TABLE business_settings ADD COLUMN org_id INTEGER REFERENCES organizations(id)")

            # 4. Add org_id to transactions if missing
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'transactions' AND column_name = 'org_id'
            """)
            if not cur.fetchone():
                cur.execute("ALTER TABLE transactions ADD COLUMN org_id INTEGER REFERENCES organizations(id)")

            # 5. Ensure default org exists
            cur.execute("SELECT id FROM organizations WHERE slug = 'gleam-and-sip'")
            row = cur.fetchone()
            if not row:
                invite = _uuid.uuid4().hex[:12]
                cur.execute("""
                    INSERT INTO organizations (slug, name, tagline, primary_color, invite_code)
                    VALUES ('gleam-and-sip', 'Gleam & Sip', 'Tax Filing Assistant', '#92702a', %s)
                    RETURNING id
                """, (invite,))
                org_id = cur.fetchone()[0]
                logger.info(f"Created default org (id={org_id}, invite_code={invite})")
            else:
                org_id = row[0]

            # 6. Backfill org_id on all tables
            cur.execute("UPDATE users SET org_id = %s WHERE org_id IS NULL", (org_id,))
            cur.execute("UPDATE business_settings SET org_id = %s WHERE org_id IS NULL", (org_id,))
            cur.execute("UPDATE transactions SET org_id = %s WHERE org_id IS NULL", (org_id,))

            # 7. Mark first user as superadmin
            cur.execute("SELECT id FROM users WHERE is_superadmin = TRUE LIMIT 1")
            if not cur.fetchone():
                cur.execute("UPDATE users SET is_superadmin = TRUE, role = 'admin' WHERE id = (SELECT MIN(id) FROM users)")

            # 8. Add unique constraint on business_settings.org_id if missing
            cur.execute("""
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_name = 'business_settings' AND constraint_name = 'business_settings_org_id_key'
            """)
            if not cur.fetchone():
                # Drop old user_id unique constraint if present
                cur.execute("""
                    SELECT constraint_name FROM information_schema.table_constraints
                    WHERE table_name = 'business_settings' AND constraint_type = 'UNIQUE'
                """)
                for r in cur.fetchall():
                    cur.execute(f"ALTER TABLE business_settings DROP CONSTRAINT {r[0]}")
                cur.execute("ALTER TABLE business_settings ADD CONSTRAINT business_settings_org_id_key UNIQUE (org_id)")

            # 9. Add linked_to column for linking related invoices
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'transactions' AND column_name = 'linked_to'
            """)
            if not cur.fetchone():
                cur.execute("ALTER TABLE transactions ADD COLUMN linked_to TEXT NOT NULL DEFAULT ''")

            # 10. Add ocr_bbox column for multi-receipt bounding boxes
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'transactions' AND column_name = 'ocr_bbox'
            """)
            if not cur.fetchone():
                cur.execute("ALTER TABLE transactions ADD COLUMN ocr_bbox TEXT NOT NULL DEFAULT ''")

            # 11. Add indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_txn_org ON transactions(org_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_txn_org_date ON transactions(org_id, date)")

            # 12. Add doc_type, match_group, match_role, payment_method, statement_period columns
            for col, defn in [
                ("doc_type", "TEXT NOT NULL DEFAULT 'receipt'"),
                ("match_group", "TEXT NOT NULL DEFAULT ''"),
                ("match_role", "TEXT NOT NULL DEFAULT 'primary'"),
                ("payment_method", "TEXT NOT NULL DEFAULT ''"),
                ("statement_period", "TEXT NOT NULL DEFAULT ''"),
            ]:
                cur.execute("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'transactions' AND column_name = %s
                """, (col,))
                if not cur.fetchone():
                    cur.execute(f"ALTER TABLE transactions ADD COLUMN {col} {defn}")

            # Backfill doc_type from source
            cur.execute("""
                UPDATE transactions SET doc_type = 'bank'
                WHERE doc_type = 'receipt' AND source = 'Bank'
            """)
            cur.execute("""
                UPDATE transactions SET doc_type = 'credit_card'
                WHERE doc_type = 'receipt' AND source = 'Credit Card'
            """)
            cur.execute("""
                UPDATE transactions SET doc_type = 'receipt'
                WHERE doc_type = 'receipt' AND (source LIKE 'Receipt%%' OR source = 'Telegram')
            """)
            cur.execute("""
                UPDATE transactions SET doc_type = 'pos'
                WHERE doc_type = 'receipt' AND source IN ('Toast', 'Square', 'GoDaddy')
            """)

            # Backfill match_role = 'supporting' for linked transactions
            cur.execute("""
                UPDATE transactions SET match_role = 'supporting'
                WHERE linked_to IS NOT NULL AND linked_to != '' AND match_role = 'primary'
            """)

            # 13. Create match_suggestions table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS match_suggestions (
                    id SERIAL PRIMARY KEY,
                    org_id INTEGER REFERENCES organizations(id),
                    txn_id_a TEXT NOT NULL,
                    txn_id_b TEXT NOT NULL,
                    confidence NUMERIC NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_match_org ON match_suggestions(org_id, status)")

            # 14. Create ai_news_subscribers table for the AI News Telegram bot
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ai_news_subscribers (
                    id SERIAL PRIMARY KEY,
                    chat_id TEXT UNIQUE NOT NULL,
                    first_name TEXT NOT NULL DEFAULT '',
                    username TEXT NOT NULL DEFAULT '',
                    subscribed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    is_active BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)


def init_db():
    """Try to initialize DB. If Postgres isn't ready, it will be retried lazily."""
    global _db_initialized
    for attempt in range(5):
        try:
            _run_schema()
            _apply_migrations()
            _db_initialized = True
            return
        except (psycopg2.OperationalError, RuntimeError) as e:
            logger.warning(f"DB init attempt {attempt + 1}/5 failed: {e}")
            if attempt < 4:
                time.sleep(2)
    logger.warning("DB init failed after 5 attempts; will retry lazily on first request.")


def ensure_db():
    """Called before DB operations to ensure schema is initialized."""
    global _db_initialized
    if not _db_initialized:
        _apply_migrations()
        _run_schema()
        _db_initialized = True


def close_pool():
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None


# ---------------------------------------------------------------------------
# Organizations
# ---------------------------------------------------------------------------

def create_org(name, slug, invite_code="", primary_color="#92702a", tagline="Tax Filing Assistant", logo_url=""):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO organizations (name, slug, invite_code, primary_color, tagline, logo_url)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING *
            """, (name, slug, invite_code, primary_color, tagline, logo_url))
            return dict(cur.fetchone())


def get_org_by_id(org_id):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM organizations WHERE id = %s", (org_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def get_org_by_slug(slug):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM organizations WHERE slug = %s", (slug,))
            row = cur.fetchone()
            return dict(row) if row else None


def get_org_by_invite_code(code):
    if not code:
        return None
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM organizations WHERE invite_code = %s AND is_active = TRUE", (code,))
            row = cur.fetchone()
            return dict(row) if row else None


def list_orgs():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT o.*,
                    (SELECT COUNT(*) FROM users WHERE org_id = o.id) AS user_count,
                    (SELECT COUNT(*) FROM transactions WHERE org_id = o.id) AS txn_count
                FROM organizations o ORDER BY o.id
            """)
            return [dict(r) for r in cur.fetchall()]


def update_org(org_id, fields):
    allowed = {"name", "slug", "tagline", "logo_url", "primary_color", "invite_code", "is_active"}
    safe = {k: v for k, v in fields.items() if k in allowed}
    if not safe:
        return
    set_clause = ", ".join(f"{k} = %s" for k in safe)
    values = list(safe.values()) + [org_id]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE organizations SET {set_clause} WHERE id = %s", values)


def get_org_users(org_id):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email, role, is_superadmin, created_at, last_login FROM users WHERE org_id = %s ORDER BY id",
                (org_id,),
            )
            return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def create_user(email, password_hash, org_id=None, role="member"):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (email, password_hash, org_id, role) VALUES (%s, %s, %s, %s) RETURNING id",
                (email, password_hash, org_id, role),
            )
            user_id = cur.fetchone()[0]
            # Create business_settings keyed to org if this is the first user in the org
            if org_id:
                cur.execute("SELECT 1 FROM business_settings WHERE org_id = %s", (org_id,))
                if not cur.fetchone():
                    cur.execute("INSERT INTO business_settings (user_id, org_id) VALUES (%s, %s)", (user_id, org_id))
            else:
                cur.execute("INSERT INTO business_settings (user_id) VALUES (%s)", (user_id,))
            return user_id


def get_user_by_email(email):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            return cur.fetchone()


def get_user_by_id(user_id):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            return cur.fetchone()


def get_user_by_phone(phone):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE phone = %s", (phone,))
            return cur.fetchone()


def update_user_phone(user_id, phone):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET phone = %s WHERE id = %s", (phone, user_id))


def get_user_by_telegram_id(chat_id):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE telegram_chat_id = %s", (str(chat_id),))
            return cur.fetchone()


def update_user_telegram_id(user_id, chat_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET telegram_chat_id = %s WHERE id = %s",
                (str(chat_id), user_id),
            )


def update_last_login(user_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (user_id,))


def get_all_telegram_chat_ids():
    """Return list of all distinct telegram_chat_id values (non-null)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT telegram_chat_id FROM users "
                "WHERE telegram_chat_id IS NOT NULL AND telegram_chat_id != ''"
            )
            return [row[0] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# AI News Subscribers (for the separate AI News Telegram bot)
# ---------------------------------------------------------------------------

def add_ai_news_subscriber(chat_id, first_name="", username=""):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_news_subscribers (chat_id, first_name, username, is_active)
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (chat_id)
                DO UPDATE SET is_active = TRUE, first_name = EXCLUDED.first_name,
                             username = EXCLUDED.username
            """, (str(chat_id), first_name, username))


def remove_ai_news_subscriber(chat_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ai_news_subscribers SET is_active = FALSE WHERE chat_id = %s",
                (str(chat_id),)
            )


def is_ai_news_subscriber(chat_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT is_active FROM ai_news_subscribers WHERE chat_id = %s",
                (str(chat_id),)
            )
            row = cur.fetchone()
            return row[0] if row else False


def get_ai_news_subscribers():
    """Return list of active subscriber chat_ids."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT chat_id FROM ai_news_subscribers WHERE is_active = TRUE"
            )
            return [row[0] for row in cur.fetchall()]


def count_ai_news_subscribers():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ai_news_subscribers WHERE is_active = TRUE")
            return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Business Settings (scoped to org_id)
# ---------------------------------------------------------------------------

def load_settings(org_id):
    ensure_db()
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM business_settings WHERE org_id = %s", (org_id,))
            row = cur.fetchone()
            if not row:
                return _default_settings()
            return {
                "business": {
                    "name": row["name"],
                    "bn": row["bn"],
                    "province": row["province"],
                    "fiscal_year_end": row["fiscal_year_end"],
                    "ccpc": row["ccpc"],
                },
                "manual_adjustments": {
                    "opening_inventory": float(row["opening_inventory"]),
                    "closing_inventory": float(row["closing_inventory"]),
                    "depreciation_cca": float(row["depreciation_cca"]),
                    "gst_installments_paid": float(row["gst_installments_paid"]),
                    "auto_estimate_hst": row["auto_estimate_hst"],
                },
                "anthropic_api_key": row["anthropic_api_key"],
            }


def save_settings(org_id, data):
    biz = data.get("business", {})
    adj = data.get("manual_adjustments", {})
    api_key = data.get("anthropic_api_key", "")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO business_settings (org_id, name, bn, province, fiscal_year_end, ccpc,
                    opening_inventory, closing_inventory, depreciation_cca, gst_installments_paid,
                    auto_estimate_hst, anthropic_api_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (org_id) DO UPDATE SET
                    name = EXCLUDED.name, bn = EXCLUDED.bn, province = EXCLUDED.province,
                    fiscal_year_end = EXCLUDED.fiscal_year_end, ccpc = EXCLUDED.ccpc,
                    opening_inventory = EXCLUDED.opening_inventory, closing_inventory = EXCLUDED.closing_inventory,
                    depreciation_cca = EXCLUDED.depreciation_cca, gst_installments_paid = EXCLUDED.gst_installments_paid,
                    auto_estimate_hst = EXCLUDED.auto_estimate_hst, anthropic_api_key = EXCLUDED.anthropic_api_key
            """, (
                org_id,
                biz.get("name", ""), biz.get("bn", ""), biz.get("province", "ON"),
                biz.get("fiscal_year_end", "2025-12-31"), biz.get("ccpc", True),
                adj.get("opening_inventory", 0), adj.get("closing_inventory", 0),
                adj.get("depreciation_cca", 0), adj.get("gst_installments_paid", 0),
                adj.get("auto_estimate_hst", True), api_key,
            ))


def _default_settings():
    return {
        "business": {
            "name": "", "bn": "", "province": "ON",
            "fiscal_year_end": "2025-12-31", "ccpc": True,
        },
        "manual_adjustments": {
            "opening_inventory": 0, "closing_inventory": 0,
            "depreciation_cca": 0, "gst_installments_paid": 0,
            "auto_estimate_hst": True,
        },
        "anthropic_api_key": "",
    }


# ---------------------------------------------------------------------------
# Transactions (scoped to org_id)
# ---------------------------------------------------------------------------

def load_txns(org_id, year=None):
    ensure_db()
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if year:
                # Include transactions matching the year, plus any with missing/invalid dates
                cur.execute(
                    """SELECT * FROM transactions WHERE org_id = %s
                       AND ((date >= %s AND date <= %s) OR date IS NULL OR date = '' OR LENGTH(date) < 4)
                       ORDER BY date""",
                    (org_id, f"{year}-01-01", f"{year}-12-31"),
                )
            else:
                cur.execute(
                    "SELECT * FROM transactions WHERE org_id = %s ORDER BY date",
                    (org_id,),
                )
            rows = cur.fetchall()
            return [_row_to_txn(r) for r in rows]


def load_pending_txns(org_id):
    """Load all unreviewed transactions across all years."""
    ensure_db()
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT * FROM transactions WHERE org_id = %s
                   AND (reviewed IS NULL OR reviewed = false)
                   ORDER BY date""",
                (org_id,),
            )
            rows = cur.fetchall()
            return [_row_to_txn(r) for r in rows]


def get_txn_years(org_id):
    """Return sorted list of distinct years that have transactions."""
    ensure_db()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT SUBSTRING(date FROM 1 FOR 4) AS yr
                FROM transactions
                WHERE org_id = %s AND date != '' AND LENGTH(date) >= 4
                ORDER BY yr
            """, (org_id,))
            return [row[0] for row in cur.fetchall() if row[0] and row[0].isdigit()]


def save_txn(org_id, user_id, txn):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO transactions (id, user_id, org_id, date, description, store_name, store_address,
                    card_info, amount, tax, source, category, ocr_text, ocr_file, file_hash, reviewed,
                    possible_shared_expense, ocr_bbox, doc_type, match_group, match_role,
                    payment_method, statement_period)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                txn["id"], user_id, org_id, txn.get("date", ""), txn.get("description", ""),
                txn.get("store_name", ""), txn.get("store_address", ""), txn.get("card_info", ""),
                txn.get("amount", 0), txn.get("tax", 0), txn.get("source", ""),
                txn.get("category", "other"), txn.get("ocr_text", ""), txn.get("ocr_file", ""),
                txn.get("file_hash", ""), txn.get("reviewed", False),
                txn.get("possible_shared_expense", False), txn.get("ocr_bbox", ""),
                txn.get("doc_type", "receipt"), txn.get("match_group", ""),
                txn.get("match_role", "primary"), txn.get("payment_method", ""),
                txn.get("statement_period", ""),
            ))


def save_txns_bulk(org_id, user_id, txns):
    """Insert multiple transactions in a single commit."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for txn in txns:
                cur.execute("""
                    INSERT INTO transactions (id, user_id, org_id, date, description, store_name, store_address,
                        card_info, amount, tax, source, category, ocr_text, ocr_file, file_hash, reviewed,
                        possible_shared_expense, ocr_bbox, doc_type, match_group, match_role,
                        payment_method, statement_period)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    txn["id"], user_id, org_id, txn.get("date", ""), txn.get("description", ""),
                    txn.get("store_name", ""), txn.get("store_address", ""), txn.get("card_info", ""),
                    txn.get("amount", 0), txn.get("tax", 0), txn.get("source", ""),
                    txn.get("category", "other"), txn.get("ocr_text", ""), txn.get("ocr_file", ""),
                    txn.get("file_hash", ""), txn.get("reviewed", False),
                    txn.get("possible_shared_expense", False), txn.get("ocr_bbox", ""),
                    txn.get("doc_type", "receipt"), txn.get("match_group", ""),
                    txn.get("match_role", "primary"), txn.get("payment_method", ""),
                    txn.get("statement_period", ""),
                ))


def update_txn(org_id, txn_id, fields):
    """Update specific fields on a transaction. fields is a dict of column: value."""
    if not fields:
        return
    allowed = {
        "date", "description", "store_name", "store_address", "card_info",
        "amount", "tax", "category", "reviewed", "possible_shared_expense",
        "doc_type", "match_group", "match_role", "payment_method", "statement_period",
    }
    safe_fields = {k: v for k, v in fields.items() if k in allowed}
    if not safe_fields:
        return
    set_clause = ", ".join(f"{k} = %s" for k in safe_fields)
    values = list(safe_fields.values()) + [txn_id, org_id]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE transactions SET {set_clause} WHERE id = %s AND org_id = %s",
                values,
            )


def link_txn(org_id, txn_id, link_to_id):
    """Link txn_id to link_to_id (marks txn_id as supporting doc, excluded from totals)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET linked_to = %s WHERE id = %s AND org_id = %s",
                (link_to_id, txn_id, org_id),
            )


def unlink_txn(org_id, txn_id):
    """Remove link from a transaction."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET linked_to = '' WHERE id = %s AND org_id = %s",
                (txn_id, org_id),
            )


def delete_txn(org_id, txn_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM transactions WHERE id = %s AND org_id = %s",
                (txn_id, org_id),
            )


def bulk_update_categories(org_id, id_to_cat):
    """Update category for multiple transactions."""
    if not id_to_cat:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            for txn_id, cat in id_to_cat.items():
                cur.execute(
                    "UPDATE transactions SET category = %s WHERE id = %s AND org_id = %s",
                    (cat, txn_id, org_id),
                )


def clear_txns(org_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transactions WHERE org_id = %s", (org_id,))


# ---------------------------------------------------------------------------
# Dedup (scoped to org_id)
# ---------------------------------------------------------------------------

def check_file_hash_exists(org_id, file_hash):
    """Check if this exact file was already uploaded by this org."""
    if not file_hash:
        return False
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM transactions WHERE org_id = %s AND file_hash = %s LIMIT 1",
                (org_id, file_hash),
            )
            return cur.fetchone() is not None


def check_cross_org_hash(org_id, file_hash):
    """Check if another org uploaded the same file. Returns the other org's name or None."""
    if not file_hash:
        return None
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT o.name FROM transactions t
                JOIN organizations o ON o.id = t.org_id
                WHERE t.file_hash = %s AND t.org_id != %s
                LIMIT 1
            """, (file_hash, org_id))
            row = cur.fetchone()
            return row["name"] if row else None


def check_cross_user_transaction(org_id, date, amount, store_name):
    """Check if another org has a similar transaction (possible shared expense)."""
    if not date or not amount:
        return False
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM transactions
                WHERE org_id != %s AND date = %s
                    AND ABS(amount - %s) < 0.50
                    AND store_name != '' AND store_name = %s
                LIMIT 1
            """, (org_id, date, amount, store_name))
            return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Match Suggestions (scoped to org_id)
# ---------------------------------------------------------------------------

def save_match_suggestion(org_id, txn_a, txn_b, confidence, reason):
    """Insert a new match suggestion between two transactions."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO match_suggestions (org_id, txn_id_a, txn_id_b, confidence, reason)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (org_id, txn_a, txn_b, confidence, reason))
            return cur.fetchone()[0]


def get_match_suggestions(org_id, status='pending'):
    """Get match suggestions with joined transaction data."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT ms.*,
                    ta.date AS txn_a_date, ta.description AS txn_a_description,
                    ta.amount AS txn_a_amount, ta.tax AS txn_a_tax,
                    ta.source AS txn_a_source,
                    ta.doc_type AS txn_a_doc_type, ta.store_name AS txn_a_store_name,
                    ta.ocr_file AS txn_a_ocr_file,
                    tb.date AS txn_b_date, tb.description AS txn_b_description,
                    tb.amount AS txn_b_amount, tb.tax AS txn_b_tax,
                    tb.source AS txn_b_source,
                    tb.doc_type AS txn_b_doc_type, tb.store_name AS txn_b_store_name,
                    tb.ocr_file AS txn_b_ocr_file
                FROM match_suggestions ms
                LEFT JOIN transactions ta ON ta.id = ms.txn_id_a
                LEFT JOIN transactions tb ON tb.id = ms.txn_id_b
                WHERE ms.org_id = %s AND ms.status = %s
                ORDER BY ms.confidence DESC, ms.created_at DESC
            """, (org_id, status))
            rows = cur.fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["confidence"] = float(d["confidence"])
                if d.get("txn_a_amount") is not None:
                    d["txn_a_amount"] = float(d["txn_a_amount"])
                if d.get("txn_b_amount") is not None:
                    d["txn_b_amount"] = float(d["txn_b_amount"])
                result.append(d)
            return result


def accept_match(org_id, suggestion_id):
    """Accept a match suggestion: set match_group and match_role on both txns,
    update suggestion status to 'accepted'."""
    import uuid as _uuid
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Fetch the suggestion
            cur.execute("""
                SELECT * FROM match_suggestions
                WHERE id = %s AND org_id = %s AND status = 'pending'
            """, (suggestion_id, org_id))
            row = cur.fetchone()
            if not row:
                return False

            txn_a_id = row["txn_id_a"]
            txn_b_id = row["txn_id_b"]

            # Check if txn_a already has a match_group; reuse it if so
            cur.execute("SELECT match_group FROM transactions WHERE id = %s AND org_id = %s",
                        (txn_a_id, org_id))
            a_row = cur.fetchone()
            existing_group = a_row["match_group"] if a_row and a_row["match_group"] else ""

            match_group = existing_group if existing_group else _uuid.uuid4().hex[:16]

            # txn_a is primary, txn_b is supporting
            cur.execute("""
                UPDATE transactions SET match_group = %s, match_role = 'primary'
                WHERE id = %s AND org_id = %s
            """, (match_group, txn_a_id, org_id))
            cur.execute("""
                UPDATE transactions SET match_group = %s, match_role = 'supporting'
                WHERE id = %s AND org_id = %s
            """, (match_group, txn_b_id, org_id))

            # Update suggestion status
            cur.execute("""
                UPDATE match_suggestions SET status = 'accepted'
                WHERE id = %s AND org_id = %s
            """, (suggestion_id, org_id))
            return True


def reject_match(org_id, suggestion_id):
    """Reject a match suggestion."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE match_suggestions SET status = 'rejected'
                WHERE id = %s AND org_id = %s AND status = 'pending'
            """, (suggestion_id, org_id))
            return cur.rowcount > 0


def get_unmatched_txns(org_id, doc_type=None):
    """Get transactions with empty match_group, optionally filtered by doc_type."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if doc_type:
                cur.execute("""
                    SELECT * FROM transactions
                    WHERE org_id = %s AND (match_group IS NULL OR match_group = '')
                          AND doc_type = %s
                    ORDER BY date
                """, (org_id, doc_type))
            else:
                cur.execute("""
                    SELECT * FROM transactions
                    WHERE org_id = %s AND (match_group IS NULL OR match_group = '')
                    ORDER BY date
                """, (org_id,))
            return [_row_to_txn(r) for r in cur.fetchall()]


def _row_to_txn(row):
    """Convert a database Row to a dict matching the old JSON format."""
    d = dict(row)
    d["amount"] = float(d["amount"])
    d["tax"] = float(d["tax"])
    d.pop("user_id", None)
    d.pop("org_id", None)
    d.pop("created_at", None)
    # Ensure new columns have defaults if not yet migrated
    d.setdefault("doc_type", "receipt")
    d.setdefault("match_group", "")
    d.setdefault("match_role", "primary")
    d.setdefault("payment_method", "")
    d.setdefault("statement_period", "")
    return d
