"""
Migration 001: Add organizations table and org_id columns.

Run once against the production database:
    python migrations/001_orgs.py

This migration is idempotent — safe to re-run.
"""

import os
import sys
import uuid
import shutil
from pathlib import Path

import psycopg2
import psycopg2.extras

DB_URL = os.environ.get("DATABASE_URL", "")
if not DB_URL:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)

UPLOAD_DIR = Path(__file__).parent.parent / "uploads"


def run():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # 1. Create organizations table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS organizations (
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

        # 2. Create default org if it doesn't exist
        cur.execute("SELECT id FROM organizations WHERE slug = 'gleam-and-sip'")
        default_org = cur.fetchone()
        if not default_org:
            invite = uuid.uuid4().hex[:12]
            cur.execute("""
                INSERT INTO organizations (slug, name, tagline, primary_color, invite_code)
                VALUES ('gleam-and-sip', 'Gleam & Sip', 'Tax Filing Assistant', '#92702a', %s)
                RETURNING id
            """, (invite,))
            org_id = cur.fetchone()["id"]
            print(f"Created default org 'Gleam & Sip' (id={org_id}, invite_code={invite})")
        else:
            org_id = default_org["id"]
            print(f"Default org already exists (id={org_id})")

        # 3. Add org_id, role, is_superadmin to users
        for col, defn in [
            ("org_id", f"INTEGER REFERENCES organizations(id)"),
            ("role", "TEXT NOT NULL DEFAULT 'member'"),
            ("is_superadmin", "BOOLEAN NOT NULL DEFAULT FALSE"),
        ]:
            cur.execute(f"""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = '{col}'
            """)
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
                print(f"Added users.{col}")

        # 4. Backfill all users → default org
        cur.execute("UPDATE users SET org_id = %s WHERE org_id IS NULL", (org_id,))
        print(f"Backfilled users.org_id → {org_id}")

        # 5. Mark first user as superadmin + admin role
        cur.execute("SELECT id, email FROM users ORDER BY id LIMIT 1")
        first_user = cur.fetchone()
        if first_user:
            cur.execute(
                "UPDATE users SET is_superadmin = TRUE, role = 'admin' WHERE id = %s",
                (first_user["id"],)
            )
            print(f"Marked {first_user['email']} as superadmin")

        # 6. Add org_id to business_settings
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'business_settings' AND column_name = 'org_id'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE business_settings ADD COLUMN org_id INTEGER REFERENCES organizations(id)")
            print("Added business_settings.org_id")

        # Backfill business_settings.org_id
        cur.execute("UPDATE business_settings SET org_id = %s WHERE org_id IS NULL", (org_id,))
        print(f"Backfilled business_settings.org_id → {org_id}")

        # Add unique constraint on org_id (drop old user_id unique first if needed)
        cur.execute("""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_name = 'business_settings' AND constraint_type = 'UNIQUE'
        """)
        existing = [r["constraint_name"] for r in cur.fetchall()]
        for name in existing:
            if "user_id" in name:
                cur.execute(f"ALTER TABLE business_settings DROP CONSTRAINT {name}")
                print(f"Dropped old unique constraint: {name}")

        # Add org_id unique if not present
        cur.execute("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name = 'business_settings' AND constraint_name = 'business_settings_org_id_key'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE business_settings ADD CONSTRAINT business_settings_org_id_key UNIQUE (org_id)")
            print("Added UNIQUE constraint on business_settings.org_id")

        # 7. Add org_id to transactions
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'transactions' AND column_name = 'org_id'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE transactions ADD COLUMN org_id INTEGER REFERENCES organizations(id)")
            print("Added transactions.org_id")

        # Backfill transactions.org_id
        cur.execute("UPDATE transactions SET org_id = %s WHERE org_id IS NULL", (org_id,))
        print(f"Backfilled transactions.org_id → {org_id}")

        # Add indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_txn_org ON transactions(org_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_txn_org_date ON transactions(org_id, date)")

        # 8. Migrate upload directories: uploads/{user_id}/ → uploads/{org_id}/
        if UPLOAD_DIR.exists():
            org_upload_dir = UPLOAD_DIR / str(org_id)
            org_upload_dir.mkdir(parents=True, exist_ok=True)

            cur.execute("SELECT id FROM users WHERE org_id = %s", (org_id,))
            user_ids = [r["id"] for r in cur.fetchall()]

            for uid in user_ids:
                user_dir = UPLOAD_DIR / str(uid)
                if user_dir.exists() and user_dir != org_upload_dir:
                    for f in user_dir.iterdir():
                        dest = org_upload_dir / f.name
                        if not dest.exists():
                            shutil.move(str(f), str(dest))
                    # Remove empty user dir
                    if not any(user_dir.iterdir()):
                        user_dir.rmdir()
                        print(f"Migrated uploads/{uid}/ → uploads/{org_id}/")

        conn.commit()
        print("\nMigration complete!")

    except Exception as e:
        conn.rollback()
        print(f"Migration failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run()
