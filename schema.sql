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
);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    phone TEXT DEFAULT '',
    telegram_chat_id TEXT DEFAULT '',
    org_id INTEGER REFERENCES organizations(id),
    role TEXT NOT NULL DEFAULT 'member',
    is_superadmin BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login TIMESTAMP
);

CREATE TABLE IF NOT EXISTS business_settings (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    org_id INTEGER UNIQUE REFERENCES organizations(id),
    name TEXT NOT NULL DEFAULT '',
    bn TEXT NOT NULL DEFAULT '',
    province TEXT NOT NULL DEFAULT 'ON',
    fiscal_year_end TEXT NOT NULL DEFAULT '2025-12-31',
    ccpc BOOLEAN NOT NULL DEFAULT TRUE,
    opening_inventory NUMERIC NOT NULL DEFAULT 0,
    closing_inventory NUMERIC NOT NULL DEFAULT 0,
    depreciation_cca NUMERIC NOT NULL DEFAULT 0,
    gst_installments_paid NUMERIC NOT NULL DEFAULT 0,
    auto_estimate_hst BOOLEAN NOT NULL DEFAULT TRUE,
    anthropic_api_key TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    org_id INTEGER REFERENCES organizations(id),
    date TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    store_name TEXT NOT NULL DEFAULT '',
    store_address TEXT NOT NULL DEFAULT '',
    card_info TEXT NOT NULL DEFAULT '',
    amount NUMERIC NOT NULL DEFAULT 0,
    tax NUMERIC NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT 'other',
    ocr_text TEXT NOT NULL DEFAULT '',
    ocr_file TEXT NOT NULL DEFAULT '',
    file_hash TEXT NOT NULL DEFAULT '',
    reviewed BOOLEAN NOT NULL DEFAULT FALSE,
    possible_shared_expense BOOLEAN NOT NULL DEFAULT FALSE,
    doc_type TEXT NOT NULL DEFAULT 'receipt',
    match_group TEXT NOT NULL DEFAULT '',
    match_role TEXT NOT NULL DEFAULT 'primary',
    payment_method TEXT NOT NULL DEFAULT '',
    statement_period TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_txn_user ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_txn_user_date ON transactions(user_id, date);
CREATE INDEX IF NOT EXISTS idx_txn_user_category ON transactions(user_id, category);
CREATE INDEX IF NOT EXISTS idx_txn_file_hash ON transactions(file_hash);
CREATE INDEX IF NOT EXISTS idx_txn_org ON transactions(org_id);
CREATE INDEX IF NOT EXISTS idx_txn_org_date ON transactions(org_id, date);

CREATE TABLE IF NOT EXISTS match_suggestions (
    id SERIAL PRIMARY KEY,
    org_id INTEGER REFERENCES organizations(id),
    txn_id_a TEXT NOT NULL,
    txn_id_b TEXT NOT NULL,
    confidence NUMERIC NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_match_org ON match_suggestions(org_id, status);
