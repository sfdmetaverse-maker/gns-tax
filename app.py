import base64
import csv
import hashlib
import io
import json
import logging
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_from_directory, abort
from flask_login import login_required, current_user

import pytesseract
from PIL import Image
import fitz  # PyMuPDF

import db
import auth

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-fallback-key-change-in-prod")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB upload limit
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent
UPLOAD_DIR = DATA_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

auth.init_app(app)

from telegram_bot import telegram_bp
app.register_blueprint(telegram_bp)

from daily_ai_news import ai_news_bp
app.register_blueprint(ai_news_bp)

db.init_db()


# ---------------------------------------------------------------------------
# Branding context processor + superadmin decorator
# ---------------------------------------------------------------------------

@app.context_processor
def inject_branding():
    if current_user.is_authenticated and current_user.org_id:
        org = db.get_org_by_id(current_user.org_id)
        return {"branding": org}
    return {"branding": None}


def superadmin_required(f):
    @wraps(f)
    @login_required
    def decorated(*args, **kwargs):
        if not current_user.is_superadmin:
            abort(403)
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Tax constants
# ---------------------------------------------------------------------------
FEDERAL_GENERAL_RATE = 0.15
FEDERAL_SB_RATE = 0.09
SB_LIMIT = 500_000
PROVINCIAL_RATES = {
    "AB": {"general": 0.08, "small": 0.02},
    "BC": {"general": 0.12, "small": 0.02},
    "MB": {"general": 0.12, "small": 0.00},
    "NB": {"general": 0.14, "small": 0.025},
    "NL": {"general": 0.15, "small": 0.03},
    "NS": {"general": 0.14, "small": 0.025},
    "NT": {"general": 0.115, "small": 0.02},
    "NU": {"general": 0.12, "small": 0.03},
    "ON": {"general": 0.115, "small": 0.032},
    "PE": {"general": 0.16, "small": 0.01},
    "QC": {"general": 0.115, "small": 0.032},
    "SK": {"general": 0.12, "small": 0.01},
    "YT": {"general": 0.12, "small": 0.00},
}
GST_RATE = 0.05
HST_RATES = {
    "ON": 0.13, "NB": 0.15, "NL": 0.15, "NS": 0.15, "PE": 0.15,
}

# ---------------------------------------------------------------------------
# Categories
# ---------------------------------------------------------------------------
CATEGORIES = {
    # Revenue
    "sales": "Sales Revenue",
    "other_income": "Other Income",
    # COGS
    "purchases": "Inventory / Purchases (COGS)",
    # Expenses
    "wages": "Wages & Benefits",
    "rent": "Rent / Lease",
    "utilities": "Utilities",
    "insurance": "Insurance",
    "office_supplies": "Office Supplies",
    "professional_fees": "Professional Fees",
    "advertising": "Advertising & Marketing",
    "travel": "Travel",
    "meals_entertainment": "Meals & Entertainment",
    "depreciation_cca": "Capital Cost Allowance (CCA)",
    "interest": "Interest & Bank Charges",
    "other": "Other Expenses",
    # Special
    "transfer": "Transfer (non-taxable)",
    "owner_draw": "Owner Draw / Dividend",
    "skip": "Skip / Ignore",
}

REVENUE_CATS = {"sales", "other_income"}
COGS_CATS = {"purchases"}
EXPENSE_CATS = {
    "wages", "rent", "utilities", "insurance", "office_supplies",
    "professional_fees", "advertising", "travel", "meals_entertainment",
    "depreciation_cca", "interest", "other",
}
IGNORE_CATS = {"transfer", "owner_draw", "skip"}

# ---------------------------------------------------------------------------
# Auto-categorization keywords
# ---------------------------------------------------------------------------
AUTO_CAT_RULES = [
    # Revenue (credits / deposits from POS)
    (["toast deposit", "square deposit", "godaddy payout", "pos deposit",
      "merchant deposit"], "sales"),
    # Wages
    (["payroll", "salary", "wages", "adp", "ceridian", "wagepoint"], "wages"),
    # Rent
    (["rent", "lease payment", "landlord"], "rent"),
    # Utilities
    (["hydro", "electricity", "enbridge", "gas bill", "water bill",
      "toronto hydro", "bell", "rogers", "telus", "internet", "phone bill"], "utilities"),
    # Insurance
    (["insurance", "intact", "aviva", "wawanesa"], "insurance"),
    # Office supplies
    (["staples", "office depot", "amazon", "office supplies"], "office_supplies"),
    # Professional fees
    (["accountant", "lawyer", "legal", "accounting", "bookkeep", "cpa"], "professional_fees"),
    # Advertising
    (["facebook ads", "google ads", "meta ads", "advertising", "marketing",
      "canva", "mailchimp", "yelp", "instagram"], "advertising"),
    # Travel
    (["uber", "lyft", "air canada", "westjet", "hotel", "parking",
      "gas station", "petro", "esso", "shell", "mileage"], "travel"),
    # Meals & entertainment
    (["restaurant", "uber eats", "doordash", "skip the dishes",
      "catering", "coffee", "tim hortons", "starbucks"], "meals_entertainment"),
    # Interest & bank charges
    (["interest charge", "bank fee", "service charge", "nsf fee",
      "monthly fee", "annual fee", "credit card fee", "interchange"], "interest"),
    # Inventory / purchases (COGS)
    (["wholesale", "supplier", "inventory", "product purchase",
      "raw material", "packaging"], "purchases"),
    # Transfers to ignore
    (["transfer", "e-transfer to self", "payment - thank you",
      "credit card payment"], "transfer"),
    # Owner draws
    (["owner draw", "shareholder", "dividend"], "owner_draw"),
]


def auto_categorize(description):
    desc_lower = description.lower()
    for keywords, cat in AUTO_CAT_RULES:
        for kw in keywords:
            if kw in desc_lower:
                return cat
    return "other"


# ---------------------------------------------------------------------------
# Aggregate transactions into tax buckets
# ---------------------------------------------------------------------------

def aggregate_txns(txns, data):
    revenue = {"sales": 0, "other_income": 0}
    cogs = {"purchases": 0}
    expenses = {k: 0 for k in EXPENSE_CATS}
    gst_collected = 0
    gst_itc = 0

    adj = data.get("manual_adjustments", {})
    auto_estimate = adj.get("auto_estimate_hst", True)
    province = data.get("business", {}).get("province", "ON")
    hst_rate = HST_RATES.get(province, GST_RATE)
    hst_exempt_cats = {"wages", "depreciation_cca", "insurance"}
    estimated_itc = 0

    for t in txns:
        # Skip supporting transactions (matched duplicates, not counted)
        if t.get("match_role") == "supporting":
            continue
        cat = t.get("category", "other")
        amt = abs(t.get("amount", 0))
        tax = abs(t.get("tax", 0))

        if cat in REVENUE_CATS:
            revenue[cat] += amt
            gst_collected += tax
        elif cat in COGS_CATS:
            if auto_estimate and tax == 0 and amt > 0 and cat not in hst_exempt_cats:
                est_tax = round(amt * hst_rate / (1 + hst_rate), 2)
                pre_tax = round(amt - est_tax, 2)
                cogs[cat] += pre_tax
                gst_itc += est_tax
                estimated_itc += est_tax
            else:
                cogs[cat] += amt
                gst_itc += tax
        elif cat in EXPENSE_CATS:
            if auto_estimate and tax == 0 and amt > 0 and cat not in hst_exempt_cats:
                est_tax = round(amt * hst_rate / (1 + hst_rate), 2)
                pre_tax = round(amt - est_tax, 2)
                expenses[cat] += pre_tax
                gst_itc += est_tax
                estimated_itc += est_tax
            else:
                expenses[cat] += amt
                gst_itc += tax
        # IGNORE_CATS: skip

    return {
        "revenue": revenue,
        "cogs": {
            "opening_inventory": adj.get("opening_inventory", 0),
            "purchases": cogs["purchases"],
            "closing_inventory": adj.get("closing_inventory", 0),
        },
        "expenses": {**expenses, "depreciation_cca": adj.get("depreciation_cca", 0)},
        "gst_hst": {
            "collected": gst_collected,
            "itc_total": gst_itc,
            "installments_paid": adj.get("gst_installments_paid", 0),
            "estimated_itc": round(estimated_itc, 2),
        },
    }


# ---------------------------------------------------------------------------
# Tax calculations (same logic as before, using aggregated data)
# ---------------------------------------------------------------------------

def calc_t2(agg, data):
    rev = agg["revenue"]
    cogs_d = agg["cogs"]
    exp = agg["expenses"]
    biz = data["business"]
    province = biz["province"]
    is_ccpc = biz.get("ccpc", True)

    total_revenue = rev["sales"] + rev["other_income"]
    cost_of_goods = (cogs_d["opening_inventory"] + cogs_d["purchases"]
                     - cogs_d["closing_inventory"])
    gross_profit = total_revenue - cost_of_goods

    adjusted_meals = exp.get("meals_entertainment", 0) * 0.5
    total_expenses = sum(
        v for k, v in exp.items() if k != "meals_entertainment"
    ) + adjusted_meals

    net_income = gross_profit - total_expenses
    taxable_income = max(net_income, 0)

    if is_ccpc and taxable_income <= SB_LIMIT:
        federal_tax = taxable_income * FEDERAL_SB_RATE
    elif is_ccpc:
        federal_tax = SB_LIMIT * FEDERAL_SB_RATE + (taxable_income - SB_LIMIT) * FEDERAL_GENERAL_RATE
    else:
        federal_tax = taxable_income * FEDERAL_GENERAL_RATE

    prov_rates = PROVINCIAL_RATES.get(province, {"general": 0.115, "small": 0.032})
    if is_ccpc and taxable_income <= SB_LIMIT:
        provincial_tax = taxable_income * prov_rates["small"]
    elif is_ccpc:
        provincial_tax = SB_LIMIT * prov_rates["small"] + (taxable_income - SB_LIMIT) * prov_rates["general"]
    else:
        provincial_tax = taxable_income * prov_rates["general"]

    return {
        "total_revenue": total_revenue,
        "cost_of_goods": cost_of_goods,
        "gross_profit": gross_profit,
        "total_expenses": total_expenses,
        "meals_disallowed": exp.get("meals_entertainment", 0) * 0.5,
        "net_income": net_income,
        "taxable_income": taxable_income,
        "federal_tax": federal_tax,
        "provincial_tax": provincial_tax,
        "total_tax": federal_tax + provincial_tax,
        "province": province,
    }


def calc_gst(agg, data):
    gst = agg["gst_hst"]
    province = data["business"]["province"]
    rate = HST_RATES.get(province, GST_RATE)
    rate_label = "HST" if province in HST_RATES else "GST"

    collected = gst["collected"]
    total_itc = gst["itc_total"]
    net_tax = collected - total_itc
    installments = gst["installments_paid"]
    balance = net_tax - installments

    return {
        "rate": rate,
        "rate_pct": f"{rate * 100:.0f}%",
        "rate_label": rate_label,
        "collected": collected,
        "total_itc": total_itc,
        "net_tax": net_tax,
        "installments": installments,
        "balance": balance,
    }


# ---------------------------------------------------------------------------
# CSV parsing — auto-detect source format
# ---------------------------------------------------------------------------

def parse_money(val):
    """Parse a money string like '$1,234.56' or '(100.00)' into a float."""
    if not val:
        return 0.0
    val = val.strip().replace("$", "").replace(",", "")
    negative = False
    if val.startswith("(") and val.endswith(")"):
        val = val[1:-1]
        negative = True
    if val.startswith("-"):
        val = val[1:]
        negative = True
    try:
        result = float(val)
    except ValueError:
        return 0.0
    return -result if negative else result


def parse_date(val):
    """Try common date formats, return YYYY-MM-DD string."""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%Y/%m/%d",
                "%b %d, %Y", "%B %d, %Y", "%m-%d-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return val.strip()


def normalize_headers(row):
    """Lowercase and strip headers for matching."""
    return [h.strip().lower().replace('"', '') for h in row]


def detect_and_parse_csv(text, source_label):
    """Parse CSV text, auto-detecting format. Returns list of transaction dicts."""
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if len(rows) < 2:
        return []

    headers = normalize_headers(rows[0])
    txns = []

    # Detect format by header keywords
    fmt = detect_format(headers, source_label)

    for row in rows[1:]:
        if not row or all(c.strip() == "" for c in row):
            continue
        cells = dict(zip(headers, row))
        txn = fmt(cells, source_label)
        if txn:
            txns.append(txn)

    return txns


def detect_format(headers, source_label):
    h = " ".join(headers)
    # Toast: has "order id" or "order #" and "tax"
    if "order" in h and ("check" in h or "tax" in h):
        return parse_toast_row
    # Square: has "transaction id" or "payment id"
    if "transaction id" in h or "payment id" in h:
        return parse_square_row
    # GoDaddy: has "payout" or "godaddy" in source, or "order id" + "total"
    if "godaddy" in source_label.lower() or ("event type" in h):
        return parse_godaddy_row
    # Credit card: has "posted date" or "transaction date" and single amount
    if "posted date" in h or "transaction date" in h:
        return parse_credit_card_row
    # Bank statement: has "debit" and "credit" columns or "withdrawal" and "deposit"
    if ("debit" in h and "credit" in h) or ("withdrawal" in h and "deposit" in h):
        return parse_bank_row
    # Generic fallback: date + description + amount
    return parse_generic_row


def make_txn(date, description, amount, tax, source):
    return {
        "id": str(uuid.uuid4())[:8],
        "date": parse_date(date),
        "description": description.strip(),
        "amount": round(amount, 2),
        "tax": round(tax, 2),
        "source": source,
        "category": auto_categorize(description),
    }


def _get(cells, *keys):
    for k in keys:
        if k in cells and cells[k].strip():
            return cells[k].strip()
    return ""


def parse_toast_row(cells, source):
    date = _get(cells, "date", "order date", "business date")
    desc = _get(cells, "description", "item", "check #", "order id", "order #")
    amount = parse_money(_get(cells, "net amount", "amount", "net sales", "total"))
    tax = parse_money(_get(cells, "tax", "tax amount", "taxes"))
    if not date:
        return None
    return make_txn(date, desc or "Toast sale", amount, tax, "Toast")


def parse_square_row(cells, source):
    date = _get(cells, "date", "transaction date", "payment date")
    desc = _get(cells, "description", "customer name", "memo", "source")
    amount = parse_money(_get(cells, "net amount", "total collected", "gross sales", "amount"))
    tax = parse_money(_get(cells, "tax", "total tax"))
    fee = parse_money(_get(cells, "fee", "fees", "processing fee"))
    if not date:
        return None
    return make_txn(date, desc or "Square sale", amount, tax, "Square")


def parse_godaddy_row(cells, source):
    date = _get(cells, "date", "order date", "event date", "created at")
    desc = _get(cells, "description", "event type", "type", "item name")
    amount = parse_money(_get(cells, "total", "amount", "net", "gross"))
    tax = parse_money(_get(cells, "tax", "tax amount"))
    if not date:
        return None
    return make_txn(date, desc or "GoDaddy transaction", amount, tax, "GoDaddy")


def parse_bank_row(cells, source):
    date = _get(cells, "date", "posting date", "transaction date")
    desc = _get(cells, "description", "details", "memo", "transaction details",
                "transaction description", "payee")
    debit = parse_money(_get(cells, "debit", "withdrawal", "withdrawals"))
    credit = parse_money(_get(cells, "credit", "deposit", "deposits"))
    if not date:
        return None
    # Debit = money out (expense), credit = money in (revenue)
    if credit > 0:
        return make_txn(date, desc or "Bank deposit", credit, 0, source or "Bank")
    elif debit > 0:
        return make_txn(date, desc or "Bank payment", -debit, 0, source or "Bank")
    return None


def parse_credit_card_row(cells, source):
    date = _get(cells, "posted date", "transaction date", "date")
    desc = _get(cells, "description", "payee", "merchant", "name", "memo")
    amount = parse_money(_get(cells, "amount", "charge", "debit"))
    credit = parse_money(_get(cells, "credit", "payment"))
    if not date:
        return None
    # Credit card: positive = charge (expense), negative or credit col = payment
    if credit > 0:
        return make_txn(date, desc or "CC payment", credit, 0, source or "Credit Card")
    # Expenses are charges (positive amounts mean you spent money)
    return make_txn(date, desc or "CC charge", -abs(amount), 0, source or "Credit Card")


def parse_generic_row(cells, source):
    date = _get(cells, "date", "transaction date", "posting date")
    desc = _get(cells, "description", "details", "memo", "name", "payee")
    amount = parse_money(_get(cells, "amount", "total", "net", "value"))
    tax = parse_money(_get(cells, "tax", "hst", "gst"))
    if not date and not desc:
        return None
    return make_txn(date or "", desc or "Transaction", amount, tax, source or "Import")


# ---------------------------------------------------------------------------
# Data quality checks
# ---------------------------------------------------------------------------

def check_missing_months(txns, data):
    """Check which months of the fiscal year are missing bank/CC transactions."""
    fye = data["business"].get("fiscal_year_end", "2025-12-31")
    try:
        fye_date = datetime.strptime(fye, "%Y-%m-%d")
    except ValueError:
        fye_date = datetime(2025, 12, 31)

    # Fiscal year: 12 months ending on fye
    fy_end_month = fye_date.month
    fy_end_year = fye_date.year
    fy_start_year = fy_end_year - 1 if fy_end_month < 12 else fy_end_year
    fy_start_month = (fy_end_month % 12) + 1
    if fy_end_month == 12:
        fy_start_month = 1
        fy_start_year = fy_end_year

    # Build list of all 12 months in fiscal year as (year, month)
    fy_months = []
    y, m = fy_start_year, fy_start_month
    for _ in range(12):
        fy_months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    # Collect months that have bank or CC transactions
    bank_sources = {"Bank", "Credit Card"}
    bank_months = set()
    for t in txns:
        if t.get("source") not in bank_sources:
            continue
        d = t.get("date", "")
        if len(d) >= 7:
            try:
                dt = datetime.strptime(d[:10], "%Y-%m-%d")
                bank_months.add((dt.year, dt.month))
            except ValueError:
                pass

    missing = []
    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    for y, m in fy_months:
        if (y, m) not in bank_months:
            missing.append(f"{month_names[m]} {y}")

    return missing, fy_months, bank_months


def check_duplicates(txns):
    """Find likely duplicate transactions (same date + similar amount from different sources)."""
    dupes = []
    by_date = {}
    for t in txns:
        key = t.get("date", "")
        by_date.setdefault(key, []).append(t)

    for date, group in by_date.items():
        if len(group) < 2:
            continue
        seen = []
        for t in group:
            amt = abs(t.get("amount", 0))
            for s in seen:
                s_amt = abs(s.get("amount", 0))
                if (s.get("source") != t.get("source")
                        and abs(amt - s_amt) < 0.02
                        and t.get("category") not in IGNORE_CATS
                        and s.get("category") not in IGNORE_CATS):
                    dupes.append((s, t))
            seen.append(t)
    return dupes


def check_hst_gaps(txns, data):
    """Find expense transactions with no tax recorded where HST likely applies."""
    province = data["business"].get("province", "ON")
    rate = HST_RATES.get(province, GST_RATE)
    taxable_cats = EXPENSE_CATS - {"wages", "depreciation_cca"}  # wages/CCA are HST-exempt

    missing_tax = []
    estimated_itc = 0
    for t in txns:
        cat = t.get("category", "")
        tax = abs(t.get("tax", 0))
        amt = abs(t.get("amount", 0))
        if cat in taxable_cats and tax == 0 and amt > 0:
            est = round(amt * rate / (1 + rate), 2)  # back-calculate tax from total
            missing_tax.append({**t, "estimated_tax": est})
            estimated_itc += est

    return missing_tax, round(estimated_itc, 2)


def check_revenue_reconciliation(txns):
    """Compare POS sales totals vs bank deposit totals."""
    POS_SOURCES = {"Toast", "Square", "GoDaddy"}
    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    pos_total = 0.0
    bank_deposits = 0.0
    monthly_pos = {}
    monthly_bank = {}

    for t in txns:
        source = t.get("source", "")
        cat = t.get("category", "")
        amt = t.get("amount", 0)
        date_str = t.get("date", "")
        month_key = None
        if len(date_str) >= 7:
            try:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
                month_key = (dt.year, dt.month)
            except ValueError:
                pass

        if source in POS_SOURCES and cat in REVENUE_CATS:
            pos_total += amt
            if month_key:
                monthly_pos[month_key] = monthly_pos.get(month_key, 0) + amt
        if source == "Bank" and amt > 0:
            bank_deposits += amt
            if month_key:
                monthly_bank[month_key] = monthly_bank.get(month_key, 0) + amt

    pos_total = round(pos_total, 2)
    bank_deposits = round(bank_deposits, 2)
    gap = round(pos_total - bank_deposits, 2)
    gap_pct = round((gap / pos_total * 100), 1) if pos_total else 0.0

    all_months = sorted(set(list(monthly_pos.keys()) + list(monthly_bank.keys())))
    monthly_gaps = []
    for mk in all_months:
        pos_m = round(monthly_pos.get(mk, 0), 2)
        bank_m = round(monthly_bank.get(mk, 0), 2)
        diff = round(pos_m - bank_m, 2)
        threshold = max(pos_m, bank_m) * 0.05
        if abs(diff) > threshold and abs(diff) > 1.00:
            monthly_gaps.append({
                "month": f"{month_names[mk[1]]} {mk[0]}",
                "pos": pos_m, "bank": bank_m, "diff": diff,
            })

    return {
        "pos_total": pos_total, "bank_deposits": bank_deposits,
        "gap": gap, "gap_pct": gap_pct, "monthly_gaps": monthly_gaps,
    }


def check_expected_categories(txns, data):
    """Flag likely missing or sparse expense categories."""
    warnings = []
    EXPECTED_CATS = {
        "rent": "No rent/lease expenses found. Most businesses have monthly rent.",
        "utilities": "No utilities found. Check for hydro, internet, phone bills.",
        "insurance": "No insurance expenses found. Most businesses carry liability insurance.",
        "wages": "No wages/payroll found. If you have employees, import payroll data.",
        "interest": "No interest or bank charges found. Most accounts have monthly fees.",
    }

    cat_months = {}
    total_revenue = 0
    total_expenses = 0

    for t in txns:
        cat = t.get("category", "")
        amt = abs(t.get("amount", 0))
        date_str = t.get("date", "")
        if cat in REVENUE_CATS:
            total_revenue += amt
        elif cat in EXPENSE_CATS:
            total_expenses += amt
        if cat in EXPENSE_CATS and amt > 0 and len(date_str) >= 7:
            try:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
                cat_months.setdefault(cat, set()).add((dt.year, dt.month))
            except ValueError:
                pass

    for cat_key, message in EXPECTED_CATS.items():
        if cat_key not in cat_months:
            warnings.append({"type": "missing_category", "label": CATEGORIES.get(cat_key, cat_key), "message": message})

    all_months = set()
    for t in txns:
        d = t.get("date", "")
        if len(d) >= 7:
            try:
                dt = datetime.strptime(d[:10], "%Y-%m-%d")
                all_months.add((dt.year, dt.month))
            except ValueError:
                pass

    if len(all_months) >= 12:
        for cat_key in ("rent", "utilities", "interest"):
            if cat_key in cat_months and len(cat_months[cat_key]) < 12:
                n = len(cat_months[cat_key])
                warnings.append({"type": "sparse_category", "label": CATEGORIES.get(cat_key, cat_key),
                                  "message": f"{CATEGORIES.get(cat_key, cat_key)} only in {n} of 12 months."})

    if total_revenue > 100_000 and total_expenses < 10_000:
        warnings.append({"type": "ratio_check", "label": "Revenue/Expense Ratio",
                          "message": f"Revenue is ${total_revenue:,.0f} but expenses only ${total_expenses:,.0f}. Verify all expenses are imported."})

    return warnings


def check_completeness(txns, data):
    """Run all data quality checks and return a warnings dict."""
    warnings = {}

    # Missing months
    missing_months, fy_months, bank_months = check_missing_months(txns, data)
    if missing_months:
        warnings["missing_months"] = missing_months
    warnings["fy_months_covered"] = f"{12 - len(missing_months)}/12"

    # Duplicates
    dupes = check_duplicates(txns)
    if dupes:
        warnings["duplicates"] = dupes

    # HST gaps
    missing_tax, estimated_itc = check_hst_gaps(txns, data)
    if missing_tax:
        warnings["hst_gaps"] = len(missing_tax)
        warnings["estimated_missing_itc"] = estimated_itc

    # Basic checks
    has_revenue = any(t.get("category") in REVENUE_CATS for t in txns)
    has_expenses = any(t.get("category") in EXPENSE_CATS for t in txns)
    if not has_revenue and txns:
        warnings["no_revenue"] = True
    if not has_expenses and txns:
        warnings["no_expenses"] = True

    # Revenue reconciliation
    recon = check_revenue_reconciliation(txns)
    if abs(recon["gap_pct"]) > 5 or recon["monthly_gaps"]:
        warnings["revenue_recon"] = recon

    # Expected category checks
    expected_cat_warnings = check_expected_categories(txns, data)
    if expected_cat_warnings:
        warnings["expected_categories"] = expected_cat_warnings

    return warnings


# ---------------------------------------------------------------------------
# Auto-matching engine
# ---------------------------------------------------------------------------

def find_matches(org_id):
    """Auto-match receipts against credit card / bank entries."""
    receipts = db.get_unmatched_txns(org_id, doc_type='receipt')
    cc_entries = db.get_unmatched_txns(org_id, doc_type='credit_card')
    bank_entries = db.get_unmatched_txns(org_id, doc_type='bank')

    # Load existing pending suggestions to avoid duplicates
    existing = db.get_match_suggestions(org_id, status='pending')
    existing_pairs = set()
    for s in existing:
        pair = (s.get("txn_id_a"), s.get("txn_id_b"))
        existing_pairs.add(pair)
        existing_pairs.add((pair[1], pair[0]))

    for receipt in receipts:
        r_amt = abs(receipt.get("amount", 0))
        r_date_str = receipt.get("date", "")
        if not r_date_str or r_amt == 0:
            continue
        try:
            r_date = datetime.strptime(r_date_str[:10], "%Y-%m-%d")
        except ValueError:
            continue

        # Determine payment method from receipt
        payment_method = (receipt.get("payment_method") or "").lower()
        card_info = (receipt.get("card_info") or "").lower()
        combined_pay = payment_method + " " + card_info

        # Decide which pool to search
        is_credit_card = any(kw in combined_pay for kw in ("visa", "mastercard", "amex", "credit"))
        is_debit = "debit" in combined_pay

        if is_credit_card:
            candidates = cc_entries
            max_date_diff = 5
        elif is_debit:
            candidates = bank_entries
            max_date_diff = 3
        else:
            # Try both pools if payment method is unclear
            candidates = cc_entries + bank_entries
            max_date_diff = 5

        best_match = None
        best_confidence = 0

        for entry in candidates:
            e_amt = abs(entry.get("amount", 0))
            e_date_str = entry.get("date", "")
            if not e_date_str:
                continue
            try:
                e_date = datetime.strptime(e_date_str[:10], "%Y-%m-%d")
            except ValueError:
                continue

            date_diff = abs((r_date - e_date).days)
            amt_diff = abs(r_amt - e_amt)

            if date_diff > max_date_diff:
                continue
            if amt_diff > 0.50:
                continue

            # Compute confidence score
            confidence = 0
            if amt_diff < 0.02 and date_diff <= 2:
                confidence = 0.95
            elif amt_diff < 0.02 and date_diff <= 5:
                confidence = 0.85
            elif amt_diff <= 0.50 and date_diff <= 3:
                confidence = 0.70
            else:
                continue  # no match at this threshold

            if confidence > best_confidence:
                best_confidence = confidence
                best_match = entry

        if best_match and best_confidence >= 0.70:
            pair_key = (receipt["id"], best_match["id"])
            if pair_key not in existing_pairs:
                amt_diff = abs(r_amt - abs(best_match.get("amount", 0)))
                r_date_obj = datetime.strptime(receipt["date"][:10], "%Y-%m-%d")
                e_date_obj = datetime.strptime(best_match["date"][:10], "%Y-%m-%d")
                date_diff = abs((r_date_obj - e_date_obj).days)
                reason = (
                    f"Amount diff: ${amt_diff:.2f}, "
                    f"Date diff: {date_diff} day(s), "
                    f"Receipt: {receipt.get('description', '')[:40]}, "
                    f"Entry: {best_match.get('description', '')[:40]}"
                )
                db.save_match_suggestion(org_id, receipt["id"], best_match["id"],
                                         best_confidence, reason)
                existing_pairs.add(pair_key)
                existing_pairs.add((pair_key[1], pair_key[0]))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def landing():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    return render_template("landing.html")


def _dashboard_url(anchor="", year=None):
    """Build dashboard redirect URL preserving the year param."""
    if year is None:
        year = request.form.get("year") or request.args.get("year") or ""
    url = url_for("dashboard", year=year) if year else url_for("dashboard")
    return url + (f"#{anchor}" if anchor else "")


def _group_by_statement(txns):
    """Group transactions by file_hash (each uploaded statement PDF).
    Returns list of dicts: {label, file_hash, ocr_file, count, total_debit, total_credit, txns}."""
    from collections import OrderedDict
    groups = OrderedDict()
    for t in txns:
        key = t.get("file_hash") or t.get("statement_period") or "other"
        if key not in groups:
            # Build a label from statement_period or date range
            period = t.get("statement_period", "")
            ocr_file = t.get("ocr_file", "")
            groups[key] = {
                "key": key,
                "label": period,
                "ocr_file": ocr_file,
                "count": 0,
                "total_debit": 0,
                "total_credit": 0,
                "txns": [],
                "min_date": t.get("date", ""),
                "max_date": t.get("date", ""),
            }
        g = groups[key]
        g["txns"].append(t)
        g["count"] += 1
        amt = t.get("amount", 0)
        if amt < 0:
            g["total_debit"] += abs(amt)
        else:
            g["total_credit"] += amt
        d = t.get("date", "")
        if d:
            if not g["min_date"] or d < g["min_date"]:
                g["min_date"] = d
            if not g["max_date"] or d > g["max_date"]:
                g["max_date"] = d

    # Build better labels
    for g in groups.values():
        if g["label"]:
            # Convert 2025-01 to "Jan 2025"
            try:
                from datetime import datetime as _dt
                dt = _dt.strptime(g["label"], "%Y-%m")
                g["label"] = dt.strftime("%b %Y")
            except ValueError:
                pass
        elif g["min_date"] and g["max_date"]:
            g["label"] = f"{g['min_date']} to {g['max_date']}"
        else:
            g["label"] = "Imported"

    return list(groups.values())


@app.route("/dashboard")
@login_required
def dashboard():
    data = db.load_settings(current_user.org_id)

    # Year filtering
    available_years = db.get_txn_years(current_user.org_id)
    year_param = request.args.get("year", "")
    if year_param and year_param in available_years:
        selected_year = year_param
    elif available_years:
        current_yr = str(datetime.now().year)
        selected_year = current_yr if current_yr in available_years else available_years[-1]
    else:
        selected_year = None

    txns = db.load_txns(current_user.org_id, year=selected_year)
    agg = aggregate_txns(txns, data)
    t2 = calc_t2(agg, data)
    gst = calc_gst(agg, data)

    # Build linked groups: primary_id -> [child txns]
    linked_children = {}
    child_ids = set()
    for t in txns:
        if t.get("linked_to"):
            linked_children.setdefault(t["linked_to"], []).append(t)
            child_ids.add(t["id"])
    # Primary txns = not a child
    primary_txns = [t for t in txns if t["id"] not in child_ids]

    # Pending review: only RECEIPT type, unreviewed, across all years
    all_pending = db.load_pending_txns(current_user.org_id)
    pending_child_ids = set()
    pending_linked_children = {}
    for t in all_pending:
        if t.get("linked_to"):
            pending_linked_children.setdefault(t["linked_to"], []).append(t)
            pending_child_ids.add(t["id"])
    pending_txns = [t for t in all_pending
                    if t["id"] not in pending_child_ids
                    and t.get("doc_type") == "receipt"]

    # Checked txns for the transactions tab (year-filtered, reviewed only, receipts + POS)
    checked_txns = [t for t in primary_txns
                    if t.get("reviewed")
                    and t.get("doc_type") in ("receipt", "pos")]

    # Bank entries (year-filtered), grouped by statement
    bank_txns = [t for t in primary_txns if t.get("doc_type") == "bank"]
    bank_statements = _group_by_statement(bank_txns)

    # Credit card entries (year-filtered), grouped by statement
    cc_txns = [t for t in primary_txns if t.get("doc_type") == "credit_card"]
    cc_statements = _group_by_statement(cc_txns)

    # Summary counts
    txn_count = len(primary_txns)
    pending_count = len(pending_txns)
    sources = sorted(set(t.get("source", "?") for t in txns))

    # Data quality checks
    warnings = check_completeness(txns, data) if txns else {}

    # Match suggestions
    match_suggestions = db.get_match_suggestions(current_user.org_id)
    match_count = len(match_suggestions)

    return render_template("index.html",
                           data=data, t2=t2, gst=gst, agg=agg,
                           txns=txns, primary_txns=primary_txns,
                           linked_children=linked_children,
                           checked_txns=checked_txns,
                           bank_txns=bank_txns,
                           bank_statements=bank_statements,
                           cc_txns=cc_txns,
                           cc_statements=cc_statements,
                           pending_txns=pending_txns,
                           pending_linked_children=pending_linked_children,
                           txn_count=txn_count,
                           pending_count=pending_count, sources=sources,
                           categories=CATEGORIES, warnings=warnings,
                           match_suggestions=match_suggestions,
                           match_count=match_count,
                           provinces=sorted(PROVINCIAL_RATES.keys()),
                           user=current_user,
                           available_years=available_years,
                           selected_year=selected_year)


@app.route("/receipt", methods=["POST"])
@login_required
def add_receipt():
    form = request.form
    date = form.get("r_date", "")
    desc = form.get("r_desc", "")
    amount = float(form.get("r_amount", 0) or 0)
    tax = float(form.get("r_tax", 0) or 0)
    cat = form.get("r_category", "other")
    txn_type = form.get("r_type", "expense")

    if not date or not desc:
        flash("Date and description are required.")
        return redirect(_dashboard_url("add"))

    # Expenses stored as negative, revenue as positive
    if txn_type == "expense" and amount > 0:
        amount = -amount

    txn = {
        "id": str(uuid.uuid4())[:8],
        "date": date,
        "description": desc,
        "amount": round(amount, 2),
        "tax": round(tax, 2),
        "source": "Receipt",
        "category": cat,
    }
    db.save_txn(current_user.org_id, current_user.id, txn)
    flash(f"Receipt added: {desc}")
    return redirect(_dashboard_url("add"))


def _ocr_statement_pdf(pdf_bytes, source, doc_type, api_key):
    """Use Claude Vision to extract transactions from each page of a statement PDF."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    all_txns = []

    for page_num in range(len(doc)):
        pix = doc[page_num].get_pixmap(dpi=200)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        img_bytes = buf.getvalue()

        b64 = base64.standard_b64encode(img_bytes).decode("utf-8")
        client = anthropic.Anthropic(api_key=api_key)
        source_label = source or ("Credit Card" if doc_type == "credit_card" else "Bank")

        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4000,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}},
                        {"type": "text", "text": (
                            f"This is page {page_num+1} of a {source_label.lower()} statement. "
                            "Extract ALL transaction lines (not balances, fees summary, or headers).\n"
                            "Return ONLY a valid JSON array:\n"
                            '[{"date": "YYYY-MM-DD", "description": "payee/description", '
                            '"amount": 0.00, "type": "debit_or_credit"}]\n'
                            "Rules:\n"
                            "- One object per transaction\n"
                            "- date in YYYY-MM-DD format (infer the year from statement context)\n"
                            "- description = payee name or transaction description\n"
                            "- amount = positive number (absolute value)\n"
                            '- type = "debit" for purchases/withdrawals/payments out, '
                            '"credit" for deposits/refunds/payments in\n'
                            "- Skip opening/closing balances, subtotals, interest rates, headers\n"
                            "- If no transactions on this page, return []"
                        )}
                    ]
                }]
            )
            text = msg.content[0].text.strip()
            if "```" in text:
                text = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL).group(1).strip()
            raw = json.loads(text)
            if isinstance(raw, dict):
                raw = [raw]

            for item in raw:
                amt = abs(float(item.get("amount", 0) or 0))
                if amt == 0:
                    continue
                is_credit = item.get("type", "").lower() == "credit"

                if doc_type == "credit_card":
                    final_amt = amt if is_credit else -amt
                else:
                    final_amt = amt if is_credit else -amt

                txn = make_txn(item.get("date", ""), item.get("description", ""), final_amt, 0, source_label)
                txn["doc_type"] = doc_type
                all_txns.append(txn)
        except Exception as e:
            logger.error("OCR statement page %d failed: %s", page_num, e)
            continue

    doc.close()
    return all_txns if all_txns else None


def parse_bank_statement_pdf(pdf_bytes, source):
    """Extract transactions from a bank/credit card statement PDF."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    full_text = ""
    for page in doc:
        text = page.get_text()
        if text.strip():
            full_text += text + "\n"
        else:
            pix = page.get_pixmap(dpi=300)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            full_text += ocr_image(img) + "\n"
    doc.close()

    txns = []
    lines = full_text.split("\n")

    # Pattern: date followed by description and amount(s)
    # Common bank formats:
    #   "Jan 15  GROCERY STORE  -45.67"
    #   "01/15  GROCERY STORE  45.67  1,234.00"
    #   "2025-01-15  GROCERY STORE  45.67"
    date_line_pattern = re.compile(
        r"(?P<date>"
        r"\d{4}[-/]\d{2}[-/]\d{2}"           # 2025-01-15
        r"|[A-Z][a-z]{2}\s+\d{1,2}(?:,?\s+\d{4})?"  # Jan 15 or Jan 15, 2025
        r"|\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?"       # 01/15 or 01/15/2025
        r")\s+"
        r"(?P<desc>.+?)\s+"
        r"(?P<amounts>-?\$?[\d,]+\.\d{2}(?:\s+-?\$?[\d,]+\.\d{2})*)\s*$"
    )

    for line in lines:
        line = line.strip()
        if not line:
            continue
        m = date_line_pattern.match(line)
        if not m:
            continue

        date_str = m.group("date")
        desc = m.group("desc").strip()
        amounts_str = m.group("amounts")

        # Skip header-like lines and running balances
        if any(skip in desc.upper() for skip in [
            "OPENING BALANCE", "CLOSING BALANCE", "STATEMENT", "PAGE",
            "ACCOUNT NUMBER", "PREVIOUS BALANCE", "NEW BALANCE",
        ]):
            continue

        # Parse all amounts on the line
        raw_amounts = re.findall(r"-?\$?[\d,]+\.\d{2}", amounts_str)
        parsed_amounts = [parse_money(a) for a in raw_amounts]

        if not parsed_amounts:
            continue

        # Heuristic: if multiple amounts, last is usually balance (skip it)
        # The transaction amount is the first (or second if debit/credit cols)
        if len(parsed_amounts) >= 3:
            # debit, credit, balance — take whichever of debit/credit is nonzero
            amt = parsed_amounts[0] if parsed_amounts[0] != 0 else parsed_amounts[1]
        elif len(parsed_amounts) == 2:
            # amount + balance, take the first
            amt = parsed_amounts[0]
        else:
            amt = parsed_amounts[0]

        # For bank statements: negative = money out, positive = money in
        # For credit cards: positive charge = expense
        if "credit card" in source.lower():
            if amt > 0:
                amt = -amt  # charges are expenses

        txn = make_txn(date_str, desc, amt, 0, source)

        # Set doc_type based on source
        source_lower = source.lower()
        if any(kw in source_lower for kw in ("credit card", "visa", "mastercard")):
            txn["doc_type"] = "credit_card"
        else:
            txn["doc_type"] = "bank"

        # Set statement_period from the transaction date
        if txn.get("date") and len(txn["date"]) >= 7:
            txn["statement_period"] = txn["date"][:7]  # YYYY-MM

        txns.append(txn)

    return txns


@app.route("/import-csv", methods=["POST"])
@login_required
def import_csv():
    source = request.form.get("csv_source", "Import")
    file = request.files.get("csv_file")
    if not file or not file.filename:
        flash("No file selected.")
        return redirect(_dashboard_url("import"))

    filename = file.filename.lower()
    file_bytes = file.read()

    org_upload_dir = UPLOAD_DIR / str(current_user.org_id)
    org_upload_dir.mkdir(parents=True, exist_ok=True)

    # Handle PDF uploads
    if filename.endswith(".pdf"):
        save_name = f"{uuid.uuid4().hex[:8]}_{file.filename}"
        (org_upload_dir / save_name).write_bytes(file_bytes)

        new_txns = parse_bank_statement_pdf(file_bytes, source)

        # If text parsing fails, use Claude Vision OCR on each page
        if not new_txns:
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            if api_key:
                doc_type = "credit_card" if any(kw in source.lower() for kw in ("credit", "visa", "mastercard")) else "bank"
                new_txns = _ocr_statement_pdf(file_bytes, source, doc_type, api_key)

        if not new_txns:
            flash("No transactions found in PDF. The format may not be supported, or try CSV export from your bank instead.")
            return redirect(_dashboard_url("add"))

        # Set doc_type based on source
        doc_type = "credit_card" if any(kw in source.lower() for kw in ("credit", "visa", "mastercard")) else "bank"
        for t in new_txns:
            t.setdefault("doc_type", doc_type)
            t["file_hash"] = hashlib.sha256(file_bytes).hexdigest()
            if t.get("date") and len(t["date"]) >= 7:
                t.setdefault("statement_period", t["date"][:7])

        db.save_txns_bulk(current_user.org_id, current_user.id, new_txns)
        find_matches(current_user.org_id)
        tab = "creditcard" if doc_type == "credit_card" else "bank"
        flash(f"Imported {len(new_txns)} transactions from {source} PDF.")
        return redirect(_dashboard_url(tab))

    # Handle CSV uploads
    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = file_bytes.decode("latin-1")

    new_txns = detect_and_parse_csv(text, source)
    if not new_txns:
        flash("No transactions found in file. Check the CSV format.")
        return redirect(_dashboard_url("import"))

    db.save_txns_bulk(current_user.org_id, current_user.id, new_txns)
    find_matches(current_user.org_id)
    flash(f"Imported {len(new_txns)} transactions from {source}.")
    return redirect(_dashboard_url("transactions"))


@app.route("/update-txn", methods=["POST"])
@login_required
def update_txn():
    txn_id = request.form.get("txn_id")
    fields = {}
    if request.form.get("date"):
        fields["date"] = request.form["date"]
    if request.form.get("description") is not None:
        fields["description"] = request.form["description"]
    if request.form.get("amount") is not None:
        try:
            fields["amount"] = -abs(float(request.form["amount"]))
        except ValueError:
            pass
    if request.form.get("tax") is not None:
        try:
            fields["tax"] = float(request.form["tax"])
        except ValueError:
            pass
    if request.form.get("store_name") is not None:
        fields["store_name"] = request.form["store_name"]
    if request.form.get("store_address") is not None:
        fields["store_address"] = request.form["store_address"]
    if request.form.get("card_info") is not None:
        fields["card_info"] = request.form["card_info"]
    if request.form.get("category"):
        fields["category"] = request.form["category"]
    if request.form.get("reviewed") is not None:
        fields["reviewed"] = request.form["reviewed"] == "true"
    db.update_txn(current_user.org_id, txn_id, fields)
    flash("Transaction updated.")
    redir = request.form.get("redirect", _dashboard_url("add"))
    return redirect(redir)


@app.route("/update-review-status", methods=["POST"])
@login_required
def update_review_status():
    payload = request.get_json(force=True)
    txn_id = payload.get("txn_id")
    reviewed = payload.get("reviewed", False)
    db.update_txn(current_user.org_id, txn_id, {"reviewed": reviewed})
    return jsonify({"ok": True})


@app.route("/delete-txn", methods=["POST"])
@login_required
def delete_txn():
    txn_id = request.form.get("txn_id")
    db.delete_txn(current_user.org_id, txn_id)
    flash("Transaction deleted.")
    redir = request.form.get("redirect")
    if redir:
        return redirect(redir)
    return redirect(_dashboard_url("transactions"))


@app.route("/link-txn", methods=["POST"])
@login_required
def link_txn():
    txn_id = request.form.get("txn_id")
    link_to = request.form.get("link_to")
    if txn_id and link_to and txn_id != link_to:
        db.link_txn(current_user.org_id, txn_id, link_to)
        flash("Transactions linked. The linked record won't be counted in totals.")
    return redirect(_dashboard_url("transactions"))


@app.route("/unlink-txn", methods=["POST"])
@login_required
def unlink_txn():
    txn_id = request.form.get("txn_id")
    if txn_id:
        db.unlink_txn(current_user.org_id, txn_id)
        flash("Transaction unlinked.")
    return redirect(_dashboard_url("transactions"))


@app.route("/bulk-update", methods=["POST"])
@login_required
def bulk_update():
    mapping = {}
    for key, val in request.form.items():
        if key.startswith("cat_"):
            tid = key[4:]
            mapping[tid] = val
    db.bulk_update_categories(current_user.org_id, mapping)
    updated = len(mapping)
    flash(f"Updated {updated} transaction(s).")
    return redirect(_dashboard_url("transactions"))


@app.route("/save-settings", methods=["POST"])
@login_required
def save_settings():
    form = request.form
    data = {
        "business": {
            "name": form.get("biz_name", ""),
            "bn": form.get("biz_bn", ""),
            "province": form.get("biz_province", "ON"),
            "fiscal_year_end": form.get("biz_fye", "2025-12-31"),
            "ccpc": form.get("biz_ccpc") == "on",
        },
        "manual_adjustments": {
            "opening_inventory": float(form.get("adj_opening_inventory", 0) or 0),
            "closing_inventory": float(form.get("adj_closing_inventory", 0) or 0),
            "depreciation_cca": float(form.get("adj_depreciation_cca", 0) or 0),
            "gst_installments_paid": float(form.get("adj_gst_installments", 0) or 0),
            "auto_estimate_hst": form.get("adj_auto_estimate_hst") == "on",
        },
    }
    # API key is managed server-side via ANTHROPIC_API_KEY env var
    phone = form.get("phone_number", "").strip()
    if phone:
        data["phone_number"] = phone
    db.save_settings(current_user.org_id, data)
    flash("Settings saved.")
    return redirect(_dashboard_url("settings"))


@app.route("/clear-txns", methods=["POST"])
@login_required
def clear_txns():
    db.clear_txns(current_user.org_id)
    flash("All transactions cleared.")
    return redirect(_dashboard_url())


# ---------------------------------------------------------------------------
# Receipt OCR
# ---------------------------------------------------------------------------

ALLOWED_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".tif", ".pdf", ".webp"}


def ocr_image(img):
    """Run OCR on a PIL Image and return the text."""
    return pytesseract.image_to_string(img)


def ocr_pdf(pdf_bytes):
    """Extract text from a PDF. Try text extraction first, fall back to OCR."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    texts = []
    for page in doc:
        text = page.get_text()
        if text.strip():
            texts.append(text)
        else:
            # Render page to image for OCR
            pix = page.get_pixmap(dpi=300)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            texts.append(ocr_image(img))
    doc.close()
    return "\n".join(texts)


def claude_vision_receipt(file_bytes, ext, api_key):
    """Use Claude's vision API to extract receipt data.
    Returns a list of parsed dicts (one per receipt found), or None on failure."""
    if not HAS_ANTHROPIC or not api_key:
        logger.warning("Claude Vision skipped: HAS_ANTHROPIC=%s, api_key=%s", HAS_ANTHROPIC, bool(api_key))
        return None

    mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                ".gif": "image/gif", ".webp": "image/webp", ".pdf": "application/pdf"}
    media_type = mime_map.get(ext, "image/jpeg")

    # For PDFs, render first page to image
    if ext == ".pdf":
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        pix = doc[0].get_pixmap(dpi=200)
        buf = io.BytesIO()
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        img.save(buf, format="JPEG", quality=85)
        file_bytes = buf.getvalue()
        media_type = "image/jpeg"
        doc.close()

    # Resize large images to speed up API call (max 1568px on longest side)
    if ext in (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".tif", ".webp"):
        img = Image.open(io.BytesIO(file_bytes))
        max_dim = 1568
        if max(img.size) > max_dim:
            img.thumbnail((max_dim, max_dim), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=80)
        file_bytes = buf.getvalue()
        media_type = "image/jpeg"

    b64 = base64.standard_b64encode(file_bytes).decode("utf-8")

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
                    '"card_info": "card type and last 4 digits", '
                    '"payment_method": "visa/mastercard/amex/debit/cash/other", '
                    '"bbox": {"top": 0, "left": 0, "width": 100, "height": 100}}]\n'
                    "Rules:\n"
                    "- One object per receipt in the image\n"
                    "- vendor = store/business name at top of receipt\n"
                    "- address = full street address, city, province as shown\n"
                    "- items = brief summary of what was purchased\n"
                    "- subtotal = amount before tax\n"
                    "- tax = HST, GST, PST, or any sales tax shown\n"
                    "- total = final amount paid\n"
                    "- card_info = e.g. 'Visa *1234' or 'Mastercard *5678' from payment line\n"
                    "- payment_method = the payment method used: visa, mastercard, amex, debit, cash, or other. "
                    "Determine from card info, payment line, or tender type on the receipt\n"
                    "- bbox = bounding box of this receipt as percentage of total image (0-100). "
                    "top/left = position of top-left corner, width/height = size of receipt area. "
                    "For a single receipt use {\"top\":0,\"left\":0,\"width\":100,\"height\":100}\n"
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
        logger.info("Claude Vision raw response for receipt: %s", text[:500])
        # Extract JSON from response (handle markdown code blocks)
        if "```" in text:
            text = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL).group(1).strip()
        raw = json.loads(text)

        # Normalize: always work with a list
        if isinstance(raw, dict):
            raw = [raw]

        results = []
        for parsed in raw:
            result = {
                "description": parsed.get("items", "") or parsed.get("vendor", ""),
                "store_name": parsed.get("vendor", ""),
                "store_address": parsed.get("address", ""),
                "card_info": parsed.get("card_info", ""),
                "payment_method": parsed.get("payment_method", ""),
                "date": parsed.get("date", ""),
                "amount": float(parsed.get("subtotal", 0) or 0),
                "tax": float(parsed.get("tax", 0) or 0),
            }
            # Bounding box (percentage-based)
            bbox = parsed.get("bbox")
            if bbox and isinstance(bbox, dict):
                result["bbox"] = json.dumps(bbox)
            else:
                result["bbox"] = ""
            # If subtotal is 0 but total exists, derive it
            if result["amount"] == 0 and parsed.get("total"):
                result["amount"] = round(float(parsed["total"]) - result["tax"], 2)
            logger.info("Claude Vision parsed: date=%s amount=%s tax=%s desc=%s bbox=%s",
                         result["date"], result["amount"], result["tax"],
                         result["description"][:50], result.get("bbox", ""))
            results.append(result)

        return results if results else None
    except Exception as e:
        logger.error("Claude Vision JSON parse failed: %s — raw: %s", e, text[:200] if text else "empty")
        return None


def parse_receipt_text(text):
    """Extract date, vendor, total, and tax from OCR text. Best-effort."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    result = {"description": "", "date": "", "amount": 0, "tax": 0}

    # Vendor: usually the first non-empty, non-numeric line
    for line in lines[:5]:
        if line and not re.match(r"^[\d\s\-/.:]+$", line):
            result["description"] = line
            break

    # Date: look for common date patterns
    date_patterns = [
        r"(\d{4}[-/]\d{2}[-/]\d{2})",
        r"(\d{2}[-/]\d{2}[-/]\d{4})",
        r"(\d{2}[-/]\d{2}[-/]\d{2})\b",
        r"([A-Z][a-z]{2}\s+\d{1,2},?\s+\d{4})",
        r"(\d{1,2}\s+[A-Z][a-z]{2,}\s+\d{4})",
    ]
    for pattern in date_patterns:
        m = re.search(pattern, text)
        if m:
            result["date"] = parse_date(m.group(1))
            break

    # Total: look for "total" followed by a dollar amount
    total_patterns = [
        r"(?:TOTAL|Total|AMOUNT DUE|Amount Due|BALANCE|Balance)[:\s]*\$?([\d,]+\.\d{2})",
        r"(?:TOTAL|Total)\s*\$?([\d,]+\.\d{2})",
    ]
    for pattern in total_patterns:
        m = re.search(pattern, text)
        if m:
            result["amount"] = parse_money(m.group(1))
            break

    # If no total found, find the largest dollar amount
    if result["amount"] == 0:
        amounts = re.findall(r"\$?([\d,]+\.\d{2})", text)
        if amounts:
            parsed = [parse_money(a) for a in amounts]
            result["amount"] = max(parsed) if parsed else 0

    # Tax: look for HST, GST, TAX
    tax_patterns = [
        r"(?:HST|GST|TAX|H\.S\.T|G\.S\.T)[:\s]*\$?([\d,]+\.\d{2})",
        r"(?:HST|GST|TAX)\s*\(?\d*%?\)?\s*\$?([\d,]+\.\d{2})",
    ]
    for pattern in tax_patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            result["tax"] = parse_money(m.group(1))
            break

    # If we found a total and tax, subtract tax from total to get pre-tax amount
    if result["tax"] > 0 and result["amount"] > result["tax"]:
        result["amount"] = round(result["amount"] - result["tax"], 2)

    return result


@app.route("/upload-receipts", methods=["POST"])
@login_required
def upload_receipts():
    files = request.files.getlist("receipt_files")
    if not files or all(f.filename == "" for f in files):
        flash("No files selected.")
        return redirect(_dashboard_url("add"))

    org_upload_dir = UPLOAD_DIR / str(current_user.org_id)
    org_upload_dir.mkdir(parents=True, exist_ok=True)

    settings = db.load_settings(current_user.org_id)
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    results = []
    receipt_count = 0

    for file in files:
        if not file or not file.filename:
            continue
        ext = Path(file.filename).suffix.lower()
        if ext not in ALLOWED_EXTENSIONS:
            results.append(f"Skipped {file.filename}: unsupported format")
            continue

        file_bytes = file.read()

        # Compute SHA256 hash for dedup
        file_hash = hashlib.sha256(file_bytes).hexdigest()

        # Check if this file was already uploaded by this org
        if db.check_file_hash_exists(current_user.org_id, file_hash):
            flash(f"Skipped {file.filename}: duplicate file already uploaded")
            continue

        # Check if another org uploaded the same file (shared expense)
        cross_org = db.check_cross_org_hash(current_user.org_id, file_hash)

        # Save a copy
        save_name = f"{uuid.uuid4().hex[:8]}_{file.filename}"
        (org_upload_dir / save_name).write_bytes(file_bytes)

        # Try Claude Vision first, fall back to pytesseract
        parsed_list = None
        ocr_method = "Claude Vision"
        try:
            parsed_list = claude_vision_receipt(file_bytes, ext, api_key)
        except Exception as e:
            logger.error("Claude Vision failed for %s: %s", file.filename, e, exc_info=True)

        text = ""
        if not parsed_list:
            ocr_method = "Tesseract OCR"
            try:
                if ext == ".pdf":
                    text = ocr_pdf(file_bytes)
                else:
                    img = Image.open(io.BytesIO(file_bytes))
                    text = ocr_image(img)
            except Exception as e:
                results.append(f"Error reading {file.filename}: {e}")
                continue
            parsed_list = [parse_receipt_text(text)]

        for parsed in parsed_list:
            cat = auto_categorize(parsed["description"])

            txn = {
                "id": str(uuid.uuid4())[:8],
                "date": parsed["date"],
                "description": parsed["description"] or file.filename,
                "store_name": parsed.get("store_name", ""),
                "store_address": parsed.get("store_address", ""),
                "card_info": parsed.get("card_info", ""),
                "payment_method": parsed.get("payment_method", ""),
                "amount": -abs(round(parsed["amount"], 2)),  # expenses are negative
                "tax": round(parsed["tax"], 2),
                "source": f"Receipt ({ocr_method})",
                "category": cat,
                "ocr_text": text[:500] if text else "",
                "ocr_file": save_name,
                "file_hash": file_hash,
                "ocr_bbox": parsed.get("bbox", ""),
            }
            if cross_org:
                txn["shared_expense"] = True
            db.save_txn(current_user.org_id, current_user.id, txn)
            logger.info("Saved receipt txn: id=%s date=%s amount=%s method=%s file=%s",
                         txn["id"], txn["date"], txn["amount"], ocr_method, file.filename)
            receipt_count += 1

    if receipt_count > 0:
        find_matches(current_user.org_id)
        flash(f"Processed {receipt_count} receipt(s). Review them below and fix any OCR errors.")
    elif not results:
        flash("No new receipts to process.")
    for r in results:
        flash(r)
    redir = request.form.get("redirect")
    if redir:
        return redirect(redir)
    return redirect(_dashboard_url("add"))


@app.route("/uploads/<path:filename>")
@login_required
def serve_upload(filename):
    return send_from_directory(UPLOAD_DIR / str(current_user.org_id), filename)


@app.route("/api/add-txn", methods=["POST"])
@login_required
def api_add_txn():
    """API endpoint for adding transactions programmatically (e.g. from OCR)."""
    payload = request.get_json(force=True)
    if isinstance(payload, list):
        items = payload
    else:
        items = [payload]
    added = 0
    for item in items:
        txn = {
            "id": str(uuid.uuid4())[:8],
            "date": item.get("date", ""),
            "description": item.get("description", ""),
            "amount": round(float(item.get("amount", 0)), 2),
            "tax": round(float(item.get("tax", 0)), 2),
            "source": item.get("source", "Receipt"),
            "category": item.get("category", auto_categorize(item.get("description", ""))),
        }
        db.save_txn(current_user.org_id, current_user.id, txn)
        added += 1
    total = len(db.load_txns(current_user.org_id))
    return {"status": "ok", "added": added, "total": total}


@app.route("/wizard")
@login_required
def wizard():
    data = db.load_settings(current_user.org_id)
    txns = db.load_txns(current_user.org_id)
    step = int(request.args.get("step", 1))
    agg = aggregate_txns(txns, data)
    t2 = calc_t2(agg, data)
    gst = calc_gst(agg, data)
    return render_template("wizard.html",
                           data=data, txns=txns, agg=agg, t2=t2, gst=gst,
                           step=step, txn_count=len(txns),
                           categories=CATEGORIES,
                           provinces=sorted(PROVINCIAL_RATES.keys()))


@app.route("/resolve-duplicate", methods=["POST"])
@login_required
def resolve_duplicate():
    txn_id = request.form.get("txn_id")
    db.update_txn(current_user.org_id, txn_id, {"category": "skip"})
    flash("Transaction marked as duplicate (skipped).")
    return redirect(_dashboard_url())


@app.route("/resolve-all-duplicates", methods=["POST"])
@login_required
def resolve_all_duplicates():
    txns = db.load_txns(current_user.org_id)
    dupes = check_duplicates(txns)
    skip_ids = set(b["id"] for _a, b in dupes)
    mapping = {}
    for tid in skip_ids:
        mapping[tid] = "skip"
    if mapping:
        db.bulk_update_categories(current_user.org_id, mapping)
    flash(f"Auto-resolved {len(mapping)} duplicate(s).")
    return redirect(_dashboard_url())


# ---------------------------------------------------------------------------
# Match accept/reject routes
# ---------------------------------------------------------------------------

@app.route("/accept-match", methods=["POST"])
@login_required
def accept_match_route():
    suggestion_id = int(request.form.get("suggestion_id"))
    db.accept_match(current_user.org_id, suggestion_id)
    flash("Match accepted.")
    return redirect(_dashboard_url("matching"))


@app.route("/reject-match", methods=["POST"])
@login_required
def reject_match_route():
    suggestion_id = int(request.form.get("suggestion_id"))
    db.reject_match(current_user.org_id, suggestion_id)
    flash("Match rejected.")
    return redirect(_dashboard_url("matching"))


# ---------------------------------------------------------------------------
# Admin Routes (superadmin only)
# ---------------------------------------------------------------------------

@app.route("/api/bulk-import", methods=["POST"])
def api_bulk_import():
    """Temporary bulk import endpoint. Requires secret key."""
    key = request.form.get("key") or request.args.get("key")
    if key != os.environ.get("BULK_IMPORT_KEY", "gleam-bulk-2026"):
        return jsonify({"error": "unauthorized"}), 403

    import_type = request.form.get("type") or request.args.get("type", "receipt")

    # Run matching without needing a file
    if import_type == "run_matching":
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT org_id FROM users LIMIT 1")
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "no users"}), 500
                org_id = row[0]
        count = find_matches(org_id)
        return jsonify({"status": "ok", "matches_found": count})

    file = request.files.get("file")
    if not file:
        return jsonify({"error": "no file"}), 400

    # Get first user/org
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, org_id FROM users LIMIT 1")
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "no users"}), 500
            user_id, org_id = row[0], row[1]

    file_bytes = file.read()
    file_hash = hashlib.sha256(file_bytes).hexdigest()
    fname = file.filename or "upload"
    ext = Path(fname).suffix.lower()

    org_upload_dir = UPLOAD_DIR / str(org_id)
    org_upload_dir.mkdir(parents=True, exist_ok=True)

    if import_type == "receipt":
        if db.check_file_hash_exists(org_id, file_hash):
            return jsonify({"status": "skipped", "reason": "duplicate"})

        save_name = f"{uuid.uuid4().hex[:8]}_{fname}"
        (org_upload_dir / save_name).write_bytes(file_bytes)

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        parsed_list = None
        try:
            parsed_list = claude_vision_receipt(file_bytes, ext, api_key)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        if not parsed_list:
            parsed_list = [{"description": fname, "date": "", "amount": 0, "tax": 0}]

        count = 0
        for parsed in parsed_list:
            cat = auto_categorize(parsed.get("description", ""))
            txn = {
                "id": str(uuid.uuid4())[:8],
                "date": parsed["date"],
                "description": parsed.get("description", "") or fname,
                "store_name": parsed.get("store_name", ""),
                "store_address": parsed.get("store_address", ""),
                "card_info": parsed.get("card_info", ""),
                "amount": -abs(round(parsed.get("amount", 0), 2)),
                "tax": round(parsed.get("tax", 0), 2),
                "source": "Receipt (Claude Vision)",
                "category": cat,
                "ocr_file": save_name,
                "file_hash": file_hash,
                "ocr_bbox": parsed.get("bbox", ""),
                "doc_type": "receipt",
                "payment_method": parsed.get("payment_method", ""),
            }
            db.save_txn(org_id, user_id, txn)
            count += 1
        return jsonify({"status": "ok", "type": "receipt", "count": count})

    elif import_type in ("bank", "credit_card"):
        if ext == ".pdf":
            if db.check_file_hash_exists(org_id, file_hash):
                return jsonify({"status": "skipped", "reason": "duplicate"})

            save_name = f"{uuid.uuid4().hex[:8]}_{fname}"
            (org_upload_dir / save_name).write_bytes(file_bytes)

            source = "Credit Card" if import_type == "credit_card" else "Bank"
            txns = parse_bank_statement_pdf(file_bytes, source)

            # If text parsing fails, use Claude Vision on each page
            if not txns:
                api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                if api_key:
                    txns = _ocr_statement_pdf(file_bytes, source, import_type, api_key)

            if not txns:
                return jsonify({"status": "no_txns"})

            for t in txns:
                t["doc_type"] = import_type
                t["file_hash"] = file_hash
                if t.get("date") and len(t["date"]) >= 7:
                    t["statement_period"] = t["date"][:7]

            db.save_txns_bulk(org_id, user_id, txns)
            return jsonify({"status": "ok", "type": import_type, "count": len(txns)})

        elif ext == ".csv":
            text = file_bytes.decode("utf-8-sig", errors="replace")
            source = "Credit Card" if import_type == "credit_card" else "Bank"
            txns = detect_and_parse_csv(text, source)
            if not txns:
                return jsonify({"status": "no_txns"})
            for t in txns:
                t["doc_type"] = import_type
            db.save_txns_bulk(org_id, user_id, txns)
            return jsonify({"status": "ok", "type": import_type, "count": len(txns)})

    elif import_type == "run_matching":
        count = find_matches(org_id)
        return jsonify({"status": "ok", "matches_found": count})

    return jsonify({"error": "unknown type"}), 400


@app.route("/admin/")
@superadmin_required
def admin_dashboard():
    orgs = db.list_orgs()
    return render_template("admin.html", orgs=orgs)


@app.route("/admin/orgs", methods=["POST"])
@superadmin_required
def admin_create_org():
    name = request.form.get("name", "").strip()
    slug = request.form.get("slug", "").strip().lower()
    color = request.form.get("primary_color", "#92702a").strip()
    tagline = request.form.get("tagline", "Tax Filing Assistant").strip()
    invite = uuid.uuid4().hex[:12]

    if not name or not slug:
        flash("Name and slug are required.")
        return redirect(url_for("admin_dashboard"))

    # Validate slug format
    if not re.match(r'^[a-z0-9][a-z0-9-]*[a-z0-9]$', slug) and len(slug) > 1:
        flash("Slug must be lowercase alphanumeric with hyphens only.")
        return redirect(url_for("admin_dashboard"))

    existing = db.get_org_by_slug(slug)
    if existing:
        flash(f"Slug '{slug}' is already taken.")
        return redirect(url_for("admin_dashboard"))

    org = db.create_org(name, slug, invite_code=invite, primary_color=color, tagline=tagline)
    flash(f"Created org '{name}' with invite code: {invite}")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/orgs/<int:org_id>")
@superadmin_required
def admin_view_org(org_id):
    org = db.get_org_by_id(org_id)
    if not org:
        flash("Organization not found.")
        return redirect(url_for("admin_dashboard"))
    users = db.get_org_users(org_id)
    return render_template("admin_org.html", org=org, users=users)


@app.route("/admin/orgs/<int:org_id>", methods=["POST"])
@superadmin_required
def admin_update_org(org_id):
    fields = {}
    for key in ("name", "slug", "tagline", "logo_url", "primary_color"):
        val = request.form.get(key)
        if val is not None:
            fields[key] = val.strip()
    is_active = request.form.get("is_active")
    if is_active is not None:
        fields["is_active"] = is_active == "on"
    db.update_org(org_id, fields)
    flash("Organization updated.")
    return redirect(url_for("admin_view_org", org_id=org_id))


@app.route("/admin/orgs/<int:org_id>/regenerate-invite", methods=["POST"])
@superadmin_required
def admin_regenerate_invite(org_id):
    new_code = uuid.uuid4().hex[:12]
    db.update_org(org_id, {"invite_code": new_code})
    flash(f"New invite code: {new_code}")
    return redirect(url_for("admin_view_org", org_id=org_id))


# ---------------------------------------------------------------------------
# Daily AI News — scheduled + on-demand
# ---------------------------------------------------------------------------

@app.route("/api/daily-ai-news", methods=["POST"])
def api_daily_ai_news():
    """Trigger daily AI news digest. Requires BULK_IMPORT_KEY for auth."""
    key = request.form.get("key") or request.args.get("key") or request.json.get("key", "") if request.is_json else ""
    if key != os.environ.get("BULK_IMPORT_KEY", "gleam-bulk-2026"):
        return jsonify({"error": "unauthorized"}), 403

    from daily_ai_news import send_daily_news, save_digest_to_file

    # Optional: send to specific chat_id, otherwise all linked users
    chat_id = request.form.get("chat_id") or (request.json.get("chat_id") if request.is_json else None)
    chat_ids = [chat_id] if chat_id else None

    digest = send_daily_news(chat_ids=chat_ids)
    if digest:
        save_digest_to_file(digest)
        return jsonify({"status": "ok", "length": len(digest)})
    return jsonify({"error": "failed to generate digest"}), 500


def _start_news_scheduler():
    """Start APScheduler for daily AI news at 9:30 AM ET."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        import pytz
    except ImportError:
        logger.warning("APScheduler not installed — daily AI news scheduler disabled. "
                       "Install with: pip install apscheduler pytz")
        return

    def _daily_news_job():
        with app.app_context():
            from daily_ai_news import send_daily_news, save_digest_to_file
            logger.info("Running scheduled daily AI news job")
            digest = send_daily_news()
            if digest:
                save_digest_to_file(digest)
                logger.info("Daily AI news sent and saved")
            else:
                logger.error("Daily AI news job failed to generate digest")

    et = pytz.timezone("America/New_York")
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        _daily_news_job,
        CronTrigger(hour=9, minute=30, timezone=et),
        id="daily_ai_news",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Daily AI news scheduler started — 9:30 AM ET daily")


# Start scheduler when running under gunicorn (not in debug/reloader)
if not app.debug and os.environ.get("AI_NEWS_BOT_TOKEN"):
    _start_news_scheduler()


if __name__ == "__main__":
    app.run(debug=True, port=5050, host="0.0.0.0")
