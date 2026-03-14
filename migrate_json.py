#!/usr/bin/env python3
"""One-time migration: import existing data.json + transactions.json into PostgreSQL."""

import json
import os
import shutil
import sys
from pathlib import Path
from getpass import getpass

from dotenv import load_dotenv
from werkzeug.security import generate_password_hash

load_dotenv()

import db


def main():
    print("=== Gleam & Sip: Migrate JSON to PostgreSQL ===\n")

    # Initialize database tables
    db.init_db()
    print("Database tables created.\n")

    data_file = Path(__file__).parent / "data.json"
    txn_file = Path(__file__).parent / "transactions.json"
    upload_dir = Path(__file__).parent / "uploads"

    # Get user credentials
    email = input("Email for your account: ").strip().lower()
    password = getpass("Password: ")
    confirm = getpass("Confirm password: ")

    if password != confirm:
        print("Passwords don't match!")
        sys.exit(1)

    # Check if user already exists
    existing = db.get_user_by_email(email)
    if existing:
        print(f"User {email} already exists (id={existing['id']}). Skipping user creation.")
        user_id = existing["id"]
    else:
        pw_hash = generate_password_hash(password)
        user_id = db.create_user(email, pw_hash)
        print(f"Created user {email} (id={user_id})")

    # Import business settings
    if data_file.exists():
        data = json.loads(data_file.read_text())
        # Remove the API key from the old format if present
        api_key = data.pop("anthropic_api_key", "")
        settings = {
            "business": data.get("business", {}),
            "manual_adjustments": data.get("manual_adjustments", {}),
            "anthropic_api_key": api_key,
        }
        db.save_settings(user_id, settings)
        print(f"Imported business settings from data.json")
    else:
        print("No data.json found, skipping settings.")

    # Import transactions
    if txn_file.exists():
        txns = json.loads(txn_file.read_text())
        if txns:
            db.save_txns_bulk(user_id, txns)
            print(f"Imported {len(txns)} transactions from transactions.json")
        else:
            print("transactions.json is empty.")
    else:
        print("No transactions.json found, skipping.")

    # Move uploads to user directory
    if upload_dir.exists():
        user_upload_dir = upload_dir / str(user_id)
        # Move all files (not directories) from uploads/ to uploads/{user_id}/
        files = [f for f in upload_dir.iterdir() if f.is_file()]
        if files:
            user_upload_dir.mkdir(exist_ok=True)
            for f in files:
                dest = user_upload_dir / f.name
                shutil.move(str(f), str(dest))
            print(f"Moved {len(files)} upload files to uploads/{user_id}/")
        else:
            print("No upload files to move.")

    print("\nMigration complete! You can now log in at the web app.")
    print("Old JSON files (data.json, transactions.json) can be deleted once verified.")


if __name__ == "__main__":
    main()
