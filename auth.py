import os
import re
import uuid

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required

from werkzeug.security import generate_password_hash, check_password_hash

import db

auth_bp = Blueprint("auth", __name__)
login_manager = LoginManager()
login_manager.login_view = "auth.login"
login_manager.login_message = "Please log in to access your account."


class User(UserMixin):
    def __init__(self, id, email, org_id=None, role="member", is_superadmin=False, phone=""):
        self.id = id
        self.email = email
        self.org_id = org_id
        self.role = role
        self.is_superadmin = is_superadmin
        self.phone = phone


@login_manager.user_loader
def load_user(user_id):
    row = db.get_user_by_id(int(user_id))
    if row:
        return User(
            row["id"], row["email"],
            org_id=row.get("org_id"),
            role=row.get("role", "member"),
            is_superadmin=row.get("is_superadmin", False),
            phone=row.get("phone", ""),
        )
    return None


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        if not email or not password:
            flash("Email and password are required.")
            return redirect(url_for("auth.login"))

        row = db.get_user_by_email(email)
        if row and check_password_hash(row["password_hash"], password):
            user = User(
                row["id"], row["email"],
                org_id=row.get("org_id"),
                role=row.get("role", "member"),
                is_superadmin=row.get("is_superadmin", False),
                phone=row.get("phone", ""),
            )
            login_user(user, remember=True)
            db.update_last_login(row["id"])
            return redirect(url_for("dashboard"))

        flash("Invalid email or password.")
        return redirect(url_for("auth.login"))

    # Optional org branding on login page via ?org=slug
    branding = None
    org_slug = request.args.get("org")
    if org_slug:
        branding = db.get_org_by_slug(org_slug)

    return render_template("login.html", branding=branding)


def _make_slug(name):
    """Convert a business name to a URL-safe slug."""
    slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
    return slug or 'org'


def _unique_slug(name):
    """Generate a unique slug, appending random chars if needed."""
    base = _make_slug(name)
    slug = base
    while db.get_org_by_slug(slug):
        slug = f"{base}-{uuid.uuid4().hex[:4]}"
    return slug


@auth_bp.route("/register", defaults={"slug": None}, methods=["GET", "POST"])
@auth_bp.route("/register/<slug>", methods=["GET", "POST"])
def register(slug):
    # Look up org branding if slug provided (joining existing org)
    org = None
    if slug:
        org = db.get_org_by_slug(slug)
        if not org or not org["is_active"]:
            flash("Organization not found.")
            return redirect(url_for("auth.login"))

    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm", "")
        invite = request.form.get("invite_code", "").strip()
        biz_name = request.form.get("business_name", "").strip()

        if not email or not password:
            flash("Email and password are required.")
            return redirect(request.url)

        if password != confirm:
            flash("Passwords do not match.")
            return redirect(request.url)

        if len(password) < 6:
            flash("Password must be at least 6 characters.")
            return redirect(request.url)

        existing = db.get_user_by_email(email)
        if existing:
            flash("An account with that email already exists.")
            return redirect(request.url)

        if org:
            # Slug-based: joining existing org, validate invite code
            if org["invite_code"] and invite != org["invite_code"]:
                flash("Invalid invite code.")
                return redirect(request.url)
            target_org = org
            role = "member"
        elif invite:
            # Has invite code: joining existing org
            target_org = db.get_org_by_invite_code(invite)
            if not target_org:
                flash("Invalid invite code.")
                return redirect(request.url)
            role = "member"
        else:
            # Self-service: create new org from business name
            if not biz_name:
                flash("Business name is required.")
                return redirect(request.url)
            org_slug = _unique_slug(biz_name)
            org_invite = uuid.uuid4().hex[:12]
            target_org = db.create_org(biz_name, org_slug, invite_code=org_invite)
            role = "admin"

        pw_hash = generate_password_hash(password)
        user_id = db.create_user(email, pw_hash, org_id=target_org["id"], role=role)
        user = User(user_id, email, org_id=target_org["id"], role=role)
        login_user(user, remember=True)
        flash("Account created! Start by entering your business info in Settings.")
        return redirect(url_for("dashboard"))

    branding = org
    return render_template("login.html", register=True, branding=branding, slug=slug)


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out.")
    return redirect(url_for("auth.login"))


def init_app(app):
    login_manager.init_app(app)
    app.register_blueprint(auth_bp)
