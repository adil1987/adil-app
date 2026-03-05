"""
ADIL APP - Email Sender Application
====================================
Application d'envoi d'emails en masse avec multi-SMTP et rotation.
"""

from flask import Flask, render_template, request, redirect, url_for, flash, session
from functools import wraps
from datetime import datetime

# Import des fonctions database
from database import (
    init_db,
    get_db,
    get_stats_summary,
    get_stats_last_days,
    # SMTP
    get_all_smtp,
    get_smtp_by_id,
    add_smtp,
    update_smtp,
    delete_smtp,
    # Emails
    get_all_emails,
    get_email_by_id,
    add_email,
    update_email,
    delete_email,
    # Contacts
    get_all_contacts,
    count_contacts,
    count_untagged_contacts,
    get_contact_by_id,
    add_contact,
    update_contact,
    update_contact_status,
    delete_contact,
    delete_contacts_bulk,
    update_contacts_status_bulk,
    add_contacts_bulk,
    # Tags
    get_all_tags,
    get_tag_by_id,
    add_tag,
    update_tag,
    delete_tag,
    add_tags_to_contacts_bulk,
    remove_tags_from_contacts_bulk,
    get_contacts_tags_map,
    # Settings
    get_setting,
    set_setting,
    # Test emails
    get_all_test_emails,
    add_test_email,
    delete_test_email,
    # Campaigns & Send
    create_campaign,
    get_campaign_by_id,
    get_all_campaigns,
    update_campaign_status,
    update_campaign_stats,
    create_send_jobs_bulk,
    get_pending_jobs,
    update_job_status,
    get_campaign_job_stats,
    delete_pending_jobs_for_contact,
    # Campaign Logs
    add_campaign_log,
    get_campaign_logs,
    # Blacklist
    add_to_blacklist,
    is_blacklisted,
    filter_blacklisted_emails,
    get_blacklist_count,
    # Bounce
    add_bounce_log,
    get_bounce_logs,
    get_bounce_stats,
    get_all_blacklisted,
    remove_from_blacklist,
    # Offers
    get_all_offers,
    get_offer_by_id,
    add_offer,
    update_offer,
    delete_offer,
    # Users
    verify_user,
    get_user_by_id,
)
from email_filters import apply_filters
from send_worker import send_worker, start_send_worker, get_worker_status
from bounce_worker import bounce_worker
import json

# =========================
# APP
# =========================
app = Flask(__name__)
app.secret_key = "adil_app_secret_key_2024"

# Initialiser la base de données au démarrage
init_db()

@app.cli.command("run-worker")
def run_worker():
    """Start the background workers (Send & Bounce)."""
    print("Starting background workers...")
    bounce_worker.start()
    start_send_worker()
    
    # Keep the process alive
    import time
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping background workers...")
        bounce_worker.stop()
        from send_worker import stop_send_worker
        stop_send_worker()

# =========================
# LOGIN DEFINITIONS
# =========================
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        user = verify_user(username, password)
        if user:
            session['user_id'] = user['id']
            next_url = request.args.get("next")
            return redirect(next_url or url_for("dashboard"))
        flash("Identifiants incorrects.", "danger")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.pop('user_id', None)
    return redirect(url_for("login"))


# =========================
# ROUTES - DASHBOARD
# =========================
@app.route("/")
@login_required
def dashboard():
    stats = get_stats_summary()
    daily_stats = get_stats_last_days(7)
    return render_template("dashboard.html", stats=stats, daily_stats=daily_stats)


@app.route("/api/dashboard/stats")
@login_required
def api_dashboard_stats():
    """Real-time dashboard stats: hourly today + daily this month."""
    from flask import jsonify
    from database import get_db
    from datetime import datetime
    try:
        conn = get_db()
        cursor = conn.cursor()

        today = datetime.now().strftime('%Y-%m-%d')
        month_start = datetime.now().strftime('%Y-%m-01')
        current_day = datetime.now().day

        # Hourly sent today (from send_jobs)
        hourly_sent = [0] * 24
        cursor.execute("""
            SELECT CAST(strftime('%H', sent_at) AS INTEGER) as hr, COUNT(*) as cnt
            FROM send_jobs
            WHERE status = 'sent' AND sent_at IS NOT NULL AND DATE(sent_at) = ?
            GROUP BY hr
        """, (today,))
        for r in cursor.fetchall():
            if r['hr'] is not None:
                hourly_sent[r['hr']] = r['cnt']

        # Hourly bounced today (from bounce_logs)
        hourly_bounced = [0] * 24
        cursor.execute("""
            SELECT CAST(strftime('%H', processed_at) AS INTEGER) as hr, COUNT(*) as cnt
            FROM bounce_logs
            WHERE processed_at IS NOT NULL AND DATE(processed_at) = ?
            GROUP BY hr
        """, (today,))
        for r in cursor.fetchall():
            if r['hr'] is not None:
                hourly_bounced[r['hr']] = r['cnt']

        # Daily errors today
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM send_jobs
            WHERE status = 'failed' AND (
                (sent_at IS NOT NULL AND DATE(sent_at) = ?) OR
                (queued_at IS NOT NULL AND DATE(queued_at) = ?)
            )
        """, (today, today))
        daily_errors = cursor.fetchone()['cnt'] or 0

        # Monthly sent (per day)
        monthly_sent = [0] * 31
        cursor.execute("""
            SELECT CAST(strftime('%d', sent_at) AS INTEGER) as dy, COUNT(*) as cnt
            FROM send_jobs
            WHERE status = 'sent' AND sent_at IS NOT NULL AND sent_at >= ?
            GROUP BY dy
        """, (month_start,))
        for r in cursor.fetchall():
            if r['dy'] is not None and 1 <= r['dy'] <= 31:
                monthly_sent[r['dy'] - 1] = r['cnt']

        # Monthly bounced (per day)
        monthly_bounced = [0] * 31
        cursor.execute("""
            SELECT CAST(strftime('%d', processed_at) AS INTEGER) as dy, COUNT(*) as cnt
            FROM bounce_logs
            WHERE processed_at IS NOT NULL AND processed_at >= ?
            GROUP BY dy
        """, (month_start,))
        for r in cursor.fetchall():
            if r['dy'] is not None and 1 <= r['dy'] <= 31:
                monthly_bounced[r['dy'] - 1] = r['cnt']

        # Monthly opens (for true open rate)
        cursor.execute("""
            SELECT SUM(opened) as cnt
            FROM send_jobs
            WHERE opened = 1 AND sent_at IS NOT NULL AND sent_at >= ?
        """, (month_start,))
        total_opened_month = cursor.fetchone()['cnt'] or 0

        # Open rate calculation
        total_sent_month = sum(monthly_sent)
        open_rate = 0
        if total_sent_month > 0:
            open_rate = round((total_opened_month / total_sent_month) * 100)

        conn.close()

        return jsonify({
            "success": True,
            "daily": {
                "sent": hourly_sent,
                "bounced": hourly_bounced,
                "errors": daily_errors
            },
            "monthly": {
                "sent": monthly_sent,
                "bounced": monthly_bounced
            },
            "open_rate": open_rate,
            "current_day": current_day
        })
    except Exception as e:
        return jsonify({
            "success": True,
            "daily": {"sent": [0]*24, "bounced": [0]*24, "errors": 0},
            "monthly": {"sent": [0]*31, "bounced": [0]*31},
            "open_rate": 0,
            "current_day": datetime.now().day,
            "error": str(e)
        })


# =========================
# ROUTES - EMAILS (Templates)
# =========================
@app.route("/emails", methods=["GET", "POST"])
@login_required
def emails():
    emails_list = get_all_emails()
    
    # Get offers for dropdown
    offers = get_all_offers()
    
    # Formulaire par défaut
    form_data = {
        "id": None,
        "name": "",
        "subject": "",
        "from_name": "",
        "from_email": "",
        "reply_to": "",
        "mode": "text",
        "body": "",
        "text_fallback": "",
        "footer": "",
        "offer_id": None,
        "format": "multipart/alternative",
        "charset": "UTF-8",
        "transfer_encoding": "quoted-printable"
    }
    
    # Charger un email pour édition
    load_id = request.args.get("load")
    if load_id:
        email = get_email_by_id(int(load_id))
        if email:
            form_data = {
                "id": email["id"],
                "name": email["name"] or "",
                "subject": email["subject"] or "",
                "from_name": email["from_name"] or "",
                "from_email": email.get("from_email") or "",
                "reply_to": email["reply_to"] or "",
                "mode": email["mode"] or "text",
                "body": email["body"] or "",
                "text_fallback": email["text_fallback"] or "",
                "footer": email.get("footer") or "",
                "offer_id": email.get("offer_id"),
                "format": email.get("format") or "multipart/alternative",
                "charset": email.get("charset") or "UTF-8",
                "transfer_encoding": email.get("transfer_encoding") or "quoted-printable"
            }
    
    if request.method == "POST":
        action = request.form.get("action")
        
        # DELETE
        if action == "delete":
            email_id = request.form.get("email_id")
            if email_id:
                delete_email(int(email_id))
            return redirect("/emails")
        
        # DUPLICATE
        if action == "duplicate":
            email_id = request.form.get("email_id")
            if email_id:
                original = get_email_by_id(int(email_id))
                if original:
                    add_email({
                        "name": original["name"] + " (copy)",
                        "subject": original["subject"],
                        "from_name": original["from_name"],
                        "from_email": original.get("from_email"),
                        "reply_to": original["reply_to"],
                        "mode": original["mode"],
                        "body": original["body"],
                        "text_fallback": original["text_fallback"],
                        "footer": original.get("footer"),
                        "offer_id": original.get("offer_id"),
                        "format": original.get("format"),
                        "charset": original.get("charset"),
                        "transfer_encoding": original.get("transfer_encoding")
                    })
            return redirect("/emails")
        
        # Lire le formulaire
        form_data = {
            "id": request.form.get("email_id"),
            "name": request.form.get("name", ""),
            "subject": request.form.get("subject", ""),
            "from_name": request.form.get("from_name", ""),
            "from_email": request.form.get("from_email", ""),
            "reply_to": request.form.get("reply_to", ""),
            "mode": request.form.get("mode", "text"),
            "body": request.form.get("body", ""),
            "text_fallback": request.form.get("text_fallback", ""),
            "footer": request.form.get("footer", ""),
            "offer_id": request.form.get("offer_id") or None,
            "format": request.form.get("format", "multipart/alternative"),
            "charset": request.form.get("charset", "UTF-8"),
            "transfer_encoding": request.form.get("transfer_encoding", "quoted-printable")
        }
        
        # SAVE (nouveau) ou UPDATE (existant)
        if action == "save":
            email_id = request.form.get("email_id")
            then_new = request.form.get("then_new")
            if email_id:
                # Update existant
                update_email(int(email_id), form_data)
                saved_id = email_id
            else:
                # Nouveau
                saved_id = add_email(form_data)
            flash("Email sauvegardé ✓", "success")
            # If "Save & New" was clicked, redirect to blank form
            if then_new:
                return redirect("/emails")
            return redirect(f"/emails?load={saved_id}")
        
        # SAVE AND SEND TEST
        if action == "save_and_test":
            email_id = request.form.get("email_id")
            if email_id:
                update_email(int(email_id), form_data)
                saved_id = email_id
            else:
                saved_id = add_email(form_data)
            # Redirect to send page with test mode
            return redirect(f"/send?test_email_id={saved_id}")
    
    return render_template(
        "emails.html",
        emails=emails_list,
        form_data=form_data,
        offers=offers
    )


# =========================
# ROUTES - SMTP
# =========================
@app.route("/smtp", methods=["GET", "POST"])
@login_required
def smtp():
    if request.method == "POST":
        action = request.form.get("action")
        
        # DELETE
        if action == "delete":
            smtp_id = request.form.get("smtp_id")
            if smtp_id:
                delete_smtp(int(smtp_id))
            return redirect("/smtp")
        
        # TOGGLE STATUS
        if action == "toggle":
            smtp_id = request.form.get("smtp_id")
            new_status = request.form.get("new_status")
            if smtp_id:
                smtp = get_smtp_by_id(int(smtp_id))
                if smtp:
                    smtp["is_active"] = int(new_status)
                    update_smtp(int(smtp_id), smtp)
            return redirect("/smtp")
        
        # ADD / UPDATE
        data = {
            "name": request.form.get("name"),
            "host": request.form.get("host"),
            "port": request.form.get("port", 587),
            "username": request.form.get("username"),
            "password": request.form.get("password"),
            "use_tls": request.form.get("use_tls") == "on",
            "rate_limit": request.form.get("rate_limit", 100),
            "daily_limit": request.form.get("daily_limit", 500),
            "is_active": request.form.get("is_active") == "on",
            "priority": request.form.get("priority", 1),
            "domain": request.form.get("domain", ""),
            "ip_address": request.form.get("ip_address", ""),
            # New fields
            "sending_domain": request.form.get("sending_domain", ""),
            "from_email": request.form.get("from_email", ""),
            "return_path_domain": request.form.get("return_path_domain", ""),
            "dkim_selector": request.form.get("dkim_selector", ""),
            "auto_pause_on_error": request.form.get("auto_pause_on_error") == "on",
            "max_bounce_rate": request.form.get("max_bounce_rate", 5.0),
            "max_spam_rate": request.form.get("max_spam_rate", 0.1),
            "warmup_enabled": request.form.get("warmup_enabled") == "on",
            "bounce_email": request.form.get("bounce_email", ""),
            "bounce_password": request.form.get("bounce_password", ""),
            "max_connections": int(request.form.get("max_connections", 1))
        }
        
        smtp_id = request.form.get("smtp_id")
        if smtp_id and action == "edit":
            update_smtp(int(smtp_id), data)
        else:
            add_smtp(data)
        
        return redirect("/smtp")
    
    smtp_servers = get_all_smtp()
    return render_template("smtp.html", smtp_servers=smtp_servers)


# ===========================
# BOUNCE PAGE & API
# ===========================

@app.route("/bounce")
@login_required
def bounce_page():
    smtp_servers = get_all_smtp()
    logs = get_bounce_logs(limit=50)
    stats = get_bounce_stats()
    blacklisted = get_all_blacklisted(limit=100)
    bl_count = get_blacklist_count()
    return render_template("bounce.html",
        smtp_servers=smtp_servers,
        bounce_logs=logs,
        bounce_stats=stats,
        blacklisted=blacklisted,
        blacklist_count=bl_count
    )


@app.route("/api/bounce/check/<int:smtp_id>")
@login_required
def api_bounce_check(smtp_id):
    from flask import jsonify
    result = bounce_worker.check_single(smtp_id)
    return jsonify(result)


@app.route("/api/bounce/logs")
@login_required
def api_bounce_logs():
    from flask import jsonify
    logs = get_bounce_logs(limit=100)
    return jsonify(logs)


@app.route("/api/bounce/blacklist")
@login_required
def api_bounce_blacklist():
    from flask import jsonify
    search = request.args.get('search', '')
    blacklisted = get_all_blacklisted(limit=200, search=search)
    return jsonify(blacklisted)


@app.route("/api/bounce/blacklist/<path:email_addr>", methods=["DELETE"])
@login_required
def api_bounce_remove_blacklist(email_addr):
    from flask import jsonify
    try:
        remove_from_blacklist(email_addr)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/bounce/export")
@login_required
def api_bounce_export():
    from flask import Response
    blacklisted = get_all_blacklisted(limit=10000)
    csv_lines = ["email,reason,date"]
    for bl in blacklisted:
        email_val = bl.get('email', '').replace('"', '""')
        reason = (bl.get('reason', '') or '').replace('"', '""')
        date = bl.get('created_at', '')
        csv_lines.append(f'"{email_val}","{reason}","{date}"')
    csv_content = "\n".join(csv_lines)
    return Response(
        csv_content,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=blacklist_export.csv'}
    )


# ===========================
# UNSUBSCRIBE
# ===========================

import hashlib

UNSUB_SECRET = "adil_app_unsub_secret_2024"

def generate_unsub_token(email_addr):
    """Generate a simple HMAC token for unsubscribe link security."""
    return hashlib.sha256(f"{email_addr}:{UNSUB_SECRET}".encode()).hexdigest()[:16]


def generate_unsub_url(contact_email):
    """Generate the full unsubscribe URL for a contact."""
    app_url = get_setting("app_url", "").rstrip("/")
    if not app_url:
        return ""
    token = generate_unsub_token(contact_email)
    return f"{app_url}/unsubscribe?email={contact_email}&token={token}"


@app.route("/unsubscribe", methods=["GET", "POST"])
def unsubscribe():
    """Handle unsubscribe requests (one-click RFC 8058 via POST, or browser click via GET)."""
    email_addr = request.args.get("email") or request.form.get("email") or ""
    token = request.args.get("token") or request.form.get("token") or ""
    
    if not email_addr or not token:
        return render_template("unsubscribe.html", success=False, already=False, email="")
    
    # Verify token
    expected_token = generate_unsub_token(email_addr)
    if token != expected_token:
        return render_template("unsubscribe.html", success=False, already=False, email=email_addr)
    
    # Find the contact and update status
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, status FROM contacts WHERE email = ?", (email_addr,))
    contact = cursor.fetchone()
    
    if contact and contact["status"] == "unsubscribed":
        conn.close()
        return render_template("unsubscribe.html", success=False, already=True, email=email_addr)
    
    if contact:
        cursor.execute("UPDATE contacts SET status = 'unsubscribed' WHERE email = ?", (email_addr,))
        conn.commit()
    
    conn.close()
    return render_template("unsubscribe.html", success=True, already=False, email=email_addr)


import base64
from flask import Response

# =========================
# ROUTES - TRACKING
# =========================
@app.route("/track/open/<int:job_id>.gif")
def track_open(job_id):
    """Serve a 1x1 transparent GIF and mark the user as opened."""
    from database import mark_job_as_opened
    
    # Mark user as opened in background
    mark_job_as_opened(job_id)
    
    # Base64 encoded 1x1 transparent GIF
    gif_b64 = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
    gif_data = base64.b64decode(gif_b64)
    
    return Response(gif_data, mimetype="image/gif", headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
    })

# SMTP Test Connection API
@app.route("/api/smtp/<int:smtp_id>/test")
@login_required
def api_test_smtp(smtp_id):
    """Test connection for a saved SMTP server."""
    from flask import jsonify
    import smtplib
    smtp = get_smtp_by_id(smtp_id)
    if not smtp:
        return jsonify({"success": False, "error": "Server not found"})
    
    try:
        if smtp.get("use_tls"):
            server = smtplib.SMTP(smtp["host"], smtp["port"], timeout=10)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(smtp["host"], smtp["port"], timeout=10)
        
        server.login(smtp["username"], smtp["password"])
        server.quit()
        return jsonify({"success": True, "message": f"Connected to {smtp['host']}:{smtp['port']}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/smtp/test", methods=["POST"])
@login_required
def api_test_smtp_form():
    """Test connection for form data (before saving)."""
    from flask import jsonify
    import smtplib
    data = request.get_json()
    host = data.get("host")
    port = int(data.get("port", 587))
    username = data.get("username")
    password = data.get("password")
    use_tls = data.get("use_tls", True)
    
    try:
        if use_tls:
            server = smtplib.SMTP(host, port, timeout=10)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(host, port, timeout=10)
        
        server.login(username, password)
        server.quit()
        return jsonify({"success": True, "message": f"Connected to {host}:{port}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/smtp/<int:smtp_id>/check-dns")
@login_required
def api_check_dns(smtp_id):
    """Check DNS records (SPF, DKIM, DMARC) for an SMTP server's domain."""
    from flask import jsonify
    import dns.resolver
    smtp = get_smtp_by_id(smtp_id)
    if not smtp:
        return jsonify({"success": False, "error": "Server not found"})
    
    # Determine domains to check
    sending_domain = smtp.get("sending_domain") or ""
    from_email = smtp.get("from_email") or ""
    from_domain = from_email.split("@")[-1] if "@" in from_email else ""
    
    # Primary domain: sending_domain > from_email domain > domain field
    domain = sending_domain or from_domain or smtp.get("domain") or ""
    if not domain:
        return jsonify({"success": False, "error": "No domain configured"})
    
    # Root domain (e.g. abc-connect.com from action.abc-connect.com)
    parts = domain.split(".")
    root_domain = ".".join(parts[-2:]) if len(parts) >= 2 else domain
    
    dkim_selector = smtp.get("dkim_selector") or "default"
    
    result = {"success": True, "spf": False, "dkim": False, "dmarc": None, "domain": domain}
    dns_status = []
    
    # Configure a custom resolver with public DNS to avoid local VM timeouts
    resolver = dns.resolver.Resolver()
    resolver.nameservers = ['8.8.8.8', '1.1.1.1', '8.8.4.4']
    resolver.timeout = 2.0
    resolver.lifetime = 2.0
    
    # Check SPF - try sending domain first, then root domain
    domains_for_spf = [domain]
    if root_domain != domain:
        domains_for_spf.append(root_domain)
    
    for spf_domain in domains_for_spf:
        if result["spf"]:
            break
        try:
            answers = resolver.resolve(spf_domain, 'TXT')
            for rdata in answers:
                txt = str(rdata)
                if 'v=spf1' in txt:
                    result["spf"] = True
                    dns_status.append("spf_ok")
                    break
        except Exception:
            pass
    
    # Check DKIM - try configured selector first, then common ones
    dkim_selectors = [dkim_selector]
    common_selectors = ["google", "selector1", "selector2", "s1", "s2", "mail", "dkim", "k1", "default"]
    for s in common_selectors:
        if s not in dkim_selectors:
            dkim_selectors.append(s)
    
    domains_to_try = [domain]
    if root_domain != domain:
        domains_to_try.append(root_domain)
    
    for sel in dkim_selectors:
        if result["dkim"]:
            break
        for d in domains_to_try:
            try:
                dkim_domain = f"{sel}._domainkey.{d}"
                resolver.resolve(dkim_domain, 'TXT')
                result["dkim"] = True
                result["dkim_selector"] = sel
                dns_status.append("dkim_ok")
                break
            except Exception:
                continue
    
    # Check DMARC - try sending domain first, then root domain
    domains_for_dmarc = [domain]
    if root_domain != domain:
        domains_for_dmarc.append(root_domain)
    
    for dmarc_d in domains_for_dmarc:
        if result["dmarc"]:
            break
        try:
            dmarc_domain = f"_dmarc.{dmarc_d}"
            answers = resolver.resolve(dmarc_domain, 'TXT')
            for rdata in answers:
                txt = str(rdata)
                if 'v=DMARC1' in txt:
                    if 'p=reject' in txt or 'p=quarantine' in txt:
                        result["dmarc"] = "✅ DMARC enforcing"
                        dns_status.append("dmarc_ok")
                    else:
                        result["dmarc"] = "⚠️ DMARC p=none"
                        dns_status.append("dmarc_none")
                    break
        except Exception:
            pass
    
    # Update dns_status in database
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE smtp_servers SET dns_status = ? WHERE id = ?", (",".join(dns_status), smtp_id))
    conn.commit()
    conn.close()
    
    return jsonify(result)


# =========================
# ROUTES - CONTACTS
# =========================
@app.route("/contacts", methods=["GET", "POST"])
@login_required
def contacts():
    import_result = None
    
    if request.method == "POST":
        action = request.form.get("action")
        
        # IMPORT CSV/XLSX
        if action == "import":
            file = request.files.get("file")
            if file and file.filename:
                try:
                    import csv
                    import io
                    
                    col_email = request.form.get("col_email", "0")
                    col_prenom = request.form.get("col_prenom", "")
                    col_nom = request.form.get("col_nom", "")
                    skip_duplicates = request.form.get("skip_duplicates") == "on"
                    skip_header = request.form.get("skip_header") == "on"
                    
                    # Read file
                    content = file.read().decode("utf-8", errors="ignore")
                    
                    # Detect delimiter
                    delimiter = ";" if ";" in content.split("\n")[0] else ","
                    
                    reader = csv.reader(io.StringIO(content), delimiter=delimiter)
                    rows = list(reader)
                    
                    if skip_header and rows:
                        rows = rows[1:]
                    
                    # Parse contacts
                    parsed = []
                    col_email_idx = int(col_email) if col_email else 0
                    col_prenom_idx = int(col_prenom) if col_prenom else None
                    col_nom_idx = int(col_nom) if col_nom else None
                    
                    for row in rows:
                        if len(row) > col_email_idx:
                            contact = {"email": row[col_email_idx]}
                            if col_prenom_idx is not None and len(row) > col_prenom_idx:
                                contact["prenom"] = row[col_prenom_idx]
                            if col_nom_idx is not None and len(row) > col_nom_idx:
                                contact["nom"] = row[col_nom_idx]
                            parsed.append(contact)
                    
                    # Build filters dict from form checkboxes
                    filters = {
                        'validate_format': 'filter_validate_format' in request.form,
                        'check_length': 'filter_check_length' in request.form,
                        'check_chars': 'filter_check_chars' in request.form,
                        'check_tld': 'filter_check_tld' in request.form,
                        'block_disposable': 'filter_block_disposable' in request.form,
                        'block_role': 'filter_block_role' in request.form,
                        'block_free_webmail': 'filter_block_free_webmail' in request.form,
                        'only_free_webmail': 'filter_only_free_webmail' in request.form,
                        'detect_typos': 'filter_detect_typos' in request.form,
                        'auto_correct_typos': 'filter_auto_correct_typos' in request.form,
                    }
                    
                    # Apply filters to parsed contacts
                    valid_contacts, rejected_contacts, filter_stats = apply_filters(parsed, filters)
                    
                    # Import filtered contacts to database
                    added, skipped, added_ids = add_contacts_bulk(valid_contacts, source=file.filename, skip_duplicates=skip_duplicates)
                    
                    # Assign tags if selected
                    selected_tags = request.form.getlist("import_tags")
                    if selected_tags and added_ids:
                        tag_ids = [int(t) for t in selected_tags]
                        add_tags_to_contacts_bulk(added_ids, tag_ids)
                    
                    # Save rejected contacts to database
                    if rejected_contacts:
                        invalid_contacts = [r["contact"] for r in rejected_contacts]
                        add_contacts_bulk(invalid_contacts, source=file.filename, skip_duplicates=True, status="invalid")
                    
                    # Build detailed result message
                    rejected_count = len(rejected_contacts)
                    result_parts = [f"{added} contacts added"]
                    if skipped > 0:
                        result_parts.append(f"{skipped} duplicates skipped")
                    if rejected_count > 0:
                        result_parts.append(f"{rejected_count} rejected by filters, saved to 'Non Conforme' list")
                    if filter_stats.get('typo_corrected', 0) > 0:
                        result_parts.append(f"{filter_stats['typo_corrected']} typos corrected")
                    
                    # Prepare rejected list for display if requested
                    rejected_list = []
                    if rejected_count > 0 and 'show_rejected' in request.form:
                        rejected_list = rejected_contacts

                    import_result = {
                        "status": "success",
                        "title": "Import successful!",
                        "message": ", ".join(result_parts),
                        "rejected_list": rejected_list
                    }
                except Exception as e:
                    import_result = {
                        "status": "error", 
                        "title": "Import failed",
                        "message": str(e)
                    }
        
        # DELETE SINGLE CONTACT
        if action == "delete_contact":
            contact_id = request.form.get("contact_id")
            if contact_id:
                delete_contact(int(contact_id))
            return redirect(f"/contacts?status={request.args.get('status', 'active')}&per_page={request.args.get('per_page', 50)}&page={request.args.get('page', 1)}")
        
        # BULK DELETE
        if action == "bulk_delete":
            ids = request.form.get("ids", "")
            if ids:
                id_list = [int(i) for i in ids.split(",") if i.isdigit()]
                delete_contacts_bulk(id_list)
            return redirect(f"/contacts?status={request.args.get('status', 'active')}&per_page={request.args.get('per_page', 50)}")
        
        # BULK UNSUBSCRIBE
        if action == "bulk_unsubscribe":
            ids = request.form.get("ids", "")
            if ids:
                id_list = [int(i) for i in ids.split(",") if i.isdigit()]
                update_contacts_status_bulk(id_list, "unsubscribed")
            return redirect(f"/contacts?status=unsubscribed&per_page={request.args.get('per_page', 50)}")
        
        # BULK DELETE BY TAG
        if action == "bulk_delete_by_tag":
            tag_id = request.form.get("tag_id", type=int)
            if tag_id:
                # Get all contacts with this tag and delete them
                contacts_with_tag = get_all_contacts(tag_id=tag_id, limit=100000)
                if contacts_with_tag:
                    id_list = [c["id"] for c in contacts_with_tag]
                    delete_contacts_bulk(id_list)
            return redirect(f"/contacts?status={request.args.get('status', 'active')}&per_page={request.args.get('per_page', 50)}")
        
        # ADD MANUAL CONTACT
        if action == "add_manual":
            email = request.form.get("manual_email", "").strip().lower()
            prenom = request.form.get("manual_prenom", "").strip()
            nom = request.form.get("manual_nom", "").strip()
            manual_tags = request.form.getlist("manual_tags")
            
            if email and "@" in email:
                contact_id = add_contact(email, prenom, nom, source="manual")
                if contact_id and manual_tags:
                    tag_ids = [int(t) for t in manual_tags]
                    add_tags_to_contacts_bulk([contact_id], tag_ids)
            return redirect(f"/contacts?status={request.args.get('status', 'active')}&per_page={request.args.get('per_page', 50)}")
        
        # BULK ADD TAG
        if action == "bulk_add_tag":
            ids = request.form.get("ids", "")
            tag_id = request.form.get("tag_id", type=int)
            if ids and tag_id:
                id_list = [int(i) for i in ids.split(",") if i.isdigit()]
                add_tags_to_contacts_bulk(id_list, [tag_id])
            return redirect(f"/contacts?status={request.args.get('status', 'active')}&per_page={request.args.get('per_page', 50)}")
        
        # BULK REMOVE TAG
        if action == "bulk_remove_tag":
            ids = request.form.get("ids", "")
            tag_id = request.form.get("tag_id", type=int)
            if ids and tag_id:
                id_list = [int(i) for i in ids.split(",") if i.isdigit()]
                remove_tags_from_contacts_bulk(id_list, [tag_id])
            return redirect(f"/contacts?status={request.args.get('status', 'active')}&per_page={request.args.get('per_page', 50)}")
        
        # EDIT CONTACT
        if action == "edit_contact":
            contact_id = request.form.get("contact_id", type=int)
            email = request.form.get("edit_email", "").strip().lower()
            prenom = request.form.get("edit_prenom", "").strip()
            nom = request.form.get("edit_nom", "").strip()
            if contact_id and email:
                update_contact(contact_id, email, prenom, nom)
            return redirect(f"/contacts?status={request.args.get('status', 'active')}&per_page={request.args.get('per_page', 50)}&page={request.args.get('page', 1)}")
    
    # GET - Display contacts
    page = request.args.get("page", 1, type=int)
    status_filter = request.args.get("status", "active")
    search_query = request.args.get("q", "").strip()
    tag_filter_raw = request.args.get("tag", "")
    # Handle 'untagged' as string, otherwise convert to int
    tag_filter = "untagged" if tag_filter_raw == "untagged" else (int(tag_filter_raw) if tag_filter_raw.isdigit() else None)
    per_page = request.args.get("per_page", 50, type=int)
    if per_page not in [50, 100, 500, 1000]:
        per_page = 50
    
    contacts_list = get_all_contacts(
        status=status_filter,
        tag_id=tag_filter,
        search=search_query,
        limit=per_page,
        offset=(page - 1) * per_page
    )
    
    # Get tags for displayed contacts
    contact_ids = [c["id"] for c in contacts_list]
    contacts_tags = get_contacts_tags_map(contact_ids)
    
    total = count_contacts(status=status_filter)
    total_pages = (total + per_page - 1) // per_page
    
    counts = {
        "active": count_contacts("active"),
        "unsubscribed": count_contacts("unsubscribed"),
        "bounced": count_contacts("bounced"),
        "invalid": count_contacts("invalid"),
        "total": count_contacts(),
        "untagged": count_untagged_contacts()
    }
    
    # Get all tags for filters and dropdowns
    all_tags = get_all_tags()
    
    return render_template(
        "contacts.html",
        contacts=contacts_list,
        contacts_tags=contacts_tags,
        counts=counts,
        current_status=status_filter,
        current_tag=tag_filter,
        search_query=search_query,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        import_result=import_result,
        all_tags=all_tags
    )


# =========================
# ROUTES - CONTACTS EXPORT
# =========================
@app.route("/contacts/export")
@login_required
def contacts_export():
    """Export contacts as CSV."""
    import csv
    from io import StringIO
    
    status = request.args.get("status", "active")
    tag_filter_raw = request.args.get("tag", "")
    tag_filter = "untagged" if tag_filter_raw == "untagged" else (int(tag_filter_raw) if tag_filter_raw.isdigit() else None)
    search = request.args.get("q", "").strip()
    
    # Get all contacts (no limit for export)
    contacts = get_all_contacts(status=status, tag_id=tag_filter, search=search, limit=100000, offset=0)
    
    # Create CSV
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Email", "Prénom", "Nom", "Status", "Source", "Created At"])
    
    for c in contacts:
        writer.writerow([
            c.get("email", ""),
            c.get("prenom", ""),
            c.get("nom", ""),
            c.get("status", ""),
            c.get("source", ""),
            c.get("created_at", "")
        ])
    
    output.seek(0)
    
    from flask import Response
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment;filename=contacts_{status}.csv"}
    )


# =========================
# ROUTES - TAGS
# =========================
@app.route("/tags", methods=["GET", "POST"])
@login_required
def tags():
    if request.method == "POST":
        action = request.form.get("action")
        
        # ADD TAG
        if action == "save":
            tag_id = request.form.get("tag_id")
            name = request.form.get("name", "").strip()
            if name:
                if tag_id:
                    update_tag(int(tag_id), name)
                else:
                    add_tag(name)
            return redirect("/tags")
        
        # ADD BULK TAGS
        if action == "add_bulk":
            tags_text = request.form.get("tags_bulk", "")
            for line in tags_text.split("\n"):
                name = line.strip()
                if name:
                    add_tag(name)
            return redirect("/tags")
        
        # DELETE TAG
        if action == "delete":
            tag_id = request.form.get("tag_id")
            if tag_id:
                delete_tag(int(tag_id))
            return redirect("/tags")
    
    # GET
    tags_list = get_all_tags()
    
    # Check if editing
    edit_id = request.args.get("edit")
    edit_tag = get_tag_by_id(int(edit_id)) if edit_id else None
    
    return render_template("tags.html", tags=tags_list, edit_tag=edit_tag)


# =========================
# ROUTES - SETTINGS
# =========================
@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    if request.method == "POST":
        action = request.form.get("action")
        
        # Ajouter email de test
        if action == "add_test_email":
            email = request.form.get("test_email")
            provider = request.form.get("provider", "")
            if email:
                add_test_email(email, provider)
            return redirect("/settings")
        
        # Supprimer email de test
        if action == "delete_test_email":
            test_id = request.form.get("test_id")
            if test_id:
                delete_test_email(int(test_id))
            return redirect("/settings")
        
        # Sauvegarder settings généraux
        set_setting("app_name", request.form.get("app_name", "ADIL APP"))
        set_setting("app_url", request.form.get("app_url", ""))
        
        return redirect("/settings")
    
    settings_data = {
        "app_name": get_setting("app_name", "ADIL APP"),
        "app_url": get_setting("app_url", "")
    }
    
    test_emails = get_all_test_emails()
    
    return render_template(
        "settings.html",
        settings=settings_data,
        test_emails=test_emails
    )


# =========================
# ROUTES - PRODUCTION / SEND
# =========================
@app.route("/send")
@login_required
def send_page():
    """Main send campaign page."""
    from database import get_db
    emails_list = get_all_emails()
    smtp_servers = get_all_smtp()
    
    # Get tags and enrich with contact counts
    tags_list = get_all_tags()
    enriched_tags = []
    
    conn = get_db()
    cursor = conn.cursor()
    for tag in tags_list:
        cursor.execute("""
            SELECT COUNT(c.id) as cnt 
            FROM contacts c
            JOIN contact_tags ct ON c.id = ct.contact_id
            WHERE c.status = 'active' AND ct.tag_id = ?
        """, (tag['id'],))
        row = cursor.fetchone()
        
        # Create a mutable copy of the tag dict to add custom fields
        tag_dict = dict(tag)
        tag_dict['contact_count'] = row['cnt'] if row else 0
        enriched_tags.append(tag_dict)
        
    conn.close()
    
    test_emails_list = get_all_test_emails()
    
    return render_template(
        "send.html",
        emails=emails_list,
        smtp_servers=smtp_servers,
        tags=enriched_tags,
        test_emails=test_emails_list
    )


@app.route("/api/send/preview")
@login_required
def api_send_preview():
    """Get email preview with spintax variations."""
    import re
    import random
    
    email_id = request.args.get("email_id")
    if not email_id:
        return {"body": "", "variations": []}
    
    email = get_email_by_id(int(email_id))
    if not email:
        return {"body": "", "variations": []}
    
    body = email.get("body", "")
    
    # Generate spintax variations
    def spin_once(text):
        pattern = r'\{([^{}]+)\}'
        match = re.search(pattern, text)
        if match:
            options = match.group(1).split('|')
            return text[:match.start()] + random.choice(options) + text[match.end():]
        return text
    
    variations = []
    if '{' in body and '|' in body:
        for _ in range(3):
            temp = body
            for _ in range(10):  # Max 10 spintax replacements
                new_temp = spin_once(temp)
                if new_temp == temp:
                    break
                temp = new_temp
            if temp not in variations:
                variations.append(temp[:200] + "...")
    
    return {"body": body, "variations": variations}


def get_audience_contacts_list(audience_type, include_tags, exclude_tags):
    """
    Returns a unified list of valid contacts: [{"id": 1, "email": "test@test.com"}, ...]
    Applies audience_type, include_tags, exclude_tags, and active status.
    Filters out emails present in the global blacklist.
    """
    from database import get_db, filter_blacklisted_emails
    conn = get_db()
    cursor = conn.cursor()
    
    if audience_type == "all":
        cursor.execute("SELECT id, email FROM contacts WHERE status = 'active'")
        contacts = cursor.fetchall()
    elif audience_type == "tags" and include_tags:
        placeholders = ",".join(["?" for _ in include_tags])
        cursor.execute(f"""
            SELECT DISTINCT c.id, c.email FROM contacts c
            JOIN contact_tags ct ON c.id = ct.contact_id
            WHERE c.status = 'active' AND ct.tag_id IN ({placeholders})
        """, include_tags)
        contacts = cursor.fetchall()
    elif audience_type == "tags" and not include_tags:
        # No tags selected = 0 recipients (don't fall through to all)
        conn.close()
        return []
    else:
        cursor.execute("SELECT id, email FROM contacts WHERE status = 'active'")
        contacts = cursor.fetchall()
        
    contacts_list = [{"id": c["id"], "email": c["email"]} for c in contacts]
    
    # Exclude tags
    if audience_type == "tags" and exclude_tags:
        placeholders = ",".join(["?" for _ in exclude_tags])
        cursor.execute(f"""
            SELECT DISTINCT contact_id FROM contact_tags 
            WHERE tag_id IN ({placeholders})
        """, exclude_tags)
        excluded_ids = {row["contact_id"] for row in cursor.fetchall()}
        contacts_list = [c for c in contacts_list if c["id"] not in excluded_ids]
        
    conn.close()
    
    # Filter blacklisted
    if contacts_list:
        contact_emails = [c["email"] for c in contacts_list]
        valid_emails = set(filter_blacklisted_emails(contact_emails))
        contacts_list = [c for c in contacts_list if c["email"] in valid_emails]
        
    return contacts_list


@app.route("/api/send/estimate", methods=["POST"])
@login_required
def api_send_estimate():
    """Estimate recipient count based on audience selection."""
    data = request.get_json()
    audience_type = data.get("audience_type", "all")
    include_tags = [int(t) for t in data.get("include_tags", []) if t]
    exclude_tags = [int(t) for t in data.get("exclude_tags", []) if t]
    
    contacts = get_audience_contacts_list(audience_type, include_tags, exclude_tags)
    return {"count": len(contacts)}


@app.route("/api/send/validate", methods=["POST"])
@login_required
def api_send_validate():
    """Validate campaign configuration before sending."""
    from flask import jsonify
    data = request.get_json()
    errors = []
    warnings = []
    
    # Check email
    email_id = data.get("email_id")
    email = None
    if not email_id:
        errors.append("No email template selected")
    else:
        email = get_email_by_id(int(email_id))
        if not email:
            errors.append("Email template not found")
    
    # Check SMTP(s) - now supports multiple
    smtp_ids = data.get("smtp_ids", [])
    # Backwards compat: single smtp_id
    if not smtp_ids and data.get("smtp_id"):
        smtp_ids = [data.get("smtp_id")]
    
    if not smtp_ids:
        errors.append("No SMTP server selected")
    else:
        for sid in smtp_ids:
            smtp = get_smtp_by_id(int(sid))
            if not smtp:
                errors.append(f"SMTP server #{sid} not found")
            elif smtp.get("pause_reason"):
                warnings.append(f"SMTP '{smtp.get('name', sid)}' is paused: {smtp.get('pause_reason')}")
            else:
                # DNS checks on first valid SMTP
                dns = smtp.get("dns_status", "")
                if "spf_ok" not in dns:
                    warnings.append(f"SMTP '{smtp.get('name')}': SPF not configured")
                if "dkim_ok" not in dns:
                    warnings.append(f"SMTP '{smtp.get('name')}': DKIM not configured")
                
                # Domain match check
                if email:
                    email_from = email.get("from_email", "")
                    smtp_domain = smtp.get("sending_domain") or smtp.get("domain", "")
                    if email_from and smtp_domain:
                        from_domain = email_from.split("@")[-1] if "@" in email_from else ""
                        if from_domain and from_domain.lower() != smtp_domain.lower():
                            errors.append(f"From domain ({from_domain}) doesn't match SMTP '{smtp.get('name')}' domain ({smtp_domain})")
    
    # Check recipients
    estimate_resp = api_send_estimate()
    count = estimate_resp.get("count", 0) if isinstance(estimate_resp, dict) else 0
    if count == 0:
        errors.append("No recipients found")
    elif count > 1000:
        warnings.append(f"Large volume: {count:,} recipients")
    
    # Check SMTP daily limit vs recipients
    if count > 0 and smtp_ids:
        total_daily_capacity = 0
        for sid in smtp_ids:
            s = get_smtp_by_id(int(sid))
            if s:
                total_daily_capacity += s.get('daily_limit', 500)
        if total_daily_capacity > 0 and count > total_daily_capacity:
            days = -(-count // total_daily_capacity)  # ceil division
            warnings.append(f"SMTP daily limit exceeded: {count:,} recipients > {total_daily_capacity:,}/day capacity. Campaign will span ~{days} days")
    # Check test inbox
    test_email = data.get("test_email", "")
    if not test_email:
        warnings.append("No test inbox email selected")
    
    return jsonify({"errors": errors, "warnings": warnings, "valid": len(errors) == 0})


@app.route("/api/send/test", methods=["POST"])
@login_required
def api_send_test():
    """Send a test email."""
    from flask import jsonify
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    data = request.get_json()
    email_id = data.get("email_id")
    smtp_id = data.get("smtp_id")
    to_email = data.get("to")
    
    print(f"[TEST EMAIL] Request: email_id={email_id}, smtp_id={smtp_id}, to={to_email}")
    
    if not all([email_id, smtp_id, to_email]):
        print(f"[TEST EMAIL] Missing fields! email_id={email_id}, smtp_id={smtp_id}, to={to_email}")
        return jsonify({"success": False, "error": "Missing required fields"})
    
    email = get_email_by_id(int(email_id))
    smtp = get_smtp_by_id(int(smtp_id))
    
    if not email or not smtp:
        print(f"[TEST EMAIL] Not found: email={email is not None}, smtp={smtp is not None}")
        return jsonify({"success": False, "error": "Email or SMTP not found"})
    
    print(f"[TEST EMAIL] SMTP: {smtp.get('name')} / {smtp['host']}:{smtp.get('port')} / use_tls={smtp.get('use_tls')} / user={smtp.get('username')}")
    
    # Use SMTP's from_email as sender (not email template's)
    sender_email = smtp.get("from_email") or smtp.get("username", "")
    sender_name = email.get("from_name", "")
    print(f"[TEST EMAIL] From: {sender_name} <{sender_email}>")
    
    if not sender_email:
        return jsonify({"success": False, "error": "SMTP has no From Email configured. Edit the SMTP and add a From Email address."})
    
    try:
        from email.utils import formatdate
        import uuid
        
        # Generate domain-aligned Message-ID
        smtp_domain = smtp.get('sending_domain') or smtp.get('domain') or sender_email.split('@')[-1] if '@' in sender_email else 'localhost'
        msg_id = f"<{uuid.uuid4().hex[:16]}.{int(__import__('time').time())}@{smtp_domain}>"
        
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[TEST] {email.get('subject', 'No subject')}"
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = to_email
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = msg_id
        
        html_part = MIMEText(email.get("body", ""), "html", _charset="utf-8")
        html_part.replace_header('Content-Transfer-Encoding', 'quoted-printable')
        import quopri
        html_part.set_payload(quopri.encodestring(email.get('body', '').encode('utf-8')).decode('ascii'))
        msg.attach(html_part)
        
        # Handle TLS vs SSL
        if smtp.get("use_tls", 1):
            print(f"[TEST EMAIL] Connecting SMTP (STARTTLS) to {smtp['host']}:{smtp.get('port', 587)}")
            server = smtplib.SMTP(smtp["host"], int(smtp.get("port", 587)), timeout=15)
            server.ehlo()
            server.starttls()
            server.ehlo()
        else:
            print(f"[TEST EMAIL] Connecting SMTP_SSL to {smtp['host']}:{smtp.get('port', 465)}")
            server = smtplib.SMTP_SSL(smtp["host"], int(smtp.get("port", 465)), timeout=15)
        
        print(f"[TEST EMAIL] Logging in as {smtp['username']}")
        server.login(smtp["username"], smtp["password"])
        print(f"[TEST EMAIL] Sending from {sender_email} to {to_email}")
        server.sendmail(sender_email, to_email, msg.as_string())
        server.quit()
        
        print(f"[TEST EMAIL] ✅ SUCCESS! Sent to {to_email}")
        return jsonify({"success": True, "message": f"Test sent to {to_email}"})
    except Exception as e:
        import traceback
        print(f"[TEST EMAIL] ❌ ERROR: {e}")
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/campaign/save", methods=["POST"])
@login_required
def api_campaign_save():
    """Save or update a campaign as draft."""
    from flask import jsonify
    from database import get_db, get_smtp_by_id
    
    data = request.get_json()
    campaign_id = data.get("id")
    email_id = data.get("email_id")
    smtp_ids = data.get("smtp_ids", [])
    if not smtp_ids and data.get("smtp_id"):
        smtp_ids = [data.get("smtp_id")]
    
    audience_type = data.get("audience_type", "all")
    include_tags = [int(t) for t in data.get("include_tags", []) if t]
    exclude_tags = [int(t) for t in data.get("exclude_tags", []) if t]
    campaign_name = data.get("name", "Campaign")
    scheduled_at = data.get("scheduled_at")
    email_delay = data.get("email_delay", 3)
    delay_max = data.get("delay_max", 0)
    test_inbox_interval = data.get("test_inbox_interval", 100)
    test_email = data.get("test_email", "")
    
    if not smtp_ids:
        return jsonify({"success": False, "error": "No SMTP servers selected"})
        
    email = get_email_by_id(int(email_id))
    if not email:
        return jsonify({"success": False, "error": "Email template not found"})
        
    # Build snapshots
    email_snapshot = json.dumps(email)
    
    smtp_list = []
    for sid in smtp_ids:
        s = get_smtp_by_id(int(sid))
        if s:
            smtp_list.append(s)
    smtp_snapshot = json.dumps(smtp_list)
    
    audience_snapshot = json.dumps({
        "type": audience_type,
        "include_tags": include_tags,
        "exclude_tags": exclude_tags
    })
    
    # Calculate recipient count for display
    contacts = get_audience_contacts_list(audience_type, include_tags, exclude_tags)
    total_recipients = len(contacts)
    
    campaign_data = {
        "name": campaign_name,
        "email_id": int(email_id),
        "smtp_id": int(smtp_ids[0]),
        "smtp_ids": json.dumps([int(s) for s in smtp_ids]),
        "audience_type": audience_type,
        "include_tags": json.dumps(include_tags) if include_tags else None,
        "exclude_tags": json.dumps(exclude_tags) if exclude_tags else None,
        "total_recipients": total_recipients,
        "scheduled_at": scheduled_at,
        "speed": "auto",
        "email_delay": email_delay,
        "delay_max": delay_max,
        "test_inbox_interval": test_inbox_interval,
        "test_email": test_email,
        "status": "draft",
        "email_snapshot": email_snapshot,
        "smtp_snapshot": smtp_snapshot,
        "audience_snapshot": audience_snapshot
    }
    
    if campaign_id:
        update_campaign(int(campaign_id), campaign_data)
        cid = int(campaign_id)
        add_campaign_log(cid, campaign_name, "save", f"Campaign updated ({total_recipients} recipients)")
    else:
        cid = create_campaign(campaign_data)
        add_campaign_log(cid, campaign_name, "save", f"Campaign created ({total_recipients} recipients)")
        
    return jsonify({"success": True, "campaign_id": cid, "recipients": total_recipients})


@app.route("/api/campaign/<int:campaign_id>", methods=["GET"])
@login_required
def api_campaign_get(campaign_id):
    """Get a single campaign's details for editing."""
    from flask import jsonify
    from database import get_campaign_by_id
    import json as _json

    campaign = get_campaign_by_id(campaign_id)
    if not campaign:
        return jsonify({"success": False, "error": "Campaign not found"})

    # Convert to dict and parse JSON fields
    c_dict = dict(campaign)
    
    # Parse arrays specifically for the form
    try:
        c_dict["smtp_ids"] = _json.loads(c_dict.get("smtp_ids") or "[]")
    except Exception:
        c_dict["smtp_ids"] = []
        if c_dict.get("smtp_id"):
            c_dict["smtp_ids"] = [c_dict["smtp_id"]]

    try:
        c_dict["include_tags"] = _json.loads(c_dict.get("include_tags") or "[]")
    except Exception:
        c_dict["include_tags"] = []

    try:
        c_dict["exclude_tags"] = _json.loads(c_dict.get("exclude_tags") or "[]")
    except Exception:
        c_dict["exclude_tags"] = []

    return jsonify({"success": True, "campaign": c_dict})


@app.route("/api/campaigns/list", methods=["GET"])
@login_required
def api_campaigns_list():
    """List all campaigns for dashboard."""
    from flask import jsonify
    import json as _json
    campaigns = get_all_campaigns()
    # Resolve SMTP names for each campaign
    for c in campaigns:
        smtp_names = []
        # Try snapshot first
        if c.get('smtp_snapshot'):
            try:
                snap = _json.loads(c['smtp_snapshot'])
                smtp_names = [s.get('name', s.get('host', '?')) for s in snap if s]
            except Exception:
                pass
        # Fallback to smtp_ids
        if not smtp_names and c.get('smtp_ids'):
            try:
                ids = _json.loads(c['smtp_ids'])
                for sid in ids:
                    s = get_smtp_by_id(int(sid))
                    if s:
                        smtp_names.append(s.get('name', s.get('host', '?')))
            except Exception:
                pass
        c['smtp_names'] = smtp_names
    return jsonify({"success": True, "campaigns": campaigns})


@app.route("/api/campaign/delete/<int:campaign_id>", methods=["DELETE"])
@login_required
def api_campaign_delete(campaign_id):
    """Delete a campaign."""
    from flask import jsonify, request
    from database import get_db, insert_campaign_history, get_smtp_by_id
    import json as _json
    
    campaign = get_campaign_by_id(campaign_id)
    cname = campaign.get("name", "?") if campaign else "?"
    
    source = request.args.get('source', '')
    
    if campaign and source == 'dashboard':
        # Dashboard delete: archive to history + hide from dashboard, but keep in DB
        smtp_names = []
        if campaign.get('smtp_snapshot'):
            try:
                snap = _json.loads(campaign['smtp_snapshot'])
                smtp_names = [s.get('name', s.get('host', '?')) for s in snap if s]
            except Exception:
                pass
        if not smtp_names and campaign.get('smtp_ids'):
            try:
                ids = _json.loads(campaign['smtp_ids'])
                for sid in ids:
                    s = get_smtp_by_id(int(sid))
                    if s:
                        smtp_names.append(s.get('name', s.get('host', '?')))
            except Exception:
                pass
        campaign['smtp_names'] = smtp_names
        insert_campaign_history(campaign)
        
        # Mark as archived instead of deleting
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("UPDATE campaigns SET status = 'archived' WHERE id = ?", (campaign_id,))
        conn.commit()
        conn.close()
        add_campaign_log(campaign_id, cname, "delete", "Campaign archived from dashboard")
        return jsonify({"success": True})
    
    # Send page delete: fully remove from DB
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM send_jobs WHERE campaign_id = ?", (campaign_id,))
    cursor.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
    conn.commit()
    conn.close()
    add_campaign_log(campaign_id, cname, "delete", "Campaign deleted")
    return jsonify({"success": True})


@app.route("/api/send/status/<int:campaign_id>")
@login_required
def api_send_status(campaign_id):
    """Get campaign status for live monitoring."""
    campaign = get_campaign_by_id(campaign_id)
    if not campaign:
        return {"error": "Campaign not found"}
    
    job_stats = get_campaign_job_stats(campaign_id)
    worker_status = get_worker_status()
    
    return {
        "id": campaign_id,
        "status": campaign.get("status"),
        "total_recipients": campaign.get("total_recipients", 0),
        "sent_count": campaign.get("sent_count", 0),
        "error_count": campaign.get("error_count", 0),
        "queued_count": job_stats.get("queued", 0),
        "current_rate": 3600 / max(worker_status.get("adaptive_delay", 2), 1),
        "logs": worker_status.get("logs", [])
    }


@app.route("/api/send/pause/<int:campaign_id>", methods=["POST"])
@login_required
def api_send_pause(campaign_id):
    """Pause a running campaign."""
    from flask import jsonify
    campaign = get_campaign_by_id(campaign_id)
    if not campaign:
        return jsonify({"success": False, "error": "Campaign not found"})
    
    update_campaign_status(campaign_id, "paused", "User requested pause")
    add_campaign_log(campaign_id, campaign.get("name", "?"), "pause", "Campaign paused")
    return jsonify({"success": True})


@app.route("/api/send/start/<int:campaign_id>", methods=["POST"])
@login_required
def api_send_start(campaign_id):
    """Start or resume a campaign."""
    from flask import jsonify
    from database import get_db
    campaign = get_campaign_by_id(campaign_id)
    if not campaign:
        return jsonify({"success": False, "error": "Campaign not found"})
        
    status = campaign.get("status")
    
    # If starting fresh or restarting a completed/stopped campaign
    if status in ["draft", "stopped", "completed"]:
        # Re-evaluate audience
        audience_type = campaign.get("audience_type", "all")
        include_tags = json.loads(campaign.get("include_tags") or "[]")
        exclude_tags = json.loads(campaign.get("exclude_tags") or "[]")
        
        contacts = get_audience_contacts_list(audience_type, include_tags, exclude_tags)
        contact_ids = [c["id"] for c in contacts]
        
        if not contact_ids:
            return jsonify({"success": False, "error": "No valid recipients found for this audience."})
            
        # Delete old jobs for this campaign
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM send_jobs WHERE campaign_id = ?", (campaign_id,))
        conn.commit()
        conn.close()
        
        # Reset stats
        update_campaign_stats(campaign_id, sent_count=0, error_count=0, bounce_count=0)
        
        # Create new jobs
        create_send_jobs_bulk(campaign_id, contact_ids)
        
        # Update total recipients
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("UPDATE campaigns SET total_recipients = ? WHERE id = ?", (len(contact_ids), campaign_id))
        conn.commit()
        conn.close()
    
    # Clear pause_code and set to sending
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE campaigns SET status = 'sending', pause_code = NULL, pause_reason = NULL WHERE id = ?", (campaign_id,))
    conn.commit()
    conn.close()
    add_campaign_log(campaign_id, campaign.get("name", "?"), "start", f"Campaign {'resumed' if status in ['paused', 'pending'] else 'started'} ({campaign.get('total_recipients', 0)} recipients)")
    return jsonify({"success": True})

@app.route("/historique")
@login_required
def route_historique():
    """Page pour afficher l'historique des campagnes supprimées."""
    from flask import render_template
    from database import get_campaign_history
    history = get_campaign_history()
    return render_template("historique.html", history=history)


@app.route("/api/campaign/history/clear", methods=["DELETE"])
@login_required
def api_campaign_history_clear():
    """Clear all campaign history records."""
    from flask import jsonify
    from database import get_db
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM campaign_history")
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/send/stop/<int:campaign_id>", methods=["POST"])
@login_required
def api_send_stop(campaign_id):
    """Stop a campaign permanently."""
    from flask import jsonify
    campaign = get_campaign_by_id(campaign_id)
    if not campaign:
        return jsonify({"success": False, "error": "Campaign not found"})
    
    update_campaign_status(campaign_id, "stopped", "User stopped campaign")
    add_campaign_log(campaign_id, campaign.get("name", "?"), "stop", "Campaign stopped")
    return jsonify({"success": True})


@app.route("/api/campaign/logs", methods=["GET"])
@login_required
def api_campaign_logs():
    """Get recent campaign activity logs."""
    from flask import jsonify
    logs = get_campaign_logs(limit=50)
    return jsonify({"success": True, "logs": logs})


@app.route("/api/campaign/logs/clear", methods=["DELETE"])
@login_required
def api_campaign_logs_clear():
    """Clear all campaign activity logs."""
    from flask import jsonify
    from database import get_db
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM campaign_logs")
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/api/campaigns/active")
@login_required
def api_campaigns_active():
    """Get the most recent active campaign (for dashboard polling)."""
    from flask import jsonify
    from database import get_db
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM campaigns 
        WHERE status IN ('sending', 'paused', 'queued')
        ORDER BY created_at DESC LIMIT 1
    """)
    campaign = cursor.fetchone()
    conn.close()
    
    if not campaign:
        return jsonify({})
    
    campaign = dict(campaign)
    job_stats = get_campaign_job_stats(campaign['id'])
    
    return jsonify({
        "id": campaign['id'],
        "name": campaign.get('name', 'Campaign'),
        "status": campaign.get('status'),
        "total_recipients": campaign.get('total_recipients', 0),
        "sent_count": job_stats.get('sent', 0),
        "error_count": job_stats.get('error', 0),
        "started_at": campaign.get('started_at'),
        "created_at": campaign.get('created_at')
    })


# Keep old production route for backwards compatibility
@app.route("/production")
@login_required
def production():
    return redirect(url_for("send_page"))


# =========================
# ROUTES - CAMPAIGNS (placeholder)
# =========================
@app.route("/campaigns", methods=["GET", "POST"])
@login_required
def campaigns():
    # TODO: Implémenter les campagnes
    return render_template("campaigns.html", campaigns=[])


# =========================
# ROUTES - WARMING (placeholder)
# =========================
@app.route("/warming")
@login_required
def warming():
    return render_template("warming.html")


# =========================
# ROUTES - OFFERS (placeholder)
# =========================
@app.route("/offers", methods=["GET", "POST"])
@login_required
def offers():
    if request.method == "POST":
        action = request.form.get("action")
        
        # DELETE
        if action == "delete":
            offer_id = request.form.get("offer_id")
            if offer_id:
                delete_offer(int(offer_id))
            return redirect("/offers")
        
        # SAVE (new or update)
        if action == "save":
            data = {
                "name": request.form.get("name", ""),
                "sponsor": request.form.get("sponsor", ""),
                "url": request.form.get("url", ""),
                "type": request.form.get("type", "CPA"),
                "notes": request.form.get("notes", "")
            }
            offer_id = request.form.get("offer_id")
            if offer_id:
                update_offer(int(offer_id), data)
            else:
                add_offer(data)
            return redirect("/offers")
    
    offers_list = get_all_offers()
    
    # Load offer for editing
    edit_id = request.args.get("edit")
    edit_offer = get_offer_by_id(int(edit_id)) if edit_id else None
    
    return render_template("offers.html", offers=offers_list, edit_offer=edit_offer)


# =========================


@app.route("/api/contact/<int:contact_id>/history")
@login_required
def api_contact_history(contact_id):
    """API pour récupérer l'historique d'emails envoyés à un contact."""
    from flask import jsonify
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Récupère les emails envoyés à ce contact depuis send_queue
    cursor.execute("""
        SELECT sq.*, e.name as email_name, e.subject, c.name as campaign_name
        FROM send_queue sq
        LEFT JOIN emails e ON e.id = sq.email_id
        LEFT JOIN campaigns c ON c.id = sq.campaign_id
        WHERE sq.contact_id = ?
        ORDER BY sq.created_at DESC
        LIMIT 50
    """, (contact_id,))
    
    rows = cursor.fetchall()
    conn.close()
    
    emails = []
    for row in rows:
        emails.append({
            "campaign": row["campaign_name"] or "-",
            "subject": row["subject"] or row["email_name"] or "-",
            "sent_at": row["sent_at"] or "-",
            "opened": row["is_opened"] == 1 if row["is_opened"] is not None else False
        })
    
    return jsonify({"emails": emails})


# =========================
# RUN
# =========================
if __name__ == "__main__":
    app.run(debug=True)
