"""
ADIL APP - Email Sender Application
====================================
Application d'envoi d'emails en masse avec multi-SMTP et rotation.
"""

from flask import Flask, render_template, request, redirect, url_for, flash
from datetime import datetime

# Import des fonctions database
from database import (
    init_db,
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
    add_contact,
    update_contact_status,
    delete_contact,
    delete_contacts_bulk,
    update_contacts_status_bulk,
    add_contacts_bulk,
    # Settings
    get_setting,
    set_setting,
    # Test emails
    get_all_test_emails,
    add_test_email,
    delete_test_email,
)

# =========================
# APP
# =========================
app = Flask(__name__)
app.secret_key = "adil_app_secret_key_2024"

# Initialiser la base de données au démarrage
init_db()

# =========================
# ROUTES - DASHBOARD
# =========================
@app.route("/")
def dashboard():
    stats = get_stats_summary()
    daily_stats = get_stats_last_days(7)
    return render_template("dashboard.html", stats=stats, daily_stats=daily_stats)


# =========================
# ROUTES - EMAILS (Templates)
# =========================
@app.route("/emails", methods=["GET", "POST"])
def emails():
    emails_list = get_all_emails()
    
    # Formulaire par défaut
    form_data = {
        "id": None,
        "name": "",
        "subject": "",
        "from_name": "",
        "reply_to": "",
        "mode": "text",
        "body": "",
        "text_fallback": ""
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
                "reply_to": email["reply_to"] or "",
                "mode": email["mode"] or "text",
                "body": email["body"] or "",
                "text_fallback": email["text_fallback"] or ""
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
                        "reply_to": original["reply_to"],
                        "mode": original["mode"],
                        "body": original["body"],
                        "text_fallback": original["text_fallback"]
                    })
            return redirect("/emails")
        
        # Lire le formulaire
        form_data = {
            "id": request.form.get("email_id"),
            "name": request.form.get("name", ""),
            "subject": request.form.get("subject", ""),
            "from_name": request.form.get("from_name", ""),
            "reply_to": request.form.get("reply_to", ""),
            "mode": request.form.get("mode", "text"),
            "body": request.form.get("body", ""),
            "text_fallback": request.form.get("text_fallback", "")
        }
        
        # SAVE (nouveau) ou UPDATE (existant)
        if action == "save":
            email_id = request.form.get("email_id")
            if email_id:
                # Update existant
                update_email(int(email_id), form_data)
            else:
                # Nouveau
                add_email(form_data)
            return redirect("/emails")
        
        # SAVE AND SEND TEST
        if action == "save_and_test":
            email_id = request.form.get("email_id")
            if email_id:
                update_email(int(email_id), form_data)
                saved_id = email_id
            else:
                saved_id = add_email(form_data)
            # Redirect to production with test mode
            return redirect(f"/production?test_email_id={saved_id}")
    
    return render_template(
        "emails.html",
        emails=emails_list,
        form_data=form_data
    )


# =========================
# ROUTES - SMTP
# =========================
@app.route("/smtp", methods=["GET", "POST"])
def smtp():
    if request.method == "POST":
        action = request.form.get("action")
        
        # DELETE
        if action == "delete":
            smtp_id = request.form.get("smtp_id")
            if smtp_id:
                delete_smtp(int(smtp_id))
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
            "is_active": request.form.get("is_active") == "on"
        }
        
        smtp_id = request.form.get("smtp_id")
        if smtp_id:
            update_smtp(int(smtp_id), data)
        else:
            add_smtp(data)
        
        return redirect("/smtp")
    
    smtp_list = get_all_smtp()
    return render_template("smtp.html", smtp_list=smtp_list)


# =========================
# ROUTES - CONTACTS
# =========================
@app.route("/contacts", methods=["GET", "POST"])
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
                    
                    # Import to database
                    added, skipped = add_contacts_bulk(parsed, source=file.filename, skip_duplicates=skip_duplicates)
                    
                    import_result = {
                        "status": "success",
                        "title": "Import successful!",
                        "message": f"{added} contacts added, {skipped} skipped (duplicates or invalid)"
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
            return redirect(f"/contacts?status={request.args.get('status', 'active')}")
        
        # BULK DELETE
        if action == "bulk_delete":
            ids = request.form.get("ids", "")
            if ids:
                id_list = [int(i) for i in ids.split(",") if i.isdigit()]
                delete_contacts_bulk(id_list)
            return redirect(f"/contacts?status={request.args.get('status', 'active')}")
        
        # BULK UNSUBSCRIBE
        if action == "bulk_unsubscribe":
            ids = request.form.get("ids", "")
            if ids:
                id_list = [int(i) for i in ids.split(",") if i.isdigit()]
                update_contacts_status_bulk(id_list, "unsubscribed")
            return redirect("/contacts?status=unsubscribed")
    
    # GET - Display contacts
    page = request.args.get("page", 1, type=int)
    status_filter = request.args.get("status", "active")
    per_page = 50
    
    contacts_list = get_all_contacts(
        status=status_filter,
        limit=per_page,
        offset=(page - 1) * per_page
    )
    
    total = count_contacts(status=status_filter)
    total_pages = (total + per_page - 1) // per_page
    
    counts = {
        "active": count_contacts("active"),
        "unsubscribed": count_contacts("unsubscribed"),
        "bounced": count_contacts("bounced"),
        "total": count_contacts()
    }
    
    return render_template(
        "contacts.html",
        contacts=contacts_list,
        counts=counts,
        current_status=status_filter,
        page=page,
        total_pages=total_pages,
        import_result=import_result
    )


# =========================
# ROUTES - SETTINGS
# =========================
@app.route("/settings", methods=["GET", "POST"])
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
        set_setting("sender_name", request.form.get("sender_name", ""))
        set_setting("sender_email", request.form.get("sender_email", ""))
        set_setting("reply_to", request.form.get("reply_to", ""))
        set_setting("global_rate", request.form.get("global_rate", "100"))
        
        return redirect("/settings")
    
    settings_data = {
        "app_name": get_setting("app_name", "ADIL APP"),
        "sender_name": get_setting("sender_name", ""),
        "sender_email": get_setting("sender_email", ""),
        "reply_to": get_setting("reply_to", ""),
        "global_rate": get_setting("global_rate", "100")
    }
    
    test_emails = get_all_test_emails()
    
    return render_template(
        "settings.html",
        settings=settings_data,
        test_emails=test_emails
    )


# =========================
# ROUTES - PRODUCTION
# =========================
@app.route("/production", methods=["GET", "POST"])
def production():
    emails_list = get_all_emails()
    smtp_list = get_all_smtp()
    
    # TODO: Charger les campagnes en cours
    productions = []
    
    if request.method == "POST":
        action = request.form.get("action")
        
        # Pour l'instant, simulation
        if action == "test":
            test_email = request.form.get("test_email")
            return render_template(
                "production.html",
                emails=emails_list,
                smtp_list=smtp_list,
                productions=productions,
                test_result=f"Email de test simulé vers {test_email}"
            )
    
    return render_template(
        "production.html",
        emails=emails_list,
        smtp_list=smtp_list,
        productions=productions
    )


# =========================
# ROUTES - CAMPAIGNS (placeholder)
# =========================
@app.route("/campaigns", methods=["GET", "POST"])
def campaigns():
    # TODO: Implémenter les campagnes
    return render_template("campaigns.html", campaigns=[])


# =========================
# ROUTES - WARMING (placeholder)
# =========================
@app.route("/warming")
def warming():
    return render_template("warming.html")


# =========================
# ROUTES - OFFERS (placeholder)
# =========================
@app.route("/offers")
def offers():
    return render_template("offers.html")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    app.run(debug=True)
