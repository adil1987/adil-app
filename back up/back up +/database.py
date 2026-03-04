"""
Database Module - SQLite
========================
Gère toutes les opérations de base de données pour ADIL APP.
"""

import sqlite3
import os
from datetime import datetime

DATABASE_FILE = "data/app.db"


def get_db():
    """Connexion à la base de données SQLite."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row  # Pour accéder aux colonnes par nom
    return conn


def init_db():
    """Initialise la base de données avec toutes les tables."""
    
    # Créer le dossier data si nécessaire
    os.makedirs("data", exist_ok=True)
    
    conn = get_db()
    cursor = conn.cursor()
    
    # ===========================
    # TABLE: smtp_servers
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS smtp_servers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            host TEXT NOT NULL,
            port INTEGER DEFAULT 587,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            use_tls INTEGER DEFAULT 1,
            rate_limit INTEGER DEFAULT 100,
            daily_limit INTEGER DEFAULT 500,
            sent_today INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            warmup_level INTEGER DEFAULT 1,
            last_used_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ===========================
    # TABLE: contacts
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            prenom TEXT,
            nom TEXT,
            status TEXT DEFAULT 'active',
            source TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # status: 'active', 'unsubscribed', 'bounced'
    
    # ===========================
    # TABLE: emails (templates)
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            subject TEXT NOT NULL,
            from_name TEXT,
            reply_to TEXT,
            mode TEXT DEFAULT 'text',
            body TEXT,
            text_fallback TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ===========================
    # TABLE: campaigns
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email_id INTEGER,
            status TEXT DEFAULT 'draft',
            scheduled_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            total_contacts INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            bounce_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (email_id) REFERENCES emails(id)
        )
    """)
    # status: 'draft', 'scheduled', 'running', 'paused', 'completed'
    
    # ===========================
    # TABLE: send_queue
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS send_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER,
            contact_id INTEGER,
            smtp_id INTEGER,
            status TEXT DEFAULT 'pending',
            scheduled_at TEXT,
            sent_at TEXT,
            error_message TEXT,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
            FOREIGN KEY (contact_id) REFERENCES contacts(id),
            FOREIGN KEY (smtp_id) REFERENCES smtp_servers(id)
        )
    """)
    # status: 'pending', 'sending', 'sent', 'failed', 'bounced'
    
    # ===========================
    # TABLE: settings
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    
    # ===========================
    # TABLE: test_emails
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            provider TEXT,
            last_result TEXT,
            last_tested_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # last_result: 'inbox', 'spam', 'bounce', 'unknown'
    
    # ===========================
    # TABLE: stats_daily
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stats_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE NOT NULL,
            sent INTEGER DEFAULT 0,
            delivered INTEGER DEFAULT 0,
            bounced INTEGER DEFAULT 0,
            opened INTEGER DEFAULT 0,
            clicked INTEGER DEFAULT 0,
            unsubscribed INTEGER DEFAULT 0
        )
    """)
    
    conn.commit()
    conn.close()
    
    print("✅ Base de données initialisée avec succès!")


# ===========================
# HELPERS - Settings
# ===========================
def get_setting(key, default=None):
    """Récupère une valeur de settings."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key, value):
    """Définit une valeur de settings."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)
    """, (key, value))
    conn.commit()
    conn.close()


# ===========================
# HELPERS - SMTP Servers
# ===========================
def get_all_smtp():
    """Récupère tous les serveurs SMTP."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM smtp_servers ORDER BY id")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_smtp_by_id(smtp_id):
    """Récupère un serveur SMTP par ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM smtp_servers WHERE id = ?", (smtp_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def add_smtp(data):
    """Ajoute un nouveau serveur SMTP."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO smtp_servers (name, host, port, username, password, use_tls, rate_limit, daily_limit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("name"),
        data.get("host"),
        data.get("port", 587),
        data.get("username"),
        data.get("password"),
        1 if data.get("use_tls", True) else 0,
        data.get("rate_limit", 100),
        data.get("daily_limit", 500)
    ))
    conn.commit()
    smtp_id = cursor.lastrowid
    conn.close()
    return smtp_id


def update_smtp(smtp_id, data):
    """Met à jour un serveur SMTP."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE smtp_servers 
        SET name=?, host=?, port=?, username=?, password=?, use_tls=?, rate_limit=?, daily_limit=?, is_active=?
        WHERE id=?
    """, (
        data.get("name"),
        data.get("host"),
        data.get("port"),
        data.get("username"),
        data.get("password"),
        1 if data.get("use_tls", True) else 0,
        data.get("rate_limit"),
        data.get("daily_limit"),
        1 if data.get("is_active", True) else 0,
        smtp_id
    ))
    conn.commit()
    conn.close()


def delete_smtp(smtp_id):
    """Supprime un serveur SMTP."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM smtp_servers WHERE id = ?", (smtp_id,))
    conn.commit()
    conn.close()


# ===========================
# HELPERS - Contacts
# ===========================
def get_all_contacts(status=None, limit=100, offset=0):
    """Récupère les contacts avec pagination."""
    conn = get_db()
    cursor = conn.cursor()
    
    if status:
        cursor.execute(
            "SELECT * FROM contacts WHERE status = ? ORDER BY id LIMIT ? OFFSET ?",
            (status, limit, offset)
        )
    else:
        cursor.execute(
            "SELECT * FROM contacts ORDER BY id LIMIT ? OFFSET ?",
            (limit, offset)
        )
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def count_contacts(status=None):
    """Compte le nombre de contacts."""
    conn = get_db()
    cursor = conn.cursor()
    
    if status:
        cursor.execute("SELECT COUNT(*) as count FROM contacts WHERE status = ?", (status,))
    else:
        cursor.execute("SELECT COUNT(*) as count FROM contacts")
    
    row = cursor.fetchone()
    conn.close()
    return row["count"]


def add_contact(email, prenom=None, nom=None, source=None):
    """Ajoute un contact."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO contacts (email, prenom, nom, source) VALUES (?, ?, ?, ?)
        """, (email, prenom, nom, source))
        conn.commit()
        contact_id = cursor.lastrowid
    except sqlite3.IntegrityError:
        contact_id = None  # Email déjà existant
    conn.close()
    return contact_id


def update_contact_status(email, status):
    """Met à jour le statut d'un contact."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE contacts SET status = ?, updated_at = ? WHERE email = ?
    """, (status, datetime.now().isoformat(), email))
    conn.commit()
    conn.close()


def delete_contact(contact_id):
    """Supprime un contact par ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
    conn.commit()
    conn.close()


def delete_contacts_bulk(contact_ids):
    """Supprime plusieurs contacts par IDs."""
    if not contact_ids:
        return 0
    conn = get_db()
    cursor = conn.cursor()
    placeholders = ",".join("?" * len(contact_ids))
    cursor.execute(f"DELETE FROM contacts WHERE id IN ({placeholders})", contact_ids)
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def update_contacts_status_bulk(contact_ids, status):
    """Met à jour le statut de plusieurs contacts."""
    if not contact_ids:
        return 0
    conn = get_db()
    cursor = conn.cursor()
    placeholders = ",".join("?" * len(contact_ids))
    cursor.execute(
        f"UPDATE contacts SET status = ?, updated_at = ? WHERE id IN ({placeholders})",
        [status, datetime.now().isoformat()] + contact_ids
    )
    updated = cursor.rowcount
    conn.commit()
    conn.close()
    return updated


def add_contacts_bulk(contacts, source="import", skip_duplicates=True):
    """Ajoute plusieurs contacts en une fois.
    
    contacts: liste de dicts avec 'email', 'prenom', 'nom'
    Retourne (added_count, skipped_count)
    """
    conn = get_db()
    cursor = conn.cursor()
    added = 0
    skipped = 0
    
    for c in contacts:
        email = c.get("email", "").strip().lower()
        if not email or "@" not in email:
            skipped += 1
            continue
        
        try:
            cursor.execute("""
                INSERT INTO contacts (email, prenom, nom, source) VALUES (?, ?, ?, ?)
            """, (email, c.get("prenom", ""), c.get("nom", ""), source))
            added += 1
        except Exception:
            if skip_duplicates:
                skipped += 1
            else:
                # Update existing
                cursor.execute("""
                    UPDATE contacts SET prenom = ?, nom = ?, updated_at = ? WHERE email = ?
                """, (c.get("prenom", ""), c.get("nom", ""), datetime.now().isoformat(), email))
                added += 1
    
    conn.commit()
    conn.close()
    return added, skipped


# ===========================
# HELPERS - Emails (Templates)
# ===========================
def get_all_emails():
    """Récupère tous les templates d'emails."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM emails ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_email_by_id(email_id):
    """Récupère un template d'email par ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM emails WHERE id = ?", (email_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def add_email(data):
    """Ajoute un template d'email."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO emails (name, subject, from_name, reply_to, mode, body, text_fallback)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("name"),
        data.get("subject"),
        data.get("from_name"),
        data.get("reply_to"),
        data.get("mode", "text"),
        data.get("body"),
        data.get("text_fallback")
    ))
    conn.commit()
    email_id = cursor.lastrowid
    conn.close()
    return email_id


def update_email(email_id, data):
    """Met à jour un template d'email."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE emails 
        SET name=?, subject=?, from_name=?, reply_to=?, mode=?, body=?, text_fallback=?, updated_at=?
        WHERE id=?
    """, (
        data.get("name"),
        data.get("subject"),
        data.get("from_name"),
        data.get("reply_to"),
        data.get("mode"),
        data.get("body"),
        data.get("text_fallback"),
        datetime.now().isoformat(),
        email_id
    ))
    conn.commit()
    conn.close()


def delete_email(email_id):
    """Supprime un template d'email."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM emails WHERE id = ?", (email_id,))
    conn.commit()
    conn.close()


# ===========================
# HELPERS - Test Emails
# ===========================
def get_all_test_emails():
    """Récupère tous les emails de test."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test_emails ORDER BY id")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_test_email(email, provider=None):
    """Ajoute un email de test."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO test_emails (email, provider) VALUES (?, ?)
        """, (email, provider))
        conn.commit()
        test_id = cursor.lastrowid
    except sqlite3.IntegrityError:
        test_id = None
    conn.close()
    return test_id


def delete_test_email(test_id):
    """Supprime un email de test."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM test_emails WHERE id = ?", (test_id,))
    conn.commit()
    conn.close()


# ===========================
# HELPERS - Stats
# ===========================
def get_stats_summary():
    """Récupère un résumé des stats."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Stats globales depuis send_queue
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN status = 'bounced' THEN 1 ELSE 0 END) as bounced
        FROM send_queue
    """)
    row = cursor.fetchone()
    
    stats = {
        "total": row["total"] or 0,
        "sent": row["sent"] or 0,
        "failed": row["failed"] or 0,
        "bounced": row["bounced"] or 0,
        "delivered": (row["sent"] or 0) - (row["bounced"] or 0)
    }
    
    # Contacts stats
    cursor.execute("SELECT COUNT(*) as count FROM contacts WHERE status = 'active'")
    stats["active_contacts"] = cursor.fetchone()["count"]
    
    cursor.execute("SELECT COUNT(*) as count FROM contacts WHERE status = 'unsubscribed'")
    stats["unsubscribed"] = cursor.fetchone()["count"]
    
    conn.close()
    return stats


def get_stats_last_days(days=7):
    """Récupère les stats des X derniers jours."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM stats_daily 
        ORDER BY date DESC 
        LIMIT ?
    """, (days,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in reversed(rows)]


# ===========================
# Initialisation au démarrage
# ===========================
if __name__ == "__main__":
    init_db()
