"""
Database Module - SQLite
========================
Gère toutes les opérations de base de données pour ADIL APP.
"""

import sqlite3
import os
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

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
            from_email TEXT DEFAULT '',
            rate_limit INTEGER DEFAULT 100,
            daily_limit INTEGER DEFAULT 500,
            sent_today INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            warmup_level INTEGER DEFAULT 1,
            priority INTEGER DEFAULT 1,
            domain TEXT,
            ip_address TEXT,
            -- New: Sending configuration
            sending_domain TEXT,
            return_path_domain TEXT,
            dkim_selector TEXT,
            tracking_url TEXT,
            dns_status TEXT,
            -- New: Safeguards
            auto_pause_on_error INTEGER DEFAULT 1,
            max_bounce_rate REAL DEFAULT 5.0,
            max_spam_rate REAL DEFAULT 0.1,
            pause_reason TEXT,
            max_connections INTEGER DEFAULT 1,
            -- New: Warmup
            warmup_enabled INTEGER DEFAULT 1,
            warmup_start_date TEXT,
            -- New: Metrics
            total_sent INTEGER DEFAULT 0,
            total_bounced INTEGER DEFAULT 0,
            total_spam_complaints INTEGER DEFAULT 0,
            temp_error_count INTEGER DEFAULT 0,
            reputation_score REAL DEFAULT 100.0,
            -- Timestamps
            last_used_at TEXT,
            last_error TEXT,
            error_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Migrate smtp_servers: add from_email column if missing
    try:
        cursor.execute("ALTER TABLE smtp_servers ADD COLUMN from_email TEXT DEFAULT ''")
    except Exception:
        pass
    
    # Migrate smtp_servers: add bounce columns if missing
    try:
        cursor.execute("ALTER TABLE smtp_servers ADD COLUMN bounce_email TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        cursor.execute("ALTER TABLE smtp_servers ADD COLUMN bounce_password TEXT DEFAULT ''")
    except Exception:
        pass
        
    # Migrate smtp_servers: add max_connections column if missing
    try:
        cursor.execute("ALTER TABLE smtp_servers ADD COLUMN max_connections INTEGER DEFAULT 1")
    except Exception:
        pass
    
    # ===========================
    # TABLE: bounce_logs
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bounce_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            smtp_id INTEGER,
            bounced_email TEXT NOT NULL,
            bounce_type TEXT DEFAULT 'hard',
            bounce_code TEXT,
            bounce_reason TEXT,
            original_subject TEXT,
            raw_content TEXT,
            action_taken TEXT DEFAULT 'pending',
            processed_at TEXT DEFAULT CURRENT_TIMESTAMP
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
            -- New: Advanced options
            offer_id INTEGER,
            format TEXT DEFAULT 'multipart/alternative',
            charset TEXT DEFAULT 'UTF-8',
            transfer_encoding TEXT DEFAULT 'quoted-printable',
            from_email TEXT,
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
    
    # ===========================
    # TABLE: tags
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ===========================
    # TABLE: campaign_history
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaign_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_id INTEGER,
            name TEXT NOT NULL,
            created_at TEXT,
            deleted_at TEXT DEFAULT CURRENT_TIMESTAMP,
            status TEXT,
            smtp_names TEXT,
            total_recipients INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            open_count INTEGER DEFAULT 0,
            click_count INTEGER DEFAULT 0
        )
    """)
    
    # ===========================
    # TABLE: contact_tags (junction)
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contact_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE,
            UNIQUE(contact_id, tag_id)
        )
    """)
    
    # ===========================
    # TABLE: campaigns (production send)
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email_id INTEGER NOT NULL,
            smtp_id INTEGER NOT NULL,
            smtp_ids TEXT,
            -- Audience
            audience_type TEXT NOT NULL,
            include_tags TEXT,
            exclude_tags TEXT,
            total_recipients INTEGER DEFAULT 0,
            -- Timing
            scheduled_at TEXT,
            speed TEXT DEFAULT 'auto',
            email_delay INTEGER DEFAULT 3,
            delay_max INTEGER DEFAULT 0,
            test_inbox_interval INTEGER DEFAULT 100,
            test_email TEXT,
            -- Status
            status TEXT DEFAULT 'draft',
            pause_reason TEXT,
            pause_code TEXT,
            -- Stats
            sent_count INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            bounce_count INTEGER DEFAULT 0,
            open_count INTEGER DEFAULT 0,
            click_count INTEGER DEFAULT 0,
            started_at TEXT,
            completed_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (email_id) REFERENCES emails(id),
            FOREIGN KEY (smtp_id) REFERENCES smtp_servers(id)
        )
    """)
    
    # ===========================
    # TABLE: send_jobs
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS send_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER NOT NULL,
            contact_id INTEGER NOT NULL,
            status TEXT DEFAULT 'queued',
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            queued_at TEXT DEFAULT CURRENT_TIMESTAMP,
            sent_at TEXT,
            opened INTEGER DEFAULT 0,
            clicked INTEGER DEFAULT 0,
            clicked_at TEXT,
            is_bot INTEGER DEFAULT 0,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
            FOREIGN KEY (contact_id) REFERENCES contacts(id)
        )
    """)
    
    # Migrate campaigns table: add new columns if missing
    for col, coldef in [
        ("smtp_id", "INTEGER DEFAULT 0"),
        ("smtp_ids", "TEXT"),
        ("audience_type", "TEXT DEFAULT 'all'"),
        ("include_tags", "TEXT"),
        ("exclude_tags", "TEXT"),
        ("total_recipients", "INTEGER DEFAULT 0"),
        ("speed", "TEXT DEFAULT 'auto'"),
        ("email_delay", "INTEGER DEFAULT 3"),
        ("delay_max", "INTEGER DEFAULT 0"),
        ("test_inbox_interval", "INTEGER DEFAULT 100"),
        ("pause_reason", "TEXT"),
        ("error_count", "INTEGER DEFAULT 0"),
        ("email_snapshot", "TEXT"),
        ("smtp_snapshot", "TEXT"),
        ("audience_snapshot", "TEXT"),
        ("pause_code", "TEXT"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE campaigns ADD COLUMN {col} {coldef}")
        except Exception:
            pass  # Column already exists
    # ===========================
    # TABLE: campaign_logs (activity journal)
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaign_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER,
            campaign_name TEXT,
            action TEXT NOT NULL,
            detail TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ===========================
    # TABLE: blacklist (global, non-modifiable)
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blacklist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            reason TEXT NOT NULL,
            source_campaign_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ===========================
    # TABLE: offers
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sponsor TEXT,
            url TEXT NOT NULL,
            type TEXT DEFAULT 'CPA',
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ===========================
    # TABLE: test_emails
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            provider TEXT DEFAULT '',
            last_result TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ===========================
    # TABLE: users
    # ===========================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    
    # Clean up orphan contact_tags (contacts that no longer exist)
    cursor.execute("""
        DELETE FROM contact_tags 
        WHERE contact_id NOT IN (SELECT id FROM contacts)
    """)
    if cursor.rowcount > 0:
        print(f"🧹 Cleaned {cursor.rowcount} orphan contact_tags entries")
        conn.commit()
    
    conn.close()
    
    # Migrate smtp_servers table (add new columns if missing)
    _migrate_smtp_columns()
    
    # Migrate campaigns table
    _migrate_campaign_columns()
    
    # Migrate send_jobs table
    _migrate_send_jobs_columns()
    
    # Migrate campaign_history table (add open_count, click_count)
    try:
        conn2 = get_db()
        c2 = conn2.cursor()
        for col, coldef in [("open_count", "INTEGER DEFAULT 0"), ("click_count", "INTEGER DEFAULT 0")]:
            try:
                c2.execute(f"ALTER TABLE campaign_history ADD COLUMN {col} {coldef}")
            except Exception:
                pass
        conn2.commit()
        conn2.close()
    except Exception:
        pass
    
    # Migrate emails table (add new columns if missing)
    _migrate_email_columns()
    
    # Init default user
    init_admin_user()
    
    print("✅ Base de données initialisée avec succès!")


def _migrate_smtp_columns():
    """Add new columns to smtp_servers if they don't exist."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get existing columns
    cursor.execute("PRAGMA table_info(smtp_servers)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    
    new_cols = [
        ("priority", "INTEGER DEFAULT 1"),
        ("domain", "TEXT"),
        ("ip_address", "TEXT"),
        ("last_error", "TEXT"),
        ("error_count", "INTEGER DEFAULT 0"),
        # Sending configuration
        ("sending_domain", "TEXT"),
        ("return_path_domain", "TEXT"),
        # Safeguards
        ("auto_pause_on_error", "INTEGER DEFAULT 1"),
        ("max_bounce_rate", "REAL DEFAULT 5.0"),
        ("max_spam_rate", "REAL DEFAULT 0.1"),
        ("pause_reason", "TEXT"),
        # Warmup
        ("warmup_enabled", "INTEGER DEFAULT 1"),
        ("warmup_start_date", "TEXT"),
        # Metrics
        ("sent_today_date", "TEXT"),
        ("total_sent", "INTEGER DEFAULT 0"),
        ("total_bounced", "INTEGER DEFAULT 0"),
        ("total_spam_complaints", "INTEGER DEFAULT 0"),
        ("temp_error_count", "INTEGER DEFAULT 0"),
        ("reputation_score", "REAL DEFAULT 100.0"),
        # DKIM/DNS
        ("dkim_selector", "TEXT"),
        ("tracking_url", "TEXT"),
        ("dns_status", "TEXT"),
    ]
    
    for col_name, col_type in new_cols:
        if col_name not in existing_cols:
            cursor.execute(f"ALTER TABLE smtp_servers ADD COLUMN {col_name} {col_type}")
            print(f"  + Added column {col_name} to smtp_servers")
    
    conn.commit()
    conn.close()


def _migrate_email_columns():
    """Add new columns to emails table if they don't exist."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get existing columns
    cursor.execute("PRAGMA table_info(emails)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    
    new_cols = [
        ("offer_id", "INTEGER"),
        ("format", "TEXT DEFAULT 'multipart/alternative'"),
        ("charset", "TEXT DEFAULT 'UTF-8'"),
        ("transfer_encoding", "TEXT DEFAULT 'quoted-printable'"),
        ("from_email", "TEXT"),
        ("footer", "TEXT"),
    ]
    
    for col_name, col_type in new_cols:
        if col_name not in existing_cols:
            cursor.execute(f"ALTER TABLE emails ADD COLUMN {col_name} {col_type}")
            print(f"  + Added column {col_name} to emails")
    
    conn.commit()
    conn.close()


def _migrate_campaign_columns():
    """Add new columns to campaigns if they don't exist."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get existing columns
    cursor.execute("PRAGMA table_info(campaigns)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    new_cols = [
        ("delay_max", "INTEGER DEFAULT 0"),
        ("test_email", "TEXT"),
        ("open_count", "INTEGER DEFAULT 0"),
        ("click_count", "INTEGER DEFAULT 0"),
        ("pause_code", "TEXT"),
    ]

    for col_name, col_type in new_cols:
        if col_name not in existing_cols:
            try:
                cursor.execute(f"ALTER TABLE campaigns ADD COLUMN {col_name} {col_type}")
                print(f"  + Added column {col_name} to campaigns")
            except Exception as e:
                print(f"  - Failed to add column {col_name} to campaigns: {e}")
    
    conn.commit()
    conn.close()

def _migrate_send_jobs_columns():
    """Add new columns to send_jobs if they don't exist."""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("ALTER TABLE send_jobs ADD COLUMN opened INTEGER DEFAULT 0")
    except Exception:
        pass
    
    try:
        cursor.execute("ALTER TABLE send_jobs ADD COLUMN clicked INTEGER DEFAULT 0")
    except Exception:
        pass
    
    try:
        cursor.execute("ALTER TABLE send_jobs ADD COLUMN clicked_at TEXT")
    except Exception:
        pass
    
    try:
        cursor.execute("ALTER TABLE send_jobs ADD COLUMN is_bot INTEGER DEFAULT 0")
    except Exception:
        pass
        
    conn.commit()
    conn.close()


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
    
    # Check for midnight resets
    today_str = datetime.today().isoformat()[:10]
    results = []
    
    for row in rows:
        d = dict(row)
        if d.get('sent_today_date') != today_str:
            d['sent_today'] = 0
            d['sent_today_date'] = today_str
            cursor.execute("UPDATE smtp_servers SET sent_today = 0, sent_today_date = ? WHERE id = ?", (today_str, d['id']))
        results.append(d)
        
    conn.commit()
    conn.close()
    return results


def get_smtp_by_id(smtp_id):
    """Récupère un serveur SMTP par ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM smtp_servers WHERE id = ?", (smtp_id,))
    row = cursor.fetchone()
    
    if row:
        d = dict(row)
        today_str = datetime.today().isoformat()[:10]
        if d.get('sent_today_date') != today_str:
            d['sent_today'] = 0
            d['sent_today_date'] = today_str
            cursor.execute("UPDATE smtp_servers SET sent_today = 0, sent_today_date = ? WHERE id = ?", (today_str, d['id']))
            conn.commit()
    else:
        d = None
        
    conn.close()
    return d


def add_smtp(data):
    """Ajoute un nouveau serveur SMTP."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO smtp_servers (
            name, host, port, username, password, use_tls, from_email, rate_limit, daily_limit, 
            priority, domain, ip_address, sending_domain, return_path_domain, dkim_selector,
            auto_pause_on_error, max_bounce_rate, max_spam_rate, warmup_enabled,
            bounce_email, bounce_password, max_connections
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("name"),
        data.get("host"),
        data.get("port", 587),
        data.get("username"),
        data.get("password"),
        1 if data.get("use_tls", True) else 0,
        data.get("from_email", ""),
        data.get("rate_limit", 100),
        data.get("daily_limit", 500),
        data.get("priority", 1),
        data.get("domain", ""),
        data.get("ip_address", ""),
        data.get("sending_domain", ""),
        data.get("return_path_domain", ""),
        data.get("dkim_selector", ""),
        1 if data.get("auto_pause_on_error", True) else 0,
        data.get("max_bounce_rate", 5.0),
        data.get("max_spam_rate", 0.1),
        1 if data.get("warmup_enabled", True) else 0,
        data.get("bounce_email", ""),
        data.get("bounce_password", ""),
        data.get("max_connections", 1)
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
        SET name=?, host=?, port=?, username=?, password=?, use_tls=?, from_email=?, rate_limit=?, daily_limit=?, 
            is_active=?, priority=?, domain=?, ip_address=?, sending_domain=?, return_path_domain=?, dkim_selector=?,
            auto_pause_on_error=?, max_bounce_rate=?, max_spam_rate=?, warmup_enabled=?,
            bounce_email=?, bounce_password=?, max_connections=?
        WHERE id=?
    """, (
        data.get("name"),
        data.get("host"),
        data.get("port"),
        data.get("username"),
        data.get("password"),
        1 if data.get("use_tls", True) else 0,
        data.get("from_email", ""),
        data.get("rate_limit"),
        data.get("daily_limit"),
        1 if data.get("is_active", True) else 0,
        data.get("priority", 1),
        data.get("domain", ""),
        data.get("ip_address", ""),
        data.get("sending_domain", ""),
        data.get("return_path_domain", ""),
        data.get("dkim_selector", ""),
        1 if data.get("auto_pause_on_error", True) else 0,
        data.get("max_bounce_rate", 5.0),
        data.get("max_spam_rate", 0.1),
        1 if data.get("warmup_enabled", True) else 0,
        data.get("bounce_email", ""),
        data.get("bounce_password", ""),
        data.get("max_connections", 1),
        smtp_id
    ))
    conn.commit()
    conn.close()


def increment_smtp_stats(smtp_id, is_bounce=False):
    """Incrémente les statistiques d'envoi ou de rebond d'un serveur SMTP."""
    conn = get_db()
    cursor = conn.cursor()
    
    today_str = datetime.today().isoformat()[:10]
    
    if is_bounce:
        cursor.execute("UPDATE smtp_servers SET total_bounced = coalesce(total_bounced, 0) + 1 WHERE id = ?", (smtp_id,))
    else:
        cursor.execute("""
            UPDATE smtp_servers 
            SET sent_today = coalesce(sent_today, 0) + 1, 
                total_sent = coalesce(total_sent, 0) + 1,
                sent_today_date = ?
            WHERE id = ?
        """, (today_str, smtp_id))
        
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
# HELPERS - Bounce Logs
# ===========================

def add_bounce_log(smtp_id, bounced_email, bounce_type='hard', bounce_code='', bounce_reason='', original_subject='', raw_content='', action_taken='blacklisted'):
    """Add a bounce log entry."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO bounce_logs (smtp_id, bounced_email, bounce_type, bounce_code, bounce_reason, original_subject, raw_content, action_taken)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (smtp_id, bounced_email, bounce_type, bounce_code, bounce_reason, original_subject, raw_content, action_taken))
    conn.commit()
    conn.close()


def get_bounce_logs(limit=100, offset=0):
    """Get recent bounce logs."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT bl.*, ss.name as smtp_name 
        FROM bounce_logs bl 
        LEFT JOIN smtp_servers ss ON bl.smtp_id = ss.id
        ORDER BY bl.processed_at DESC 
        LIMIT ? OFFSET ?
    """, (limit, offset))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_bounce_stats():
    """Get bounce statistics per SMTP."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT smtp_id, 
               COUNT(*) as total,
               SUM(CASE WHEN bounce_type='hard' THEN 1 ELSE 0 END) as hard,
               SUM(CASE WHEN bounce_type='soft' THEN 1 ELSE 0 END) as soft,
               SUM(CASE WHEN date(processed_at) = date('now') THEN 1 ELSE 0 END) as today
        FROM bounce_logs 
        GROUP BY smtp_id
    """)
    rows = cursor.fetchall()
    conn.close()
    return {row['smtp_id']: dict(row) for row in rows}


def get_all_blacklisted(limit=200, offset=0, search=''):
    """Get all blacklisted emails."""
    conn = get_db()
    cursor = conn.cursor()
    if search:
        cursor.execute("SELECT * FROM blacklist WHERE email LIKE ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                       (f'%{search}%', limit, offset))
    else:
        cursor.execute("SELECT * FROM blacklist ORDER BY created_at DESC LIMIT ? OFFSET ?",
                       (limit, offset))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def remove_from_blacklist(email):
    """Remove an email from the blacklist."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM blacklist WHERE email = ?", (email,))
    conn.commit()
    conn.close()


def get_blacklist_count():
    """Get total blacklist count."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as cnt FROM blacklist")
    row = cursor.fetchone()
    conn.close()
    return row['cnt'] if row else 0

# ===========================
# HELPERS - Contacts
# ===========================
def get_all_contacts(status=None, tag_id=None, search=None, limit=100, offset=0):
    """Récupère les contacts avec pagination, filtres et recherche."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Build dynamic query
    conditions = []
    params = []
    
    # Base query - different for untagged vs tagged vs all
    if tag_id == "untagged":
        base = """SELECT c.* FROM contacts c
                  LEFT JOIN contact_tags ct ON ct.contact_id = c.id"""
        conditions.append("ct.contact_id IS NULL")
    elif tag_id:
        base = """SELECT c.* FROM contacts c
                  JOIN contact_tags ct ON ct.contact_id = c.id"""
        conditions.append("ct.tag_id = ?")
        params.append(tag_id)
    else:
        base = "SELECT * FROM contacts c"
    
    # Status filter
    if status:
        conditions.append("c.status = ?")
        params.append(status)
    
    # Search filter (email, prénom, nom)
    if search:
        conditions.append("(c.email LIKE ? OR c.prenom LIKE ? OR c.nom LIKE ?)")
        search_term = f"%{search}%"
        params.extend([search_term, search_term, search_term])
    
    # Build WHERE clause
    where = ""
    if conditions:
        where = " WHERE " + " AND ".join(conditions)
    
    # Final query
    query = f"{base}{where} ORDER BY c.id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_contacts_tags_map(contact_ids):
    """Récupère les tags pour plusieurs contacts en une fois.
    Retourne un dict: {contact_id: [list of tags]}
    """
    if not contact_ids:
        return {}
    
    conn = get_db()
    cursor = conn.cursor()
    placeholders = ",".join("?" * len(contact_ids))
    cursor.execute(f"""
        SELECT ct.contact_id, t.id, t.name FROM contact_tags ct
        JOIN tags t ON t.id = ct.tag_id
        WHERE ct.contact_id IN ({placeholders})
        ORDER BY t.name
    """, contact_ids)
    rows = cursor.fetchall()
    conn.close()
    
    result = {cid: [] for cid in contact_ids}
    for row in rows:
        result[row["contact_id"]].append({"id": row["id"], "name": row["name"]})
    return result


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


def count_untagged_contacts(status=None):
    """Compte les contacts sans tags."""
    conn = get_db()
    cursor = conn.cursor()
    
    if status:
        cursor.execute("""
            SELECT COUNT(*) as count FROM contacts c
            LEFT JOIN contact_tags ct ON ct.contact_id = c.id
            WHERE ct.contact_id IS NULL AND c.status = ?
        """, (status,))
    else:
        cursor.execute("""
            SELECT COUNT(*) as count FROM contacts c
            LEFT JOIN contact_tags ct ON ct.contact_id = c.id
            WHERE ct.contact_id IS NULL
        """)
    
    row = cursor.fetchone()
    conn.close()
    return row["count"]


def get_contact_by_id(contact_id):
    """Récupère un contact par son ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM contacts WHERE id = ?", (contact_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def update_contact(contact_id, email=None, prenom=None, nom=None):
    """Met à jour les infos d'un contact."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE contacts SET email = ?, prenom = ?, nom = ?, updated_at = ?
        WHERE id = ?
    """, (email, prenom, nom, datetime.now().isoformat(), contact_id))
    conn.commit()
    conn.close()
    return cursor.rowcount


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
        contact_id = None  # Email déjà existent
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
    # Delete from contact_tags first (foreign key)
    cursor.execute(f"DELETE FROM contact_tags WHERE contact_id IN ({placeholders})", contact_ids)
    # Then delete contacts
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


def add_contacts_bulk(contacts, source="import", skip_duplicates=True, status="active"):
    """Ajoute plusieurs contacts en une fois.
    
    contacts: liste de dicts avec 'email', 'prenom', 'nom'
    Retourne (added_count, skipped_count, added_ids)
    """
    conn = get_db()
    cursor = conn.cursor()
    added = 0
    skipped = 0
    added_ids = []
    
    for c in contacts:
        email = c.get("email", "").strip().lower()
        if not email or "@" not in email:
            skipped += 1
            continue
        
        # Check if contact is already unsubscribed — never re-add
        cursor.execute("SELECT status FROM contacts WHERE email = ?", (email,))
        existing = cursor.fetchone()
        if existing and existing["status"] == "unsubscribed":
            skipped += 1
            continue
        
        try:
            cursor.execute("""
                INSERT INTO contacts (email, prenom, nom, source, status) VALUES (?, ?, ?, ?, ?)
            """, (email, c.get("prenom", ""), c.get("nom", ""), source, status))
            added += 1
            added_ids.append(cursor.lastrowid)
        except Exception:
            if skip_duplicates:
                skipped += 1
            else:
                # Update existing
                cursor.execute("""
                    UPDATE contacts SET prenom = ?, nom = ?, updated_at = ?, status = ? WHERE email = ?
                """, (c.get("prenom", ""), c.get("nom", ""), datetime.now().isoformat(), status, email))
                added += 1
    
    conn.commit()
    conn.close()
    return added, skipped, added_ids


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
        INSERT INTO emails (name, subject, from_name, reply_to, mode, body, text_fallback,
                           offer_id, format, charset, transfer_encoding, from_email, footer)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("name"),
        data.get("subject"),
        data.get("from_name"),
        data.get("reply_to"),
        data.get("mode", "text"),
        data.get("body"),
        data.get("text_fallback"),
        data.get("offer_id") or None,
        data.get("format", "multipart/alternative"),
        data.get("charset", "UTF-8"),
        data.get("transfer_encoding", "quoted-printable"),
        data.get("from_email"),
        data.get("footer")
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
        SET name=?, subject=?, from_name=?, reply_to=?, mode=?, body=?, text_fallback=?,
            offer_id=?, format=?, charset=?, transfer_encoding=?, from_email=?, footer=?, updated_at=?
        WHERE id=?
    """, (
        data.get("name"),
        data.get("subject"),
        data.get("from_name"),
        data.get("reply_to"),
        data.get("mode"),
        data.get("body"),
        data.get("text_fallback"),
        data.get("offer_id") or None,
        data.get("format", "multipart/alternative"),
        data.get("charset", "UTF-8"),
        data.get("transfer_encoding", "quoted-printable"),
        data.get("from_email"),
        data.get("footer"),
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


# ===========================
# CAMPAIGN HISTORY
# ===========================
def insert_campaign_history(campaign_data):
    """Inserts a deleted campaign into the campaign_history table."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Try to extract smtp_names safely
    smtp_names = ""
    if campaign_data.get('smtp_names'):
        smtp_names = ", ".join(campaign_data['smtp_names'])
    
    cursor.execute("""
        INSERT INTO campaign_history 
        (original_id, name, created_at, status, smtp_names, total_recipients, sent_count, open_count, click_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        campaign_data.get('id'),
        campaign_data.get('name', 'Unknown'),
        campaign_data.get('created_at'),
        campaign_data.get('status', 'unknown'),
        smtp_names,
        campaign_data.get('total_recipients', 0),
        campaign_data.get('sent_count', 0),
        campaign_data.get('open_count', 0),
        campaign_data.get('click_count', 0)
    ))
    conn.commit()
    conn.close()


def get_campaign_history():
    """Retrieves all deleted campaigns from history."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM campaign_history ORDER BY deleted_at DESC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

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
    
    # Stats d'ouvertures depuis send_jobs
    cursor.execute("SELECT SUM(opened) as total_opened FROM send_jobs WHERE opened = 1")
    opened_row = cursor.fetchone()
    total_opened = opened_row["total_opened"] or 0
    total_sent = row["sent"] or 0
    
    open_rate = 0
    if total_sent > 0:
        open_rate = round((total_opened / total_sent) * 100)
    
    stats = {
        "total": row["total"] or 0,
        "sent": total_sent,
        "failed": row["failed"] or 0,
        "bounced": row["bounced"] or 0,
        "delivered": total_sent - (row["bounced"] or 0),
        "open_rate": open_rate
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
# HELPERS - Tags
# ===========================
def get_all_tags():
    """Récupère tous les tags avec le nombre de contacts."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.*, 
               (SELECT COUNT(*) FROM contact_tags ct WHERE ct.tag_id = t.id) as contact_count
        FROM tags t
        ORDER BY t.name
    """)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_tag_by_id(tag_id):
    """Récupère un tag par ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tags WHERE id = ?", (tag_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def add_tag(name):
    """Ajoute un nouveau tag."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO tags (name) VALUES (?)", (name.strip(),))
        conn.commit()
        tag_id = cursor.lastrowid
    except:
        tag_id = None  # Tag already exists
    conn.close()
    return tag_id


def update_tag(tag_id, name):
    """Met à jour un tag."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE tags SET name = ? WHERE id = ?", (name.strip(), tag_id))
    conn.commit()
    conn.close()


def delete_tag(tag_id):
    """Supprime un tag."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM contact_tags WHERE tag_id = ?", (tag_id,))
    cursor.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
    conn.commit()
    conn.close()


def get_contact_tags(contact_id):
    """Récupère les tags d'un contact."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.* FROM tags t
        JOIN contact_tags ct ON ct.tag_id = t.id
        WHERE ct.contact_id = ?
        ORDER BY t.name
    """, (contact_id,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_tags_to_contacts_bulk(contact_ids, tag_ids):
    """Ajoute des tags à plusieurs contacts."""
    if not contact_ids or not tag_ids:
        return 0
    conn = get_db()
    cursor = conn.cursor()
    added = 0
    for contact_id in contact_ids:
        for tag_id in tag_ids:
            try:
                cursor.execute(
                    "INSERT INTO contact_tags (contact_id, tag_id) VALUES (?, ?)",
                    (contact_id, tag_id)
                )
                added += 1
            except:
                pass  # Already exists
    conn.commit()
    conn.close()
    return added


def remove_tags_from_contacts_bulk(contact_ids, tag_ids):
    """Retire des tags de plusieurs contacts."""
    if not contact_ids or not tag_ids:
        return 0
    conn = get_db()
    cursor = conn.cursor()
    placeholders_contacts = ",".join("?" * len(contact_ids))
    placeholders_tags = ",".join("?" * len(tag_ids))
    cursor.execute(
        f"DELETE FROM contact_tags WHERE contact_id IN ({placeholders_contacts}) AND tag_id IN ({placeholders_tags})",
        contact_ids + tag_ids
    )
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def get_contacts_by_tag(tag_id, limit=100, offset=0):
    """Récupère les contacts ayant un tag spécifique."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.* FROM contacts c
        JOIN contact_tags ct ON ct.contact_id = c.id
        WHERE ct.tag_id = ?
        ORDER BY c.id DESC
        LIMIT ? OFFSET ?
    """, (tag_id, limit, offset))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def count_contacts_by_tag(tag_id):
    """Compte les contacts ayant un tag."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) as count FROM contact_tags WHERE tag_id = ?",
        (tag_id,)
    )
    row = cursor.fetchone()
    conn.close()
    return row["count"]


# ===========================
# HELPERS - Users & Authentication
# ===========================
def init_admin_user():
    """Initialise le compte administrateur si la table est vide."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM users")
    if cursor.fetchone()['count'] == 0:
        username = "Adil Boulal"
        password_hash = generate_password_hash("Adilboulal01031987@")
        cursor.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, password_hash))
        conn.commit()
    conn.close()

def verify_user(username, password):
    """Vérifie si le mot de passe correspond au nom d'utilisateur."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    user = cursor.fetchone()
    conn.close()
    
    if user and check_password_hash(user['password_hash'], password):
        return dict(user)
    return None

def get_user_by_id(user_id):
    """Récupère un utilisateur par ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = cursor.fetchone()
    conn.close()
    return dict(user) if user else None


# ===========================
# HELPERS - Campaigns
# ===========================
def create_campaign(data):
    """Crée une nouvelle campagne."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO campaigns (
            name, email_id, smtp_id, smtp_ids, audience_type, include_tags, exclude_tags, 
            total_recipients, scheduled_at, speed, email_delay, delay_max, test_inbox_interval, test_email, status, pause_code,
            email_snapshot, smtp_snapshot, audience_snapshot
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("name"),
        data.get("email_id"),
        data.get("smtp_id"),
        data.get("smtp_ids"),  # JSON string
        data.get("audience_type", "all"),
        data.get("include_tags"),  # JSON string
        data.get("exclude_tags"),  # JSON string
        data.get("total_recipients", 0),
        data.get("scheduled_at"),
        data.get("speed", "auto"),
        data.get("email_delay", 3),
        data.get("delay_max", 0),
        data.get("test_inbox_interval", 100),
        data.get("test_email", ""),
        data.get("status", "draft"),
        data.get("pause_code"),
        data.get("email_snapshot"),
        data.get("smtp_snapshot"),
        data.get("audience_snapshot")
    ))
    conn.commit()
    campaign_id = cursor.lastrowid
    conn.close()
    return campaign_id


def update_campaign(campaign_id, data):
    """Met à jour une campagne en attente d'envoi."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE campaigns SET
            name = ?, email_id = ?, smtp_id = ?, smtp_ids = ?, audience_type = ?, include_tags = ?, exclude_tags = ?, 
            total_recipients = ?, scheduled_at = ?, speed = ?, email_delay = ?, delay_max = ?, test_inbox_interval = ?, test_email = ?, status = ?,
            email_snapshot = ?, smtp_snapshot = ?, audience_snapshot = ?
        WHERE id = ?
    """, (
        data.get("name"),
        data.get("email_id"),
        data.get("smtp_id"),
        data.get("smtp_ids"),
        data.get("audience_type", "all"),
        data.get("include_tags"),
        data.get("exclude_tags"),
        data.get("total_recipients", 0),
        data.get("scheduled_at"),
        data.get("speed", "auto"),
        data.get("email_delay", 3),
        data.get("delay_max", 0),
        data.get("test_inbox_interval", 100),
        data.get("test_email", ""),
        data.get("status", "draft"),
        data.get("email_snapshot"),
        data.get("smtp_snapshot"),
        data.get("audience_snapshot"),
        campaign_id
    ))
    conn.commit()
    conn.close()


def get_campaign_by_id(campaign_id):
    """Récupère une campagne par ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_campaigns(status=None, limit=50):
    """Récupère toutes les campagnes avec filtre optionnel."""
    conn = get_db()
    cursor = conn.cursor()
    if status:
        cursor.execute("SELECT * FROM campaigns WHERE status = ? ORDER BY created_at DESC LIMIT ?", (status, limit))
    else:
        cursor.execute("SELECT * FROM campaigns ORDER BY created_at DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def update_campaign_status(campaign_id, status, pause_reason=None):
    """Met à jour le statut d'une campagne."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE campaigns SET status = ?, pause_reason = ? WHERE id = ?
    """, (status, pause_reason, campaign_id))
    conn.commit()
    conn.close()


def update_campaign_stats(campaign_id, sent_count=None, error_count=None, bounce_count=None):
    """Met à jour les stats d'une campagne."""
    conn = get_db()
    cursor = conn.cursor()
    updates = []
    values = []
    if sent_count is not None:
        updates.append("sent_count = ?")
        values.append(sent_count)
    if error_count is not None:
        updates.append("error_count = ?")
        values.append(error_count)
    if bounce_count is not None:
        updates.append("bounce_count = ?")
        values.append(bounce_count)
    if updates:
        values.append(campaign_id)
        cursor.execute(f"UPDATE campaigns SET {', '.join(updates)} WHERE id = ?", values)
        conn.commit()
    conn.close()


# ===========================
# HELPERS - Send Jobs
# ===========================
def create_send_jobs_bulk(campaign_id, contact_ids):
    """Crée les jobs d'envoi pour une campagne."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    jobs = [(campaign_id, cid, "queued", now) for cid in contact_ids]
    cursor.executemany("""
        INSERT INTO send_jobs (campaign_id, contact_id, status, queued_at)
        VALUES (?, ?, ?, ?)
    """, jobs)
    conn.commit()
    count = cursor.rowcount
    conn.close()
    return count


def get_pending_jobs(campaign_id, limit=50):
    """Récupère les jobs en attente pour une campagne et les marque comme 'sending'."""
    conn = get_db()
    cursor = conn.cursor()
    # Fetch queued jobs
    cursor.execute("""
        SELECT j.*, c.email, c.prenom, c.nom
        FROM send_jobs j
        JOIN contacts c ON j.contact_id = c.id
        WHERE j.campaign_id = ? AND j.status = 'queued'
        ORDER BY j.id ASC
        LIMIT ?
    """, (campaign_id, limit))
    rows = cursor.fetchall()
    jobs = [dict(row) for row in rows]
    
    # Immediately mark as 'sending' to prevent duplicates
    if jobs:
        job_ids = [j['id'] for j in jobs]
        placeholders = ",".join(["?" for _ in job_ids])
        cursor.execute(f"UPDATE send_jobs SET status = 'sending' WHERE id IN ({placeholders})", job_ids)
        conn.commit()
    
    conn.close()
    return jobs


def update_job_status(job_id, status, error_message=None):
    """Met à jour le statut d'un job."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat() if status == "sent" else None
    cursor.execute("""
        UPDATE send_jobs SET status = ?, error_message = ?, sent_at = ? WHERE id = ?
    """, (status, error_message, now, job_id))
    conn.commit()
    conn.close()


def increment_job_retry(job_id):
    """Incrémente le compteur de retry d'un job."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE send_jobs SET retry_count = retry_count + 1 WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()


def get_campaign_job_stats(campaign_id):
    """Récupère les stats des jobs d'une campagne."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT status, COUNT(*) as count FROM send_jobs WHERE campaign_id = ? GROUP BY status
    """, (campaign_id,))
    rows = cursor.fetchall()
    conn.close()
    return {row["status"]: row["count"] for row in rows}


def delete_pending_jobs_for_contact(contact_id):
    """Supprime tous les jobs en attente pour un contact (hard bounce)."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM send_jobs WHERE contact_id = ? AND status = 'queued'", (contact_id,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


# ===========================
# HELPERS - Blacklist
# ===========================
def add_to_blacklist(email, reason, campaign_id=None):
    """Ajoute un email à la blacklist globale."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO blacklist (email, reason, source_campaign_id)
            VALUES (?, ?, ?)
        """, (email.lower().strip(), reason, campaign_id))
        conn.commit()
        success = True
    except:
        success = False  # Already in blacklist
    conn.close()
    return success


def is_blacklisted(email):
    """Vérifie si un email est dans la blacklist."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM blacklist WHERE email = ?", (email.lower().strip(),))
    row = cursor.fetchone()
    conn.close()
    return row is not None


def get_blacklist_count():
    """Compte le nombre d'emails dans la blacklist."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM blacklist")
    row = cursor.fetchone()
    conn.close()
    return row["count"]


# ===========================
# HELPERS - Tracking
# ===========================
def mark_job_as_opened(job_id):
    """Marque un job d'envoi comme ouvert et incrémente le compteur de la campagne si c'est la première ouverture."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Check if already opened or bot
    cursor.execute("SELECT opened, campaign_id, is_bot FROM send_jobs WHERE id = ?", (job_id,))
    row = cursor.fetchone()
    
    if row and row['opened'] == 0 and row.get('is_bot', 0) == 0:
        # Mark job as opened
        cursor.execute("UPDATE send_jobs SET opened = 1 WHERE id = ?", (job_id,))
        
        # Increment campaign total
        if row['campaign_id']:
            cursor.execute("UPDATE campaigns SET open_count = open_count + 1 WHERE id = ?", (row['campaign_id'],))
            
        conn.commit()
        success = True
    else:
        success = False
        
    conn.close()
    return success


def mark_job_as_clicked(job_id, is_honeypot=False):
    """Mark a job as clicked. Returns 'bot' if bot detected, 'ok' if legit, 'skip' if already clicked."""
    from datetime import datetime, timedelta
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT clicked, is_bot, sent_at, campaign_id FROM send_jobs WHERE id = ?", (job_id,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        return 'skip'
    
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    
    # === HONEYPOT CHECK ===
    if is_honeypot:
        # Bot detected via honeypot! Flag the job
        cursor.execute("UPDATE send_jobs SET is_bot = 1 WHERE id = ?", (job_id,))
        # Also undo any previously counted open
        if row.get('opened') == 1 and row['campaign_id']:
            cursor.execute("UPDATE campaigns SET open_count = MAX(0, open_count - 1) WHERE id = ?", (row['campaign_id'],))
        conn.commit()
        conn.close()
        return 'bot'
    
    # === TIMING CHECK ===
    if row['sent_at']:
        try:
            sent_time = datetime.strptime(row['sent_at'][:19], '%Y-%m-%d %H:%M:%S')
            elapsed = (now - sent_time).total_seconds()
            if elapsed < 3:  # Click within 3 seconds of sending = bot
                cursor.execute("UPDATE send_jobs SET is_bot = 1 WHERE id = ?", (job_id,))
                # Also undo any previously counted open
                if row.get('opened') == 1 and row['campaign_id']:
                    cursor.execute("UPDATE campaigns SET open_count = MAX(0, open_count - 1) WHERE id = ?", (row['campaign_id'],))
                conn.commit()
                conn.close()
                return 'bot'
        except Exception:
            pass
    
    # === ALREADY BOT ===
    if row.get('is_bot', 0) == 1:
        conn.close()
        return 'bot'
    
    # === LEGIT CLICK ===
    if row['clicked'] == 0:
        cursor.execute("UPDATE send_jobs SET clicked = 1, clicked_at = ? WHERE id = ?", (now_str, job_id))
        if row['campaign_id']:
            cursor.execute("UPDATE campaigns SET click_count = click_count + 1 WHERE id = ?", (row['campaign_id'],))
        conn.commit()
        conn.close()
        return 'ok'
    
    conn.close()
    return 'skip'


def filter_blacklisted_emails(emails):
    """Filtre une liste d'emails et retourne ceux qui ne sont pas blacklistés."""
    if not emails:
        return []
        
    conn = get_db()
    cursor = conn.cursor()
    
    blacklisted = set()
    cleaned_emails = [e.lower().strip() for e in emails]
    
    # SQLite has a parameter limit (often 999), so we chunk the queries
    chunk_size = 900
    for i in range(0, len(cleaned_emails), chunk_size):
        chunk = cleaned_emails[i:i + chunk_size]
        placeholders = ",".join(["?" for _ in chunk])
        cursor.execute(f"SELECT email FROM blacklist WHERE email IN ({placeholders})", chunk)
        blacklisted.update(row["email"] for row in cursor.fetchall())
        
    conn.close()
    return [e for e in emails if e.lower().strip() not in blacklisted]


# ===========================
# HELPERS - Offers
# ===========================
def get_all_offers():
    """Récupère toutes les offres."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM offers ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_offer_by_id(offer_id):
    """Récupère une offre par ID."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM offers WHERE id = ?", (offer_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def add_offer(data):
    """Ajoute une nouvelle offre."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO offers (name, sponsor, url, type, notes)
        VALUES (?, ?, ?, ?, ?)
    """, (
        data.get("name"),
        data.get("sponsor", ""),
        data.get("url"),
        data.get("type", "CPA"),
        data.get("notes", "")
    ))
    conn.commit()
    offer_id = cursor.lastrowid
    conn.close()
    return offer_id


def update_offer(offer_id, data):
    """Met à jour une offre."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE offers 
        SET name=?, sponsor=?, url=?, type=?, notes=?, updated_at=?
        WHERE id=?
    """, (
        data.get("name"),
        data.get("sponsor", ""),
        data.get("url"),
        data.get("type", "CPA"),
        data.get("notes", ""),
        datetime.now().isoformat(),
        offer_id
    ))
    conn.commit()
    conn.close()


def delete_offer(offer_id):
    """Supprime une offre."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM offers WHERE id = ?", (offer_id,))
    conn.commit()
    conn.close()


# ===========================
# TEST EMAILS
# ===========================
def get_all_test_emails():
    """Get all test emails."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test_emails ORDER BY created_at ASC")
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def add_test_email(email, provider=""):
    """Add a test email."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO test_emails (email, provider) VALUES (?, ?)",
        (email, provider)
    )
    conn.commit()
    conn.close()
    return cursor.lastrowid


def delete_test_email(test_id):
    """Delete a test email."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM test_emails WHERE id = ?", (test_id,))
    conn.commit()
    conn.close()


# ===========================
# Campaign Logs (Activity Journal)
# ===========================
def add_campaign_log(campaign_id, campaign_name, action, detail=""):
    """Add a log entry for a campaign action."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO campaign_logs (campaign_id, campaign_name, action, detail)
        VALUES (?, ?, ?, ?)
    """, (campaign_id, campaign_name, action, detail))
    conn.commit()
    conn.close()


def get_campaign_logs(limit=50):
    """Get recent campaign logs."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, campaign_id, campaign_name, action, detail, created_at
        FROM campaign_logs
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ===========================
# Initialisation au démarrage
# ===========================
if __name__ == "__main__":
    init_db()
