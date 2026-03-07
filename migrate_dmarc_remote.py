import sqlite3

conn = sqlite3.connect('/root/adil-app/data/app.db')

try:
    conn.execute("ALTER TABLE smtp_servers ADD COLUMN dmarc_email TEXT DEFAULT ''")
except Exception as e:
    pass

try:
    conn.execute("ALTER TABLE smtp_servers ADD COLUMN dmarc_password TEXT DEFAULT ''")
except Exception as e:
    pass

conn.commit()
print("Migration DB DMARC reussie !")
