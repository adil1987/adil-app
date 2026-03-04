import sqlite3

conn = sqlite3.connect('data/app.db')
cursor = conn.cursor()

cursor.execute("""
    SELECT c.id, c.email 
    FROM contacts c
    JOIN contact_tags ct ON c.id = ct.contact_id
    WHERE c.status = 'active' AND ct.tag_id = 2
""")
contacts = cursor.fetchall()
print(f"Total rows for tag 2: {len(contacts)}")

distinct_ids = set([c[0] for c in contacts])
print(f"Total DISTINCT ids: {len(distinct_ids)}")

null_or_empty_emails = [c for c in contacts if not c[1] or not c[1].strip()]
print(f"Total Empty/Null emails: {len(null_or_empty_emails)}")

print("Empty/Null samples:")
for c in null_or_empty_emails[:10]:
    print(c)

conn.close()
