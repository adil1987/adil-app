import sqlite3

conn = sqlite3.connect('data/app.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Get available tags
cursor.execute("SELECT id, name FROM tags")
tags = cursor.fetchall()
print("AVAILABLE TAGS:")
for t in tags:
    cursor.execute("""
        SELECT COUNT(c.id) as cnt 
        FROM contacts c
        JOIN contact_tags ct ON c.id = ct.contact_id
        WHERE c.status = 'active' AND ct.tag_id = ?
    """, (t['id'],))
    cnt = cursor.fetchone()['cnt']
    print(f"Tag {t['name']} (ID {t['id']}): {cnt} contacts")

# Let's test non actif (2) which has ~57k, and amis (4) which has ~300.
test_ids = [2, 4]
if test_ids:
    placeholders = ",".join(["?" for _ in test_ids])
    query = f"""
        SELECT COUNT(DISTINCT c.id) as c 
        FROM contacts c
        JOIN contact_tags ct ON c.id = ct.contact_id
        WHERE c.status = 'active' AND ct.tag_id IN ({placeholders})
    """
    print(f"\nExecuting: {query} with {test_ids}")
    cursor.execute(query, test_ids)
    count = cursor.fetchone()["c"]
    print(f"Total Unique Active Contacts: {count}")

conn.close()
