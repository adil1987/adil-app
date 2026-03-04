import urllib.request
import json
import sqlite3

# 1. First, check actual blacklist count in DB
conn = sqlite3.connect('data/app.db')
cursor = conn.cursor()
cursor.execute("SELECT email FROM blacklist")
blacklisted_emails = [r[0] for r in cursor.fetchall()]
print(f"Total Blacklisted globally: {len(blacklisted_emails)}")

# 2. Test fetching tag 4 (amis)
data = {"audience_type": "tags", "include_tags": [4], "exclude_tags": []}
req = urllib.request.Request(
    'http://127.0.0.1:5000/api/send/estimate',
    data=json.dumps(data).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)
try:
    with urllib.request.urlopen(req) as resp:
        res = json.loads(resp.read().decode('utf-8'))
        print(f"Estimate for amis (Tag 4): {res['count']}")
except Exception as e:
    print(f"Error checking Tag 4: {e}")

# 3. Test fetching tag 2 (non actif)
data = {"audience_type": "tags", "include_tags": [2], "exclude_tags": []}
req = urllib.request.Request(
    'http://127.0.0.1:5000/api/send/estimate',
    data=json.dumps(data).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)
try:
    with urllib.request.urlopen(req) as resp:
        res = json.loads(resp.read().decode('utf-8'))
        print(f"Estimate for non actif (Tag 2): {res['count']}")
except Exception as e:
    print(f"Error checking Tag 2: {e}")

# 4. Find how many blacklisted belong to each tag
for t in [4, 2]:
    cursor.execute(f"""
        SELECT c.email FROM contacts c
        JOIN contact_tags ct ON c.id = ct.contact_id
        WHERE ct.tag_id = ? AND c.email IN ({','.join(['?']*len(blacklisted_emails))})
    """, [t] + blacklisted_emails)
    tag_bl = cursor.fetchall()
    print(f"Blacklisted emails actually in Tag {t}: {len(tag_bl)}")

conn.close()
