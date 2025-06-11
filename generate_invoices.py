import json
import os
from datetime import datetime
import psycopg2

# Dossier de sortie
OUTPUT_DIR = "invoices"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Connexion à PostgreSQL
conn = psycopg2.connect(
    dbname="telecom_billing",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Lecture des factures
cur.execute("SELECT customer_id, billing_month, total_cost FROM monthly_bills")
rows = cur.fetchall()

for row in rows:
    invoice = {
        "customer_id": row[0],
        "billing_month": row[1],
        "total_cost": float(row[2]),
        "currency": "MAD",
        "generated_at": datetime.utcnow().isoformat() + "Z"
    }
    filename = f"{row[0]}_{row[1]}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w") as f:
        json.dump(invoice, f, indent=4)

print(f"✅ {len(rows)} factures générées dans le dossier '{OUTPUT_DIR}'.")

cur.close()
conn.close()

