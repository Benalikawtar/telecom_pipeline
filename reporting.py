import os
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2

# Connexion à PostgreSQL
conn = psycopg2.connect(
    dbname="telecom_billing",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)

# Création du dossier de sortie
os.makedirs("report_charts", exist_ok=True)

# 1. Revenus par mois
query1 = """
SELECT billing_month, SUM(total_cost) as total_revenue
FROM monthly_bills
GROUP BY billing_month
ORDER BY billing_month;
"""
df1 = pd.read_sql_query(query1, conn)
plt.figure(figsize=(8, 5))
plt.plot(df1["billing_month"], df1["total_revenue"], marker='o')
plt.xticks(rotation=45)
plt.title("Revenus par mois")
plt.xlabel("Mois")
plt.ylabel("MAD")
plt.grid(True)
plt.savefig("report_charts/revenus_par_mois.png")
plt.close()

# 2. Top 10 clients par consommation (version horizontale)
query2 = """
SELECT customer_id, SUM(total_cost) as total
FROM monthly_bills
GROUP BY customer_id
ORDER BY total DESC
LIMIT 10;
"""
df2 = pd.read_sql_query(query2, conn)
plt.figure(figsize=(12, 6))  # élargir le graphique
plt.bar(df2["customer_id"].astype(str), df2["total"])
plt.xticks(rotation=60, ha='right')  # inclinaison plus grande pour éviter la coupure
plt.title("Top 10 Clients par consommation")
plt.xlabel("Client")
plt.ylabel("MAD")
plt.tight_layout()  # ajuste automatiquement pour éviter chevauchement
plt.savefig("report_charts/top10_clients.png")
plt.close()



# 3. Répartition par type de service
query3 = """
SELECT record_type, COUNT(*) as count
FROM usage_records
GROUP BY record_type;
"""
df3 = pd.read_sql_query(query3, conn)
plt.figure(figsize=(6, 6))
plt.pie(df3["count"], labels=df3["record_type"], autopct='%1.1f%%')
plt.title("Répartition des services utilisés")
plt.savefig("report_charts/repartition_services.png")
plt.close()

# 4. Enregistrements médiés par mois
query4 = """
SELECT TO_CHAR(timestamp, 'YYYY-MM') AS month, COUNT(*) AS count
FROM usage_records
WHERE rating_status = 'rated' AND timestamp IS NOT NULL
GROUP BY month
ORDER BY month;
"""

df4 = pd.read_sql_query(query4, conn)
plt.figure(figsize=(8, 5))
plt.bar(df4["month"], df4["count"])
plt.title("Enregistrements médiés par mois")
plt.xlabel("Mois")
plt.ylabel("Nombre")
plt.xticks(rotation=45)
plt.savefig("report_charts/mediation_records_per_month.png")
plt.close()

# 5. Statuts de rating
query5 = """
SELECT rating_status, COUNT(*) as count
FROM usage_records
GROUP BY rating_status;
"""
df5 = pd.read_sql_query(query5, conn)
plt.figure(figsize=(6, 6))
plt.pie(df5["count"], labels=df5["rating_status"], autopct='%1.1f%%')
plt.title("Répartition des statuts de tarification")
plt.savefig("report_charts/statuts_rating.png")
plt.close()

# 6. Top 10 clients par consommation totale de data (version corrigée)
query6 = """
SELECT customer_id, SUM(data_volume_mb) AS total_data
FROM usage_records
WHERE data_volume_mb IS NOT NULL AND customer_id IS NOT NULL
GROUP BY customer_id
ORDER BY total_data DESC
LIMIT 10;
"""
df6 = pd.read_sql_query(query6, conn)
plt.figure(figsize=(10, 6))
plt.bar(df6["customer_id"].astype(str), df6["total_data"])
plt.title("Top 10 clients par consommation totale de data")
plt.xlabel("Client")
plt.ylabel("Volume total (MB)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("report_charts/top10_clients_total_data.png")
plt.close()




conn.close()


