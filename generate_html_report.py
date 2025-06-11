import psycopg2

# Connexion à PostgreSQL
conn = psycopg2.connect(
    dbname="telecom_billing",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Total clients
cur.execute("SELECT COUNT(*) FROM customers WHERE customer_status = 'active'")
total_clients = cur.fetchone()[0]

# Total enregistrements (2025)
cur.execute("SELECT COUNT(*) FROM usage_records WHERE EXTRACT(YEAR FROM timestamp) = 2025 AND rating_status = 'rated'")
total_records = cur.fetchone()[0]

# Revenu total (2025)
cur.execute("SELECT SUM(cost) FROM usage_records WHERE EXTRACT(YEAR FROM timestamp) = 2025 AND rating_status = 'rated'")
total_revenue = round(cur.fetchone()[0] or 0, 2)

# Génération du fichier HTML
html = f"""
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>Rapport Telecom Pipeline</title>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 20px; }}
        h1, h2 {{ color: #2C3E50; }}
        .section {{ margin-bottom: 40px; }}
        .charts img {{ max-width: 100%; height: auto; margin: 10px 0; }}
        .summary {{ background: #F2F3F4; padding: 10px; border-left: 5px solid #3498DB; margin-bottom: 20px; }}
    </style>
</head>
<body>
    <h1>Rapport de Suivi - Telecom Data Pipeline</h1>

    <div class="summary">
        <p><strong>Total de clients :</strong> {total_clients}</p>
        <p><strong>Période analysée :</strong> Janvier à Décembre 2025</p>
        <p><strong>Nombre total d'enregistrements :</strong> {total_records}</p>
        <p><strong>Revenu total généré :</strong> {total_revenue} MAD</p>
    </div>

    <div class="section">
        <h2>1. Répartition des revenus mensuels</h2>
        <div class="charts">
            <img src="report_charts/revenus_par_mois.png" alt="Revenus par mois">
        </div>
    </div>

    <div class="section">
        <h2>2. Top 10 clients par consommation</h2>
        <div class="charts">
            <img src="report_charts/top10_clients.png" alt="Top clients">
        </div>
    </div>

    <div class="section">
        <h2>3. Volume total de données par client (Top 10)</h2>
        <div class="charts">
            <img src="report_charts/top10_clients_total_data.png" alt="Top data">
        </div>
    </div>

    <div class="section">
        <h2>4. Enregistrements médiés par mois</h2>
        <div class="charts">
            <img src="report_charts/mediation_records_per_month.png" alt="Médiation">
        </div>
    </div>

    <div class="section">
        <h2>5. Répartition des services utilisés</h2>
        <div class="charts">
            <img src="report_charts/repartition_services.png" alt="Services">
        </div>
    </div>

    <div class="section">
        <h2>6. Distribution des volumes de données</h2>
        <div class="charts">
            <img src="report_charts/statuts_rating.png" alt="Volume de données">
        </div>
    </div>

</body>
</html>
"""

with open("rapport_pipeline.html", "w") as f:
    f.write(html)

print("✅ Rapport HTML généré avec succès.")

