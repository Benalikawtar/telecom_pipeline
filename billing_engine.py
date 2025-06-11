from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, date_format

# Initialisation Spark
spark = SparkSession.builder \
    .appName("TelecomBillingEngine") \
    .config("spark.jars", "lib/postgresql-42.7.3.jar") \
    .getOrCreate()

# Connexion PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/telecom_billing"
db_props = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Lecture des enregistrements rated avec timestamp non nul
usage_df = spark.read \
    .jdbc(jdbc_url, "usage_records", properties=db_props) \
    .filter((col("rating_status") == "rated") & (col("timestamp").isNotNull()))

# Ajout de la colonne billing_month
usage_df = usage_df.withColumn("billing_month", date_format(col("timestamp"), "yyyy-MM"))

# Agrégation mensuelle
bills_df = usage_df.groupBy("customer_id", "billing_month") \
    .agg(spark_sum("cost").alias("total_cost"))

# Écriture dans la table de factures
bills_df.write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(jdbc_url, "monthly_bills", properties=db_props)

print("✅ Factures mensuelles générées avec succès.")

