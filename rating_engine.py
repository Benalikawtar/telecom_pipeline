from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, round as spark_round, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Initialisation Spark
spark = SparkSession.builder \
    .appName("TelecomRatingEngine") \
    .config("spark.jars", "lib/postgresql-42.7.3.jar") \
    .getOrCreate()

# Nouveau schéma adapté à telecom_cdr_topic
cdr_schema = StructType() \
    .add("record_id", StringType()) \
    .add("record_type", StringType()) \
    .add("timestamp", StringType()) \
    .add("caller_id", StringType()) \
    .add("callee_id", StringType()) \
    .add("sender_id", StringType()) \
    .add("receiver_id", StringType()) \
    .add("user_id", StringType()) \
    .add("duration_sec", IntegerType()) \
    .add("data_volume_mb", DoubleType()) \
    .add("cell_id", StringType()) \
    .add("technology", StringType()) \
    .add("status", StringType())

# Lecture depuis Kafka (telecom_cdr_topic)
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "telecom_cdr_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parsing des données Kafka
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), cdr_schema).alias("data")) \
    .select("data.*")

# Ajout dynamique de msisdn
df_parsed = df_parsed.withColumn(
    "msisdn",
    when(col("record_type") == "voice", col("caller_id"))
    .when(col("record_type") == "sms", col("sender_id"))
    .when(col("record_type") == "data", col("user_id"))
)

# Connexion PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/telecom_billing"
db_props = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Clients actifs et postpaid
customers_df = spark.read \
    .jdbc(jdbc_url, "customers", properties=db_props) \
    .filter((col("subscription_type") == "postpaid") & (col("customer_status") == "active")) \
    .selectExpr("CAST(customer_id AS STRING) AS customer_id", "rate_plan_id")

# Tarification
rates_df = spark.read \
    .jdbc(jdbc_url, "product_rates", properties=db_props)

# Fonction de calcul du coût
def compute_cost(record_type, duration_sec, data_volume_mb, unit_price):
    return when(record_type == "voice", spark_round(duration_sec * unit_price, 2)) \
        .when(record_type == "data", spark_round(data_volume_mb * unit_price, 2)) \
        .when(record_type == "sms", unit_price) \
        .otherwise(None)

# Fonction de traitement
def process_batch(batch_df, epoch_id):
    if batch_df.isEmpty():
        return

    joined_customers = batch_df.join(customers_df, batch_df.msisdn == customers_df.customer_id, "left")

    joined_rates = joined_customers.join(
        rates_df,
        (joined_customers.rate_plan_id == rates_df.rate_plan_id) &
        (joined_customers.record_type == rates_df.service_type),
        "left"
    )

    rated_df = joined_rates.withColumn(
        "cost",
        compute_cost(col("record_type"), col("duration_sec"), col("data_volume_mb"), col("unit_price"))
    )

    rated_df = rated_df.withColumn(
        "rating_status",
        when(col("customer_id").isNull(), "unmatched")
        .when(col("unit_price").isNull(), "error")
        .when(col("cost").isNull(), "error")
        .otherwise("rated")
    )

    rated_df = rated_df.withColumn("timestamp", to_timestamp(col("timestamp")))

    output_df = rated_df.select(
        "record_id", "customer_id", "record_type", "timestamp",
        "duration_sec", "data_volume_mb", "status", "rating_status", "cost"
    )

    output_df.write \
        .mode("append") \
        .jdbc(jdbc_url, "usage_records", properties=db_props)

# Lancement du streaming
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/rating_checkpoint") \
    .start()

query.awaitTermination()

