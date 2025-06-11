from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# 1. Démarrer SparkSession
spark = SparkSession.builder \
    .appName("StreamingMediation") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. Schéma commun
schema = StructType() \
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
    .add("technology", StringType())

# 3. Lire depuis Kafka avec les options corrigées
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "telecom_cdr_topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parser le JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*")

# 5. Normalisation (ajout msisdn)
df_normalized = df_json.withColumn("msisdn",
    when(col("caller_id").isNotNull(), col("caller_id"))
    .when(col("sender_id").isNotNull(), col("sender_id"))
    .when(col("user_id").isNotNull(), col("user_id"))
    .otherwise(None)
).drop("caller_id", "sender_id", "user_id")

# 6. Ajouter colonne status par défaut
df_with_status = df_normalized.withColumn("status", lit("normal"))

# 7. Validation
df_validated = df_with_status.withColumn("status",
    when(
        col("timestamp").isNull() |
        ~col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2}T.*Z$") |
        col("msisdn").isNull() |
        col("msisdn").startswith("999") |
        col("record_id").isNull() |
        col("record_type").isNull(),
        "error"
    ).otherwise(col("status"))
)

# 8. Déduplication
df_deduplicated = df_validated.dropDuplicates(["record_id"])

# 9. Séparation
df_clean = df_deduplicated.filter(col("status") == "normal")
df_errors = df_deduplicated.filter(col("status") == "error")

# 10. Envoi vers Kafka - clean records
df_clean.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query_clean = df_clean.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "clean_cdr_topic") \
    .option("checkpointLocation", "checkpoints/clean_kafka") \
    .outputMode("append") \
    .start()

# 11. Envoi vers Kafka - erreurs (dead letter)
df_errors.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query_errors = df_errors.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "dead_letter_topic") \
    .option("checkpointLocation", "checkpoints/errors_kafka") \
    .outputMode("append") \
    .start()

# 12. Attente
query_clean.awaitTermination()
query_errors.awaitTermination()

