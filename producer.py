import json
import time
import uuid
from kafka import KafkaProducer

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Lire les données du fichier
with open("telecom_records.json", "r") as f:
    records = [json.loads(line) for line in f if line.strip()]

# Ajouter record_id + envoyer à Kafka
for record in records:
    record["record_id"] = str(uuid.uuid4())  # UUID unique
    producer.send("telecom_cdr_topic", value=record)
    print(f"📤 Envoyé : {record}")
    time.sleep(0.5)

print(f"✅ {len(records)} enregistrements envoyés avec record_id.")
producer.flush()
