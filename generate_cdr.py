import json
import random
import uuid
from datetime import datetime, timedelta
import psycopg2

# Configuration
TOTAL_RECORDS = 10000
ANOMALY_RATIO = 0.05
DISTRIBUTION = {"voice": 0.6, "data": 0.3, "sms": 0.1}
TECHNOLOGIES = ["2G", "3G", "4G", "5G"]
CELL_IDS = ["ALHOCEIMA_23", "IMZOUREN_10", "NADOR_05", "TETOUAN_18"]

# Connexion à PostgreSQL pour récupérer les MSISDN
def fetch_known_msisdns():
    conn = psycopg2.connect(
        dbname="telecom_billing",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("SELECT customer_id FROM customers WHERE subscription_type = 'postpaid' AND customer_status = 'active'")
    msisdns = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return msisdns

# Préparation du pool de numéros
known_msisdns = fetch_known_msisdns()
nb_known = int(TOTAL_RECORDS * 0.8)
nb_random = TOTAL_RECORDS - nb_known
repeated_known = known_msisdns * 30  # répète chaque numéro 30 fois
random.shuffle(repeated_known)
selected_known = repeated_known[:nb_known]
random_generated = ["2126" + "".join(random.choices("0123456789", k=8)) for _ in range(nb_random)]
phone_pool = selected_known + random_generated
random.shuffle(phone_pool)
phone_index = 0

def random_phone():
    global phone_index
    phone = phone_pool[phone_index % len(phone_pool)]
    phone_index += 1
    return phone

# ✅ Nouvelle version : multi-mois (avril à juin)
def generate_timestamp():
    month = random.choice([1,2,3,4, 5, 6,7,8,9,10,11,12])  # avril, mai, juin
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    ts = datetime(2025, month, day, hour, minute, second)
    return ts.isoformat() + "Z"

def generate_record(record_type):
    tech = random.choice(TECHNOLOGIES)
    cell = random.choice(CELL_IDS)
    timestamp = generate_timestamp()

    if record_type == "voice":
        return {
            "record_type": "voice",
            "timestamp": timestamp,
            "caller_id": random_phone(),
            "callee_id": random_phone(),
            "duration_sec": random.randint(10, 600),
            "cell_id": cell,
            "technology": tech
        }

    elif record_type == "sms":
        return {
            "record_type": "sms",
            "timestamp": timestamp,
            "sender_id": random_phone(),
            "receiver_id": random_phone(),
            "cell_id": cell,
            "technology": tech
        }

    elif record_type == "data":
        return {
            "record_type": "data",
            "timestamp": timestamp,
            "user_id": random_phone(),
            "data_volume_mb": round(random.uniform(1.0, 1000.0), 2),
            "session_duration_sec": random.randint(60, 1800),
            "cell_id": cell,
            "technology": tech
        }

def inject_anomalies(record):
    anomaly_type = random.choice(["missing", "corrupted", "duplicate", "invalid_time"])
    new_record = dict(record)

    if anomaly_type == "missing":
        key_to_remove = random.choice(list(record.keys())[1:])
        del new_record[key_to_remove]

    elif anomaly_type == "corrupted":
        key_to_corrupt = random.choice(["caller_id", "duration_sec", "data_volume_mb"])
        if key_to_corrupt in new_record:
            new_record[key_to_corrupt] = "###INVALID###"

    elif anomaly_type == "duplicate":
        return [record, record]

    elif anomaly_type == "invalid_time":
        new_record["timestamp"] = "invalid-timestamp"

    return new_record

def main():
    all_records = []
    num_anomalies = int(TOTAL_RECORDS * ANOMALY_RATIO)
    service_types = (
        ["voice"] * int(DISTRIBUTION["voice"] * TOTAL_RECORDS) +
        ["data"] * int(DISTRIBUTION["data"] * TOTAL_RECORDS) +
        ["sms"] * int(DISTRIBUTION["sms"] * TOTAL_RECORDS)
    )
    random.shuffle(service_types)

    for i in range(TOTAL_RECORDS):
        record = generate_record(service_types[i])
        if i < num_anomalies:
            result = inject_anomalies(record)
            if isinstance(result, list):
                all_records.extend(result)
            else:
                all_records.append(result)
        else:
            all_records.append(record)

    with open("telecom_records.json", "w") as f:
        for r in all_records:
            f.write(json.dumps(r) + "\n")

    print(f"✅ {len(all_records)} records generated in 'telecom_records.json'")

if __name__ == "__main__":
    main()

