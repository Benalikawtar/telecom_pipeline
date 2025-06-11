import random
import psycopg2
from datetime import datetime, timedelta

ZONES = ["zone1", "zone2", "zone3"]
NAMES = ["Client " + chr(65 + i) for i in range(26)]
RATE_PLAN_IDS = [1, 2, 3]

def random_msisdn():
    return "2126" + "".join(random.choices("0123456789", k=8))

def random_date():
    return datetime(2022, 1, 1) + timedelta(days=random.randint(0, 365))

def generate_customers(n=10000):
    customers = []
    for _ in range(n):
        msisdn = random_msisdn()
        name = random.choice(NAMES)
        sub_type = "postpaid"
        rate_plan = random.choice(RATE_PLAN_IDS)
        date = random_date().date()
        status = "active"
        region = random.choice(ZONES)
        customers.append((msisdn, name, sub_type, rate_plan, date, status, region))
    return customers

def insert_into_postgres(customers):
    conn = psycopg2.connect(
        dbname="telecom_db",
        user="postgres",
        password="oumaima",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    for c in customers:
        cur.execute("""
            INSERT INTO customers (customer_id, customer_name, subscription_type,
                                   rate_plan_id, activation_date, customer_status, region)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, c)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    clients = generate_customers()
    insert_into_postgres(clients)
    print("✅ Base de10000 clients insérée dans PostgreSQL.")
