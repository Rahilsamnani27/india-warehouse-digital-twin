import pandas as pd
import numpy as np
import boto3
import random
from datetime import datetime, timedelta

# ── Configuration ──────────────────────────────────────────────────────────────
S3_BUCKET = "india-warehouse-digital-twin"
S3_PREFIX = "raw/warehouse_events/"
NUM_RECORDS = 10000
RANDOM_SEED = 42

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ── Reference Data ─────────────────────────────────────────────────────────────
WAREHOUSE_LOCATIONS = [
    ("WH001", "Mumbai", "Maharashtra"),
    ("WH002", "Delhi", "Delhi"),
    ("WH003", "Bengaluru", "Karnataka"),
    ("WH004", "Chennai", "Tamil Nadu"),
    ("WH005", "Hyderabad", "Telangana"),
    ("WH006", "Kolkata", "West Bengal"),
    ("WH007", "Pune", "Maharashtra"),
    ("WH008", "Ahmedabad", "Gujarat"),
    ("WH009", "Jaipur", "Rajasthan"),
    ("WH010", "Lucknow", "Uttar Pradesh"),
    ("WH011", "Surat", "Gujarat"),
    ("WH012", "Kanpur", "Uttar Pradesh"),
    ("WH013", "Nagpur", "Maharashtra"),
    ("WH014", "Indore", "Madhya Pradesh"),
    ("WH015", "Thane", "Maharashtra"),
    ("WH016", "Bhopal", "Madhya Pradesh"),
    ("WH017", "Visakhapatnam", "Andhra Pradesh"),
    ("WH018", "Patna", "Bihar"),
    ("WH019", "Vadodara", "Gujarat"),
    ("WH020", "Coimbatore", "Tamil Nadu"),
]

PRODUCT_CATEGORIES = [
    "Electronics", "Apparel", "Groceries", "Furniture",
    "Beauty", "Sports", "Toys", "Books", "Automotive", "Healthcare"
]


# ── Generator ──────────────────────────────────────────────────────────────────
def generate_warehouse_records(num_records: int = NUM_RECORDS) -> pd.DataFrame:
    records = []
    base_time = datetime.now() - timedelta(days=30)

    for i in range(num_records):
        wh_id, city, state = random.choice(WAREHOUSE_LOCATIONS)
        orders_received = random.randint(50, 500)
        fulfillment_rate = random.uniform(0.70, 1.0)
        orders_fulfilled = int(orders_received * fulfillment_rate)

        inventory_level = random.randint(100, 10000)
        reorder_threshold = random.randint(200, 1000)
        warehouse_capacity_pct = round(random.uniform(10.0, 99.0), 2)

        # Inject ~2% nulls
        if random.random() < 0.02:
            inventory_level = None
        if random.random() < 0.02:
            warehouse_capacity_pct = None

        # Inject ~1% out-of-range capacity
        if random.random() < 0.01:
            warehouse_capacity_pct = random.choice([-5.0, 105.0, 150.0])

        timestamp = base_time + timedelta(minutes=random.randint(0, 43200))

        records.append({
            "warehouse_id": wh_id,
            "warehouse_city": city,
            "warehouse_state": state,
            "product_id": f"PROD{random.randint(1000, 9999)}",
            "product_category": random.choice(PRODUCT_CATEGORIES),
            "inventory_level": inventory_level,
            "reorder_threshold": reorder_threshold,
            "orders_received": orders_received,
            "orders_fulfilled": orders_fulfilled,
            "orders_pending": orders_received - orders_fulfilled,
            "fulfillment_time_hours": round(random.uniform(1.0, 72.0), 2),
            "warehouse_capacity_percentage": warehouse_capacity_pct,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "ingestion_date": datetime.now().strftime("%Y-%m-%d"),
        })

    df = pd.DataFrame(records)

    # Inject ~1% duplicate rows
    duplicates = df.sample(frac=0.01, random_state=RANDOM_SEED)
    df = pd.concat([df, duplicates], ignore_index=True)

    print(f"Generated {len(df)} records ({len(duplicates)} duplicates injected)")
    return df


# ── Upload to S3 ───────────────────────────────────────────────────────────────
def upload_to_s3(df: pd.DataFrame) -> None:
    local_path = "/tmp/warehouse_data.csv"
    df.to_csv(local_path, index=False)

    s3_client = boto3.client("s3")
    date_partition = datetime.now().strftime("year=%Y/month=%m/day=%d")
    s3_key = f"{S3_PREFIX}{date_partition}/warehouse_data.csv"

    s3_client.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = generate_warehouse_records()
    upload_to_s3(df)