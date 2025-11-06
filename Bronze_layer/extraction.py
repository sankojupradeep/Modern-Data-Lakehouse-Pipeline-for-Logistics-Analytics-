# ================================================================
# 🚚 Weekly Fake Shipment Data Generator for Bronze Layer (Direct Keys)
# ---------------------------------------------------------------
# Description:
#   Generates fake shipment data for the last 7 days (50k–90k/day)
#   and uploads directly to Storj (without .env file).
#
# Usage:
#   python generate_fake_shipments_weekly_direct.py
#
# ================================================================

import os
import json
import random
import boto3
from faker import Faker
from datetime import datetime, timedelta
from tqdm import tqdm
from botocore.config import Config

# ===============================
# 1️⃣ Storj Credentials (⚠️ Local Testing Only)
# ===============================
STORJ_ACCESS_KEY = "ju2ej25nvebpjm3tfpm46cdago2a"
STORJ_SECRET_KEY = "jz3xw2r4zivq4fuphgx7ypk7cxsovrzjpyzuhh36tnll57lc54dpy"
STORJ_ENDPOINT = "https://gateway.storjshare.io"
STORJ_BUCKET = "my-data"

# ===============================
# 2️⃣ Storj Client Setup
# ===============================
s3_config = Config(
    retries={'max_attempts': 3},
    connect_timeout=10,
    read_timeout=10,
    signature_version='s3v4'
)
s3 = boto3.client(
    "s3",
    endpoint_url=STORJ_ENDPOINT,
    aws_access_key_id=STORJ_ACCESS_KEY,
    aws_secret_access_key=STORJ_SECRET_KEY,
    config=s3_config
)

# ===============================
# 3️⃣ Faker Setup
# ===============================
fake = Faker()
Faker.seed(42)

couriers = [
    "dhl", "fedex", "ups", "usps", "amazon",
    "bluedart", "correios", "delhivery", "dpd", "royalmail"
]
statuses = [
    "Pending", "InfoReceived", "InTransit", "OutForDelivery",
    "Delivered", "Exception", "FailedAttempt"
]

# ===============================
# 4️⃣ Helper Functions
# ===============================
def generate_fake_data_for_day(target_date: datetime, record_count: int):
    """Generate fake shipment data for a given date."""
    data = []
    for _ in tqdm(range(record_count), desc=f"📦 {target_date.strftime('%Y-%m-%d')}", ncols=100):
        created_at = fake.date_time_between(
            start_date=target_date - timedelta(days=1),
            end_date=target_date
        )
        updated_at = created_at + timedelta(hours=random.randint(1, 72))
        courier = random.choice(couriers)
        status = random.choice(statuses)
        origin_country = fake.country_code()
        destination_country = fake.country_code()
        checkpoints = []

        for j in range(random.randint(2, 6)):
            checkpoint_time = created_at + timedelta(hours=3 * j)
            checkpoints.append({
                "message": random.choice([
                    "Shipment information received",
                    "In transit",
                    "Arrived at facility",
                    "Out for delivery",
                    "Delivered successfully"
                ]),
                "city": fake.city(),
                "country": fake.country(),
                "checkpoint_time": checkpoint_time.isoformat(),
                "tag": random.choice(statuses)
            })

        record = {
            "id": fake.uuid4(),
            "tracking_number": fake.bothify(text="??##########"),
            "slug": courier,
            "tag": status,
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
            "origin_country_iso3": origin_country,
            "destination_country_iso3": destination_country,
            "customer_name": fake.name(),
            "shipment_weight_kg": round(random.uniform(0.1, 15.0), 2),
            "delivery_days": (updated_at - created_at).days,
            "checkpoints": checkpoints
        }
        data.append(record)
    return data


def save_and_upload(data, target_date: datetime):
    """Save JSON locally and upload to Storj in daily folder."""
    date_folder = target_date.strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    record_count = len(data)

    local_dir = os.path.join("data", "bronze", "aftership", "faker", date_folder)
    os.makedirs(local_dir, exist_ok=True)
    file_name = f"fake_shipments_{date_folder}_{record_count}records_{timestamp}.json"
    local_file = os.path.join(local_dir, file_name)
    storj_key = f"bronze/aftership/faker/{date_folder}/{file_name}"

    # Save locally
    with open(local_file, "w") as f:
        json.dump(data, f, indent=2)
    print(f"💾 Saved locally → {local_file}")

    # Upload to Storj
    try:
        s3.upload_file(local_file, STORJ_BUCKET, storj_key)
        print(f"☁️  Uploaded to Storj → {storj_key}")
    except Exception as e:
        print(f"❌ Upload failed for {date_folder}: {e}")


# ===============================
# 5️⃣ Main Generator (Includes Today)
# ===============================
if __name__ == "__main__":
    print("🚀 Generating fake shipment data for the past 7 days (including today)...")
    for i in range(6, -1, -1):  # 6 days ago → today
        target_date = datetime.now() - timedelta(days=i)
        record_count = random.randint(50000, 90000)
        print(f"\n📅 Day: {target_date.strftime('%Y-%m-%d')} | Records: {record_count:,}")

        fake_data = generate_fake_data_for_day(target_date, record_count)
        save_and_upload(fake_data, target_date)

    print("\n✅ All 7 days of fake Bronze layer data generated and uploaded successfully!")
