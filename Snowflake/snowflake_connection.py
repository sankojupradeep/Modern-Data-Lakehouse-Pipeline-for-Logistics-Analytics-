# ===============================================================
# ☁️ Load Gold Layer Data (from Storj/S3) → Snowflake
# ---------------------------------------------------------------
# Direct credentials version — for local testing only.
# ===============================================================

import boto3
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from pyspark.sql import SparkSession

# ===============================
# 1️⃣ Direct credentials
# ===============================
# --- Snowflake ---
SF_ACCOUNT   = "your_account.region.azure"
SF_USER      = "pradeepsankoju"
SF_PASSWORD  = "Pradeepbunny@123"
SF_WAREHOUSE = "COMPUTE_WH"
SF_DATABASE  = "AFTERSHIP_DB"
SF_SCHEMA    = "GOLD_SCHEMA"
SF_ROLE      = "ACCOUNTADMIN"

# --- Storj ---
STORJ_ENDPOINT  = "https://gateway.storjshare.io"
STORJ_ACCESS_KEY = "your_storj_access_key"
STORJ_SECRET_KEY = "your_storj_secret_key"
STORJ_BUCKET     = "my-data"

# ===============================
# 2️⃣ Spark session for S3 access
# ===============================
spark = (
    SparkSession.builder
    .appName("Gold_to_Snowflake")
    .config("spark.hadoop.fs.s3a.endpoint", STORJ_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", STORJ_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", STORJ_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# ===============================
# 3️⃣ Snowflake connection
# ===============================
conn = snowflake.connector.connect(
    user=SF_USER,
    password=SF_PASSWORD,
    account=SF_ACCOUNT,
    warehouse=SF_WAREHOUSE,
    database=SF_DATABASE,
    schema=SF_SCHEMA,
    role=SF_ROLE,
)
cur = conn.cursor()
print("✅ Connected to Snowflake")

# ===============================
# 4️⃣ Read gold data from Storj
# ===============================
s3 = boto3.client(
    "s3",
    endpoint_url=STORJ_ENDPOINT,
    aws_access_key_id=STORJ_ACCESS_KEY,
    aws_secret_access_key=STORJ_SECRET_KEY,
)

prefix = "gold/"
response = s3.list_objects_v2(Bucket=STORJ_BUCKET, Prefix=prefix, Delimiter="/")
date_folders = [p["Prefix"].split("/")[-2] for p in response.get("CommonPrefixes", [])]
print(f"📅 Found Gold folders: {date_folders}")

for date_str in date_folders:
    gold_path = f"s3a://{STORJ_BUCKET}/gold/{date_str}/"
    print(f"🚀 Processing {gold_path}")

    df_fact = spark.read.option("header", "true").csv(f"{gold_path}fact_shipment/")
    df_dim_courier = spark.read.option("header", "true").csv(f"{gold_path}dim_courier/")
    df_dim_status = spark.read.option("header", "true").csv(f"{gold_path}dim_shipment_status/")
    df_dim_date = spark.read.option("header", "true").csv(f"{gold_path}dim_date/")

    for name, df in {
        "fact_shipment": df_fact,
        "dim_courier": df_dim_courier,
        "dim_shipment_status": df_dim_status,
        "dim_date": df_dim_date,
    }.items():
        if df.rdd.isEmpty():
            continue

        pdf = df.toPandas()
        table_name = f"{name}_{date_str.replace('-', '_')}"

        # Create table if not exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(f'"{c}" STRING' for c in pdf.columns)}
            )
        """)

        # Write data
        success, nchunks, nrows, _ = write_pandas(conn, pdf, table_name)
        print(f"✅ Loaded {nrows} rows into {table_name}")

cur.close()
conn.close()
print("🎉 All Gold data loaded into Snowflake!")
