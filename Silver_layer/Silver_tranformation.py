# ===============================================================
# ⚪ Bronze → Silver PySpark Transformation (Past 7 Days - Storj)
# ---------------------------------------------------------------
# ✅ Reads past 7 days of Bronze data from Storj
# ✅ Cleans, flattens, and enriches each day’s data
# ✅ Writes to Silver/shipments/{date}/ folders
# ===============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, trim, upper, to_timestamp,
    current_timestamp, when, lit
)
from datetime import datetime, timedelta

# ===============================
# 1️⃣ Storj Configuration
# ===============================
STORJ_ACCESS_KEY = "ju2ej25nvebpjm3tfpm46cdago2a"
STORJ_SECRET_KEY = "jz3xw2r4zivq4fuphgx7ypk7cxsovrzjpyzuhh36tnll57lc54dpy"
STORJ_ENDPOINT = "https://gateway.storjshare.io"
STORJ_BUCKET = "my-data"

# ===============================
# 2️⃣ Spark Session with Storj (S3A)
# ===============================
spark = (
    SparkSession.builder
    .appName("Bronze_to_Silver_Past7Days")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.hadoop.fs.s3a.endpoint", STORJ_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", STORJ_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", STORJ_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .getOrCreate()
)

print("✅ Spark session initialized with Storj configuration.")

# ===============================
# 3️⃣ Generate Past 7 Dates
# ===============================
today = datetime.now()
past_7_days = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7, -1, -1)]

print(f"📅 Processing Bronze → Silver for past 7 days: {past_7_days}")

# ===============================
# 4️⃣ Loop Through Dates
# ===============================
for date_str in past_7_days:
    bronze_path = f"s3a://{STORJ_BUCKET}/bronze/aftership/faker/{date_str}/"
    silver_path = f"s3a://{STORJ_BUCKET}/silver/shipments/{date_str}/"

    print(f"\n🚀 Processing date: {date_str}")
    print(f"📥 Bronze Path: {bronze_path}")

    try:
        df_bronze = spark.read.option("multiline", True).json(bronze_path)
    except Exception as e:
        print(f"⚠️ Failed to read Bronze data for {date_str}: {e}")
        continue

    if df_bronze.rdd.isEmpty():
        print(f"⚠️ No Bronze data found for {date_str}. Skipping...")
        continue

    print(f"📦 Bronze records loaded: {df_bronze.count()}")

    # ===============================
    # 5️⃣ Flatten Nested Checkpoints
    # ===============================
    df_flat = (
        df_bronze
        .withColumn("checkpoint", explode(col("checkpoints")))
        .select(
            col("id").alias("shipment_id"),
            trim(upper(col("slug"))).alias("courier"),
            trim(upper(col("tag"))).alias("status"),
            col("tracking_number"),
            col("origin_country_iso3").alias("origin_country"),
            col("destination_country_iso3").alias("destination_country"),
            col("shipment_weight_kg").cast("double").alias("shipment_weight"),
            col("delivery_days").cast("int").alias("delivery_days"),
            to_timestamp(col("created_at")).alias("created_at"),
            to_timestamp(col("updated_at")).alias("updated_at"),
            col("checkpoint.city").alias("checkpoint_city"),
            col("checkpoint.country").alias("checkpoint_country"),
            col("checkpoint.message").alias("checkpoint_message"),
            to_timestamp(col("checkpoint.checkpoint_time")).alias("checkpoint_time")
        )
    )

    # ===============================
    # 6️⃣ Clean & Enrich Data
    # ===============================
    df_silver = (
        df_flat
        .fillna({
            "status": "UNKNOWN",
            "checkpoint_city": "UNKNOWN",
            "checkpoint_country": "UNKNOWN"
        })
        .withColumn(
            "delivery_status",
            when(col("status").isin("DELIVERED", "OUTFORDELIVERY"), "SUCCESS")
            .otherwise("PENDING")
        )
        .withColumn("load_date", lit(date_str))
        .withColumn("load_timestamp", current_timestamp())
    )

    print("✅ Flattened, cleaned, and enriched Bronze → Silver.")

    # ===============================
    # 7️⃣ Write Silver Data (Date-Wise)
    # ===============================
    df_silver.write.mode("overwrite").partitionBy("load_date").parquet(silver_path)

    print(f"☁️ Silver data written successfully → {silver_path}")
    print(f"📊 Total Silver records: {df_silver.count()}")

print("\n🎉 Bronze → Silver transformation completed for all past 7 days.")
