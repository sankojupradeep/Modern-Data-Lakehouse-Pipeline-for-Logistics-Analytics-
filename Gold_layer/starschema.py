# ===============================================================
# 🟡 Silver → Gold PySpark Transformation (Date-Wise CSV in Storj)
# ---------------------------------------------------------------
# ✅ Reads last 7 days of Silver data from Storj
# ✅ Creates Dim + Fact tables
# ✅ Writes each table date-wise in CSV format to Gold layer
# ===============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, monotonically_increasing_id, countDistinct, count, avg, round, when, lit
from datetime import datetime, timedelta

# ===============================
# 1️⃣ Storj Configuration
# ===============================
STORJ_ACCESS_KEY = "ju2ej25nvebpjm3tfpm46cdago2a"
STORJ_SECRET_KEY = "jz3xw2r4zivq4fuphgx7ypk7cxsovrzjpyzuhh36tnll57lc54dpy"
STORJ_ENDPOINT = "https://gateway.storjshare.io"
STORJ_BUCKET = "my-data"

# ===============================
# 2️⃣ Spark Session Setup
# ===============================
spark = (
    SparkSession.builder
    .appName("Silver_to_Gold_Datewise_CSV")
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

print("✅ Spark session initialized for Gold Layer.")

# ===============================
# 3️⃣ Define Date Range (Past 7 Days)
# ===============================
today = datetime.now()
past_7_days = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7, -1, -1)]

print(f"📅 Generating Gold layer for past 7 days: {past_7_days}")

# ===============================
# 4️⃣ Process Each Day's Silver Data
# ===============================
for date_str in past_7_days:
    silver_path = f"s3a://{STORJ_BUCKET}/silver/shipments/{date_str}/"
    gold_base = f"s3a://{STORJ_BUCKET}/gold/{date_str}/"

    print(f"\n🚀 Processing Gold for date: {date_str}")
    print(f"📥 Reading Silver: {silver_path}")

    try:
        df_silver = spark.read.parquet(silver_path)
    except Exception as e:
        print(f"⚠️ Failed to read Silver data for {date_str}: {e}")
        continue

    if df_silver.rdd.isEmpty():
        print(f"⚠️ No Silver data found for {date_str}. Skipping...")
        continue

    print(f"📦 Loaded Silver records: {df_silver.count()}")

    # ===============================
    # 5️⃣ DIMENSION TABLES
    # ===============================

    # dim_courier
    dim_courier = df_silver.select("courier").distinct().withColumn("courier_id", monotonically_increasing_id())
    dim_courier.write.mode("overwrite").option("header", "true").csv(f"{gold_base}dim_courier/")

    # dim_location
    dim_location = (
        df_silver.select(col("checkpoint_city"), col("checkpoint_country"))
        .distinct()
        .withColumn("location_id", monotonically_increasing_id())
    )
    dim_location.write.mode("overwrite").option("header", "true").csv(f"{gold_base}dim_location/")

    # dim_date
    dim_date = (
        df_silver.select(to_date(col("created_at")).alias("date"))
        .distinct()
        .withColumn("date_id", monotonically_increasing_id())
    )
    dim_date.write.mode("overwrite").option("header", "true").csv(f"{gold_base}dim_date/")

    # dim_shipment_status
    dim_status = df_silver.select("status").distinct().withColumn("status_id", monotonically_increasing_id())
    dim_status.write.mode("overwrite").option("header", "true").csv(f"{gold_base}dim_shipment_status/")

    print("✅ Dimension tables created for", date_str)

    # ===============================
    # 6️⃣ FACT TABLES
    # ===============================

    # fact_shipment
    fact_shipment = df_silver.select(
        "tracking_number",
        "courier",
        "origin_country",
        "destination_country",
        "shipment_weight",
        "delivery_days",
        "status",
        "created_at",
        "updated_at",
        "delivery_status",
    )
    fact_shipment.write.mode("overwrite").option("header", "true").csv(f"{gold_base}fact_shipment/")

    # fact_tracking_event
    fact_tracking_event = df_silver.select(
        "tracking_number",
        "checkpoint_city",
        "checkpoint_country",
        "checkpoint_message",
        "checkpoint_time",
        "courier",
        "status",
        "updated_at"
    )
    fact_tracking_event.write.mode("overwrite").option("header", "true").csv(f"{gold_base}fact_tracking_event/")

    print("🏆 Fact tables created for", date_str)

    # ===============================
    # 7️⃣ COURIER PERFORMANCE SUMMARY
    # ===============================
    courier_summary = (
        df_silver.groupBy("courier")
        .agg(
            countDistinct("tracking_number").alias("total_shipments"),
            count(when(col("status") == "DELIVERED", True)).alias("delivered_shipments"),
            round(avg("delivery_days"), 2).alias("avg_delivery_days")
        )
        .withColumn("delivery_success_pct", round(col("delivered_shipments") / col("total_shipments") * 100, 2))
    )
    courier_summary.write.mode("overwrite").option("header", "true").csv(f"{gold_base}fact_courier_metrics/")

    print("📊 Courier performance summary generated for", date_str)

print("\n🎉 Silver → Gold transformation (CSV, date-wise) completed successfully!")
