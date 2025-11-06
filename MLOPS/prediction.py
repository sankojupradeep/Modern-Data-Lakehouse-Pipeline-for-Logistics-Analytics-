# ===============================================================
# 🤖 ML Model Training on ALL Gold Data (fact_shipment)
# ---------------------------------------------------------------
# ✅ Reads every Gold/fact_shipment/{date}/ folder from Storj
# ✅ Combines them into one dataset
# ✅ Trains RandomForestClassifier on all data
# ✅ Uploads trained model to Storj bucket
# ===============================================================

import os
import boto3
import pandas as pd
import joblib
from pyspark.sql import SparkSession
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

# ===============================
# 1️⃣ Storj Configuration
# ===============================
STORJ_ACCESS_KEY = "ju2ej25nvebpjm3tfpm46cdago2a"
STORJ_SECRET_KEY = "jz3xw2r4zivq4fuphgx7ypk7cxsovrzjpyzuhh36tnll57lc54dpy"
STORJ_ENDPOINT = "https://gateway.storjshare.io"
STORJ_BUCKET = "my-data"

# ===============================
# 2️⃣ Spark Session
# ===============================
spark = (
    SparkSession.builder
    .appName("ML_Train_All_Gold_Data")
    .config("spark.hadoop.fs.s3a.endpoint", STORJ_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", STORJ_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", STORJ_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .getOrCreate()
)

print("✅ Spark session initialized for full Gold ML training.")

# ===============================
# 3️⃣ List All Gold Dates
# ===============================
s3 = boto3.client(
    "s3",
    endpoint_url=STORJ_ENDPOINT,
    aws_access_key_id=STORJ_ACCESS_KEY,
    aws_secret_access_key=STORJ_SECRET_KEY
)

prefix = "gold/"
response = s3.list_objects_v2(Bucket=STORJ_BUCKET, Prefix=prefix, Delimiter="/")

if "CommonPrefixes" not in response:
    print("❌ No Gold folders found. Make sure Gold data exists.")
    exit()

date_folders = [p["Prefix"].split("/")[-2] for p in response["CommonPrefixes"]]
print(f"📅 Found Gold date folders: {date_folders}")

# ===============================
# 4️⃣ Read and Combine All Gold Data
# ===============================
df_all = None
total_count = 0

for date_str in date_folders:
    gold_path = f"s3a://{STORJ_BUCKET}/gold/{date_str}/fact_shipment/"
    try:
        df_temp = spark.read.option("header", "true").csv(gold_path)
        count_temp = df_temp.count()
        if count_temp > 0:
            print(f"✅ Loaded {count_temp} rows from {gold_path}")
            df_all = df_temp if df_all is None else df_all.union(df_temp)
            total_count += count_temp
    except Exception as e:
        print(f"⚠️ Could not read data for {date_str}: {e}")

if df_all is None:
    print("❌ No data available in any Gold folder.")
    exit()

print(f"📦 Combined total records across all Gold folders: {total_count}")

# ===============================
# 5️⃣ Convert to Pandas for ML
# ===============================
df = df_all.toPandas()

# Basic cleaning
df = df.dropna(subset=["delivery_status"])
df = df.fillna({"shipment_weight": 0, "delivery_days": 0})
df = df.drop_duplicates(subset=["tracking_number"])

# Encode categorical columns
label_cols = ["courier", "origin_country", "destination_country", "status"]
for col in label_cols:
    df[col] = df[col].astype(str)
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col])

# Encode target
df["delivery_status"] = LabelEncoder().fit_transform(df["delivery_status"].astype(str))

X = df[["courier", "origin_country", "destination_country", "shipment_weight", "delivery_days", "status"]]
y = df["delivery_status"]

print(f"🧮 Feature shape: {X.shape}, Target shape: {y.shape}")

# ===============================
# 6️⃣ Train/Test Split
# ===============================
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"🔹 Train: {X_train.shape[0]} records, Test: {X_test.shape[0]} records")

# ===============================
# 7️⃣ Train Model
# ===============================
model = RandomForestClassifier(n_estimators=150, max_depth=12, random_state=42)
model.fit(X_train, y_train)

# ===============================
# 8️⃣ Evaluate
# ===============================
y_pred = model.predict(X_test)
acc = round(accuracy_score(y_test, y_pred) * 100, 2)

print(f"\n🎯 Model Accuracy: {acc}%")
print("\n📊 Confusion Matrix:\n", confusion_matrix(y_test, y_pred))
print("\n🧾 Classification Report:\n", classification_report(y_test, y_pred))

# ===============================
# 9️⃣ Save Model Locally and Upload to Storj
# ===============================
today = datetime.now().strftime("%Y-%m-%d")
model_name = f"delivery_success_model_all_{today}.pkl"
joblib.dump(model, model_name)

print(f"💾 Model saved locally as: {model_name}")

model_storj_path = f"models/{today}/{model_name}"
try:
    s3.upload_file(model_name, STORJ_BUCKET, model_storj_path)
    print(f"☁️ Uploaded to Storj: s3a://{STORJ_BUCKET}/{model_storj_path}")
except Exception as e:
    print(f"❌ Upload failed: {e}")

print("\n✅ ML Training on ALL Gold data completed successfully!")
