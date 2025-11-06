# ===============================================================
# 🚀 Delivery Prediction API (User Input + Storj Logging)
# ---------------------------------------------------------------
# ✅ User enters tracking_id or country manually
# ✅ Predicts delivery success / expected delivery date
# ✅ Logs predictions to Storj (gold/fact_predictions/{date}/)
# ===============================================================

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware  # ✅ For frontend fetch (CORS)
import boto3
import pandas as pd
import joblib
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from sklearn.preprocessing import LabelEncoder
import io
import os  # Added for file checks

app = FastAPI(
    title="📦 Interactive Delivery Prediction API",
    description="Enter Tracking ID or Country Code to get prediction results.",
    version="2.0.0"
)

# ✅ Add CORS middleware (allows localhost/file origins for frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or ["http://localhost:8000", "file://"] for specifics
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# 1️⃣ Storj Configuration
# ===============================
STORJ_ACCESS_KEY = "ju2ej25nvebpjm3tfpm46cdago2a"
STORJ_SECRET_KEY = "jz3xw2r4zivq4fuphgx7ypk7cxsovrzjpyzuhh36tnll57lc54dpy"
STORJ_ENDPOINT = "https://gateway.storjshare.io"
STORJ_BUCKET = "my-data"

# ===============================
# 2️⃣ Spark Session (for reading Gold data)
# ===============================
spark = (
    SparkSession.builder
    .appName("Interactive_Predict_API")
    .config("spark.hadoop.fs.s3a.endpoint", STORJ_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", STORJ_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", STORJ_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")  # ✅ Fix: S3A support
    .getOrCreate()
)

# ===============================
# 3️⃣ Load Latest Model from Storj (with Fallback)
# ===============================
def load_latest_model():
    s3 = boto3.client(
        "s3",
        endpoint_url=STORJ_ENDPOINT,
        aws_access_key_id=STORJ_ACCESS_KEY,
        aws_secret_access_key=STORJ_SECRET_KEY,
    )

    response = s3.list_objects_v2(Bucket=STORJ_BUCKET, Prefix="models/", Delimiter="/")
    if "CommonPrefixes" not in response:
        print("⚠️ No models found—using dummy fallback.")
        from sklearn.dummy import DummyClassifier
        return DummyClassifier(strategy="most_frequent")  # Fallback: Always predicts majority class

    latest_folder = sorted([p["Prefix"].split("/")[-2] for p in response["CommonPrefixes"]])[-1]
    model_key = f"models/{latest_folder}/delivery_success_model_all_{latest_folder}.pkl"
    local_model = "delivery_success_model.pkl"

    try:
        s3.download_file(STORJ_BUCKET, model_key, local_model)
        print(f"✅ Loaded model from: {model_key}")
        return joblib.load(local_model)
    except Exception as e:
        print(f"⚠️ Model load failed ({e})—using dummy fallback.")
        from sklearn.dummy import DummyClassifier
        return DummyClassifier(strategy="most_frequent")

# Load model (safe)
try:
    model = load_latest_model()
except ImportError:
    print("❌ joblib not installed—install with 'pip install joblib scikit-learn'")
    raise

# ===============================
# 4️⃣ Load Gold Layer Data (All Dates)
# ===============================
def load_gold_data():
    s3 = boto3.client(
        "s3",
        endpoint_url=STORJ_ENDPOINT,
        aws_access_key_id=STORJ_ACCESS_KEY,
        aws_secret_access_key=STORJ_SECRET_KEY,
    )

    response = s3.list_objects_v2(Bucket=STORJ_BUCKET, Prefix="gold/", Delimiter="/")
    if "CommonPrefixes" not in response:
        print("⚠️ No gold folders found.")
        return pd.DataFrame()

    all_dates = [p["Prefix"].split("/")[-2] for p in response["CommonPrefixes"]]
    df_all = None
    for d in all_dates:
        gold_path = f"s3a://{STORJ_BUCKET}/gold/{d}/fact_shipment/"
        try:
            df_temp = spark.read.option("header", "true").csv(gold_path)
            df_all = df_temp if df_all is None else df_all.union(df_temp)
        except Exception as e:
            print(f"⚠️ Skipped {d}: {e}")
            continue
    if df_all is None:
        return pd.DataFrame()

    df = df_all.toPandas()
    print(f"✅ Loaded {len(df)} total records from Gold Layer.")
    return df

df_gold = load_gold_data()

# ===============================
# 🧠 Helper: Feature Preparation
# ===============================
def prepare_features(df):
    label_cols = ["courier", "origin_country", "destination_country", "status"]
    for col in label_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = LabelEncoder().fit_transform(df[col])
    df = df.fillna({"shipment_weight": 0, "delivery_days": 0})
    return df

# ===============================
# ☁️ Helper: Log Predictions to Storj
# ===============================
def log_to_storj(data_dict, log_type="tracking"):
    today = datetime.now().strftime("%Y-%m-%d")
    csv_buffer = io.StringIO()
    pd.DataFrame([data_dict]).to_csv(csv_buffer, index=False)
    key = f"gold/fact_predictions/{today}/{log_type}_predictions.csv"

    s3 = boto3.client(
        "s3",
        endpoint_url=STORJ_ENDPOINT,
        aws_access_key_id=STORJ_ACCESS_KEY,
        aws_secret_access_key=STORJ_SECRET_KEY,
    )

    try:
        s3.put_object(Bucket=STORJ_BUCKET, Key=key, Body=csv_buffer.getvalue())
        print(f"📝 Logged prediction to Storj: {key}")
    except Exception as e:
        print(f"⚠️ Failed to log prediction: {e}")

# ===============================
# 📦 1️⃣ Predict Delivery by Tracking ID
# ===============================
@app.get("/predict/tracking/")
def predict_tracking(tracking_id: str = Query(..., description="Enter Tracking ID")):
    if df_gold.empty:
        return {"error": "No Gold data found."}

    shipment = df_gold[df_gold["tracking_number"] == tracking_id]
    if shipment.empty:
        return {"error": f"Tracking ID '{tracking_id}' not found."}

    shipment = prepare_features(shipment)
    # Ensure required cols exist
    required_cols = ["courier", "origin_country", "destination_country", "shipment_weight", "delivery_days", "status"]
    X = shipment[[c for c in required_cols if c in shipment.columns]]
    pred = model.predict(X)[0]
    status = "SUCCESS" if pred == 1 else "PENDING"

    result = {
        "tracking_id": tracking_id,
        "courier": str(shipment.iloc[0]["courier"]) if "courier" in shipment.columns else "UNKNOWN",
        "origin": str(shipment.iloc[0]["origin_country"]) if "origin_country" in shipment.columns else "UNKNOWN",
        "destination": str(shipment.iloc[0]["destination_country"]) if "destination_country" in shipment.columns else "UNKNOWN",
        "predicted_status": status,
        "predicted_on": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    log_to_storj(result, log_type="tracking")
    return result

# ===============================
# 🌍 2️⃣ Predict Expected Delivery by Country
# ===============================
@app.get("/predict/country/")
def predict_country(country: str = Query(..., description="Enter Country Code (e.g., USA, IND)")):
    if df_gold.empty:
        return {"error": "No Gold data available."}

    df_country = df_gold[df_gold["destination_country"].str.upper() == country.upper()]
    if df_country.empty:
        return {"error": f"No data for country '{country}'."}

    df_country["delivery_days"] = pd.to_numeric(df_country["delivery_days"], errors="coerce")
    avg_days = int(df_country["delivery_days"].mean())
    expected_date = (datetime.now() + timedelta(days=avg_days)).strftime("%Y-%m-%d")

    result = {
        "country": country.upper(),
        "average_delivery_days": avg_days,
        "expected_delivery_date": expected_date
    }

    log_to_storj(result, log_type="country")
    return result

# ===============================
# 🏠 Root Endpoint
# ===============================
@app.get("/")
def root():
    return {
        "message": "📦 Interactive Delivery Prediction API",
        "usage": {
            "/predict/tracking/?tracking_id=ABC123": "Predict delivery success by Tracking ID",
            "/predict/country/?country=USA": "Get expected delivery date by country"
        }
    }