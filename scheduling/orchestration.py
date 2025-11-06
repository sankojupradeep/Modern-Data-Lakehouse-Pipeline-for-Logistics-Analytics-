"""
Airflow DAG: ETL AfterShip/Correios → Bronze → Silver → Gold → Snowflake

Place this file into your Airflow DAGs folder. It expects the following helper scripts
to exist in SCRIPTS_DIR:
  - generate_fake_shipments_weekly_direct.py   (optional)
  - bronze_to_silver_pyspark_storej_past7days.py  or bronze_to_silver_all_dates.py
  - silver_to_gold_pyspark_storej_datewise_csv.py or silver_to_gold_all_dates.py
  - load_gold_to_snowflake.py

This DAG:
  1. Optionally generate Bronze (faker)
  2. Verify Bronze data present in Storj (past N days)
  3. Transform Bronze -> Silver (date-wise)
  4. Transform Silver -> Gold (date-wise)
  5. Load Gold -> Snowflake
"""

from datetime import datetime, timedelta
import os
import logging
import boto3

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# ---------- CONFIG ----------
# adjust paths to wherever your scripts live
SCRIPTS_DIR = "/mnt/c/Users/Hello/OneDrive/Desktop/logistics1/scripts"
# S3/Storj configuration used only in the pre-check; your scripts themselves handle their credentials
STORJ_ENDPOINT = "https://gateway.storjshare.io"
STORJ_BUCKET = "my-data"
# How many past days to process by default
DEFAULT_LOOKBACK_DAYS = 7

# Default DAG args
default_args = {
    "owner": "you",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

log = logging.getLogger(__name__)

# ---------- Helper functions ----------
def check_bronze_folders(**context):
    """
    Verify that bronze folders exist for the last N days in the Storj bucket.
    Fails (raises) if no bronze folders found.
    """
    # Allow override via dag_run.conf
    lookback_days = int(context.get("params", {}).get("lookback_days", DEFAULT_LOOKBACK_DAYS))

    s3 = boto3.client(
        "s3",
        endpoint_url=STORJ_ENDPOINT,
        aws_access_key_id=os.environ.get("STORJ_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("STORJ_SECRET_KEY"),
    )

    prefix = "bronze/aftership/faker/"
    response = s3.list_objects_v2(Bucket=STORJ_BUCKET, Prefix=prefix, Delimiter="/")
    folders = []
    if "CommonPrefixes" in response:
        folders = [p["Prefix"].split("/")[-2] for p in response["CommonPrefixes"]]

    if not folders:
        raise RuntimeError("No bronze date folders found in Storj under prefix: " + prefix)

    # Optionally check that at least `lookback_days` exist
    if len(folders) < lookback_days:
        log.warning(f"Found {len(folders)} bronze folders but lookback_days={lookback_days}. Continuing anyway.")
    else:
        log.info(f"Found {len(folders)} bronze folders; OK for lookback_days={lookback_days}.")

    # store discovered folders in XCom for downstream tasks if needed
    context['ti'].xcom_push(key='bronze_folders', value=folders)
    return folders

# ---------- DAG ----------
with DAG(
    dag_id="etl_aftership_correios_full_pipeline",
    default_args=default_args,
    description="Bronze -> Silver -> Gold -> Load to Snowflake pipeline (Storj)",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "aftership", "correios", "storj", "snowflake"],
) as dag:

    # 1. Optional generate fake bronze data (enable when needed)
    generate_bronze = BashOperator(
        task_id="generate_bronze_faker",
        bash_command=(
            f"python3 {os.path.join(SCRIPTS_DIR, 'generate_fake_shipments_weekly_direct.py')} "
            f"|| echo 'generate_fake_shipments_weekly_direct.py not present or failed (skipping)'"
        ),
        retries=0,
    )

    # 2. Check Bronze existence in Storj (PythonOperator uses boto3; assumes STORJ env vars set)
    check_bronze = PythonOperator(
        task_id="check_bronze_folders",
        python_callable=check_bronze_folders,
        provide_context=True,
        params={"lookback_days": DEFAULT_LOOKBACK_DAYS},
    )

    # 3. Bronze -> Silver transformation (call script)
    # Update script name to whichever you use (past7days or all_dates)
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"python3 {os.path.join(SCRIPTS_DIR, 'bronze_to_silver_pyspark_storej_past7days.py')} "
            f"|| python3 {os.path.join(SCRIPTS_DIR, 'bronze_to_silver_all_dates.py')}"
        ),
        env=os.environ,
        retries=1,
        retry_delay=timedelta(minutes=5)
    )

    # 4. Silver -> Gold transformation
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"python3 {os.path.join(SCRIPTS_DIR, 'silver_to_gold_pyspark_storej_datewise_csv.py')} "
            f"|| python3 {os.path.join(SCRIPTS_DIR, 'silver_to_gold_all_dates.py')}"
        ),
        env=os.environ,
        retries=1,
        retry_delay=timedelta(minutes=5)
    )

    # 5. Load Gold to Snowflake
    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command=f"python3 {os.path.join(SCRIPTS_DIR, 'load_gold_to_snowflake.py')}",
        env=os.environ,
        retries=2,
        retry_delay=timedelta(minutes=10)
    )

    # 6. Optional: notify success (placeholder)
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "ETL pipeline completed successfully."'
    )

    # 7. Optional: notify failure (placeholder)
    notify_failure = BashOperator(
        task_id="notify_failure",
        bash_command='echo "ETL pipeline failed. Check logs."',
        trigger_rule="one_failed"
    )

    # ---------- define dependencies ----------
    # generate_bronze is optional; skip it when not needed
    generate_bronze >> check_bronze
    check_bronze >> bronze_to_silver >> silver_to_gold >> load_to_snowflake >> notify_success
    # failure notification
    [bronze_to_silver, silver_to_gold, load_to_snowflake] >> notify_failure
