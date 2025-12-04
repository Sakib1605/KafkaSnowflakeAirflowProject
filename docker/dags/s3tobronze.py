import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import snowflake.connector
from dotenv import load_dotenv

# ------------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# ------------------------------------------------------
load_dotenv(dotenv_path="/opt/airflow/dags/.env")

# ------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------

# MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "bronze/")

# Snowflake
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "UBER_BRONZE")

# Local Temp File
LOCAL_TEMP_PATH = os.getenv("LOCAL_TEMP_PATH", "/tmp/uber_raw.json")


# ------------------------------------------------------
# EXTRACT FROM MINIO
# ------------------------------------------------------

def extract_from_minio():
    """
    Extract all JSON files from MinIO bronze folder and combine into a local file.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=MINIO_PREFIX)
    contents = response.get("Contents", [])

    all_events = []

    for obj in contents:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        data = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        lines = data["Body"].read().decode("utf-8").splitlines()

        for line in lines:
            try:
                all_events.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    with open(LOCAL_TEMP_PATH, "w") as f:
        json.dump(all_events, f)

    print(f"✅ Extracted {len(all_events)} Uber events → {LOCAL_TEMP_PATH}")
    return LOCAL_TEMP_PATH


# ------------------------------------------------------
# LOAD INTO SNOWFLAKE (BRONZE)
# ------------------------------------------------------

def load_raw_to_snowflake(**context):
    """
    Load raw Uber ride-sharing JSON events into Snowflake Bronze table.
    """
    file_path = context["ti"].xcom_pull(task_ids="extract_data")

    with open(file_path, "r") as f:
        events = json.load(f)

    if not events:
        print("⚠️ No Uber events found to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    # Bronze Table Schema (supports all event types)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        event_id STRING,
        event_type STRING,
        timestamp STRING,

        user_id STRING,
        driver_id STRING,
        vehicle_type STRING,

        pickup_lat FLOAT,
        pickup_lng FLOAT,
        dropoff_lat FLOAT,
        dropoff_lng FLOAT,

        trip_distance FLOAT,
        trip_minutes FLOAT,
        fare_amount FLOAT,

        payment_type STRING,
        region STRING,

        surge_multiplier FLOAT,  -- surge events

        lat FLOAT,               -- driver location
        lng FLOAT,
        status STRING
    );
    """

    cur.execute(create_table_sql)

    insert_sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} (
            event_id, event_type, timestamp,
            user_id, driver_id, vehicle_type,
            pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
            trip_distance, trip_minutes, fare_amount,
            payment_type, region,
            surge_multiplier,
            lat, lng, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for event in events:
        cur.execute(insert_sql, (
            event.get("event_id"),
            event.get("event_type"),
            event.get("timestamp"),

            event.get("user_id"),
            event.get("driver_id"),
            event.get("vehicle_type"),

            event.get("pickup_lat"),
            event.get("pickup_lng"),
            event.get("dropoff_lat"),
            event.get("dropoff_lng"),

            event.get("trip_distance"),
            event.get("trip_minutes"),
            event.get("fare_amount"),

            event.get("payment_type"),
            event.get("region"),

            event.get("surge_multiplier"),

            event.get("lat"),
            event.get("lng"),
            event.get("status"),
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"🚀 Loaded {len(events)} Uber events into Snowflake Bronze table: {SNOWFLAKE_TABLE}")


# ------------------------------------------------------
# AIRFLOW DAG
# ------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "uber_minio_to_snowflake_bronze",
    default_args=default_args,
    description="Load raw Uber ride-sharing events (Bronze) into Snowflake",
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_from_minio
    )

    load_task = PythonOperator(
        task_id="load_raw_to_snowflake",
        python_callable=load_raw_to_snowflake,
        provide_context=True
    )

    extract_task >> load_task
