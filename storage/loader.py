import json
import os
from kafka import KafkaConsumer
from datetime import datetime
import boto3
from dotenv import load_dotenv

# --------------------------------------------
# Load environment variables
# --------------------------------------------
load_dotenv()

# --------------------------------------------
# Configuration
# --------------------------------------------
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "uber-events")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "uber-consumer")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))

# --------------------------------------------
# Connect to MinIO (S3 Compatible)
# --------------------------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Idempotent bucket creation
try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket '{MINIO_BUCKET}' already exists.")
except Exception:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    print(f"Created bucket '{MINIO_BUCKET}'.")

# --------------------------------------------
# Kafka Consumer Setup
# --------------------------------------------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"🚖 Listening for Uber events on Kafka topic '{KAFKA_TOPIC}'...")

batch = []

# --------------------------------------------
# Consume & Store in MinIO (Bronze Layer)
# --------------------------------------------
for message in consumer:
    event = message.value
    batch.append(event)

    if len(batch) >= BATCH_SIZE:
        now = datetime.utcnow()

        # Partitioning: bronze/date=YYYY-MM-DD/hour=HH/uber_events_timestamp.json
        date_path = now.strftime("date=%Y-%m-%d/hour=%H")
        file_name = f"uber_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        file_path = f"bronze/{date_path}/{file_name}"

        # Write newline-delimited JSON (NDJSON)
        json_data = "\n".join(json.dumps(e) for e in batch)

        # Upload to MinIO
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=file_path,
            Body=json_data.encode("utf-8")
        )

        print(f"✅ Uploaded {len(batch)} Uber events → {file_path}")

        batch = []
