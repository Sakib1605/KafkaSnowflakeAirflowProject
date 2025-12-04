import os
import json
import time
import uuid
import random
from faker import Faker
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# ----------------------------------------------------
# Load environment variables
# ----------------------------------------------------
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "uber-events")
EVENT_INTERVAL_SECONDS = int(os.getenv("EVENT_INTERVAL_SECONDS", 1))

fake = Faker()

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ----------------------------------------------------
# Static Lookups
# ----------------------------------------------------
regions = [
    "Downtown", "Midtown", "Uptown", "West End",
    "East Side", "Financial District", "Airport"
]

vehicle_types = ["UberX", "UberXL", "Uber Black", "Uber Comfort", "Uber Green"]
payment_types = ["Uber Pay", "Cash", "Google Pay", "Amazon Pay"]
trip_status_events = ["trip_requested", "driver_assigned", "trip_started", "trip_completed"]

# ----------------------------------------------------
# Generate Users (NO SEGMENTS)
# ----------------------------------------------------
def generate_users(n=50):
    return [{"user_id": str(uuid.uuid4())} for _ in range(n)]

# ----------------------------------------------------
# Generate Drivers
# ----------------------------------------------------
def generate_drivers(n=25):
    drivers = []
    for _ in range(n):
        drivers.append({
            "driver_id": str(uuid.uuid4()),
            "vehicle_type": random.choice(vehicle_types),
            "rating": round(random.uniform(3.8, 5.0), 2)
        })
    return drivers

users = generate_users()
drivers = generate_drivers()

# ----------------------------------------------------
# Helper – realistic Toronto coordinates
# ----------------------------------------------------
def random_lat_lng():
    return {
        "lat": round(random.uniform(43.60, 43.75), 6),
        "lng": round(random.uniform(-79.50, -79.20), 6)
    }

# ----------------------------------------------------
# TRIP EVENT GENERATOR — LOGICALLY CONSISTENT
# ----------------------------------------------------
def generate_trip_event():
    user = random.choice(users)
    driver = random.choice(drivers)
    region = random.choice(regions)

    pickup = random_lat_lng()
    dropoff = random_lat_lng()

    event_type = random.choice(trip_status_events)

    # Base event structure
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",

        "user_id": user["user_id"],
        "driver_id": driver["driver_id"],
        "vehicle_type": driver["vehicle_type"],
        "driver_rating": driver["rating"],
        "region": region,

        "pickup_lat": pickup["lat"],
        "pickup_lng": pickup["lng"],
        "dropoff_lat": dropoff["lat"],
        "dropoff_lng": dropoff["lng"],

        # defaults
        "trip_distance": 0,
        "trip_minutes": 0,
        "fare_amount": 0,
        "payment_type": None
    }

    # Only trip_completed gets distance, fare, minutes
    if event_type == "trip_completed":
        distance = round(random.uniform(1.0, 15.0), 2)
        minutes = random.randint(5, 35)
        fare = round(3 + distance * random.uniform(1.2, 2.0), 2)

        event["trip_distance"] = distance
        event["trip_minutes"] = minutes
        event["fare_amount"] = fare
        event["payment_type"] = random.choice(payment_types)

    return event


# ----------------------------------------------------
# SURGE PRICING EVENT
# ----------------------------------------------------
def generate_surge_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "surge_pricing",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "region": random.choice(regions),
        "surge_multiplier": round(random.uniform(1.1, 2.8), 2)
    }


# ----------------------------------------------------
# DRIVER LOCATION UPDATE
# ----------------------------------------------------
def generate_driver_location_update():
    driver = random.choice(drivers)
    loc = random_lat_lng()

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "driver_location_update",
        "timestamp": datetime.utcnow().isoformat() + "Z",

        "driver_id": driver["driver_id"],
        "vehicle_type": driver["vehicle_type"],
        "driver_rating": driver["rating"],

        "lat": loc["lat"],
        "lng": loc["lng"],
        "status": random.choice(["online", "offline", "on_trip"])
    }


# ----------------------------------------------------
# MAIN EVENT SELECTOR
# ----------------------------------------------------
def generate_event():
    r = random.random()
    if r < 0.70:
        return generate_trip_event()
    elif r < 0.85:
        return generate_surge_event()
    else:
        return generate_driver_location_update()


# ----------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------
if __name__ == "__main__":
    print("🚖 Starting Uber simulator (NO user segments)...")
    print(f"Users: {len(users)}, Drivers: {len(drivers)}")

    while True:
        event = generate_event()
        producer.send(KAFKA_TOPIC, event)
        print(f"Produced → {event['event_type']} | user={event.get('user_id')} | driver={event.get('driver_id')}")
        time.sleep(EVENT_INTERVAL_SECONDS)
