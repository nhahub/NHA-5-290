import json
import time
import random
import uuid
import csv
from datetime import datetime, timezone
from kafka import KafkaProducer
from config import KAFKA_SERVERS, KAFKA_TOPIC, CSV_HEADERS, SENSOR_PROFILES, get_daily_csv_path

# Initialize Kafka Producer with safety check
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    raise RuntimeError(f"Failed to connect to Kafka at {KAFKA_SERVERS}: {e}")

# Track stateful battery levels per sensor (starts between 85% and 100%)
battery_state = {sensor_id: round(random.uniform(85.0, 100.0), 2) for sensor_id in SENSOR_PROFILES}

print("Started IoT Simulator (Daily CSV + Kafka + Gradual Battery Drain)... Press Ctrl+C to stop.")

try:
    while True:
        sensor_id, profile = random.choice(list(SENSOR_PROFILES.items()))
        zone = profile["zone"]
        
        # Gradually drain battery (0.05% to 0.2% per reading); recharge if it hits 10%
        battery_state[sensor_id] = round(max(10.0, battery_state[sensor_id] - random.uniform(0.05, 0.20)), 2)
        if battery_state[sensor_id] <= 12.0 and random.random() < 0.2:
            battery_state[sensor_id] = 100.0  # Simulate battery replacement

        # 5% Critical Anomaly, 8% Warning Band, 87% Normal
        roll = random.random()
        if roll < 0.05:
            # Critical breach (High temp, Low temp, or High humidity)
            breach_mode = random.choice(["high_temp", "low_temp", "high_hum"])
            if breach_mode == "high_temp":
                temp = round(random.uniform(profile["max_temp"] + 0.5, profile["max_temp"] + 8.0), 1)
                humidity = round(random.uniform(*profile["normal_hum"]), 1)
            elif breach_mode == "low_temp":
                temp = round(random.uniform(profile["min_temp"] - 6.0, profile["min_temp"] - 0.5), 1)
                humidity = round(random.uniform(*profile["normal_hum"]), 1)
            else:
                temp = round(random.uniform(*profile["normal_temp"]), 1)
                humidity = round(random.uniform(profile["max_hum"] + 1.0, min(99.0, profile["max_hum"] + 15.0)), 1)
            status_label = "🚨 Anomaly"
        elif roll < 0.13:
            # Warning zone (within 2.5 degrees below max_temp)
            temp = round(random.uniform(profile["max_temp"] - 2.4, profile["max_temp"] - 0.1), 1)
            humidity = round(random.uniform(*profile["normal_hum"]), 1)
            status_label = "⚠️ Warning"
        else:
            # Normal operating range
            temp = round(random.uniform(*profile["normal_temp"]), 1)
            humidity = round(random.uniform(*profile["normal_hum"]), 1)
            status_label = "✅ Normal"

        now_utc = datetime.now(timezone.utc)
        data = {
            "reading_id": str(uuid.uuid4()),
            "sensor_id": sensor_id,
            "zone": zone,
            "temperature_c": temp,
            "humidity_pct": humidity,
            "battery_pct": battery_state[sensor_id],
            "timestamp_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        # 1. Send to Kafka
        producer.send(KAFKA_TOPIC, value=data)
        producer.flush()

        # 2. Write to today's CSV file (auto-create header if new day/file)
        csv_path = get_daily_csv_path(now_utc.strftime("%Y-%m-%d"))
        is_new_file = not csv_path.exists()
        with open(csv_path, mode="a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            if is_new_file:
                writer.writeheader()
            writer.writerow(data)

        print(
            f"[{now_utc.strftime('%H:%M:%S')}] {sensor_id} ({zone}) | "
            f"Temp: {temp}°C | Hum: {humidity}% | Bat: {battery_state[sensor_id]}% | {status_label}"
        )
        time.sleep(5)

except KeyboardInterrupt:
    print("\nSimulator stopped gracefully.")
finally:
    producer.close()
