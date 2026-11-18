import sys
import json
import time
from pathlib import Path
from datetime import datetime, timezone
import requests
import psycopg2
from kafka import KafkaConsumer

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import DB_DSN, KAFKA_SERVERS, KAFKA_TOPIC, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

def get_db_connection():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    return conn

def load_sensor_thresholds(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT sensor_id, zone_name, min_temp_threshold, max_temp_threshold, max_humidity_threshold FROM dim_sensors")
        rows = cur.fetchall()
    return {
        r[0]: {"zone": r[1], "min_temp": r[2], "max_temp": r[3], "max_hum": r[4]}
        for r in rows
    }

def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "PUT_YOUR_NEW_BOT_TOKEN_HERE":
        print("⚠️ Telegram token not configured in .env. Skipping Telegram push.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            print("✅ Telegram alert sent!")
        else:
            print(f"❌ Telegram API error: {resp.text}")
    except Exception as e:
        print(f"❌ Telegram request failed: {e}")

print("Connecting to PostgreSQL & loading zone thresholds...")
conn = get_db_connection()
thresholds = load_sensor_thresholds(conn)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    group_id="iot_alert_service_group",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=5000  # Allows periodic heartbeat check
)

# State tracking for cooldowns and offline heartbeat detection
last_alert_time = {}   # (sensor_id, alert_type) -> epoch_seconds
last_seen_time = {sid: time.time() for sid in thresholds}
ALERT_COOLDOWN_SEC = 60
OFFLINE_TIMEOUT_SEC = 60

print("🚨 Smart Alert Service RUNNING... Monitoring Temp, Humidity, Battery & Sensor Heartbeats.")

while True:
    try:
        for message in consumer:
            data = message.value
            sid = data["sensor_id"]
            zone = data["zone"]
            temp = float(data["temperature_c"])
            hum = float(data["humidity_pct"])
            bat = float(data["battery_pct"])
            ts = data["timestamp_utc"]

            now_epoch = time.time()
            last_seen_time[sid] = now_epoch

            rule = thresholds.get(sid, {"min_temp": 5.0, "max_temp": 40.0, "max_hum": 80.0})
            triggered_alerts = []

            if temp > rule["max_temp"]:
                triggered_alerts.append(("HIGH_TEMP_BREACH", f"Temp {temp}°C > Max {rule['max_temp']}°C"))
            elif temp < rule["min_temp"]:
                triggered_alerts.append(("LOW_TEMP_BREACH", f"Temp {temp}°C < Min {rule['min_temp']}°C"))

            if hum > rule["max_hum"]:
                triggered_alerts.append(("HIGH_HUMIDITY_BREACH", f"Humidity {hum}% > Max {rule['max_hum']}%"))

            if bat < 20.0:
                triggered_alerts.append(("LOW_BATTERY_ALERT", f"Battery critically low at {bat}%"))

            for alert_type, detail in triggered_alerts:
                cooldown_key = (sid, alert_type)
                if now_epoch - last_alert_time.get(cooldown_key, 0) < ALERT_COOLDOWN_SEC:
                    continue  # Skip duplicate spam within cooldown window

                last_alert_time[cooldown_key] = now_epoch
                alert_msg = (
                    f"🚨 *CRITICAL IOT ALERT* 🚨\n"
                    f"*Type:* `{alert_type}`\n"
                    f"*Sensor:* `{sid}` ({zone})\n"
                    f"*Detail:* {detail}\n"
                    f"*Readings:* Temp `{temp}°C` | Hum `{hum}%` | Bat `{bat}%`\n"
                    f"*Time:* `{ts}`"
                )
                print("\n" + "=" * 45 + "\n" + alert_msg + "\n" + "=" * 45)
                send_telegram_alert(alert_msg)

                # Safe DB insert with automatic reconnect
                try:
                    if conn.closed:
                        conn = get_db_connection()
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO fact_iot_alerts
                            (sensor_id, zone, temperature_c, humidity_pct, battery_pct, alert_time, alert_type)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (sid, zone, temp, hum, bat, ts, alert_type)
                        )
                except Exception as db_err:
                    print(f"❌ DB Insert Error (reconnecting): {db_err}")
                    conn = get_db_connection()

        # Check for offline sensors after consumer loop poll
        now_epoch = time.time()
        for sid, last_seen in last_seen_time.items():
            if now_epoch - last_seen > OFFLINE_TIMEOUT_SEC:
                cooldown_key = (sid, "SENSOR_OFFLINE_ALERT")
                if now_epoch - last_alert_time.get(cooldown_key, 0) >= ALERT_COOLDOWN_SEC:
                    last_alert_time[cooldown_key] = now_epoch
                    zone = thresholds[sid]["zone"]
                    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    offline_msg = (
                        f"⚠️ *SENSOR OFFLINE ALERT* ⚠️\n"
                        f"*Sensor:* `{sid}` ({zone})\n"
                        f"*Detail:* No data received for over {int(now_epoch - last_seen)} seconds!\n"
                        f"*Time:* `{now_iso}`"
                    )
                    print("\n" + offline_msg)
                    send_telegram_alert(offline_msg)

    except KeyboardInterrupt:
        print("\nAlert service stopped.")
        break
