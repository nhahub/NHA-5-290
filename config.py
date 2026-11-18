import os
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables from .env
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Directories
DATA_DIR = BASE_DIR / "data"
ARCHIVE_DIR = BASE_DIR / "archive"
DATA_DIR.mkdir(exist_ok=True)
ARCHIVE_DIR.mkdir(exist_ok=True)

# Database & Kafka Configurations
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://localhost:5432/iot_db")
DB_DSN = os.getenv("DB_DSN", "dbname=iot_db host=localhost port=5432")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot_sensor_data")

# Telegram Configurations
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# CSV Headers
CSV_HEADERS = [
    "reading_id", "sensor_id", "zone",
    "temperature_c", "humidity_pct", "battery_pct", "timestamp_utc"
]

# Zone-specific profiles (Realistic normal ranges & thresholds per sensor)
SENSOR_PROFILES = {
    "SENSOR_01": {
        "zone": "Factory_Floor_1",
        "normal_temp": (20.0, 34.0),
        "min_temp": 10.0, "max_temp": 40.0,
        "normal_hum": (35.0, 65.0), "max_hum": 80.0
    },
    "SENSOR_02": {
        "zone": "Server_Room_B",
        "normal_temp": (18.0, 24.0),
        "min_temp": 12.0, "max_temp": 28.0,
        "normal_hum": (30.0, 50.0), "max_hum": 65.0
    },
    "SENSOR_03": {
        "zone": "Cold_Storage_A",
        "normal_temp": (2.0, 5.5),
        "min_temp": -2.0, "max_temp": 8.0,
        "normal_hum": (55.0, 75.0), "max_hum": 85.0
    },
    "SENSOR_04": {
        "zone": "Assembly_Line_2",
        "normal_temp": (19.0, 32.0),
        "min_temp": 10.0, "max_temp": 38.0,
        "normal_hum": (35.0, 60.0), "max_hum": 75.0
    },
    "SENSOR_05": {
        "zone": "Packaging_Area",
        "normal_temp": (18.0, 30.0),
        "min_temp": 10.0, "max_temp": 36.0,
        "normal_hum": (30.0, 60.0), "max_hum": 75.0
    }
}

def get_daily_csv_path(date_str: str = None) -> Path:
    """Return the CSV path for a given UTC date (YYYY-MM-DD)."""
    if not date_str:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return DATA_DIR / f"raw_sensor_logs_{date_str}.csv"

def evaluate_reading(temp: float, hum: float, min_temp: float, max_temp: float, max_hum: float):
    """Shared rule engine for ETL and Streaming Alert Service."""
    warning_margin = 2.5
    is_anomaly = bool(temp > max_temp or temp < min_temp or hum > max_hum)
    
    if temp > max_temp or temp < min_temp:
        temp_status = "Critical"
    elif temp >= (max_temp - warning_margin):
        temp_status = "Warning"
    else:
        temp_status = "Normal"
        
    return is_anomaly, temp_status
