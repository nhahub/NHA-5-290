import sys
import shutil
import argparse
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert

# Allow importing config from project root
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import DB_URL, ARCHIVE_DIR, get_daily_csv_path

engine = create_engine(DB_URL)

def insert_ignore_duplicates(table, conn, keys, data_iter):
    """Idempotent insert: ON CONFLICT (reading_id) DO NOTHING."""
    rows = [dict(zip(keys, r)) for r in data_iter]
    if not rows:
        return
    stmt = insert(table.table).values(rows).on_conflict_do_nothing(index_elements=["reading_id"])
    conn.execute(stmt)

def upsert_hourly_metrics(agg_df: pd.DataFrame):
    """Upsert hourly aggregations without dropping table constraints."""
    if agg_df.empty:
        return
    upsert_sql = text("""
        INSERT INTO agg_hourly_metrics (
            sensor_id, record_hour, avg_temp, max_temp, min_temp,
            avg_humidity, min_battery_pct, readings_count, anomalies_count
        ) VALUES (
            :sensor_id, :record_hour, :avg_temp, :max_temp, :min_temp,
            :avg_humidity, :min_battery_pct, :readings_count, :anomalies_count
        )
        ON CONFLICT (sensor_id, record_hour) DO UPDATE SET
            avg_temp = EXCLUDED.avg_temp,
            max_temp = EXCLUDED.max_temp,
            min_temp = EXCLUDED.min_temp,
            avg_humidity = EXCLUDED.avg_humidity,
            min_battery_pct = EXCLUDED.min_battery_pct,
            readings_count = EXCLUDED.readings_count,
            anomalies_count = EXCLUDED.anomalies_count;
    """)
    records = agg_df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(upsert_sql, records)

def run_pipeline(target_date: str, archive_after: bool = False):
    csv_path = get_daily_csv_path(target_date)
    print(f"Starting Batch ETL for date: {target_date} | File: {csv_path}")

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found for date {target_date}: {csv_path}")

    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError(f"CSV file {csv_path} is empty.")

    # 1. Data Quality Checks: Filter out missing or physically impossible readings
    initial_count = len(df)
    df.drop_duplicates(subset=["reading_id"], inplace=True)

    valid_mask = (
        df["reading_id"].notna() &
        df["sensor_id"].notna() &
        df["temperature_c"].between(-50.0, 100.0) &
        df["humidity_pct"].between(0.0, 100.0) &
        df["battery_pct"].between(0.0, 100.0)
    )
    rejected_df = df[~valid_mask].copy()
    df = df[valid_mask].copy()

    if not rejected_df.empty:
        audit_rows = pd.DataFrame({
            "reading_id": rejected_df["reading_id"].astype(str),
            "sensor_id": rejected_df["sensor_id"].astype(str),
            "raw_payload": rejected_df.to_json(orient="records", lines=True).splitlines(),
            "rejection_reason": "Out of physical bounds or null required fields"
        })
        audit_rows.to_sql("rejected_readings", engine, if_exists="append", index=False)
        print(f"Logged {len(rejected_df)} invalid rows to 'rejected_readings'.")

    # 2. Fetch zone thresholds from dim_sensors
    dim_df = pd.read_sql(
        "SELECT sensor_id, min_temp_threshold, max_temp_threshold, max_humidity_threshold FROM dim_sensors",
        engine
    )
    df = df.merge(dim_df, on="sensor_id", how="left")

    # 3. Vectorized Transformations (Fast & uses DB thresholds)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True).dt.tz_localize(None)
    
    df["is_anomaly"] = (
        (df["temperature_c"] > df["max_temp_threshold"]) |
        (df["temperature_c"] < df["min_temp_threshold"]) |
        (df["humidity_pct"] > df["max_humidity_threshold"])
    )

    df["temp_status"] = "Normal"
    warning_mask = (df["temperature_c"] >= (df["max_temp_threshold"] - 2.5)) & (df["temperature_c"] <= df["max_temp_threshold"])
    critical_mask = (df["temperature_c"] > df["max_temp_threshold"]) | (df["temperature_c"] < df["min_temp_threshold"])
    df.loc[warning_mask, "temp_status"] = "Warning"
    df.loc[critical_mask, "temp_status"] = "Critical"

    # 4. Load into fact_sensor_readings (Idempotent)
    fact_cols = [
        "reading_id", "sensor_id", "temperature_c", "humidity_pct",
        "battery_pct", "timestamp_utc", "temp_status", "is_anomaly"
    ]
    fact_df = df[fact_cols]
    fact_df.to_sql(
        "fact_sensor_readings",
        engine,
        if_exists="append",
        index=False,
        method=insert_ignore_duplicates
    )
    print(f"Processed {len(fact_df)} valid rows (from {initial_count} raw rows) into 'fact_sensor_readings'.")

    # 5. Hourly Aggregations (Upsert)
    df["record_hour"] = df["timestamp_utc"].dt.floor("h")
    agg_df = df.groupby(["sensor_id", "record_hour"]).agg(
        avg_temp=("temperature_c", "mean"),
        max_temp=("temperature_c", "max"),
        min_temp=("temperature_c", "min"),
        avg_humidity=("humidity_pct", "mean"),
        min_battery_pct=("battery_pct", "min"),
        readings_count=("reading_id", "count"),
        anomalies_count=("is_anomaly", "sum")
    ).reset_index()

    upsert_hourly_metrics(agg_df)
    print(f"Upserted {len(agg_df)} hourly summary rows into 'agg_hourly_metrics'.")

    # 6. Archive closed day files if requested
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if archive_after and target_date != today_str:
        dest = ARCHIVE_DIR / csv_path.name
        shutil.move(str(csv_path), str(dest))
        print(f"Archived closed day file to: {dest}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run IoT Batch ETL Pipeline")
    parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Target UTC date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--archive",
        action="store_true",
        help="Archive CSV file after successful load (only if not today's active file)"
    )
    args = parser.parse_args()
    run_pipeline(target_date=args.date, archive_after=args.archive)
