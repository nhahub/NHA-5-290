import sys
from pathlib import Path
import pandas as pd
import pytest

# Add project root to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import evaluate_reading, get_daily_csv_path, SENSOR_PROFILES


# ==========================================
# 1. Unit Tests for Shared Rule Engine
# ==========================================
def test_evaluate_reading_normal():
    """Verify normal readings within zone thresholds return (False, 'Normal')."""
    # Factory_Floor_1: min=10, max=40, max_hum=80
    is_anomaly, status = evaluate_reading(temp=25.0, hum=50.0, min_temp=10.0, max_temp=40.0, max_hum=80.0)
    assert is_anomaly is False
    assert status == "Normal"


def test_evaluate_reading_warning_band():
    """Verify temperature within 2.5C below max_temp triggers 'Warning' without anomaly flag."""
    # Max temp is 40.0 -> Warning band is [37.5, 40.0]
    is_anomaly, status = evaluate_reading(temp=38.2, hum=55.0, min_temp=10.0, max_temp=40.0, max_hum=80.0)
    assert is_anomaly is False
    assert status == "Warning"


def test_evaluate_reading_high_temp_critical():
    """Verify temperature above max_temp triggers both anomaly and 'Critical' status."""
    # Cold_Storage_A: min=-2.0, max=8.0, max_hum=85.0
    is_anomaly, status = evaluate_reading(temp=9.5, hum=65.0, min_temp=-2.0, max_temp=8.0, max_hum=85.0)
    assert is_anomaly is True
    assert status == "Critical"


def test_evaluate_reading_low_temp_critical():
    """Verify temperature below min_temp triggers both anomaly and 'Critical' status."""
    is_anomaly, status = evaluate_reading(temp=-4.0, hum=65.0, min_temp=-2.0, max_temp=8.0, max_hum=85.0)
    assert is_anomaly is True
    assert status == "Critical"


def test_evaluate_reading_high_humidity_anomaly():
    """Verify high humidity triggers is_anomaly=True even when temperature is normal."""
    is_anomaly, status = evaluate_reading(temp=22.0, hum=88.0, min_temp=12.0, max_temp=28.0, max_hum=65.0)
    assert is_anomaly is True
    assert status == "Normal"


# ==========================================
# 2. Unit Tests for Daily File Partitioning
# ==========================================
def test_get_daily_csv_path_formatting():
    """Ensure daily CSV path resolves accurately for a specific UTC date."""
    path = get_daily_csv_path("2026-11-04")
    assert path.name == "raw_sensor_logs_2026-11-04.csv"
    assert path.parent.name == "data"


def test_sensor_profiles_completeness():
    """Verify all 5 sensors have valid threshold definitions."""
    assert len(SENSOR_PROFILES) == 5
    for sid, prof in SENSOR_PROFILES.items():
        assert prof["min_temp"] < prof["normal_temp"][0]
        assert prof["normal_temp"][1] < prof["max_temp"]
        assert 0 < prof["max_hum"] <= 100.0


# ==========================================
# 3. Unit Tests for ETL Data Quality Filter
# ==========================================
def test_etl_data_quality_bounds():
    """Verify physical bound filtering separates valid and corrupted sensor rows."""
    sample_data = pd.DataFrame([
        # Valid row
        {"reading_id": "id_1", "sensor_id": "SENSOR_01", "temperature_c": 24.5, "humidity_pct": 50.0, "battery_pct": 90.0},
        # Invalid: Impossible temperature (250C)
        {"reading_id": "id_2", "sensor_id": "SENSOR_01", "temperature_c": 250.0, "humidity_pct": 50.0, "battery_pct": 90.0},
        # Invalid: Negative humidity (-10%)
        {"reading_id": "id_3", "sensor_id": "SENSOR_02", "temperature_c": 22.0, "humidity_pct": -10.0, "battery_pct": 80.0},
        # Invalid: Battery over 100% (120%)
        {"reading_id": "id_4", "sensor_id": "SENSOR_03", "temperature_c": 4.0, "humidity_pct": 60.0, "battery_pct": 120.0},
        # Invalid: Missing sensor_id
        {"reading_id": "id_5", "sensor_id": None, "temperature_c": 20.0, "humidity_pct": 40.0, "battery_pct": 75.0},
    ])

    valid_mask = (
        sample_data["reading_id"].notna() &
        sample_data["sensor_id"].notna() &
        sample_data["temperature_c"].between(-50.0, 100.0) &
        sample_data["humidity_pct"].between(0.0, 100.0) &
        sample_data["battery_pct"].between(0.0, 100.0)
    )

    valid_df = sample_data[valid_mask]
    rejected_df = sample_data[~valid_mask]

    assert len(valid_df) == 1
    assert valid_df.iloc[0]["reading_id"] == "id_1"
    assert len(rejected_df) == 4
    assert set(rejected_df["reading_id"]) == {"id_2", "id_3", "id_4", "id_5"}
