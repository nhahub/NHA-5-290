-- 1. Dimension Table: Sensors with realistic zone-specific thresholds
CREATE TABLE IF NOT EXISTS dim_sensors (
    sensor_id VARCHAR(50) PRIMARY KEY,
    zone_name VARCHAR(100) NOT NULL,
    min_temp_threshold FLOAT NOT NULL,
    max_temp_threshold FLOAT NOT NULL,
    max_humidity_threshold FLOAT NOT NULL,
    install_date DATE DEFAULT CURRENT_DATE
);

INSERT INTO dim_sensors (sensor_id, zone_name, min_temp_threshold, max_temp_threshold, max_humidity_threshold)
VALUES 
    ('SENSOR_01', 'Factory_Floor_1', 10.0, 40.0, 80.0),
    ('SENSOR_02', 'Server_Room_B',   12.0, 28.0, 65.0),
    ('SENSOR_03', 'Cold_Storage_A',  -2.0,  8.0, 85.0),
    ('SENSOR_04', 'Assembly_Line_2', 10.0, 38.0, 75.0),
    ('SENSOR_05', 'Packaging_Area',  10.0, 36.0, 75.0)
ON CONFLICT (sensor_id) DO UPDATE SET
    zone_name = EXCLUDED.zone_name,
    min_temp_threshold = EXCLUDED.min_temp_threshold,
    max_temp_threshold = EXCLUDED.max_temp_threshold,
    max_humidity_threshold = EXCLUDED.max_humidity_threshold;

-- 2. Fact Table: Cleaned Sensor Readings
CREATE TABLE IF NOT EXISTS fact_sensor_readings (
    reading_id VARCHAR(50) PRIMARY KEY,
    sensor_id VARCHAR(50) REFERENCES dim_sensors(sensor_id),
    temperature_c FLOAT NOT NULL,
    humidity_pct FLOAT NOT NULL,
    battery_pct FLOAT NOT NULL,
    timestamp_utc TIMESTAMP NOT NULL,
    temp_status VARCHAR(20) NOT NULL,
    is_anomaly BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_sensor_ts ON fact_sensor_readings (sensor_id, timestamp_utc DESC);

-- 3. Aggregate Table: Hourly Metrics (Includes Battery Health)
CREATE TABLE IF NOT EXISTS agg_hourly_metrics (
    sensor_id VARCHAR(50) REFERENCES dim_sensors(sensor_id),
    record_hour TIMESTAMP NOT NULL,
    avg_temp FLOAT,
    max_temp FLOAT,
    min_temp FLOAT,
    avg_humidity FLOAT,
    min_battery_pct FLOAT,
    readings_count INT,
    anomalies_count INT,
    PRIMARY KEY (sensor_id, record_hour)
);

-- 4. Fact Table: Real-Time IoT Alerts
CREATE TABLE IF NOT EXISTS fact_iot_alerts (
    alert_id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) REFERENCES dim_sensors(sensor_id),
    zone VARCHAR(100),
    temperature_c FLOAT,
    humidity_pct FLOAT,
    battery_pct FLOAT,
    alert_time TIMESTAMP NOT NULL,
    alert_type VARCHAR(50) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_time ON fact_iot_alerts (alert_time DESC);

-- 5. Data Quality Audit Table: Rejected Readings
CREATE TABLE IF NOT EXISTS rejected_readings (
    reading_id VARCHAR(50),
    sensor_id VARCHAR(50),
    raw_payload TEXT,
    rejection_reason VARCHAR(255),
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
