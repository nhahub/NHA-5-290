# 🚀 Real-Time IoT Data Pipeline & Analytics System

**Group:** DKH5_AIS5_S2 | **Repository:** `NHA-5-290`  
**Architecture:** Hybrid / Lambda Data Pipeline (Speed Layer + Batch Layer)

An end-to-end, production-grade Industrial IoT Data Engineering pipeline. This system simulates real-time telemetry from 5 industrial zones, streams live events through **Apache Kafka** for instant **Telegram Bot** alerting, orchestrates daily idempotent **Batch ETL** workflows using **Apache Airflow**, persists clean star-schema data into **PostgreSQL**, and visualizes both real-time and historical metrics on an interactive **Streamlit** dashboard.

---

## 📋 Table of Contents
1. [System Architecture](#-system-architecture)
2. [Key Engineering Features](#-key-engineering-features)
3. [Project Directory Structure](#-project-directory-structure)
4. [Prerequisites & System Installation](#-prerequisites--system-installation)
5. [Step-by-Step Project Setup](#-step-by-step-project-setup)
6. [How to Run the Pipeline (Execution Order)](#-how-to-run-the-pipeline-execution-order)
7. [Orchestration with Apache Airflow](#-orchestration-with-apache-airflow)
8. [Running Automated Unit Tests](#-running-automated-unit-tests)
9. [Troubleshooting Common Issues](#-troubleshooting-common-issues)
10. [Team & Role Distribution](#-team--role-distribution)

---

## 🏗️ System Architecture

```text
                       ┌────────────────────────────────────┐
                       │          iot_simulator.py          │
                       │ (5 Industrial Zones, Battery Drain)│
                       └─────────────────┬──────────────────┘
                                         │
                      ┌──────────────────┴──────────────────┐
                      │ Dual-Write Ingestion                │
                      ▼ (Speed Layer)                       ▼ (Batch Layer)
           ┌─────────────────────┐               ┌────────────────────────┐
           │    Apache Kafka     │               │  Daily Partitioned CSV │
           │  (iot_sensor_data)  │               │ (raw_sensor_logs_*.csv)│
           └──────────┬──────────┘               └───────────┬────────────┘
                      │                                      │
                      ▼ Real-Time Consumer                   ▼ Scheduled / Triggered
           ┌─────────────────────┐               ┌────────────────────────┐
           │  alert_service.py   │               │     Apache Airflow     │
           │ (Cooldown + Offline)│               │     (batch_etl.py)     │
           └────┬───────────┬────┘               └───────────┬────────────┘
                │           │                                │
                ▼           ▼                                ▼
      ┌──────────────┐   ┌──────────────────────────────────────────────┐
      │ Telegram Bot │   │             PostgreSQL (iot_db)              │
      │ Instant Push │   │ • dim_sensors          • agg_hourly_metrics  │
      └──────────────┘   │ • fact_sensor_readings • rejected_readings   │
                         │ • fact_iot_alerts                            │
                         └───────────────────────┬──────────────────────┘
                                                 │
                                                 ▼
                                 ┌──────────────────────────────┐
                                 │     Streamlit Dashboard      │
                                 │ (Live Telemetry + DB History)│
                                 └──────────────────────────────┘
```

---

## ✨ Key Engineering Features

1. **Zone-Specific Profiles (`dim_sensors`):** Each industrial sensor operates under realistic physical thresholds (e.g., `Cold_Storage_A` requires $-2^\circ\text{C}$ to $8^\circ\text{C}$, while `Factory_Floor_1` tolerates up to $40^\circ\text{C}$).
2. **Full Telemetry Utilization:** Captures temperature, humidity, stateful gradual battery drain (`battery_pct`), and sensor heartbeats (`timestamp_utc`) to detect both environmental breaches and offline devices.
3. **Race-Condition Free Daily Partitioning:** The simulator writes to daily logs (`raw_sensor_logs_YYYY-MM-DD.csv`) with automatic header initialization, allowing the ETL to safely archive closed days.
4. **Idempotent & Upsert Batch ETL:**
   * Fact loads use `ON CONFLICT (reading_id) DO NOTHING` to prevent duplicate failures on re-runs.
   * Hourly aggregations use `ON CONFLICT (sensor_id, record_hour) DO UPDATE` (Upsert) to preserve primary/foreign key constraints.
5. **Automated Data Quality Auditing:** Physically impossible or corrupted rows are filtered out and logged into a dedicated `rejected_readings` table with rejection reasons.
6. **Smart Alerting with Cooldown:** Prevents Telegram notification spam via a 60-second per-sensor cooldown window and alerts when any sensor goes offline for $>60$ seconds or drops below $20\%$ battery.

---

## 📂 Project Directory Structure

```text
iot_pipeline/
├── .env                    # Environment variables (Tokens, DB URLs - Git ignored)
├── .gitignore              # Prevents committing secrets, cache, and raw CSV data
├── README.md               # Complete setup and operational documentation
├── requirements.txt        # Python package dependencies
├── config.py               # Centralized configuration, sensor profiles & rule engine
├── schema.sql              # PostgreSQL DDL (Star Schema, Indexes, Audit tables)
├── iot_simulator.py        # Dual-write IoT data generator (Kafka + Daily CSV)
├── data/                   # Active daily CSV logs generated by the simulator
├── archive/                # Processed CSV logs archived by the Batch ETL
├── etl/
│   └── batch_etl.py        # Idempotent Batch ETL & Data Quality script
├── streaming/
│   └── alert_service.py    # Real-time Kafka consumer & Telegram alert service
├── dashboard/
│   └── app.py              # Interactive Streamlit dashboard (Live + Historical tabs)
└── tests/
    └── test_pipeline.py    # Pytest unit test suite (8 automated tests)
```

---

## 🛠️ Prerequisites & System Installation

Before setting up the Python project, ensure **Python 3.11+**, **Apache Kafka**, and **PostgreSQL** are installed and running on your machine.

### For macOS (using Homebrew):
```bash
# 1. Install Kafka and PostgreSQL
brew install kafka postgresql

# 2. Start both services in the background
brew services start kafka
brew services start postgresql
```

### For Ubuntu / Debian Linux (or WSL2):
```bash
sudo apt update
sudo apt install -y python3-pip python3-venv postgresql postgresql-contrib default-jre
sudo service postgresql start
# Ensure Apache Kafka is downloaded and running on localhost:9092
```

---

## ⚙️ Step-by-Step Project Setup

Follow these exact steps to get the project running on any machine:

### Step 1: Clone the Repository
Place the project in a standard directory (avoid `Desktop` on macOS so Airflow background processes have read/write permissions):
```bash
mkdir -p ~/projects
cd ~/projects
git clone https://github.com/<your-username>/NHA-5-290.git iot_pipeline
cd iot_pipeline
```

### Step 2: Create & Activate a Python Virtual Environment (Recommended)
```bash
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Python Dependencies
```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

### Step 4: Create the PostgreSQL Database & Tables
```bash
# Create the database
createdb iot_db

# Execute the schema script to create tables, indexes, and seed dim_sensors
psql -d iot_db -f schema.sql
```

### Step 5: Configure Environment Variables (`.env`)
Create a `.env` file in the root directory (`~/projects/iot_pipeline/.env`):
```bash
nano .env
```
Paste the following configuration and replace the Telegram placeholders with your actual credentials:
```ini
TELEGRAM_BOT_TOKEN=your_botfather_token_here
TELEGRAM_CHAT_ID=your_telegram_chat_id_here
DB_URL=postgresql+psycopg2://localhost:5432/iot_db
DB_DSN=dbname=iot_db host=localhost port=5432
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=iot_sensor_data
```
> **💡 How to get Telegram Credentials (Optional but Recommended):**
> 1. Message `@BotFather` on Telegram and send `/newbot` to get your `TELEGRAM_BOT_TOKEN`.
> 2. Open a chat with your newly created bot and click **Start**.
> 3. Message `@userinfobot` on Telegram to get your numeric `TELEGRAM_CHAT_ID`.

---

## 🚀 How to Run the Pipeline (Execution Order)

To run the complete end-to-end system locally, open **4 Terminal windows** inside `~/projects/iot_pipeline` (ensure your virtual environment is activated if you created one):

### Terminal 1: Start the IoT Data Simulator (Milestone 1)
Generates sensor readings every 5 seconds, sends JSON events to Kafka, and appends rows to today's CSV file (`data/raw_sensor_logs_YYYY-MM-DD.csv`).
```bash
python3 iot_simulator.py
```

### Terminal 2: Start the Real-Time Alert Service (Milestone 3)
Listens to the Kafka topic in real time, checks readings against zone thresholds in `dim_sensors`, logs breaches into `fact_iot_alerts`, and sends instant Telegram notifications.
```bash
python3 streaming/alert_service.py
```

### Terminal 3: Run the Batch ETL Pipeline (Milestone 2)
Cleans today's CSV file, logs any corrupted records to `rejected_readings`, performs idempotent inserts into `fact_sensor_readings`, and upserts hourly metrics into `agg_hourly_metrics`.
```bash
python3 etl/batch_etl.py
```
*(You can also pass a specific date: `python3 etl/batch_etl.py --date 2026-11-04`)*

### Terminal 4: Launch the Live & Historical Dashboard (Milestone 4)
Opens the interactive Streamlit web application in your browser at `http://localhost:8501`.
```bash
streamlit run dashboard/app.py
```
* **Tab 1 (Live Speed Layer):** Auto-refreshes every 3 seconds without blocking the UI, showing live KPIs, per-sensor online/offline heartbeat status, battery drain progress bars, zone charts, and real-time PostgreSQL alerts.
* **Tab 2 (Batch Layer):** Displays historical hourly aggregations from `agg_hourly_metrics`, temperature status distributions (`Normal`, `Warning`, `Critical`), and the `rejected_readings` Data Quality audit table.

---

## ⏱️ Orchestration with Apache Airflow

To automate the Batch ETL pipeline on a daily schedule using **Apache Airflow 3**:

1. **Set Airflow Home & Copy the DAG:**
   ```bash
   export AIRFLOW_HOME=~/airflow
   mkdir -p ~/airflow/dags
   cp dags/iot_etl_dag.py ~/airflow/dags/
   ```
   *(Make sure `PROJECT_ROOT` inside `~/airflow/dags/iot_etl_dag.py` matches your local project path).*

2. **Start Airflow Standalone Server:**
   ```bash
   airflow standalone
   ```
   Airflow will initialize its SQLite database, start the scheduler and API server, and print the `admin` password in the terminal.

3. **Trigger & Monitor the DAG:**
   * Open `http://localhost:8080` and log in with username `admin` and the generated password.
   * Locate **`iot_batch_etl_pipeline`**, unpause the toggle switch, and click the **Play (▶) -> Trigger DAG** button.
   * Watch the 3 tasks (`check_daily_csv_exists` ➔ `run_etl_and_aggregations` ➔ `archive_processed_csv`) turn green!

---

## 🧪 Running Automated Unit Tests

The repository includes a `pytest` suite (`tests/test_pipeline.py`) that verifies the rule engine (`Normal`, `Warning`, `Critical`, and humidity anomalies), daily file path partitioning, sensor profile bounds, and Data Quality filtering.

Run the tests from the project root using:
```bash
python3 -m pytest -v
```
Expected output: **`8 passed`** in green.

---

## 🔧 Troubleshooting Common Issues

* **`KafkaUnavailableError` or Connection Refused on Port 9092:**
  Ensure Kafka is running via `brew services start kafka` (macOS). If it crashes, restart it with `brew services restart kafka`.
* **`psycopg2.OperationalError: connection to server at "localhost" failed`:**
  Ensure PostgreSQL is running (`brew services start postgresql`) and that you created the database (`createdb iot_db`) and ran `psql -d iot_db -f schema.sql`.
* **`zsh: /opt/homebrew/bin/pytest: bad interpreter`:**
  Always invoke tools using `python3 -m pytest -v` or `python3 -m streamlit run dashboard/app.py` to guarantee your active Python interpreter is used.
* **No Telegram Messages Received:**
  Verify that `.env` has no quotes around the values, that you clicked **Start** inside your bot's chat on Telegram, and that `TELEGRAM_CHAT_ID` is your numeric user ID (from `@userinfobot`), not your phone number.

---