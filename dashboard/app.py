import sys
from pathlib import Path
from datetime import datetime, timezone
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import DB_URL, SENSOR_PROFILES, get_daily_csv_path, evaluate_reading

# Page Configuration
st.set_page_config(
    page_title="IoT Command Center",
    page_icon="🎛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Modern Industrial UI & Animated Status Cards
# Custom CSS for Modern Industrial UI & Uniform Card Heights
st.markdown("""
<style>
    /* Styling Metric Cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.9), rgba(15, 23, 42, 0.95));
        border: 1px solid rgba(255, 255, 255, 0.12);
        padding: 15px 20px;
        border-radius: 14px;
        color: #f8fafc !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.15);
    }
    div[data-testid="stMetric"] label, div[data-testid="stMetric"] div {
        color: #f8fafc !important;
    }
    /* Custom Sensor Zone Cards with uniform height */
    .sensor-card {
        padding: 16px;
        border-radius: 14px;
        margin-bottom: 12px;
        min-height: 185px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        color: #0f172a;
        transition: transform 0.2s ease;
        box-sizing: border-box;
    }
    .sensor-card:hover {
        transform: translateY(-3px);
    }
    .card-normal {
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.18), rgba(6, 78, 59, 0.30));
        border: 1.5px solid rgba(16, 185, 129, 0.55);
    }
    .card-warning {
        background: linear-gradient(135deg, rgba(245, 158, 11, 0.20), rgba(120, 53, 15, 0.35));
        border: 1.5px solid rgba(245, 158, 11, 0.7);
    }
    .card-critical {
        background: linear-gradient(135deg, rgba(239, 68, 68, 0.22), rgba(127, 29, 29, 0.40));
        border: 1.5px solid rgba(239, 68, 68, 0.85);
        animation: pulse-red 2s infinite;
    }
    @keyframes pulse-red {
        0% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.5); }
        70% { box-shadow: 0 0 0 10px rgba(239, 68, 68, 0); }
        100% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0); }
    }
    .badge {
        display: inline-block;
        padding: 2px 6px;
        border-radius: 6px;
        font-size: 0.72rem;
        font-weight: 700;
        white-space: nowrap;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.title("🎛️ Industrial IoT Telemetry & Analytics Command Center")
st.caption("Hybrid Lambda Architecture: Real-Time Speed Layer (Kafka/CSV) & Historical Batch Layer (PostgreSQL/Airflow)")

@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

engine = get_engine()

# --- Sidebar Controls ---
st.sidebar.header("⚙️ Control Panel")
all_zones = [p["zone"] for p in SENSOR_PROFILES.values()]
selected_zones = st.sidebar.multiselect("📍 Filter Zones", options=all_zones, default=all_zones)
window_size = st.sidebar.slider("📉 Live Chart Window (Readings)", min_value=30, max_value=300, value=100, step=10)
focus_zone = st.sidebar.selectbox("🎯 Highlight Zone Thresholds", options=["All Zones (Overview)"] + all_zones)

st.sidebar.divider()
st.sidebar.markdown("### 🗺️ Zone Threshold Reference")
for sid, prof in SENSOR_PROFILES.items():
    with st.sidebar.expander(f"{sid} — {prof['zone']}"):
        st.write(f"🌡️ **Temp Limits:** `{prof['min_temp']}°C` to `{prof['max_temp']}°C`")
        st.write(f"💧 **Max Humidity:** `{prof['max_hum']}%`")

tab_live, tab_history = st.tabs([
    "⚡ Live Speed Layer (Real-Time Telemetry)",
    "📦 Batch Layer (Historical Analytics & Data Quality)"
])

@st.fragment(run_every=3)
def render_live_tab():
    csv_path = get_daily_csv_path()
    if not csv_path.exists():
        st.warning(f"⏳ Waiting for today's telemetry file: `{csv_path.name}` (Ensure `iot_simulator.py` is running).")
        return

    try:
        df_live = pd.read_csv(csv_path)
    except Exception as e:
        st.error(f"Error reading live CSV: {e}")
        return

    if df_live.empty:
        st.info("No sensor readings recorded yet today.")
        return

    df_live["timestamp_utc"] = pd.to_datetime(df_live["timestamp_utc"], utc=True)

    # Evaluate status for every row dynamically using zone profiles
    statuses, anomalies = [], []
    for _, row in df_live.iterrows():
        prof = SENSOR_PROFILES.get(row["sensor_id"], {"min_temp": 0, "max_temp": 40, "max_hum": 80})
        is_anom, status = evaluate_reading(
            row["temperature_c"], row["humidity_pct"],
            prof["min_temp"], prof["max_temp"], prof["max_hum"]
        )
        anomalies.append(is_anom)
        statuses.append(status)
    df_live["is_anomaly"] = anomalies
    df_live["temp_status"] = statuses

    df_filtered = df_live[df_live["zone"].isin(selected_zones)].copy()
    if df_filtered.empty:
        st.warning("Please select at least one zone from the sidebar.")
        return

    # Fetch Alerts from PostgreSQL
    total_alerts = 0
    df_alerts = pd.DataFrame()
    df_alert_types = pd.DataFrame()
    try:
        with engine.connect() as conn:
            total_alerts = pd.read_sql("SELECT COUNT(*) AS cnt FROM fact_iot_alerts", conn).iloc[0]["cnt"]
            df_alerts = pd.read_sql(
                "SELECT alert_time, sensor_id, zone, alert_type, temperature_c, humidity_pct, battery_pct "
                "FROM fact_iot_alerts ORDER BY alert_time DESC LIMIT 12",
                conn
            )
            df_alert_types = pd.read_sql(
                "SELECT alert_type, COUNT(*) AS count FROM fact_iot_alerts GROUP BY alert_type",
                conn
            )
    except Exception as e:
        st.sidebar.error(f"DB Connection Warning: {e}")

    # --- 1. Top KPI Cards with Deltas ---
    latest_row = df_filtered.iloc[-1]
    avg_temp = df_filtered["temperature_c"].mean()
    avg_hum = df_filtered["humidity_pct"].mean()
    avg_bat = df_filtered["battery_pct"].mean()
    anom_rate = (df_filtered["is_anomaly"].sum() / len(df_filtered)) * 100

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("📡 Today's Readings", f"{len(df_filtered):,}", delta=f"Latest: {latest_row['sensor_id']}")
    k2.metric("🌡️ Avg Temperature", f"{avg_temp:.1f} °C", delta=f"{latest_row['temperature_c'] - avg_temp:+.1f} °C (Latest)")
    k3.metric("💧 Avg Humidity", f"{avg_hum:.1f} %", delta=f"{latest_row['humidity_pct'] - avg_hum:+.1f} % (Latest)")
    k4.metric("🔋 Fleet Battery Avg", f"{avg_bat:.1f} %", delta=f"{df_filtered['battery_pct'].min():.1f}% Min")
    k5.metric("🚨 Total Alerts (Anomaly %)", f"{int(total_alerts)}", delta=f"{anom_rate:.1f}% rate", delta_color="inverse")

    st.markdown("---")

    # --- 2. Interactive Zone Health Cards ---
    
    st.subheader("🏭 Live Zone Status & Sensor Heartbeats")
    now_utc = datetime.now(timezone.utc)
    latest_per_sensor = df_live.sort_values("timestamp_utc").groupby("sensor_id").last().reset_index()

    cols = st.columns(len(SENSOR_PROFILES))
    for idx, (sid, prof) in enumerate(SENSOR_PROFILES.items()):
        with cols[idx]:
            row = latest_per_sensor[latest_per_sensor["sensor_id"] == sid]
            if not row.empty:
                r = row.iloc[0]
                lag_sec = (now_utc - r["timestamp_utc"]).total_seconds()
                is_online = lag_sec <= 60
                online_text = "🟢 ONLINE" if is_online else f"🔴 OFFLINE ({int(lag_sec)}s)"
                t_status = r["temp_status"]

                card_class = "card-normal"
                if not is_online or t_status == "Critical" or r["is_anomaly"]:
                    card_class = "card-critical"
                elif t_status == "Warning" or r["battery_pct"] < 25:
                    card_class = "card-warning"

                st.markdown(f"""
                <div class="sensor-card {card_class}">
                    <div>
                        <div style="display:flex; flex-direction:column; align-items:flex-start; gap:2px; margin-bottom:4px;">
                            <span class="badge">{online_text}</span>
                            <span style="font-weight:800; font-size:1.1rem;">{sid}</span>
                        </div>
                        <div style="font-size:0.82rem; opacity:0.85;">📍 {prof['zone']}</div>
                    </div>
                    <div style="margin: 10px 0;">
                        <div style="font-size:1.35rem; font-weight:800; line-height:1.2;">🌡️ {r['temperature_c']}°C</div>
                        <div style="font-size:1.0rem; font-weight:600; margin-top:4px;">💧 {r['humidity_pct']}%</div>
                    </div>
                    <div style="font-size:0.78rem; opacity:0.9;">
                        Status: <b>{t_status}</b><br>Limits: [{prof['min_temp']}°..{prof['max_temp']}°C]
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.progress(int(r["battery_pct"]), text=f"🔋 Battery: {r['battery_pct']:.1f}%")
            else:
                st.info(f"{sid} ({prof['zone']}): Waiting for signal...")

    # --- 3. Real-Time Gauges & Status Distribution ---
    st.subheader("🧭 Fleet Gauges & Telemetry Distribution")
    g1, g2, g3 = st.columns([1, 1, 1.2])

    with g1:
        fig_gauge_temp = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=latest_row["temperature_c"],
            delta={"reference": avg_temp, "position": "top"},
            title={"text": f"Latest Temp ({latest_row['zone']})"},
            gauge={
                "axis": {"range": [-10, 55]},
                "bar": {"color": "#38bdf8"},
                "steps": [
                    {"range": [-10, 10], "color": "rgba(56, 189, 248, 0.2)"},
                    {"range": [10, 35], "color": "rgba(16, 185, 129, 0.2)"},
                    {"range": [35, 55], "color": "rgba(239, 68, 68, 0.3)"}
                ],
                "threshold": {"line": {"color": "#ef4444", "width": 4}, "thickness": 0.75, "value": 40}
            }
        ))
        fig_gauge_temp.update_layout(height=260, margin=dict(l=20, r=20, t=45, b=10), paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_gauge_temp, use_container_width=True)

    with g2:
        fig_gauge_hum = go.Figure(go.Indicator(
            mode="gauge+number",
            value=latest_row["humidity_pct"],
            title={"text": f"Latest Humidity ({latest_row['zone']})"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#818cf8"},
                "steps": [
                    {"range": [0, 75], "color": "rgba(129, 140, 248, 0.15)"},
                    {"range": [75, 100], "color": "rgba(239, 68, 68, 0.3)"}
                ],
                "threshold": {"line": {"color": "#f43f5e", "width": 4}, "thickness": 0.75, "value": 80}
            }
        ))
        fig_gauge_hum.update_layout(height=260, margin=dict(l=20, r=20, t=45, b=10), paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_gauge_hum, use_container_width=True)

    with g3:
        status_counts = df_filtered["temp_status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        color_map = {"Normal": "#10b981", "Warning": "#f59e0b", "Critical": "#ef4444"}
        fig_donut = px.pie(
            status_counts, names="Status", values="Count", hole=0.55,
            color="Status", color_discrete_map=color_map,
            title="Live Reading Status Breakdown"
        )
        fig_donut.update_layout(height=260, margin=dict(l=10, r=10, t=45, b=10), paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_donut, use_container_width=True)

    # --- 4. Interactive Plotly Spline Charts ---
    st.subheader(f"📈 Live Telemetry Stream (Last {window_size} Readings)")
    df_tail = df_filtered.tail(window_size)
    if focus_zone != "All Zones (Overview)":
        df_tail = df_tail[df_tail["zone"] == focus_zone]

    c_chart1, c_chart2 = st.columns(2)
    with c_chart1:
        fig_temp = px.line(
            df_tail, x="timestamp_utc", y="temperature_c", color="zone",
            markers=True, line_shape="spline",
            title="🌡️ Temperature (°C) Stream by Zone"
        )
        if focus_zone != "All Zones (Overview)":
            z_prof = next(p for p in SENSOR_PROFILES.values() if p["zone"] == focus_zone)
            fig_temp.add_hline(y=z_prof["max_temp"], line_dash="dash", line_color="#ef4444", annotation_text="Max Threshold")
            fig_temp.add_hline(y=z_prof["min_temp"], line_dash="dash", line_color="#38bdf8", annotation_text="Min Threshold")
        fig_temp.update_layout(height=350, margin=dict(l=10, r=10, t=40, b=10), legend_title_text="Zone")
        st.plotly_chart(fig_temp, use_container_width=True)

    with c_chart2:
        fig_hum = px.area(
            df_tail, x="timestamp_utc", y="humidity_pct", color="zone",
            line_shape="spline",
            title="💧 Humidity (%) Area Stream by Zone"
        )
        if focus_zone != "All Zones (Overview)":
            z_prof = next(p for p in SENSOR_PROFILES.values() if p["zone"] == focus_zone)
            fig_hum.add_hline(y=z_prof["max_hum"], line_dash="dash", line_color="#ef4444", annotation_text="Max Humidity")
        fig_hum.update_layout(height=350, margin=dict(l=10, r=10, t=40, b=10), legend_title_text="Zone")
        st.plotly_chart(fig_hum, use_container_width=True)

    # --- 5. Zone Variance Box Plot & Alert Breakdown ---
    b1, b2 = st.columns([1.3, 1])
    with b1:
        fig_box = px.box(
            df_filtered, x="zone", y="temperature_c", color="zone",
            points="outliers", title="📊 Temperature Spread & Outliers per Zone"
        )
        fig_box.update_layout(height=320, margin=dict(l=10, r=10, t=40, b=10), showlegend=False)
        st.plotly_chart(fig_box, use_container_width=True)

    with b2:
        if not df_alert_types.empty:
            fig_alerts_bar = px.bar(
                df_alert_types, x="count", y="alert_type", orientation="h",
                color="alert_type", title="🚨 Triggered Alerts by Category (DB)"
            )
            fig_alerts_bar.update_layout(height=320, margin=dict(l=10, r=10, t=40, b=10), showlegend=False)
            st.plotly_chart(fig_alerts_bar, use_container_width=True)
        else:
            st.info("No alerts logged in database yet.")

    # --- 6. Styled Live Alerts Feed ---
    st.subheader("⚠️ Real-Time Incident Log (`fact_iot_alerts`)")
    if not df_alerts.empty:
        st.dataframe(
            df_alerts,
            column_config={
                "alert_time": st.column_config.DatetimeColumn("Timestamp (UTC)", format="YYYY-MM-DD HH:mm:ss"),
                "temperature_c": st.column_config.NumberColumn("Temp (°C)", format="%.1f °C"),
                "humidity_pct": st.column_config.NumberColumn("Humidity (%)", format="%.1f %%"),
                "battery_pct": st.column_config.ProgressColumn("Battery (%)", min_value=0, max_value=100, format="%.1f%%")
            },
            width="stretch"
        )
    else:
        st.success("✅ All industrial zones are operating within safe parameters.")

with tab_live:
    render_live_tab()

with tab_history:
    st.subheader("📦 Warehouse Batch Analytics (`agg_hourly_metrics`)")
    try:
        with engine.connect() as conn:
            df_agg = pd.read_sql(
                "SELECT a.record_hour, a.sensor_id, d.zone_name, a.avg_temp, a.min_temp, a.max_temp, "
                "a.avg_humidity, a.min_battery_pct, a.readings_count, a.anomalies_count "
                "FROM agg_hourly_metrics a JOIN dim_sensors d ON a.sensor_id = d.sensor_id "
                "ORDER BY a.record_hour DESC",
                conn
            )
            df_rejected = pd.read_sql("SELECT * FROM rejected_readings ORDER BY rejected_at DESC LIMIT 25", conn)

        if not df_agg.empty:
            h1, h2 = st.columns(2)
            with h1:
                # Heatmap of Average Temperature per Zone across Hours
                heat_pivot = df_agg.pivot(index="zone_name", columns="record_hour", values="avg_temp")
                fig_heat = px.imshow(
                    heat_pivot, aspect="auto", color_continuous_scale="RdYlBu_r",
                    title="🔥 Hourly Average Temperature Heatmap by Zone"
                )
                st.plotly_chart(fig_heat, use_container_width=True)

            with h2:
                fig_anom_bar = px.bar(
                    df_agg, x="record_hour", y="anomalies_count", color="zone_name",
                    barmode="group", title="🚨 Hourly Anomalies Count per Zone"
                )
                st.plotly_chart(fig_anom_bar, use_container_width=True)

            st.markdown("#### 📋 Hourly Summary Table")
            st.dataframe(df_agg, width="stretch")
        else:
            st.info("No hourly aggregations yet. Run `python3 etl/batch_etl.py` or trigger the Airflow DAG.")

        st.markdown("---")
        st.subheader("🛡️ Data Quality Quarantine (`rejected_readings`)")
        if not df_rejected.empty:
            st.dataframe(df_rejected, width="stretch")
        else:
            st.success("✅ 0 corrupted readings quarantined. 100% Data Quality pass rate!")

    except Exception as e:
        st.error(f"Could not load historical batch analytics: {e}")