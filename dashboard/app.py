"""
StreamGuard — Live Fraud Analyst Dashboard
============================================
A Streamlit dashboard that connects to:
  - Kafka (for real-time transaction feed)
  - Snowflake (for fraud summary metrics)

Run with:
  streamlit run dashboard/app.py

Prerequisites:
  pip install streamlit plotly folium streamlit-folium kafka-python snowflake-connector-python python-dotenv
  Copy .env.example → .env and fill in Snowflake credentials
"""

import json
import os
import time
from datetime import datetime, timedelta
from threading import Thread
from collections import deque

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from kafka import KafkaConsumer
import snowflake.connector

load_dotenv()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="StreamGuard — Fraud Detection",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
REFRESH_INTERVAL = 3    # seconds between dashboard refreshes
MAX_FEED_ROWS = 200     # Keep last 200 transactions in the live feed

# ── Snowflake connection ──────────────────────────────────────────────────────
@st.cache_resource
def get_snowflake_conn():
    """Cache Snowflake connection across reruns."""
    try:
        return snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=os.getenv("SNOWFLAKE_DATABASE", "STREAMGUARD"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "STREAMGUARD_WH"),
        )
    except Exception as e:
        st.warning(f"Snowflake not connected: {e}. Using simulated data.")
        return None


def query_snowflake(sql: str, conn) -> pd.DataFrame:
    """Run a SQL query and return results as a DataFrame."""
    if conn is None:
        return pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cols = [desc[0].lower() for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Snowflake query error: {e}")
        return pd.DataFrame()


# ── Kafka live feed (runs in background thread) ───────────────────────────────
# We use a deque (double-ended queue) as a thread-safe circular buffer.
# The Kafka consumer thread pushes messages in; the dashboard reads from it.
if "transaction_buffer" not in st.session_state:
    st.session_state.transaction_buffer = deque(maxlen=MAX_FEED_ROWS)
    st.session_state.kafka_running = False


def start_kafka_consumer():
    """Background thread: consume fraud-alerts and push to buffer."""
    consumer = KafkaConsumer(
        "fraud-alerts",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        group_id="streamlit-dashboard",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    while st.session_state.get("kafka_running", False):
        try:
            for message in consumer:
                st.session_state.transaction_buffer.append(message.value)
        except Exception:
            time.sleep(1)
    consumer.close()


if not st.session_state.kafka_running:
    st.session_state.kafka_running = True
    Thread(target=start_kafka_consumer, daemon=True).start()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("StreamGuard")
    st.caption("Real-Time Fraud Detection")
    st.divider()

    st.subheader("Filters")
    risk_filter = st.multiselect(
        "Risk Level",
        options=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        default=["CRITICAL", "HIGH"],
    )
    time_window = st.selectbox(
        "Time Window",
        options=["Last 1 hour", "Last 24 hours", "Last 7 days", "All time"],
        index=1,
    )

    st.divider()
    st.subheader("Customer Lookup")
    customer_search = st.text_input("Customer ID", placeholder="CUST_00001")

    st.divider()
    st.caption("Auto-refreshes every 3 seconds")
    st.caption(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")


# ── Main dashboard ────────────────────────────────────────────────────────────
st.title("StreamGuard — Live Fraud Detection")
st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

conn = get_snowflake_conn()

# ── Row 1: KPI metrics ────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)

# Pull today's summary from Snowflake Gold layer
summary_df = query_snowflake("""
    SELECT * FROM STREAMGUARD.GOLD.FRAUD_SUMMARY_DAILY
    WHERE transaction_date = CURRENT_DATE()
""", conn)

if not summary_df.empty:
    row = summary_df.iloc[0]
    total_txns     = int(row.get("total_transactions", 0))
    flagged        = int(row.get("flagged_transactions", 0))
    flag_rate      = float(row.get("fraud_flag_rate_pct", 0))
    total_vol      = float(row.get("total_volume_usd", 0))
    flagged_vol    = float(row.get("flagged_volume_usd", 0))
else:
    # Show live buffer stats when Snowflake isn't connected yet
    buf = list(st.session_state.transaction_buffer)
    total_txns  = len(buf)
    flagged     = sum(1 for t in buf if t.get("fraud_score", 0) > 0)
    flag_rate   = (flagged / total_txns * 100) if total_txns > 0 else 0
    total_vol   = sum(t.get("amount", 0) for t in buf)
    flagged_vol = sum(t.get("amount", 0) for t in buf if t.get("fraud_score", 0) > 0)

with col1:
    st.metric("Total Transactions", f"{total_txns:,}")
with col2:
    st.metric("Flagged", f"{flagged:,}", delta=f"+{flagged}" if flagged > 0 else None, delta_color="inverse")
with col3:
    st.metric("Fraud Rate", f"{flag_rate:.2f}%", delta="~2% expected")
with col4:
    st.metric("Total Volume", f"${total_vol:,.0f}")
with col5:
    st.metric("Flagged Volume", f"${flagged_vol:,.0f}", delta_color="inverse")

st.divider()

# ── Row 2: Fraud rate gauge + live feed ──────────────────────────────────────
left, right = st.columns([1, 2])

with left:
    st.subheader("Fraud Rate Gauge")
    # Plotly gauge chart
    gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=flag_rate,
        delta={"reference": 2.0, "valueformat": ".2f"},
        title={"text": "Fraud Rate (%)"},
        gauge={
            "axis": {"range": [0, 10]},
            "bar": {"color": "darkred"},
            "steps": [
                {"range": [0, 2],   "color": "lightgreen"},
                {"range": [2, 5],   "color": "yellow"},
                {"range": [5, 10],  "color": "orangered"},
            ],
            "threshold": {
                "line": {"color": "red", "width": 4},
                "thickness": 0.75,
                "value": 5,
            },
        },
    ))
    gauge.update_layout(height=300, margin=dict(t=30, b=0, l=0, r=0))
    st.plotly_chart(gauge, use_container_width=True)

with right:
    st.subheader("Live Fraud Alert Feed")
    buf = list(st.session_state.transaction_buffer)
    if buf:
        feed_df = pd.DataFrame(buf)
        # Filter by selected risk levels
        if "risk_level" in feed_df.columns and risk_filter:
            feed_df = feed_df[feed_df["risk_level"].isin(risk_filter)]
        # Show most relevant columns
        display_cols = [c for c in [
            "customer_id", "amount", "merchant_category",
            "merchant_country", "risk_level", "fraud_score", "fraud_reason"
        ] if c in feed_df.columns]
        st.dataframe(
            feed_df[display_cols].head(20),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Waiting for fraud alerts from Kafka... (Is the Spark detector running?)")

st.divider()

# ── Row 3: Merchant risk chart + geo map ─────────────────────────────────────
left2, right2 = st.columns(2)

with left2:
    st.subheader("Fraud by Merchant Category")
    merchant_df = query_snowflake("""
        SELECT merchant_category, fraud_rate_pct, total_flags, merchant_risk_tier
        FROM STREAMGUARD.GOLD.MERCHANT_RISK_SCORES
        ORDER BY fraud_rate_pct DESC
        LIMIT 10
    """, conn)

    if not merchant_df.empty:
        color_map = {"CRITICAL": "red", "HIGH": "orange", "MEDIUM": "yellow", "LOW": "green"}
        fig = px.bar(
            merchant_df,
            x="fraud_rate_pct",
            y="merchant_category",
            orientation="h",
            color="merchant_risk_tier",
            color_discrete_map=color_map,
            labels={"fraud_rate_pct": "Fraud Rate (%)", "merchant_category": "Category"},
            title="Top 10 High-Risk Merchant Categories",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Merchant risk data loads after first dbt run")

with right2:
    st.subheader("Geographic Fraud Hotspots")
    buf = list(st.session_state.transaction_buffer)
    if buf:
        geo_df = pd.DataFrame([
            t for t in buf
            if t.get("fraud_score", 0) > 0
            and t.get("customer_lat") and t.get("customer_lon")
        ])
        if not geo_df.empty:
            fig_map = px.scatter_mapbox(
                geo_df,
                lat="customer_lat",
                lon="customer_lon",
                color="risk_level",
                size="fraud_score",
                hover_name="customer_id",
                hover_data=["amount", "merchant_country", "fraud_reason"],
                color_discrete_map={"CRITICAL": "red", "HIGH": "orange", "MEDIUM": "yellow"},
                mapbox_style="carto-darkmatter",
                zoom=1,
                title="Fraud Alert Locations",
            )
            fig_map.update_layout(height=400, margin=dict(t=30, b=0, l=0, r=0))
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.info("No geo-tagged fraud alerts yet")
    else:
        st.info("Waiting for fraud alerts...")

st.divider()

# ── Row 4: High-risk customers table ─────────────────────────────────────────
st.subheader("Top 10 High-Risk Customers")
customers_df = query_snowflake("""
    SELECT customer_id, customer_name, total_flags, fraud_rate_pct,
           max_fraud_score, risk_tier, last_flagged, countries_used
    FROM STREAMGUARD.GOLD.HIGH_RISK_CUSTOMERS
    ORDER BY fraud_rank
    LIMIT 10
""", conn)

if not customers_df.empty:
    st.dataframe(customers_df, use_container_width=True, hide_index=True)
else:
    st.info("High-risk customer data loads after first dbt run")

# ── Customer lookup ───────────────────────────────────────────────────────────
if customer_search:
    st.subheader(f"Customer Profile: {customer_search}")
    cust_df = query_snowflake(f"""
        SELECT * FROM STREAMGUARD.GOLD.HIGH_RISK_CUSTOMERS
        WHERE customer_id = '{customer_search.upper()}'
    """, conn)
    if not cust_df.empty:
        st.dataframe(cust_df, use_container_width=True, hide_index=True)

        # Show their recent alerts
        alerts_df = query_snowflake(f"""
            SELECT transaction_id, event_time, amount, merchant_name,
                   merchant_country, risk_level, fraud_reason
            FROM STREAMGUARD.PUBLIC.FRAUD_ALERTS
            WHERE customer_id = '{customer_search.upper()}'
            ORDER BY event_time DESC
            LIMIT 20
        """, conn)
        if not alerts_df.empty:
            st.write("Recent Alerts:")
            st.dataframe(alerts_df, use_container_width=True, hide_index=True)
    else:
        st.info(f"No fraud records found for {customer_search}")

# ── Auto-refresh ──────────────────────────────────────────────────────────────
time.sleep(REFRESH_INTERVAL)
st.rerun()
