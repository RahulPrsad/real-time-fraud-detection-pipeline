"""
Streamlit Fraud Detection Dashboard
=====================================
Uses streamlit-autorefresh for safe periodic refresh — no while-loop,
no st.rerun(), avoids React #185 infinite update error.
"""

import glob
import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./output")
REFRESH_SECS = 15

st.set_page_config(
    page_title="Fraud Detection Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Trigger a safe re-run every REFRESH_SECS seconds
st_autorefresh(interval=REFRESH_SECS * 1000, key="dashboard_refresh")


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_parquet_dir(path: str) -> pd.DataFrame:
    all_files = glob.glob(f"{path}/**/*.parquet", recursive=True)
    files = [f for f in all_files if "_temporary" not in f]
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception:
            pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_data():
    transactions = load_parquet_dir(f"{OUTPUT_PATH}/transactions")
    fraud_alerts  = load_parquet_dir(f"{OUTPUT_PATH}/fraud_alerts")
    return transactions, fraud_alerts


# ── Load ──────────────────────────────────────────────────────────────────────

transactions, fraud = load_data()

all_txns = pd.concat(
    [df for df in [transactions, fraud] if not df.empty],
    ignore_index=True,
) if (not transactions.empty or not fraud.empty) else pd.DataFrame()

total       = len(all_txns)
fraud_count = len(fraud)
fraud_rate  = fraud_count / max(total, 1) * 100

# ── Title ─────────────────────────────────────────────────────────────────────

st.title("Real-Time Fraud Detection Dashboard")
st.caption(
    f"Auto-refreshes every {REFRESH_SECS}s · last run: "
    f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
)

# ── KPIs ──────────────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Transactions", f"{total:,}")
c2.metric("Fraud Alerts",       f"{fraud_count:,}")
c3.metric("Fraud Rate",         f"{fraud_rate:.2f}%")
c4.metric(
    "Avg Fraud Amount",
    f"${fraud['amount'].mean():,.2f}" if not fraud.empty else "—",
)

st.divider()

# ── Table + Pie ───────────────────────────────────────────────────────────────

col_left, col_right = st.columns([3, 2])

with col_left:
    st.subheader("Latest Fraud Alerts")
    if fraud.empty:
        st.info("No fraud alerts yet — waiting for Spark output…")
    else:
        show_cols = [c for c in ["transaction_id", "user_id", "amount", "city",
                                  "fraud_reason", "timestamp"] if c in fraud.columns]
        display = fraud[show_cols].sort_values("timestamp", ascending=False).head(20)

        def highlight_fraud(val):
            return (
                "background-color: #fff3f3; color: #8b0000"
                if isinstance(val, str) and "high_value" in val
                else ""
            )

        styled = display.style
        if "fraud_reason" in display.columns:
            styled = styled.map(highlight_fraud, subset=["fraud_reason"])
        st.dataframe(styled, use_container_width=True)

with col_right:
    st.subheader("Fraud by Reason")
    if not fraud.empty and "fraud_reason" in fraud.columns:
        reason_counts = fraud["fraud_reason"].value_counts().reset_index()
        reason_counts.columns = ["Reason", "Count"]
        fig = px.pie(
            reason_counts, values="Count", names="Reason",
            color_discrete_sequence=["#e63946", "#f4a261", "#2a9d8f"],
            hole=0.4,
        )
        fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=280)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for labelled fraud records…")

st.divider()

# ── Time-series ───────────────────────────────────────────────────────────────

st.subheader("⏱ Transaction Volume Over Time")
if not all_txns.empty and "timestamp" in all_txns.columns:
    try:
        all_txns["ts"]     = pd.to_datetime(all_txns["timestamp"], utc=True, errors="coerce")
        all_txns           = all_txns.dropna(subset=["ts"])
        all_txns["minute"] = all_txns["ts"].dt.floor("min")
        ts_counts = (
            all_txns.groupby(["minute", "is_fraud"])
            .size().reset_index(name="count")
        )
        fig2 = px.bar(
            ts_counts, x="minute", y="count", color="is_fraud",
            color_discrete_map={True: "#e63946", False: "#457b9d"},
            labels={"minute": "Time", "count": "Transactions", "is_fraud": "Fraud"},
            barmode="stack",
        )
        fig2.update_layout(margin=dict(t=10, b=10), height=260)
        st.plotly_chart(fig2, use_container_width=True)
    except Exception as e:
        st.warning(f"Chart error: {e}")
else:
    st.info("Waiting for transaction data…")

# ── Geo map ───────────────────────────────────────────────────────────────────

if not fraud.empty and "latitude" in fraud.columns and "longitude" in fraud.columns:
    st.subheader(" Fraud Alert Locations")
    fig3 = px.scatter_geo(
        fraud.dropna(subset=["latitude", "longitude"]),
        lat="latitude", lon="longitude",
        color="amount", size="amount",
        hover_name="city",
        hover_data=["user_id", "fraud_reason", "amount"],
        color_continuous_scale="Reds",
        projection="natural earth",
        title="",
    )
    fig3.update_layout(margin=dict(t=0, b=0), height=340)
    st.plotly_chart(fig3, use_container_width=True)