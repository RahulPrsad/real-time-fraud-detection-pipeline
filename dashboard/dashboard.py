"""
Streamlit Fraud Detection Dashboard
=====================================
Auto-refreshes every 15 seconds by re-reading Parquet output from Spark.
"""

import glob
import os
import time
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./output")
REFRESH_SECS = 15

st.set_page_config(
    page_title="Fraud Detection Dashboard",
   
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_parquet_dir(path: str) -> pd.DataFrame:
    files = glob.glob(f"{path}/**/*.parquet", recursive=True)
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def load_data():
    transactions = load_parquet_dir(f"{OUTPUT_PATH}/transactions")
    fraud_alerts = load_parquet_dir(f"{OUTPUT_PATH}/fraud_alerts")
    return transactions, fraud_alerts


# ── UI ────────────────────────────────────────────────────────────────────────

st.title("Real-Time Fraud Detection Dashboard")
st.caption(f"Data sourced from Spark output · auto-refreshes every {REFRESH_SECS}s")

placeholder = st.empty()

while True:
    transactions, fraud = load_data()

    all_txns = pd.concat([transactions, fraud], ignore_index=True) if not fraud.empty or not transactions.empty else pd.DataFrame()
    total = len(all_txns)
    fraud_count = len(fraud)
    fraud_rate = fraud_count / max(total, 1) * 100

    with placeholder.container():

        # ── KPI row ───────────────────────────────────────────────────────────
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(" Total Transactions", f"{total:,}")
        c2.metric(" Fraud Alerts", f"{fraud_count:,}", delta=None)
        c3.metric(" Fraud Rate", f"{fraud_rate:.2f}%")
        c4.metric(
            " Avg Fraud Amount",
            f"${fraud['amount'].mean():,.2f}" if not fraud.empty else "—"
        )

        st.divider()

        # ── Fraud alerts table ────────────────────────────────────────────────
        col_left, col_right = st.columns([3, 2])

        with col_left:
            st.subheader(" Latest Fraud Alerts")
            if fraud.empty:
                st.info("No fraud alerts yet — waiting for Spark output…")
            else:
                show_cols = ["transaction_id", "user_id", "amount", "city",
                             "fraud_reason", "timestamp"]
                show_cols = [c for c in show_cols if c in fraud.columns]
                display = fraud[show_cols].sort_values("timestamp", ascending=False).head(20)
                st.dataframe(
                    display.style.applymap(
                        lambda v: "background-color: #fff3f3; color: #8b0000" if isinstance(v, str) and "high_value" in v else "",
                        subset=["fraud_reason"] if "fraud_reason" in display.columns else []
                    ),
                    use_container_width=True,
                )

        with col_right:
            st.subheader(" Fraud by Reason")
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

        # ── Time-series chart ─────────────────────────────────────────────────
        st.subheader("⏱ Transaction Volume Over Time")
        if not all_txns.empty and "timestamp" in all_txns.columns:
            try:
                all_txns["ts"] = pd.to_datetime(all_txns["timestamp"], utc=True, errors="coerce")
                all_txns = all_txns.dropna(subset=["ts"])
                all_txns["minute"] = all_txns["ts"].dt.floor("min")
                ts_counts = all_txns.groupby(["minute", "is_fraud"]).size().reset_index(name="count")

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

        # ── Geo heatmap ───────────────────────────────────────────────────────
        if not fraud.empty and "latitude" in fraud.columns and "longitude" in fraud.columns:
            st.subheader(" Fraud Alert Locations")
            fig3 = px.scatter_geo(
                fraud.dropna(subset=["latitude", "longitude"]),
                lat="latitude", lon="longitude",
                color="amount",
                size="amount",
                hover_name="city",
                hover_data=["user_id", "fraud_reason", "amount"],
                color_continuous_scale="Reds",
                projection="natural earth",
                title="",
            )
            fig3.update_layout(margin=dict(t=0, b=0), height=340)
            st.plotly_chart(fig3, use_container_width=True)

        st.caption(f"Last refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    time.sleep(REFRESH_SECS)
    st.rerun()
