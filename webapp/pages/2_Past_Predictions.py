import os
from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Past Predictions", page_icon="📊", layout="wide")
st.title("📊 Past Predictions")
st.markdown("Explore historical predictions made by the model.")

# ── Filters ──────────────────────────────────────────────────────
with st.form("filter_form"):
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        start_date = st.date_input(
            "Start Date",
            value=date.today() - timedelta(days=7),
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=date.today(),
        )
    with col3:
        source = st.selectbox(
            "Prediction Source",
            options=["all", "webapp", "scheduled"],
            index=0,
        )
    with col4:
        limit = st.number_input(
            "Max Rows",
            min_value=1,
            max_value=5000,
            value=500,
            step=50,
        )

    search = st.form_submit_button("🔍 Fetch Predictions", use_container_width=True)

# ── Results ───────────────────────────────────────────────────────
if search:
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "source": source,
        "limit": limit,
    }
    try:
        resp = requests.get(f"{API_URL}/past-predictions", params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if not data:
                st.info("No predictions found for the selected filters.")
            else:
                df = pd.DataFrame(data)
                df["predicted_at"] = pd.to_datetime(df["predicted_at"])
                df["reordered"] = df["reordered"].map({True: "✅ Yes", False: "❌ No"})
                df["probability"] = df["probability"].apply(lambda x: f"{x:.1%}")

                # ── Summary metrics ───────────────────────────────
                total = len(df)
                reorder_count = (df["reordered"] == "✅ Yes").sum()
                reorder_pct = reorder_count / total * 100 if total else 0

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Total Predictions", total)
                m2.metric("Will Reorder", f"{reorder_count} ({reorder_pct:.1f}%)")
                m3.metric("Won't Reorder", f"{total - reorder_count}")
                m4.metric("Sources", df["source"].nunique() if "source" in df.columns else "-")

                st.markdown("---")

                # ── Reorder rate over time chart ──────────────────
                chart_df = pd.DataFrame(data)
                chart_df["predicted_at"] = pd.to_datetime(chart_df["predicted_at"])
                chart_df["date"] = chart_df["predicted_at"].dt.date
                daily = (
                    chart_df.groupby("date")
                    .agg(total=("reordered", "count"), reordered=("reordered", "sum"))
                    .reset_index()
                )
                daily["reorder_rate"] = daily["reordered"] / daily["total"]

                st.subheader("📈 Daily Reorder Rate")
                st.line_chart(daily.set_index("date")["reorder_rate"])

                # ── Source breakdown ──────────────────────────────
                if "source" in df.columns:
                    st.subheader("🗂️ Predictions by Source")
                    source_counts = chart_df["source"].value_counts().reset_index()
                    source_counts.columns = ["Source", "Count"]
                    st.bar_chart(source_counts.set_index("Source"))

                # ── Data table ────────────────────────────────────
                st.subheader("📋 Prediction Records")
                display_cols = [
                    "id", "predicted_at", "order_dow", "order_hour_of_day",
                    "days_since_prior_order", "add_to_cart_order",
                    "department_id", "aisle_id",
                    "reordered", "probability", "model_version", "source",
                ]
                display_cols = [c for c in display_cols if c in df.columns]
                st.dataframe(df[display_cols], use_container_width=True)

                # ── Download ──────────────────────────────────────
                raw_df = pd.DataFrame(data)
                st.download_button(
                    "⬇️ Download as CSV",
                    data=raw_df.to_csv(index=False),
                    file_name="past_predictions.csv",
                    mime="text/csv",
                )
        else:
            st.error(f"API error {resp.status_code}: {resp.text}")
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to the API. Is it running?")
