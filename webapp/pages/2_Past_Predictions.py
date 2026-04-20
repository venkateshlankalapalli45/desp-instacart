import os
from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://api:8000")

st.set_page_config(page_title="Past Predictions", page_icon="📊", layout="wide")
st.title("📊 Past Predictions")
st.markdown("Explore historical predictions made by the model.")

with st.form("filter_form"):
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        start_date = st.date_input("Start Date", value=date.today() - timedelta(days=7))
    with col2:
        end_date = st.date_input("End Date", value=date.today())
    with col3:
        source = st.selectbox("Prediction Source", options=["all", "webapp", "scheduled"])
    with col4:
        limit = st.number_input("Max Rows", min_value=1, max_value=5000, value=500, step=50)

    search = st.form_submit_button("🔍 Fetch Predictions", use_container_width=True)

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
                df["created_at"] = pd.to_datetime(df["created_at"])

                total = len(df)
                reorder_count = int(df["prediction_result"].sum())
                m1, m2, m3 = st.columns(3)
                m1.metric("Total Predictions", total)
                m2.metric("Will Reorder", f"{reorder_count} ({reorder_count/total:.1%})")
                m3.metric("Won't Reorder", total - reorder_count)

                st.markdown("---")

                # Daily reorder rate chart
                chart_df = df.copy()
                chart_df["date"] = chart_df["created_at"].dt.date
                daily = (
                    chart_df.groupby("date")
                    .agg(total=("prediction_result", "count"),
                         reordered=("prediction_result", "sum"))
                    .reset_index()
                )
                daily["reorder_rate"] = daily["reordered"] / daily["total"]
                st.subheader("📈 Daily Reorder Rate")
                st.line_chart(daily.set_index("date")["reorder_rate"])

                st.subheader("📋 Prediction Records")
                st.dataframe(df, use_container_width=True)

                st.download_button(
                    "⬇️ Download as CSV",
                    data=df.to_csv(index=False),
                    file_name="past_predictions.csv",
                    mime="text/csv",
                )
        else:
            st.error(f"API error {resp.status_code}: {resp.text}")
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to the API. Is it running?")
