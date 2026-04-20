import os

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://api:8000")

st.set_page_config(page_title="Prediction", page_icon="🔮", layout="wide")
st.title("🔮 Make a Prediction")
st.markdown("Predict whether a user will reorder — single or batch.")

tab_single, tab_batch = st.tabs(["Single Prediction", "Batch Prediction (CSV)"])

# ── Single ─────────────────────────────────────────────────────
with tab_single:
    with st.form("single_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            user_id = st.number_input("User ID", min_value=1, value=1, step=1)
        with col2:
            order_dow = st.selectbox(
                "Day of Week",
                options=list(range(7)),
                format_func=lambda x: ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"][x],
            )
            order_hour = st.slider("Hour of Day", 0, 23, 14)
        with col3:
            days_since = st.number_input(
                "Days Since Prior Order", min_value=0.0, max_value=30.0, value=7.0, step=0.5
            )

        submitted = st.form_submit_button("🔮 Predict", use_container_width=True)

    if submitted:
        payload = {"predictions": [{
            "user_id": user_id,
            "features": {
                "order_dow": order_dow,
                "order_hour_of_day": order_hour,
                "days_since_prior": days_since,
            },
            "source": "webapp",
        }]}
        try:
            resp = requests.post(f"{API_URL}/predict", json=payload, timeout=10)
            if resp.status_code == 200:
                result = resp.json()["predictions"][0]
                col_r, col_p = st.columns(2)
                label = "✅ Will Reorder" if result["reordered"] else "❌ Won't Reorder"
                col_r.metric("Prediction", label)
                col_p.metric("Probability", f"{result['probability']:.1%}")
            else:
                st.error(f"API error {resp.status_code}: {resp.text}")
        except requests.exceptions.ConnectionError:
            st.error("Cannot connect to the API. Is it running?")

# ── Batch ───────────────────────────────────────────────────────
with tab_batch:
    st.markdown("""
    Upload a CSV with columns: `user_id, order_dow, order_hour_of_day, days_since_prior`
    """)
    sample = pd.DataFrame([{
        "user_id": 1, "order_dow": 2,
        "order_hour_of_day": 14, "days_since_prior": 7.0,
    }])
    st.download_button(
        "⬇️ Download sample CSV",
        data=sample.to_csv(index=False),
        file_name="sample_instacart.csv",
        mime="text/csv",
    )

    uploaded = st.file_uploader("Upload CSV file", type=["csv"])
    if uploaded:
        try:
            df = pd.read_csv(uploaded)
            st.write(f"Loaded {len(df)} rows")
            st.dataframe(df.head(), use_container_width=True)

            if st.button("🔮 Run Batch Prediction"):
                required = ["user_id", "order_dow", "order_hour_of_day", "days_since_prior"]
                missing = [c for c in required if c not in df.columns]
                if missing:
                    st.error(f"Missing columns: {missing}")
                else:
                    records = []
                    for _, row in df.iterrows():
                        records.append({
                            "user_id": int(row["user_id"]),
                            "features": {
                                "order_dow": int(row["order_dow"]),
                                "order_hour_of_day": int(row["order_hour_of_day"]),
                                "days_since_prior": float(row["days_since_prior"]),
                            },
                            "source": "webapp",
                        })
                    payload = {"predictions": records}
                    with st.spinner("Predicting..."):
                        resp = requests.post(f"{API_URL}/predict", json=payload, timeout=60)
                    if resp.status_code == 200:
                        preds = resp.json()["predictions"]
                        result_df = df.copy()
                        result_df["reordered"] = [p["reordered"] for p in preds]
                        result_df["probability"] = [p["probability"] for p in preds]
                        st.success(f"✅ {len(preds)} predictions completed!")
                        st.dataframe(result_df, use_container_width=True)
                        st.download_button(
                            "⬇️ Download predictions",
                            data=result_df.to_csv(index=False),
                            file_name="predictions.csv",
                            mime="text/csv",
                        )
                    else:
                        st.error(f"API error {resp.status_code}: {resp.text}")
        except Exception as exc:
            st.error(f"Error processing file: {exc}")
