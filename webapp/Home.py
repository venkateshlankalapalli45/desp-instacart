import streamlit as st

st.set_page_config(
    page_title="Instacart Reorder Predictor",
    page_icon="🛒",
    layout="wide",
)

st.title("🛒 Instacart Reorder Prediction")
st.markdown("""
Welcome to the Instacart Reorder Prediction app.

Use the sidebar to navigate:
- **Predict** — make single or batch predictions
- **Past Predictions** — explore historical predictions
""")
