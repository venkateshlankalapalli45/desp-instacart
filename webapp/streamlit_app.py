import streamlit as st

st.set_page_config(
    page_title="Instacart Prediction Service",
    page_icon="🛒",
    layout="wide",
)

st.title("🛒 Instacart Reorder Prediction")
st.markdown("""
Welcome to the Instacart Reorder Prediction service.

Use the sidebar to navigate:
- **Prediction** — make single or batch predictions
- **Past Predictions** — explore historical predictions
""")
