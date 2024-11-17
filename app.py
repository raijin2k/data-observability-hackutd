# app.py
import streamlit as st
from src.creation import DataCreationTracker
from src.access import AccessMonitor
from src.movement import DataMovementTracker
from src.usage import UsageAnalytics

def main():
    st.set_page_config(
        page_title="Data Observability Dashboard",
        layout="wide"
    )

    # Sidebar for navigation
    page = st.sidebar.selectbox(
        "Navigate",
        ["Creation", "Access", "Movement", "Usage"]
    )

    if page == "Creation":
        st.title("Data Creation Tracking")
        creation_tracker = DataCreationTracker()
        # Implement creation tracking UI

    elif page == "Access":
        st.title("Access Monitoring")
        access_monitor = AccessMonitor()
        # Implement access monitoring UI

    elif page == "Movement":
        st.title("Data Movement Tracking")
        movement_tracker = DataMovementTracker()
        # Implement movement tracking UI

    elif page == "Usage":
        st.title("Usage Analytics")
        usage_analytics = UsageAnalytics()
        # Implement usage analytics UI

if __name__ == "__main__":
    main()