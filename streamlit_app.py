# streamlit_app.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from src.core.database import DatabaseManager, SimpleAnomalyDetector

class DataObservabilityDashboard:
    def __init__(self):
        self.db = DatabaseManager()
        self.detector = SimpleAnomalyDetector()

    def main(self):
        # Page configuration
        st.set_page_config(
            page_title="Data Observability Dashboard",
            layout="wide",
            initial_sidebar_state="expanded"
        )

        # Main title
        st.title("Data Observability Dashboard")
        st.markdown("---")

        # Sidebar filters
        self.render_sidebar()

        # Main content tabs
        tabs = st.tabs(["Overview", "Creation", "Access", "Movement", "Usage", "Anomaly"])

        with tabs[0]:
            self.render_overview()

        with tabs[1]:
            self.render_creation_metrics()

        with tabs[2]:
            self.render_access_patterns()

        with tabs[3]:
            self.render_movement_tracking()

        with tabs[4]:
            self.render_usage_analytics()

        with tabs[5]:
            self.render_load_analysis()

    def render_sidebar(self):
        st.sidebar.header("Filters")
        
        # Date range selector
        default_start = datetime.now() - timedelta(days=7)
        default_end = datetime.now()
        
        start_date = st.sidebar.date_input(
            "Start Date",
            value=default_start,
            max_value=default_end
        )
        
        end_date = st.sidebar.date_input(
            "End Date",
            value=default_end,
            min_value=start_date,
            max_value=default_end
        )

        # Store dates in session state
        st.session_state['start_date'] = datetime.combine(start_date, datetime.min.time())
        st.session_state['end_date'] = datetime.combine(end_date, datetime.max.time())

        # Refresh button
        if st.sidebar.button("Refresh Data"):
            st.experimental_rerun()

    def render_overview(self):
        st.subheader("System Overview")
        
        # Get metrics from different sources
        creation_metrics = self.db.get_creation_metrics(
            st.session_state['start_date'],
            st.session_state['end_date']
        )
        
        access_patterns = self.db.get_access_patterns(
            st.session_state['start_date'],
            st.session_state['end_date']
        )
        
        movement_data = self.db.get_movement_data(
            st.session_state['start_date'],
            st.session_state['end_date']
        )

        # Display key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Data Points",
                f"{creation_metrics['total_count']:,}" if creation_metrics else "N/A",
                "Last 7 days"
            )

        with col2:
            st.metric(
                "Active Users",
                len(access_patterns['by_user']) if access_patterns else "N/A",
                "Unique users"
            )

        with col3:
            st.metric(
                "Data Movements",
                f"{movement_data['summary']['total_movements']:,}" if movement_data else "N/A",
                "Total transfers"
            )

        with col4:
            st.metric(
                "System Health",
                "✅ Good",
                "All systems operational"
            )

    def render_creation_metrics(self):
        st.subheader("Data Creation Analysis")
        
        metrics = self.db.get_creation_metrics(
            st.session_state['start_date'],
            st.session_state['end_date']
        )
        
        if not metrics:
            st.warning("No creation metrics available")
            return

        col1, col2 = st.columns(2)

        with col1:
            # Creation trend over time
            df_trend = pd.DataFrame(metrics['trend_data'])
            if not df_trend.empty:
                fig = px.line(
                    df_trend,
                    x='date',
                    y='count',
                    color='source',
                    title='Data Creation Trend'
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Source distribution pie chart
            source_df = pd.DataFrame({
                'Source': list(metrics['by_source'].keys()),
                'Count': list(metrics['by_source'].values())
            })
            fig = px.pie(
                source_df,
                values='Count',
                names='Source',
                title='Data Creation by Source'
            )
            st.plotly_chart(fig, use_container_width=True)

        # Hourly pattern heatmap
        hour_df = pd.DataFrame({
            'Hour': list(metrics['by_hour'].keys()),
            'Count': list(metrics['by_hour'].values())
        })
        fig = px.bar(
            hour_df,
            x='Hour',
            y='Count',
            title='Creation Pattern by Hour'
        )
        st.plotly_chart(fig, use_container_width=True)

    def render_access_patterns(self):
        st.subheader("Access Pattern Analysis")
        
        patterns = self.db.get_access_patterns(
            st.session_state['start_date'],
            st.session_state['end_date']
        )
        
        if not patterns:
            st.warning("No access patterns available")
            return

        # Access pattern over time
        time_df = pd.DataFrame({
            'Timestamp': list(patterns['by_hour'].keys()),
            'Count': list(patterns['by_hour'].values())
        })
        
        fig = px.line(
            time_df,
            x='Timestamp',
            y='Count',
            title='Access Pattern Over Time'
        )
        st.plotly_chart(fig, use_container_width=True)

        
    def render_movement_tracking(self):
        st.subheader("Data Movement Tracking")
        
        movement_data = self.db.get_movement_data(
            st.session_state['start_date'],
            st.session_state['end_date']
        )
        
        if not movement_data:
            st.warning("No movement data available")
            return

        col1, col2 = st.columns(2)

        with col1:
            # Movement status distribution
            fig = px.pie(
                values=list(movement_data['summary']['by_status'].values()),
                names=list(movement_data['summary']['by_status'].keys()),
                title='Movement Status Distribution'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Source distribution
            fig = px.bar(
                x=list(movement_data['summary']['by_source'].keys()),
                y=list(movement_data['summary']['by_source'].values()),
                title='Data Movement by Source'
            )
            st.plotly_chart(fig, use_container_width=True)

        # Movement timeline
        df_movements = pd.DataFrame(movement_data['movements'])
        fig = px.line(
            df_movements,
            x='hour',
            y='movement_count',
            color='status',
            title='Data Movement Timeline'
        )
        st.plotly_chart(fig, use_container_width=True)

    def render_usage_analytics(self):
        st.subheader("Usage Analytics")
        
        usage_data = self.db.get_usage_analytics(
            st.session_state['start_date'],
            st.session_state['end_date']
        )
        
        if not usage_data:
            st.warning("No usage analytics available")
            return

        # Current metrics
        st.write("Current System Metrics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Total Records",
                f"{usage_data['current_metrics']['total_records']:,}"
            )

        with col2:
            # Create a DataFrame for source metrics
            source_df = pd.DataFrame({
                'Source': list(usage_data['current_metrics']['by_source'].keys()),
                'Count': list(usage_data['current_metrics']['by_source'].values())
            })
            fig = px.pie(
                source_df,
                values='Count',
                names='Source',
                title='Usage by Source'
            )
            st.plotly_chart(fig, use_container_width=True)

        # Historical metrics
        st.subheader("Historical Usage Patterns")
        col1, col2 = st.columns(2)

        with col1:
            # Usage over time
            time_df = pd.DataFrame({
                'Timestamp': list(usage_data['historical_metrics']['over_time'].keys()),
                'Count': list(usage_data['historical_metrics']['over_time'].values())
            })
            fig = px.line(
                time_df,
                x='Timestamp',
                y='Count',
                title='Usage Over Time'
            )
            st.plotly_chart(fig, use_container_width=True)

    def render_load_analysis(self):
        st.subheader("Load Pattern Analysis")
        
        creation_metrics = self.db.get_creation_metrics(
            st.session_state['start_date'],
            st.session_state['end_date']
        )
        
        if creation_metrics and 'by_hour' in creation_metrics:
            analysis = self.detector.analyze_load_patterns(creation_metrics['by_hour'])
            
            if analysis:
                # Display summary metrics
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric(
                        "Average Load",
                        f"{analysis['summary']['average_load']:.0f}",
                        "events/hour"
                    )
                
                with col2:
                    st.metric(
                        "High Load Hours",
                        analysis['summary']['high_load_hours'],
                        "needs attention"
                    )
                
                with col3:
                    st.metric(
                        "Low Load Hours",
                        analysis['summary']['low_load_hours'],
                        "potential savings"
                    )
                
                # Display peak hours
                st.subheader("Peak Hours")
                for pattern in analysis['summary']['peak_hours']:
                    status_color = (
                        "🔴" if pattern['status'] == 'high'
                        else "🟡" if pattern['status'] == 'normal'
                        else "🟢"
                    )
                    st.write(
                        f"{status_color} Hour {pattern['hour']:02d}:00 - "
                        f"{pattern['count']} events "
                        f"({pattern['load_ratio']:.1f}x average load)"
                    )
                
                # Show detailed patterns in expandable section
                with st.expander("View All Hours"):
                    for pattern in analysis['patterns']:
                        st.write(
                            f"Hour {pattern['hour']:02d}:00 - "
                            f"{pattern['count']} events "
                            f"({'Work' if pattern['is_work_hour'] else 'Off'} hours) - "
                            f"Status: {pattern['status'].upper()}"
                        )
        else:
            st.info("No data available for analysis")

if __name__ == "__main__":
    dashboard = DataObservabilityDashboard()
    dashboard.main()