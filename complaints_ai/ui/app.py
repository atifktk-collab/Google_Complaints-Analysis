import streamlit as st
from sqlalchemy import text
import pandas as pd
import plotly.express as px
import polars as pl
from datetime import datetime, date, timedelta
import time
import os
import sys

# Add parent directory of 'complaints_ai' to path to import as package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from complaints_ai.db.mysql import get_engine, get_session
from complaints_ai.db.models import DailyAnomalies, DailyTrends, DailyVariations, ExecInsights
from complaints_ai.orchestrator import Orchestrator
from complaints_ai.agents.trend_plotter_agent import TrendPlotterAgent
from complaints_ai.agents.surge_highlighter_agent import SurgeHighlighterAgent
from complaints_ai.agents.repeat_highlighter_agent import RepeatHighlighterAgent
from complaints_ai.ui.plotly_utils import create_area_chart, create_multi_line_chart, COLORS

# Config
st.set_page_config(
    page_title="Telecom Complaints Analytics",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize Orchestrator (cached?)
@st.cache_resource
def get_orchestrator():
    return Orchestrator()

@st.cache_resource
def get_trend_plotter():
    return TrendPlotterAgent()

@st.cache_resource
def get_surge_highlighter():
    return SurgeHighlighterAgent()

@st.cache_resource
def get_repeat_highlighter():
    return RepeatHighlighterAgent()

orchestrator = get_orchestrator()
trend_plotter = get_trend_plotter()
surge_highlighter = get_surge_highlighter()
repeat_highlighter = get_repeat_highlighter()
engine = get_engine()

# Helper function to display surge cards
def _display_surge_card(surge, st_module):
    """Display a surge as a premium styled card."""
    severity = surge['severity']
    border_color = "#e74c3c" if severity == 'CRITICAL' else "#f39c12"
    bg_color = "rgba(231, 76, 60, 0.05)" if severity == 'CRITICAL' else "rgba(243, 156, 18, 0.05)"
    emoji = "🔴" if severity == 'CRITICAL' else "🟠"
    
    card_html = f"""
    <div style="
        border-left: 5px solid {border_color};
        background-color: {bg_color};
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 20px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
    ">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <h3 style="margin: 0; color: {border_color}; font-family: 'Inter', sans-serif;">{emoji} {surge['name']}</h3>
            <span style="
                background-color: {border_color};
                color: white;
                padding: 2px 10px;
                border-radius: 20px;
                font-size: 0.8rem;
                font-weight: bold;
            ">{severity}</span>
        </div>
        <p style="margin: 5px 0; color: #666; font-size: 0.9rem;">📍 Location: {surge.get('parent', 'All Regions')}</p>
        <hr style="border: 0; border-top: 1px solid rgba(0,0,0,0.1); margin: 10px 0;">
        <div style="display: flex; justify-content: space-around; text-align: center;">
            <div>
                <div style="font-size: 0.8rem; color: #888;">Current</div>
                <div style="font-size: 1.2rem; font-weight: bold;">{surge['current_count']}</div>
            </div>
            <div>
                <div style="font-size: 0.8rem; color: #888;">vs MTD</div>
                <div style="font-size: 1.2rem; font-weight: bold; color: {border_color};">+{surge['mtd_surge_percent']}%</div>
            </div>
            <div>
                <div style="font-size: 0.8rem; color: #888;">vs Last Week</div>
                <div style="font-size: 1.2rem; font-weight: bold; color: {border_color};">+{surge['wow_surge_percent']}%</div>
            </div>
        </div>
    </div>
    """
    st_module.markdown(card_html, unsafe_allow_html=True)

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["CSV Upload", "Daily Dashboard", "Trend Plotter", "Surge Highlighter", "Repeat Analysis", "Executive Insights"])

st.sidebar.markdown("---")
st.sidebar.subheader("📅 Global Controls")

# Initialize global date in session state
if 'global_date' not in st.session_state:
    st.session_state.global_date = date.today() - timedelta(days=1)

# Global Date Selection
target_date = st.sidebar.date_input("Select Analysis Date", st.session_state.global_date)
st.session_state.global_date = target_date

# Global Analysis Trigger
if st.sidebar.button("🚀 Run Analysis for this Date", type="primary"):
    with st.spinner(f"Running full analysis pipeline for {target_date}..."):
        result = orchestrator.run_pipeline(
            target_date=str(target_date),
            run_ingestion=False
        )
        if result.get('status') == 'success':
            st.sidebar.success(f"Analysis complete for {target_date}!")
            # Use a timestamp to force refresh if needed, or just rely on streamlit state
            st.session_state.last_analysis_run = time.time()
        else:
            st.sidebar.error(f"Analysis failed: {result.get('message')}")

st.sidebar.markdown("---")
st.sidebar.info("System Status: **Active** (Daily Analysis Mode)")

# DB Stats for debugging
try:
    with engine.connect() as conn:
        res = conn.execute(text("SELECT count(*) FROM complaints_raw"))
        total_rows = res.scalar()
        st.sidebar.write(f"📁 Total Data: {total_rows} rows")
except Exception as e:
    st.sidebar.error(f"❌ DB Error: {e}")

# --- Page 1: CSV Upload ---
if page == "CSV Upload":
    st.title("📂 Data Ingestion")
    st.write("Upload daily complaint data CSV files for analysis.")
    
    uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'])
    
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"Targeting upload for: **{target_date}**")
        st.caption("Change date in sidebar if uploading for a different day.")
    with col2:
        run_baseline = st.checkbox("Recalculate Baselines", value=False)
    
    if uploaded_file is not None:
        st.info(f"File uploaded: {uploaded_file.name} ({uploaded_file.size / 1024:.2f} KB)")
        
        if st.button("Start Processing"):
            # Save temp file
            temp_path = os.path.join("temp_upload.csv")
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                # Run full pipeline
                status_text.text("Running daily analysis pipeline...")
                progress_bar.progress(10)
                
                result = orchestrator.run_pipeline(
                    file_path=temp_path,
                    target_date=str(target_date),
                    run_ingestion=True,
                    run_baseline=run_baseline
                )
                
                progress_bar.progress(100)
                
                if result and result.get('status') == 'error':
                     status_text.text("Failed.")
                     st.error(f"Pipeline Failed: {result.get('message')}")
                     if 'diagnostics' in result:
                         with st.expander("Diagnostic Details"):
                             st.json(result['diagnostics'])
                else:
                     status_text.text("Complete!")
                     st.success(f"Successfully processed data for {target_date}")
                     if result and 'diagnostics' in result:
                         with st.expander("Ingestion Details"):
                             st.json(result['diagnostics'])
                
                # cleanup
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    
            except Exception as e:
                st.error(f"An error occurred: {e}")

# --- Page 2: Daily Dashboard ---
elif page == "Daily Dashboard":
    st.title("📊 Daily Analytics Dashboard")
    st.info(f"Showing analysis for: **{target_date}** (Change in sidebar)")

    # KPIs
    # Query Data
    query_raw = f"""
        SELECT count(*) as cnt FROM complaints_raw 
        WHERE sr_open_dt = '{target_date}'
    """
    total_complaints = pd.read_sql(query_raw, engine).iloc[0]['cnt']
    
    query_anom = f"""
        SELECT count(*) as cnt FROM daily_anomalies 
        WHERE anomaly_date = '{target_date}'
    """
    total_anomalies = pd.read_sql(query_anom, engine).iloc[0]['cnt']
    
    query_critical = f"""
        SELECT count(*) as cnt FROM daily_anomalies 
        WHERE anomaly_date = '{target_date}'
        AND severity = 'CRITICAL'
    """
    critical_anomalies = pd.read_sql(query_critical, engine).iloc[0]['cnt']
    
    query_trends_up = f"""
        SELECT count(*) as cnt FROM daily_trends 
        WHERE trend_date = '{target_date}' AND trend_direction = 'UP'
        AND window_days = 30
    """
    trends_up = pd.read_sql(query_trends_up, engine).iloc[0]['cnt']

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Total Complaints", total_complaints)
    kpi2.metric("Total Anomalies", total_anomalies)
    kpi3.metric("Critical Alerts", critical_anomalies, delta_color="inverse")
    kpi4.metric("Upward Trends (30d)", trends_up)

    st.markdown("---")
    
    # Charts Row 1
    c1, c2 = st.columns([2, 1])
    
    with c1:
        # Line Chart: Complaints over last 30 days
        st.subheader("Complaints Trend (Last 30 Days)")
        end_date = target_date
        start_date = end_date - timedelta(days=30)
        
        q_trend = f"""
            SELECT sr_open_dt, count(*) as count 
            FROM complaints_raw 
            WHERE sr_open_dt BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY sr_open_dt
            ORDER BY sr_open_dt
        """
        trend_df = pd.read_sql(q_trend, engine)
        if not trend_df.empty:
            trend_df['sr_open_dt'] = pd.to_datetime(trend_df['sr_open_dt'])
            
            # Plotly Infographic Style Line Chart
            fig = create_area_chart(trend_df, 'sr_open_dt', 'count', "Complaints Volume Trend")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data for trend.")

    with c2:
        # Pie Chart: Anomaly Distribution
        st.subheader("Anomaly Breakdown")
        q_pie = f"""
            SELECT dimension, count(*) as cnt 
            FROM daily_anomalies 
            WHERE anomaly_date = '{target_date}'
            GROUP BY dimension
        """
        pie_df = pd.read_sql(q_pie, engine)
        if not pie_df.empty:
            fig_pie = px.pie(
                pie_df, 
                values='cnt', 
                names='dimension',
                hole=0.4,
                template="plotly_white",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_pie.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                height=300,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No anomalies recorded.")

    # Variations Section
    st.markdown("---")
    st.subheader("📈 Daily Variations")
    
    # Query Total Company Variations
    q_total_vars = f"""
        SELECT variation_type, current_value, previous_value, variation_percent
        FROM daily_variations 
        WHERE variation_date = '{target_date}' AND dimension = 'Total'
    """
    total_vars_df = pd.read_sql(q_total_vars, engine)
    
    v_col1, v_col2, v_col3 = st.columns(3)
    
    def get_var_data(vtype):
        if total_vars_df.empty: return None
        row = total_vars_df[total_vars_df['variation_type'] == vtype]
        if row.empty: return None
        return row.iloc[0]

    with v_col1:
        st.markdown("**Day-over-Day (DoD)**")
        st.caption("Target Date vs Same Day Last Week")
        data = get_var_data('DOD')
        if data is not None:
            st.metric(
                "Current Count", 
                f"{data['current_value']:.0f}", 
                f"{data['variation_percent']:+.1f}%",
                delta_color="inverse"
            )
            st.markdown(f"<small>Previous: {data['previous_value']:.0f}</small>", unsafe_allow_html=True)
        else:
            st.info("No DoD data")
    
    with v_col2:
        st.markdown("**Week-over-Week (WoW)**")
        st.caption("WTD Avg vs Prev Week Avg")
        data = get_var_data('WOW')
        if data is not None:
            st.metric(
                "WTD Average", 
                f"{data['current_value']:.1f}", 
                f"{data['variation_percent']:+.1f}%",
                delta_color="inverse"
            )
            st.markdown(f"<small>Prev WTD Avg: {data['previous_value']:.1f}</small>", unsafe_allow_html=True)
        else:
            st.info("No WoW data")
    
    with v_col3:
        st.markdown("**Month-over-Month (MoM)**")
        st.caption("MTD Avg vs Prev Month Avg")
        data = get_var_data('MOM')
        if data is not None:
            st.metric(
                "MTD Average", 
                f"{data['current_value']:.1f}", 
                f"{data['variation_percent']:+.1f}%",
                delta_color="inverse"
            )
            st.markdown(f"<small>Prev MTD Avg: {data['previous_value']:.1f}</small>", unsafe_allow_html=True)
        else:
            st.info("No MoM data")

    # Significant Dimension-wise Variations (Show only top 5 significant across all types)
    st.markdown("### Top Significant Variations by Dimension")
    q_sig_vars = f"""
        SELECT dimension, dimension_key, variation_type, variation_percent 
        FROM daily_variations 
        WHERE variation_date = '{target_date}' AND dimension != 'Total'
        AND is_significant = 1
        ORDER BY ABS(variation_percent) DESC
        LIMIT 5
    """
    sig_vars_df = pd.read_sql(q_sig_vars, engine)
    if not sig_vars_df.empty:
        st.dataframe(sig_vars_df, use_container_width=True, hide_index=True)
    else:
        st.info("No significant dimension variations detected.")

    # Trends Section
    st.markdown("---")
    st.subheader("📊 Trend Analysis (30-Day Window)")
    
    q_trends = f"""
        SELECT dimension, dimension_key, trend_direction, trend_strength, significance
        FROM daily_trends 
        WHERE trend_date = '{target_date}' AND window_days = 30
        AND trend_direction != 'STABLE'
        ORDER BY ABS(trend_strength) DESC
        LIMIT 10
    """
    trends_df = pd.read_sql(q_trends, engine)
    if not trends_df.empty:
        # Add arrow indicators
        trends_df['Direction'] = trends_df['trend_direction'].apply(
            lambda x: '↑ UP' if x == 'UP' else '↓ DOWN'
        )
        trends_df['Strength'] = trends_df['trend_strength'].apply(lambda x: f"{x:.1f}%")
        trends_df['Significant'] = trends_df['significance'].apply(lambda x: 'Yes' if x < 0.05 else 'No')
        
        st.dataframe(
            trends_df[['dimension', 'dimension_key', 'Direction', 'Strength', 'Significant']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No significant trends detected.")

    # Table: Anomalies
    st.markdown("---")
    st.subheader("🚨 Detected Anomalies")
    q_table = f"""
        SELECT dimension, dimension_key, metric_value, z_score, severity, rca_context 
        FROM daily_anomalies 
        WHERE anomaly_date = '{target_date}'
        ORDER BY z_score DESC
    """
    anom_table = pd.read_sql(q_table, engine)
    
    if not anom_table.empty:
        # Stylize
        def color_severity(val):
            color = 'red' if val == 'CRITICAL' else 'orange' if val == 'WARNING' else 'blue'
            return f'color: {color}'
        
        st.dataframe(anom_table.style.applymap(color_severity, subset=['severity']), use_container_width=True)
    else:
        st.info("No anomalies detected for this date.")

# --- Page 3: Trend Plotter ---
elif page == "Trend Plotter":
    st.title("📈 Trend Plotter - Historical Analysis")
    st.markdown(f"Visualizing complaint trends ending **{target_date}** (Change in sidebar).")
    
    # Configuration section
    days_back = st.slider("Days to Show", 7, 90, 30)
    
    # Generate trend data
    with st.spinner("Loading trend data..."):
        trend_data = trend_plotter.run({
            "target_date": str(target_date),
            "days_back": days_back
        })
    
    if trend_data['status'] != 'success':
        st.error(f"Failed to load trend data: {trend_data.get('message', 'Unknown error')}")
    else:
        st.success(f"Loaded data from {trend_data['start_date']} to {trend_data['end_date']}")
        
        # Tab layout for different views
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Total & Regional", "🏢 Exchange & City", "📋 SR Sub-Type", "🔍 RCA Analysis"])
        
        # --- Tab 1: Total & Regional ---
        with tab1:
            st.subheader("Total Complaints Trend")
            
            # Total trend chart
            if trend_data['total_trend']:
                total_df = pd.DataFrame(trend_data['total_trend'])
                total_df['date'] = pd.to_datetime(total_df['date'])
                
                fig_total = create_area_chart(total_df, 'date', 'count', "")
                st.plotly_chart(fig_total, use_container_width=True)
                
                # Show statistics
                col1, col2, col3 = st.columns(3)
                col1.metric("Average Daily", f"{total_df['count'].mean():.0f}")
                col2.metric("Peak Day", f"{total_df['count'].max():.0f}")
                col3.metric("Lowest Day", f"{total_df['count'].min():.0f}")
            else:
                st.info("No data available for total trend.")
            
            st.markdown("---")
            st.subheader("Region-wise Trends")
            
            # Region filter
            if trend_data['region_trend']:
                regions = list(trend_data['region_trend'].keys())
                selected_regions = st.multiselect(
                    "Select Regions to Display",
                    regions,
                    default=regions[:5] if len(regions) > 5 else regions
                )
                
                if selected_regions:
                    # Prepare data for multi-line chart
                    region_chart_data = pd.DataFrame()
                    for region in selected_regions:
                        region_df = pd.DataFrame(trend_data['region_trend'][region])
                        region_df['date'] = pd.to_datetime(region_df['date'])
                        region_df = region_df.set_index('date')
                        region_df = region_df.rename(columns={'count': region})
                        
                        if region_chart_data.empty:
                            region_chart_data = region_df
                        else:
                            region_chart_data = region_chart_data.join(region_df, how='outer')
                    
                    region_chart_data = region_chart_data.fillna(0).reset_index()
                    fig_region = create_multi_line_chart(region_chart_data, 'date', selected_regions, "Comparison by Region")
                    st.plotly_chart(fig_region, use_container_width=True)
                else:
                    st.info("Select at least one region to display.")
            else:
                st.info("No regional data available.")
        
        # --- Tab 2: Exchange & City ---
        with tab2:
            st.subheader("Exchange-wise Trends (Hierarchical Drill-down)")
            
            if trend_data['exchange_trend']:
                # Region selector for exchange view
                regions_with_exchanges = list(trend_data['exchange_trend'].keys())
                selected_region = st.selectbox("Select Region", regions_with_exchanges)
                
                if selected_region:
                    exchanges = list(trend_data['exchange_trend'][selected_region].keys())
                    selected_exchanges = st.multiselect(
                        f"Select Exchanges in {selected_region}",
                        exchanges,
                        default=exchanges[:5] if len(exchanges) > 5 else exchanges
                    )
                    
                    if selected_exchanges:
                        # Prepare exchange chart data
                        exchange_chart_data = pd.DataFrame()
                        for exchange in selected_exchanges:
                            exchange_df = pd.DataFrame(trend_data['exchange_trend'][selected_region][exchange])
                            exchange_df['date'] = pd.to_datetime(exchange_df['date'])
                            exchange_df = exchange_df.set_index('date')
                            exchange_df = exchange_df.rename(columns={'count': exchange})
                            
                            if exchange_chart_data.empty:
                                exchange_chart_data = exchange_df
                            else:
                                exchange_chart_data = exchange_chart_data.join(exchange_df, how='outer')
                        
                        exchange_chart_data = exchange_chart_data.fillna(0).reset_index()
                        fig_exc = create_multi_line_chart(exchange_chart_data, 'date', selected_exchanges, f"Exchanges in {selected_region}")
                        st.plotly_chart(fig_exc, use_container_width=True)
                    else:
                        st.info("Select at least one exchange to display.")
                
                st.markdown("---")
                st.subheader("City-wise Trends")
                
                if trend_data['city_trend'] and selected_region:
                    if selected_region in trend_data['city_trend']:
                        # Exchange selector for City view
                        exchanges_with_city = list(trend_data['city_trend'][selected_region].keys())
                        selected_exchange_city = st.selectbox("Select Exchange for City View", exchanges_with_city)
                        
                        if selected_exchange_city:
                            cities = list(trend_data['city_trend'][selected_region][selected_exchange_city].keys())
                            selected_cities = st.multiselect(
                                f"Select Cities in {selected_exchange_city}",
                                cities,
                                default=cities[:5] if len(cities) > 5 else cities
                            )
                            
                            if selected_cities:
                                # Prepare City chart data
                                city_chart_data = pd.DataFrame()
                                for city in selected_cities:
                                    city_df = pd.DataFrame(trend_data['city_trend'][selected_region][selected_exchange_city][city])
                                    city_df['date'] = pd.to_datetime(city_df['date'])
                                    city_df = city_df.set_index('date')
                                    city_df = city_df.rename(columns={'count': city})
                                    
                                    if city_chart_data.empty:
                                        city_chart_data = city_df
                                    else:
                                        city_chart_data = city_chart_data.join(city_df, how='outer')
                                
                                city_chart_data = city_chart_data.fillna(0).reset_index()
                                fig_city = create_multi_line_chart(city_chart_data, 'date', selected_cities, f"Cities in {selected_exchange_city}")
                                st.plotly_chart(fig_city, use_container_width=True)
                            else:
                                st.info("Select at least one City to display.")
                    else:
                        st.info("No City data available for selected region.")
            else:
                st.info("No exchange/City data available.")
        
        # --- Tab 3: SR Sub-Type ---
        with tab3:
            st.subheader("SR Sub-Type Trends")
            
            if trend_data['sr_subtype_trend']:
                subtypes = list(trend_data['sr_subtype_trend'].keys())
                selected_subtypes = st.multiselect(
                    "Select SR Sub-Types to Display",
                    subtypes,
                    default=subtypes[:5] if len(subtypes) > 5 else subtypes
                )
                
                if selected_subtypes:
                    # Prepare SR sub-type chart data
                    subtype_chart_data = pd.DataFrame()
                    for subtype in selected_subtypes:
                        subtype_df = pd.DataFrame(trend_data['sr_subtype_trend'][subtype])
                        subtype_df['date'] = pd.to_datetime(subtype_df['date'])
                        subtype_df = subtype_df.set_index('date')
                        subtype_df = subtype_df.rename(columns={'count': subtype})
                        
                        if subtype_chart_data.empty:
                            subtype_chart_data = subtype_df
                        else:
                            subtype_chart_data = subtype_chart_data.join(subtype_df, how='outer')
                    
                    subtype_chart_data = subtype_chart_data.fillna(0).reset_index()
                    fig_subtype = create_multi_line_chart(subtype_chart_data, 'date', selected_subtypes, "SR Sub-Type Historical")
                    st.plotly_chart(fig_subtype, use_container_width=True)
                    
                    # Show top sub-types table
                    st.markdown("### Top SR Sub-Types (Total Count)")
                    subtype_totals = []
                    for subtype in selected_subtypes:
                        total = sum([row['count'] for row in trend_data['sr_subtype_trend'][subtype]])
                        subtype_totals.append({"SR Sub-Type": subtype, "Total Count": total})
                    
                    subtype_totals_df = pd.DataFrame(subtype_totals).sort_values('Total Count', ascending=False)
                    st.dataframe(subtype_totals_df, use_container_width=True, hide_index=True)
                else:
                    st.info("Select at least one SR sub-type to display.")
            else:
                st.info("No SR sub-type data available.")
        
        # --- Tab 4: RCA Sub-Type Analysis ---
        with tab4:
            st.subheader("RCA_Sub_type Trends")
            
            if trend_data.get('rca_subtype_trend'):
                rcas = list(trend_data['rca_subtype_trend'].keys())
                selected_rcas = st.multiselect(
                    "Select RCA_Sub_types to Display",
                    rcas,
                    default=rcas[:5] if len(rcas) > 5 else rcas
                )
                
                if selected_rcas:
                    # Prepare RCA chart data
                    rca_chart_data = pd.DataFrame()
                    for rca in selected_rcas:
                        rca_df = pd.DataFrame(trend_data['rca_subtype_trend'][rca])
                        rca_df['date'] = pd.to_datetime(rca_df['date'])
                        rca_df = rca_df.set_index('date')
                        rca_df = rca_df.rename(columns={'count': rca})
                        
                        if rca_chart_data.empty:
                            rca_chart_data = rca_df
                        else:
                            rca_chart_data = rca_chart_data.join(rca_df, how='outer')
                    
                    rca_chart_data = rca_chart_data.fillna(0).reset_index()
                    fig_rca = create_multi_line_chart(rca_chart_data, 'date', selected_rcas, "RCA_Sub_type Historical Trends")
                    st.plotly_chart(fig_rca, use_container_width=True)
                    
                    # Show top RCAs table
                    st.markdown("### Top RCA_Sub_types (Total Count)")
                    rca_totals = []
                    for rca in selected_rcas:
                        total = sum([row['count'] for row in trend_data['rca_subtype_trend'][rca]])
                        rca_totals.append({"RCA_Sub_type": rca, "Total Count": total})
                    
                    rca_totals_df = pd.DataFrame(rca_totals).sort_values('Total Count', ascending=False)
                    st.dataframe(rca_totals_df, use_container_width=True, hide_index=True)
                else:
                    st.info("Select at least one RCA to display.")
            else:
                st.info("No RCA data available.")

# --- Page 4: Surge Highlighter ---
elif page == "Surge Highlighter":
    st.title("🚨 Surge Highlighter - Complaint Spike Detection")
    st.markdown(f"Detecting complaint surges for **{target_date}** compared with MTD average and same day last week.")
    
    # Configuration section
    col1, col2 = st.columns(2)
    with col1:
        alarming_threshold = st.number_input("Alarming Threshold (%)", min_value=0.0, max_value=100.0, value=20.0, step=5.0)
    with col2:
        critical_threshold = st.number_input("Critical Threshold (%)", min_value=0.0, max_value=200.0, value=50.0, step=10.0)
    
    # Run analysis for this view (or use global if already run, but thresholds can change)
    with st.spinner("Analyzing complaint surges..."):
        surge_data = surge_highlighter.run({
            "target_date": str(target_date),
            "alarming_threshold": alarming_threshold,
            "critical_threshold": critical_threshold
        })
        
        if surge_data['status'] != 'success':
            st.error(f"Failed to analyze surges: {surge_data.get('message', 'Unknown error')}")
        else:
            # Display analysis info
            st.success(f"✅ Analysis Complete for {surge_data['target_date']}")
            
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"📅 **Comparing with Same Day Last Week:** {surge_data['same_day_last_week']}")
            with col2:
                st.info(f"📊 **MTD Period:** {surge_data['mtd_period']}")
            
            # Count surges by severity
            all_surges = (surge_data['surges']['total'] + 
                         surge_data['surges']['regions'] + 
                         surge_data['surges']['exchanges'] + 
                         surge_data['surges']['cities'])
            
            critical_count = sum(1 for s in all_surges if s['severity'] == 'CRITICAL')
            alarming_count = sum(1 for s in all_surges if s['severity'] == 'ALARMING')
            
            # Summary metrics
            st.markdown("### 📈 Surge Summary")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Surges", len(all_surges))
            col2.metric("Critical", critical_count, delta_color="inverse")
            col3.metric("Alarming", alarming_count, delta_color="inverse")
            col4.metric("Threshold", f">{alarming_threshold}%")
            
            if len(all_surges) == 0:
                st.success("🎉 No surges detected! All complaint levels are within normal range.")
            else:
                # Display surges in tabs
                tab1, tab2, tab3, tab4 = st.tabs(["📊 Total", "🌍 Regions", "🏢 Exchanges", "🏙️ Cities"])
                
                # --- Tab 1: Total ---
                with tab1:
                    if surge_data['surges']['total']:
                        st.subheader("Total Complaints Surge")
                        for surge in surge_data['surges']['total']:
                            _display_surge_card(surge, st)
                    else:
                        st.info("No surge detected at total level.")
                
                # --- Tab 2: Regions ---
                with tab2:
                    if surge_data['surges']['regions']:
                        st.subheader(f"Regional Surges ({len(surge_data['surges']['regions'])} detected)")
                        
                        # Sort by severity and max surge percent
                        sorted_regions = sorted(
                            surge_data['surges']['regions'],
                            key=lambda x: (0 if x['severity'] == 'CRITICAL' else 1, -x['max_surge_percent'])
                        )
                        
                        for surge in sorted_regions:
                            _display_surge_card(surge, st)
                    else:
                        st.info("No surges detected at regional level.")
                
                # --- Tab 3: Exchanges ---
                with tab3:
                    if surge_data['surges']['exchanges']:
                        st.subheader(f"Exchange Surges ({len(surge_data['surges']['exchanges'])} detected)")
                        
                        # Sort by severity and max surge percent
                        sorted_exchanges = sorted(
                            surge_data['surges']['exchanges'],
                            key=lambda x: (0 if x['severity'] == 'CRITICAL' else 1, -x['max_surge_percent'])
                        )
                        
                        for surge in sorted_exchanges:
                            _display_surge_card(surge, st)
                    else:
                        st.info("No surges detected at exchange level.")
                
                # --- Tab 4: Cities ---
                with tab4:
                    if surge_data['surges']['cities']:
                        st.subheader(f"City Surges ({len(surge_data['surges']['cities'])} detected)")
                        
                        # Sort by severity and max surge percent
                        sorted_cities = sorted(
                            surge_data['surges']['cities'],
                            key=lambda x: (0 if x['severity'] == 'CRITICAL' else 1, -x['max_surge_percent'])
                        )
                        
                        for surge in sorted_cities:
                            _display_surge_card(surge, st)
                    else:
                        st.info("No surges detected at City level.")
                
                # Detailed table view
                st.markdown("---")
                st.subheader("📋 Detailed Surge Table")
                
                # Prepare data for table
                table_data = []
                for surge in all_surges:
                    row = {
                        "Level": surge['level'],
                        "Name": surge['name'],
                        "Parent": surge.get('parent', '-'),
                        "Current": surge['current_count'],
                        "MTD Avg": surge['mtd_avg'],
                        "Last Week": surge['last_week_count'],
                        "vs MTD (%)": f"{surge['mtd_surge_percent']:+.1f}%",
                        "vs Last Week (%)": f"{surge['wow_surge_percent']:+.1f}%",
                        "Max Surge (%)": f"{surge['max_surge_percent']:.1f}%",
                        "Severity": surge['severity']
                    }
                    table_data.append(row)
                
                if table_data:
                    df = pd.DataFrame(table_data)
                    
                    # Color code by severity
                    def color_severity(val):
                        if val == 'CRITICAL':
                            return 'background-color: #ffcccc; color: darkred; font-weight: bold'
                        elif val == 'ALARMING':
                            return 'background-color: #fff4cc; color: darkorange; font-weight: bold'
                        return ''
                    
                    styled_df = df.style.applymap(color_severity, subset=['Severity'])
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)


# --- Page 5: Repeat Analysis ---
elif page == "Repeat Analysis":
    st.title("🔄 Repeat Highlighter - MDN Frequency Analysis")
    st.markdown(f"Tracking complaints from the same MDN for the 30-day period ending **{target_date}**.")
    
    # Run analysis automatically for the global date
    with st.spinner("Analyzing MDN repetitions..."):
        repeat_data = repeat_highlighter.run({
            "target_date": str(target_date)
        })
        
        if repeat_data['status'] != 'success':
            st.error(f"Failed to analyze repeats: {repeat_data.get('message', 'Unknown error')}")
        else:
            st.success(f"✅ Analysis for 30-day period: {repeat_data['period']}")
            
            # KPI Row
            st.subheader("📊 Repetition Summary")
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("Total Repeaters", repeat_data['total_repeaters'])
            
            # Get summaries safely
            summaries = repeat_data.get('summaries', {})
            
            # Get severity counts
            sev_data = summaries.get('severity', [])
            sev_counts = {s['severity']: s['count'] for s in sev_data}
            critical_total = sev_counts.get('CRITICAL', 0) + sev_counts.get('VERY ALARMING', 0)
            kpi2.metric("Critical (>6)", critical_total, delta_color="inverse")
            kpi3.metric("Very Alarming (>10)", sev_counts.get('VERY ALARMING', 0), delta_color="inverse")
            
            subtype_overall = summaries.get('subtype_overall', [])
            top_subtype = subtype_overall[0].get('SR_Sub_Type', "N/A") if subtype_overall else "N/A"
            kpi4.metric("Top Repeat Sub-Type", top_subtype)
            
            st.markdown("---")
            
            # Tabs for different repeat perspectives
            tab1, tab2, tab3, tab4 = st.tabs(["🌎 Regional & Severity", "🏢 Exchange & City", "📋 SR Sub-Type Insights", "🔍 Detailed List"])
            
            with tab1:
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("Severity Distribution")
                    sev_list = summaries.get('severity', [])
                    if sev_list:
                        sev_df = pd.DataFrame(sev_list)
                        fig_sev = px.pie(
                            sev_df, values='count', names='severity',
                            hole=0.4,
                            color='severity',
                            color_discrete_map={
                                'VERY ALARMING': '#d62728',
                                'CRITICAL': '#ff7f0e',
                                'ALARMING': '#ffbb78',
                                'NORMAL REPEAT': '#1f77b4'
                            },
                            template="plotly_white"
                        )
                        st.plotly_chart(fig_sev, use_container_width=True)
                
                with c2:
                    st.subheader("Regional Repeats")
                    reg_list = summaries.get('regional', [])
                    if reg_list:
                        reg_df = pd.DataFrame(reg_list)
                        fig_reg = px.bar(
                            reg_df.sort_values('count', ascending=False),
                            x='region', y='count',
                            template="plotly_white",
                            color_discrete_sequence=['#1f77b4']
                        )
                        st.plotly_chart(fig_reg, use_container_width=True)

            with tab2:
                st.subheader("Exchange & City Level Repeats")
                col_e1, col_e2 = st.columns(2)
                
                with col_e1:
                    st.markdown("**Top 10 Exchanges (by Repeats)**")
                    exc_list = summaries.get('exchange', [])
                    if exc_list:
                        exc_df = pd.DataFrame(exc_list)
                        fig_exc = px.bar(
                            exc_df.sort_values('count', ascending=False).head(10),
                            x='exc_id', y='count',
                            template="plotly_white",
                            color_discrete_sequence=['#ff7f0e']
                        )
                        st.plotly_chart(fig_exc, use_container_width=True)
                
                with col_e2:
                    st.markdown("**Top 10 Cities (by Repeats)**")
                    city_list = summaries.get('city', [])
                    if city_list:
                        city_df = pd.DataFrame(city_list)
                        fig_city = px.bar(
                            city_df.sort_values('count', ascending=False).head(10),
                            x='city', y='count',
                            template="plotly_white",
                            color_discrete_sequence=['#2ca02c']
                        )
                        st.plotly_chart(fig_city, use_container_width=True)

            with tab3:
                st.subheader("SR Sub-Type Repeat Analysis")
                st.markdown("Identifying which types of complaints are most frequently repeated.")
                
                # Overall Sub-Type Pie
                subtype_list = summaries.get('subtype_overall', [])
                if subtype_list:
                    sub_df = pd.DataFrame(subtype_list)
                    # Check if SR_Sub_Type is in columns to avoid KeyError
                    if 'SR_Sub_Type' in sub_df.columns:
                        fig_sub = px.bar(
                            sub_df.head(10), 
                            x='count', y='SR_Sub_Type',
                            orientation='h',
                            title="Top Repeated SR Sub-Types (Overall)",
                            template="plotly_white",
                            color='count',
                            color_continuous_scale=px.colors.sequential.Blues
                        )
                        fig_sub.update_layout(yaxis={'categoryorder':'total ascending'})
                        st.plotly_chart(fig_sub, use_container_width=True)
                    else:
                        st.info("No sub-type data available for chart.")
                
                st.markdown("---")
                st.markdown("**Hierarchical Sub-Type Repeats**")
                
                sel_dim = st.selectbox("View Sub-Type Repeats by:", ["Region", "Exchange", "City"])
                
                if sel_dim == "Region":
                    data = summaries.get('regional_subtype', [])
                    key = "region"
                elif sel_dim == "Exchange":
                    data = summaries.get('exchange_subtype', [])
                    key = "exc_id"
                else:
                    data = summaries.get('city_subtype', [])
                    key = "city"
                
                if data:
                    hier_df = pd.DataFrame(data)
                    # Filter top 20 rows for readability
                    if 'SR_Sub_Type' in hier_df.columns:
                        fig_hier = px.bar(
                            hier_df.sort_values('count', ascending=False).head(20),
                            x=key, y='count', color='SR_Sub_Type',
                            barmode='stack',
                            title=f"Repeats by {sel_dim} and Sub-Type",
                            template="plotly_white"
                        )
                        st.plotly_chart(fig_hier, use_container_width=True)
                    else:
                        st.info("No sub-type breakdown available.")

            with tab4:
                st.subheader("📋 Detailed Repeater List")
                if repeat_data['top_repeaters']:
                    df_repeaters = pd.DataFrame(repeat_data['top_repeaters'])
                    # Ensure all required columns exist before indexing
                    required_cols = ['mdn', 'repeat_count', 'severity', 'SR_Sub_Type', 'region', 'exc_id', 'cabinet_id']
                    available_cols = [col for col in required_cols if col in df_repeaters.columns]
                    
                    display_df = df_repeaters[available_cols]
                    display_df = display_df.rename(columns={
                        'mdn': 'MDN', 'repeat_count': 'Count', 'severity': 'Severity',
                        'SR_Sub_Type': 'Top Sub-Type', 'region': 'Region',
                        'exc_id': 'Exchange', 'city': 'City'
                    })
                    
                    def color_sev(val):
                        if val == 'VERY ALARMING': return 'background-color: #ffcccc; color: #990000; font-weight: bold'
                        if val == 'CRITICAL': return 'background-color: #ffe5cc; color: #994c00; font-weight: bold'
                        if val == 'ALARMING': return 'background-color: #ffffcc; color: #999900; font-weight: bold'
                        return ''
                    
                    st.dataframe(
                        display_df.style.applymap(color_sev, subset=['Severity']),
                        use_container_width=True,
                        hide_index=True
                    )

# --- Page 6: Executive Insights ---
elif page == "Executive Insights":
    st.title("📑 Executive Insights")
    
    date_filter = st.date_input("Filter Date", date.today())
    
    session = get_session()
    insights = session.query(ExecInsights)\
        .filter(ExecInsights.created_at >= date_filter)\
        .order_by(ExecInsights.created_at.desc())\
        .all()
    session.close()
    
    if not insights:
        st.info("No insights generated for this date.")
    
    for note in insights:
        with st.expander(f"[{note.severity}] {note.title} - {note.created_at.strftime('%Y-%m-%d')}", expanded=True):
            st.markdown(f"**{note.summary}**")
            # Severity badge
            if note.severity == 'CRITICAL':
                st.error("Priority: High Application")
            elif note.severity == 'WARNING':
                st.warning("Priority: Medium")
            else:
                st.info("Priority: Low")
