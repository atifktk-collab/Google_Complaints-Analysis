import streamlit as st
import pandas as pd
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

orchestrator = get_orchestrator()
trend_plotter = get_trend_plotter()
surge_highlighter = get_surge_highlighter()
engine = get_engine()

# Helper function to display surge cards
def _display_surge_card(surge, st_module):
    """Display a surge as a colored card."""
    severity_color = "red" if surge['severity'] == 'CRITICAL' else "orange"
    severity_emoji = "🔴" if surge['severity'] == 'CRITICAL' else "🟠"
    
    with st_module.container():
        if surge['severity'] == 'CRITICAL':
            st_module.error(f"### {severity_emoji} {surge['name']}")
        else:
            st_module.warning(f"### {severity_emoji} {surge['name']}")
        
        if 'parent' in surge:
            st_module.caption(f"📍 Location: {surge['parent']}")
        
        col1, col2, col3 = st_module.columns(3)
        
        with col1:
            st_module.metric(
                "Current Count",
                surge['current_count'],
                delta=None
            )
        
        with col2:
            st_module.metric(
                "vs MTD Average",
                f"{surge['mtd_surge_percent']:+.1f}%",
                delta=f"{surge['mtd_surge_increase']:+.1f}",
                delta_color="inverse"
            )
            st_module.caption(f"MTD Avg: {surge['mtd_avg']:.1f}")
        
        with col3:
            st_module.metric(
                "vs Last Week",
                f"{surge['wow_surge_percent']:+.1f}%",
                delta=f"{surge['wow_surge_increase']:+.1f}",
                delta_color="inverse"
            )
            st_module.caption(f"Last Week: {surge['last_week_count']}")
        
        st_module.markdown("---")

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["CSV Upload", "Daily Dashboard", "Trend Plotter", "Surge Highlighter", "Executive Insights"])

st.sidebar.markdown("---")
st.sidebar.info("System Status: **Active** (Daily Analysis Mode)")

# --- Page 1: CSV Upload ---
if page == "CSV Upload":
    st.title("📂 Data Ingestion")
    st.write("Upload daily complaint data CSV files for analysis.")
    
    uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'])
    
    col1, col2 = st.columns(2)
    with col1:
        target_date = st.date_input("Data Date", date.today() - timedelta(days=1))
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
    
    # Filters
    col1, col2 = st.columns([2, 1])
    with col1:
        target_date = st.date_input("Select Date", date.today() - timedelta(days=1))
    with col2:
        if st.button("Run Analysis for this Date"):
             with st.spinner("Running Daily Analysis..."):
                 orchestrator.run_pipeline(
                     target_date=str(target_date),
                     run_ingestion=False
                 )
                 st.success("Analysis Complete!")

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
            st.line_chart(trend_df.set_index('sr_open_dt'))
        else:
            st.info("No data for trend.")

    with c2:
        # Pie Chart: Anomaly Distribution
        st.subheader("Anomaly Breakdown by Type")
        q_pie = f"""
            SELECT dimension, count(*) as cnt 
            FROM daily_anomalies 
            WHERE anomaly_date = '{target_date}'
            GROUP BY dimension
        """
        pie_df = pd.read_sql(q_pie, engine)
        if not pie_df.empty:
            st.dataframe(pie_df, hide_index=True)
        else:
            st.write("No anomalies.")

    # Variations Section
    st.markdown("---")
    st.subheader("📈 Daily Variations")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Day-over-Day (DoD)**")
        q_dod = f"""
            SELECT dimension_key, variation_percent 
            FROM daily_variations 
            WHERE variation_date = '{target_date}' AND variation_type = 'DOD'
            AND is_significant = 1
            ORDER BY ABS(variation_percent) DESC
            LIMIT 5
        """
        dod_df = pd.read_sql(q_dod, engine)
        if not dod_df.empty:
            for _, row in dod_df.iterrows():
                delta = f"{row['variation_percent']:+.1f}%"
                st.metric(row['dimension_key'], delta, delta)
        else:
            st.info("No significant DoD variations")
    
    with col2:
        st.markdown("**Week-over-Week (WoW)**")
        q_wow = f"""
            SELECT dimension_key, variation_percent 
            FROM daily_variations 
            WHERE variation_date = '{target_date}' AND variation_type = 'WOW'
            AND is_significant = 1
            ORDER BY ABS(variation_percent) DESC
            LIMIT 5
        """
        wow_df = pd.read_sql(q_wow, engine)
        if not wow_df.empty:
            for _, row in wow_df.iterrows():
                delta = f"{row['variation_percent']:+.1f}%"
                st.metric(row['dimension_key'], delta, delta)
        else:
            st.info("No significant WoW variations")
    
    with col3:
        st.markdown("**Month-over-Month (MoM)**")
        q_mom = f"""
            SELECT dimension_key, variation_percent 
            FROM daily_variations 
            WHERE variation_date = '{target_date}' AND variation_type = 'MOM'
            AND is_significant = 1
            ORDER BY ABS(variation_percent) DESC
            LIMIT 5
        """
        mom_df = pd.read_sql(q_mom, engine)
        if not mom_df.empty:
            for _, row in mom_df.iterrows():
                delta = f"{row['variation_percent']:+.1f}%"
                st.metric(row['dimension_key'], delta, delta)
        else:
            st.info("No significant MoM variations")

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
    st.title("📈 Trend Plotter - 30-Day Historical Analysis")
    st.markdown("Visualize complaint trends over the last 30 days with hierarchical drill-down capabilities.")
    
    # Date selection
    col1, col2 = st.columns([2, 1])
    with col1:
        end_date = st.date_input("End Date", date.today() - timedelta(days=1))
    with col2:
        days_back = st.slider("Days to Show", 7, 90, 30)
    
    # Generate trend data
    with st.spinner("Loading trend data..."):
        trend_data = trend_plotter.run({
            "target_date": str(end_date),
            "days_back": days_back
        })
    
    if trend_data['status'] != 'success':
        st.error(f"Failed to load trend data: {trend_data.get('message', 'Unknown error')}")
    else:
        st.success(f"Loaded data from {trend_data['start_date']} to {trend_data['end_date']}")
        
        # Tab layout for different views
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Total & Regional", "🏢 Exchange & NE", "📋 SR Sub-Type", "🔍 RCA Analysis"])
        
        # --- Tab 1: Total & Regional ---
        with tab1:
            st.subheader("Total Complaints Trend")
            
            # Total trend chart
            if trend_data['total_trend']:
                total_df = pd.DataFrame(trend_data['total_trend'])
                total_df['date'] = pd.to_datetime(total_df['date'])
                
                st.line_chart(total_df.set_index('date')['count'], use_container_width=True)
                
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
                    
                    region_chart_data = region_chart_data.fillna(0)
                    st.line_chart(region_chart_data, use_container_width=True)
                else:
                    st.info("Select at least one region to display.")
            else:
                st.info("No regional data available.")
        
        # --- Tab 2: Exchange & NE ---
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
                        
                        exchange_chart_data = exchange_chart_data.fillna(0)
                        st.line_chart(exchange_chart_data, use_container_width=True)
                    else:
                        st.info("Select at least one exchange to display.")
                
                st.markdown("---")
                st.subheader("Network Element (NE) Trends")
                
                if trend_data['ne_trend'] and selected_region:
                    if selected_region in trend_data['ne_trend']:
                        # Exchange selector for NE view
                        exchanges_with_ne = list(trend_data['ne_trend'][selected_region].keys())
                        selected_exchange_ne = st.selectbox("Select Exchange for NE View", exchanges_with_ne)
                        
                        if selected_exchange_ne:
                            nes = list(trend_data['ne_trend'][selected_region][selected_exchange_ne].keys())
                            selected_nes = st.multiselect(
                                f"Select NEs in {selected_exchange_ne}",
                                nes,
                                default=nes[:5] if len(nes) > 5 else nes
                            )
                            
                            if selected_nes:
                                # Prepare NE chart data
                                ne_chart_data = pd.DataFrame()
                                for ne in selected_nes:
                                    ne_df = pd.DataFrame(trend_data['ne_trend'][selected_region][selected_exchange_ne][ne])
                                    ne_df['date'] = pd.to_datetime(ne_df['date'])
                                    ne_df = ne_df.set_index('date')
                                    ne_df = ne_df.rename(columns={'count': ne})
                                    
                                    if ne_chart_data.empty:
                                        ne_chart_data = ne_df
                                    else:
                                        ne_chart_data = ne_chart_data.join(ne_df, how='outer')
                                
                                ne_chart_data = ne_chart_data.fillna(0)
                                st.line_chart(ne_chart_data, use_container_width=True)
                            else:
                                st.info("Select at least one NE to display.")
                    else:
                        st.info("No NE data available for selected region.")
            else:
                st.info("No exchange/NE data available.")
        
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
                    
                    subtype_chart_data = subtype_chart_data.fillna(0)
                    st.line_chart(subtype_chart_data, use_container_width=True)
                    
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
        
        # --- Tab 4: RCA Analysis ---
        with tab4:
            st.subheader("RCA (Root Cause Analysis) Trends")
            
            if trend_data['rca_trend']:
                rcas = list(trend_data['rca_trend'].keys())
                selected_rcas = st.multiselect(
                    "Select RCAs to Display",
                    rcas,
                    default=rcas[:5] if len(rcas) > 5 else rcas
                )
                
                if selected_rcas:
                    # Prepare RCA chart data
                    rca_chart_data = pd.DataFrame()
                    for rca in selected_rcas:
                        rca_df = pd.DataFrame(trend_data['rca_trend'][rca])
                        rca_df['date'] = pd.to_datetime(rca_df['date'])
                        rca_df = rca_df.set_index('date')
                        rca_df = rca_df.rename(columns={'count': rca})
                        
                        if rca_chart_data.empty:
                            rca_chart_data = rca_df
                        else:
                            rca_chart_data = rca_chart_data.join(rca_df, how='outer')
                    
                    rca_chart_data = rca_chart_data.fillna(0)
                    st.line_chart(rca_chart_data, use_container_width=True)
                    
                    # Show top RCAs table
                    st.markdown("### Top RCAs (Total Count)")
                    rca_totals = []
                    for rca in selected_rcas:
                        total = sum([row['count'] for row in trend_data['rca_trend'][rca]])
                        rca_totals.append({"RCA": rca, "Total Count": total})
                    
                    rca_totals_df = pd.DataFrame(rca_totals).sort_values('Total Count', ascending=False)
                    st.dataframe(rca_totals_df, use_container_width=True, hide_index=True)
                else:
                    st.info("Select at least one RCA to display.")
            else:
                st.info("No RCA data available.")

# --- Page 4: Surge Highlighter ---
elif page == "Surge Highlighter":
    st.title("🚨 Surge Highlighter - Complaint Spike Detection")
    st.markdown("Detect and highlight complaint surges by comparing with MTD average and same day last week.")
    
    # Configuration section
    col1, col2, col3 = st.columns(3)
    with col1:
        target_date = st.date_input("Target Date", date.today() - timedelta(days=1))
    with col2:
        alarming_threshold = st.number_input("Alarming Threshold (%)", min_value=0.0, max_value=100.0, value=20.0, step=5.0)
    with col3:
        critical_threshold = st.number_input("Critical Threshold (%)", min_value=0.0, max_value=200.0, value=50.0, step=10.0)
    
    if st.button("🔍 Analyze Surges", type="primary"):
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
                         surge_data['surges']['nes'])
            
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
                tab1, tab2, tab3, tab4 = st.tabs(["📊 Total", "🌍 Regions", "🏢 Exchanges", "📡 Network Elements"])
                
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
                
                # --- Tab 4: NEs ---
                with tab4:
                    if surge_data['surges']['nes']:
                        st.subheader(f"Network Element Surges ({len(surge_data['surges']['nes'])} detected)")
                        
                        # Sort by severity and max surge percent
                        sorted_nes = sorted(
                            surge_data['surges']['nes'],
                            key=lambda x: (0 if x['severity'] == 'CRITICAL' else 1, -x['max_surge_percent'])
                        )
                        
                        for surge in sorted_nes:
                            _display_surge_card(surge, st)
                    else:
                        st.info("No surges detected at NE level.")
                
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


# --- Page 5: Executive Insights ---
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
