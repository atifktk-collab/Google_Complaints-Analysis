
import os
import csv
import logging
from datetime import datetime, timedelta
from complaints_ai.db.mysql import get_engine, init_db, get_session
from complaints_ai.db.models import ComplaintsRaw, DailyAnomalies, DailyTrends, ExecInsights
from complaints_ai.orchestrator import Orchestrator
from complaints_ai.agents.trend_plotter_agent import TrendPlotterAgent

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyPipeline")

def create_dummy_data(filename, target_date):
    """Creates dummy complaint data for 30 days up to target_date."""
    print(f"Creating dummy data in {filename}...")
    
    headers = ["sr_row_id", "sr_open_dttm", "sr_type", "region", "exc_id", "olt_id", "rca", "status", "vendor", "product_id", "cabinet_id", "sr_sub_type"]
    rows = []
    
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    start_dt = target_dt - timedelta(days=30)
    
    count = 0
    current_dt = start_dt
    while current_dt <= target_dt:
        dt_str = current_dt.strftime("%Y-%m-%d")
        
        # Base load: 20 complaints/day normally
        # 5 for Karachi, 5 for Lahore, 10 others
        daily_count = 20
        
        # ANOMALY: On target_date, Karachi has 100 complaints (5x normal)
        if current_dt.date() == target_dt.date():
            print(f"Injecting anomaly on {dt_str}")
            daily_count = 100 # Spike
        
        for i in range(daily_count):
            count += 1
            row_id = f"ROW_{count}"
            # Time distribution
            hour = 10 + (i % 10)
            # Format must match IngestionAgent expectation: 20-Jan-26 11:46:46
            # %d-%b-%y %H:%M:%S
            timestamp = current_dt.replace(hour=hour, minute=0, second=0).strftime("%d-%b-%y %H:%M:%S")
            
            # Defaults
            region = "Islamabad"
            exc = "F-8"
            cabinet = "Cab1"
            rca = "Fiber Cut"
            sr_type = "Internet"
            subtype = "Slow Speed"
            
            # Regional logic
            if i < 5:
                region = "Lahore"
                exc = "Cantt"
            elif i < 10 or (current_dt.date() == target_dt.date() and i < 80): # Heavy skew to Karachi on anomaly day
                region = "Karachi"
                exc = "Clifton"
                if current_dt.date() == target_dt.date():
                    rca = "Power Outage" # Clear RCA for anomaly
            
            rows.append([
                row_id, timestamp, sr_type, region, exc, "OLT1", rca, "Closed", "Huawei", "GPON", cabinet, subtype
            ])
            
        current_dt += timedelta(days=1)
        
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
        
    print(f"Generated {len(rows)} rows.")
    return len(rows)

def verify_pipeline():
    print("=== STARTING PIPELINE VERIFICATION ===")
    
    # 1. Setup
    init_db()
    session = get_session()
    
    # Clear existing data for clean test
    session.query(ComplaintsRaw).delete()
    session.query(DailyAnomalies).delete()
    session.query(DailyTrends).delete()
    session.query(ExecInsights).delete()
    session.commit()
    
    target_date = datetime.now().strftime("%Y-%m-%d")
    filename = "test_complaints_daily.csv"
    
    # 2. Generate Data
    expected_rows = create_dummy_data(filename, target_date)
    
    # 3. Run Orchestrator
    # We must set run_baseline=True to establish baseline from the generated history
    print("\n>>> Running Orchestrator...")
    orchestrator = Orchestrator()
    result = orchestrator.run_pipeline(
        file_path=filename,
        target_date=target_date,
        run_ingestion=True,
        run_baseline=True 
    )
    print(f"Orchestrator Result: {result}")
    
    if result.get('status') == 'error':
        print(f"!!! PIPELINE FAILED with error: {result.get('message')} !!!")
        return
    
    # 4. Verifications
    print("\n>>> Verifying Results...")
    
    # Check 1: Ingestion
    row_count = session.query(ComplaintsRaw).count()
    print(f"1. Ingestion Count: {row_count} / {expected_rows} ... ", end="")
    if row_count == expected_rows:
        print("PASS")
    else:
        print("FAIL")
    
    # Check 2: Anomalies
    # We expect an anomaly in Karachi on target_date
    anomalies = session.query(DailyAnomalies).filter_by(anomaly_date=target_date).all()
    print(f"2. Anomalies Found: {len(anomalies)} ... ", end="")
    found_karachi = False
    for a in anomalies:
        print(f"\n   - {a.dimension}:{a.dimension_key} (Z={a.z_score:.2f}, Val={a.metric_value})")
        if a.dimension == 'region' and a.dimension_key == 'Karachi':
            found_karachi = True
    
    if found_karachi:
        print("PASS (Karachi Anomaly Found)")
    else:
        print("FAIL (Karachi Anomaly NOT Found)")
        
    # Check 3: Trends
    trends = session.query(DailyTrends).filter_by(trend_date=target_date).all()
    print(f"3. Trends Calculated: {len(trends)} ... ", end="")
    if len(trends) > 0:
        print("PASS")
    else:
        print("FAIL")

    # Check 4: Insights
    insights = session.query(ExecInsights).all()
    print(f"4. Insights Generated: {len(insights)} ... ", end="")
    if len(insights) > 0:
        print("PASS")
        print(f"   - Title: {insights[0].title}")
        print(f"   - Summary: {insights[0].summary[:100]}...")
    else:
        print("FAIL")
        
    # Check 5: Trend Plotter (UI Support)
    print("\n>>> Testing Trend Plotter Agent...")
    tp_agent = TrendPlotterAgent()
    tp_result = tp_agent.run({"target_date": target_date, "days_back": 15})
    
    if tp_result['status'] == 'success':
        print("5. Trend Plotter: PASS")
        print(f"   - Total Trend Points: {len(tp_result.get('total_trend', []))}")
        print(f"   - Regions: {list(tp_result.get('region_trend', {}).keys())}")
    else:
        print(f"5. Trend Plotter: FAIL ({tp_result.get('message')})")

    # Cleanup
    session.close()
    if os.path.exists(filename):
        os.remove(filename)
    
    print("\n=== VERIFICATION COMPLETE ===")

if __name__ == "__main__":
    verify_pipeline()
