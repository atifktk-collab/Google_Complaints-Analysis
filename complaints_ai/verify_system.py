import os
import csv
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text

from .db.mysql import get_engine, init_db, get_session
from .db.models import ComplaintsRaw, HourlyAnomalies, ExecInsights
from .orchestrator import Orchestrator

def verify():
    print("=== Starting Verification ===")
    
    # 1. Init DB
    print("Initializing Database...")
    try:
        init_db()
        print("Database initialized.")
    except Exception as e:
        print(f"DB Init Failed: {e}")
        return

    # 2. Create Dummy Data
    print("Creating Test Data...")
    test_file = "test_complaints.csv"
    
    # Create valid rows
    headers = ["sr_row_id", "sr_open_dttm", "sr_type", "region", "exc_id", "olt_id", "rca", "status", "vendor", "product_id"]
    
    # Generate 50 rows, with a spike in Region 'Karachi' for 'Internet'
    rows = []
    base_time = datetime.now()
    
    # 1. Historical Data (7 days ago) - Normal Volume (5 per hour)
    history_date = base_time - timedelta(days=7)
    for h in range(24): # Fill 24 hours
        for i in range(5):
            row_id = f"HIST_{h}_{i}"
            ts = history_date.replace(hour=h, minute=10).strftime("%Y-%m-%d %H:%M:%S")
            rows.append([
                row_id, ts, "Internet", "Karachi", "Exc1", "OLT1", "Router", "Closed", "Huawei", "GPON"
            ])

    # 2. Today's Data (Spike)
    target_date = base_time.strftime("%Y-%m-%d")
    target_hour = base_time.hour
    
    for i in range(50):
        # Spike in Karachi Internet for current hour
        row_id = f"CURR_{i}"
        ts = base_time.strftime("%Y-%m-%d %H:%M:%S")
        region = "Karachi"
        sr_type = "Internet"
        rca = "Fiber Cut" if i < 30 else "Router"
        
        rows.append([
            row_id, ts, sr_type, region, "Exc1", "OLT1", rca, "Closed", "Huawei", "GPON"
        ])
        
    with open(test_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
        
    print(f"Created {test_file} with {len(rows)} rows.")
    
    # 3. Run Pipeline
    print("Running Orchestrator...")
    orchestrator = Orchestrator()
    try:
        # Run pipeline
        orchestrator.run_pipeline(
            file_path=test_file,
            run_ingestion=True,
            run_baseline=True # Calculate baseline (will be based on this data if no history, might be weird but strictly 0 history => valid baseline of 0?)
            # Actually BaselineAgent excludes current date. So history will be empty.
        )
        print("Pipeline finished.")
    except Exception as e:
        print(f"Pipeline Failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # 4. Verify DB Content
    session = get_session()
    
    # Check Ingestion
    count = session.query(ComplaintsRaw).count()
    print(f"ComplaintsRaw Count: {count}")
    
    # Check Anomaly
    # Since we have no history, baseline is 0. Current is 40. Z-score should be high.
    anomalies = session.query(HourlyAnomalies).all()
    print(f"Anomalies Found: {len(anomalies)}")
    for a in anomalies:
        print(f" - [{a.severity}] {a.dimension}:{a.dimension_key} (Z={a.z_score:.2f})")
        if a.rca_context:
            print(f"   Context: {a.rca_context}")

    # Check Insights
    insights = session.query(ExecInsights).all()
    print(f"Insights Generated: {len(insights)}")
    for i in insights:
        try:
            print(f" - {i.title}: {i.summary}")
        except UnicodeEncodeError:
            print(f" - {i.title}: {i.summary.encode('ascii', 'ignore').decode()}")

    session.close()
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
        
    print("=== Verification Complete ===")

if __name__ == "__main__":
    verify()
