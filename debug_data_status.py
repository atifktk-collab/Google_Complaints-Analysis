
import logging
from sqlalchemy import text
from complaints_ai.db.mysql import get_engine, get_session
from complaints_ai.db.models import ComplaintsRaw

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DebugDB")

def debug_data():
    print("=== DEBUGGING DATABASE CONTENT ===")
    from complaints_ai.db import mysql
    url = mysql.get_db_url()
    parts = url.split(":")
    print(f"DEBUG: URL (masked): {parts[0]}:{parts[1]}:***@{parts[2].split('@')[-1]}")
    
    session = get_session()
    engine = get_engine()
    
    # 1. Check Total Count
    count = session.query(ComplaintsRaw).count()
    print(f"Total Rows in complaints_raw: {count}")
    
    if count == 0:
        print("!! TABLE IS EMPTY !!")
        return

    # 2. Check Date Range
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MIN(sr_open_dt), MAX(sr_open_dt) FROM complaints_raw"))
        min_date, max_date = result.fetchone()
        print(f"Date Range: {min_date} to {max_date}")
        
    # 3. Check Distinct Dates
    print("\nDistinct Dates in DB:")
    with engine.connect() as conn:
        dates = conn.execute(text("SELECT DISTINCT sr_open_dt FROM complaints_raw ORDER BY sr_open_dt")).fetchall()
        for d in dates:
            print(f" - {d[0]}")
            
    # 4. Sample Rows
    print("\nSample Rows (Limit 5):")
    rows = session.query(ComplaintsRaw).limit(5).all()
    for r in rows:
        print(f"ID: {r.sr_row_id}, DTTM: {r.sr_open_dttm}, DT: {r.sr_open_dt}, Region: {r.region}")

    session.close()
    print("=== DEBUG COMPLETE ===")

if __name__ == "__main__":
    debug_data()
