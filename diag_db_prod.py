import os
import sys

# Production import sequence
from complaints_ai.db.mysql import get_engine, get_session
from complaints_ai.db.models import ComplaintsRaw

print("Imports successful.")

try:
    # 1. Check generated URL
    from complaints_ai.db import mysql
    url = mysql.get_db_url()
    parts = url.split(":")
    print(f"URL (masked): {parts[0]}:{parts[1]}:***@{parts[2].split('@')[-1]}")
    
    # 2. Check engine construction
    engine = get_engine()
    print("Engine created.")
    
    # 3. Try a real query
    with engine.connect() as conn:
        from sqlalchemy import text
        res = conn.execute(text("SELECT count(*) FROM complaints_raw"))
        print(f"Total rows in DB: {res.scalar()}")
    print("SUCCESS: Connection and query both worked!")
    
except Exception as e:
    print(f"FAILURE: {e}")
    if "1045" in str(e):
        print("Detail: Access Denied error detected.")
