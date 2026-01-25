from complaints_ai.db.mysql import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as conn:
    print("Updating schema for daily_anomalies...")
    conn.execute(text("ALTER TABLE daily_anomalies MODIFY COLUMN dimension ENUM('Type', 'Region', 'Exchange', 'OLT', 'RCA', 'Total')"))
    
    print("Updating schema for daily_trends...")
    conn.execute(text("ALTER TABLE daily_trends MODIFY COLUMN dimension ENUM('Type', 'Region', 'Exchange', 'OLT', 'RCA', 'Total')"))
    
    print("Updating schema for daily_variations...")
    conn.execute(text("ALTER TABLE daily_variations MODIFY COLUMN dimension ENUM('Type', 'Region', 'Exchange', 'OLT', 'RCA', 'Total')"))
    
    conn.commit()
    print("DB Schema Updated Successfully")
