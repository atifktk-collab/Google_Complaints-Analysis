import polars as pl
import logging
from typing import Dict, Any

from ..db.mysql import get_engine, get_session
from ..db.models import DailyAnomalies

logger = logging.getLogger(__name__)

class RCAAgent:
    """
    Agent responsible for identifying probable Root Causes (RCA) for detected anomalies.
    """
    
    def __init__(self):
        self.engine = get_engine()

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Identifies RCA for anomalies.
        """
        target_date_str = context.get('target_date')
        
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
            
        logger.info("Running RCA identification...")
        
        try:
            session = get_session()
            anomalies = session.query(DailyAnomalies).filter_by(
                anomaly_date=target_date_str
            ).all()
            
            updates = 0
            
            for anomaly in anomalies:
                # If dimension is already RCA, no need to find RCA
                if anomaly.dimension == "RCA":
                    continue
                
                # Fetch detailed data for this anomaly's scope
                dim_map = {
                    "Type": "sr_type", "Region": "region", "Exchange": "exc_id",
                    "OLT": "olt_id"
                }
                col_name = dim_map.get(anomaly.dimension)
                if not col_name: continue
                
                query = f"""
                    SELECT rca
                    FROM complaints_raw
                    WHERE sr_open_dt = '{target_date_str}'
                    AND {col_name} = '{anomaly.dimension_key}'
                """
                
                df = pl.read_database(query, self.engine)
                
                if df.is_empty(): continue
                
                # Find top RCA
                top_rca = df.group_by("rca").len().sort("len", descending=True).head(3)
                
                if not top_rca.is_empty():
                    # Format: "Fiber Cut (50%), Hardware Fault (20%)"
                    total = len(df)
                    rca_strs = []
                    for row in top_rca.iter_rows(named=True):
                        pct = (row['len'] / total) * 100
                        rca_strs.append(f"{row['rca']} ({pct:.1f}%)")
                    
                    rca_text = f"Probable RCA: {', '.join(rca_strs)}"
                    
                    # Update anomaly
                    existing = anomaly.rca_context or ""
                    if existing:
                        anomaly.rca_context = existing + " | " + rca_text
                    else:
                        anomaly.rca_context = rca_text
                    
                    updates += 1
            
            if updates:
                session.commit()
                logger.info(f"Identified RCA for {updates} anomalies.")
                
            session.close()
            return {"status": "success", "updates": updates}

        except Exception as e:
            logger.exception("RCA identification failed")
            return {"status": "error", "message": str(e)}
