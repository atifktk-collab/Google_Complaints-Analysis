import polars as pl
import logging
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import Dict, Any, List

from ..db.mysql import get_engine, get_session
from ..db.models import DailyAnomalies

logger = logging.getLogger(__name__)

class CorrelationAgent:
    """
    Agent responsible for finding correlations between detected anomalies and other dimensions.
    """
    
    def __init__(self):
        self.engine = get_engine()

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyzes correlations for anomalies on the target date.
        
        Args:
            context: Must contain 'target_date'.
        """
        target_date_str = context.get('target_date')
        
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
            
        logger.info(f"Running daily correlation analysis for {target_date_str}")
        
        try:
            session = get_session()
            # 1. Get Anomalies for the day
            anomalies = session.query(DailyAnomalies).filter_by(
                anomaly_date=target_date_str
            ).all()
            
            if not anomalies:
                logger.info("No anomalies to correlate.")
                session.close()
                return {"status": "success", "correlations_found": 0}

            # 2. Prepare data for correlation (Last 30 days counts)
            # We need a dataframe of daily counts for all dimensions over last 30 days.
            end_date = datetime.strptime(target_date_str, "%Y-%m-%d")
            start_date = end_date - timedelta(days=30)
            
            query = f"""
                SELECT sr_open_dt, sr_type, region, exc_id, city, rca
                FROM complaints_raw
                WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            """
            
            raw_df = pl.read_database(query, self.engine)
            
            if raw_df.is_empty():
                session.close()
                return {"status": "warning", "message": "No history for correlation"}
                
            # Use date as time bucket
            raw_df = raw_df.rename({"sr_open_dt": "time_bucket"})
            
            # Helper to get series for a dimension key
            def get_series(dim_col, dim_value):
                # Filter for value, group by time, count
                # Ensure all hours are present? Pearson corr handles alignments if we join.
                s_df = raw_df.filter(pl.col(dim_col) == dim_value) \
                             .group_by("time_bucket").len().rename({"len": "count"}) \
                             .sort("time_bucket")
                return s_df

            # Pre-calculate counts for top items in other dimensions to compare against?
            # Doing exhaustive search is expensive.
            # Strategy: For each anomaly, check against top 5 items in other dimensions.
            
            # Identify "Top" items
            top_regions = raw_df.group_by("region").len().sort("len", descending=True).limit(5)["region"].to_list()
            top_types = raw_df.group_by("sr_type").len().sort("len", descending=True).limit(5)["sr_type"].to_list()
            # Add others as needed
            
            updates = 0
            
            for anomaly in anomalies:
                primary_dim = anomaly.dimension
                primary_key = anomaly.dimension_key
                
                # Get map of column name
                dim_map = {
                    "Type": "sr_type", "Region": "region", "Exchange": "exc_id",
                    "City": "city", "RCA": "rca"
                }
                primary_col = dim_map.get(primary_dim)
                if not primary_col: continue
                
                s1 = get_series(primary_col, primary_key)
                if s1.height < 3: continue # Not enough points
                
                correlations = []
                
                # Check against other dimensions.
                # Example: If Anomaly is Region=Karachi, check against Top Types.
                targets = []
                if primary_dim != "Type":
                    targets.extend([("sr_type", t) for t in top_types])
                if primary_dim != "Region":
                    targets.extend([("region", r) for r in top_regions])
                    
                # Calculate
                for t_col, t_val in targets:
                    s2 = get_series(t_col, t_val)
                    if s2.height < 3: continue
                    
                    # Join on time_bucket
                    joined = s1.join(s2, on="time_bucket", how="inner", suffix="_2")
                    if joined.height < 3: continue
                    
                    # Pearson
                    corr = joined.select(pl.corr("count", "count_2")).item()
                    
                    if corr and corr > 0.7:
                        correlations.append(f"{t_val} ({corr:.2f})")
                
                if correlations:
                    # Update anomaly record
                    existing_ctx = anomaly.rca_context or ""
                    new_ctx = f"Correlated with: {', '.join(correlations)}"
                    if existing_ctx:
                        anomaly.rca_context = existing_ctx + " | " + new_ctx
                    else:
                        anomaly.rca_context = new_ctx
                    
                    updates += 1

            if updates > 0:
                session.commit()
                logger.info(f"Updated {updates} anomalies with correlation info.")
            
            session.close()
            return {"status": "success", "updates": updates}

        except Exception as e:
            logger.exception("Correlation analysis failed")
            return {"status": "error", "message": str(e)}
