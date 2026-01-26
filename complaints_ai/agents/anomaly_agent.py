import polars as pl
import os
import logging
import yaml
from datetime import datetime
from typing import Dict, Any, List

from ..db.mysql import get_engine, get_session
from ..db.models import DailyAnomalies, Base

logger = logging.getLogger(__name__)

class AnomalyAgent:
    """
    Agent responsible for detecting anomalies using Z-scores against historical baselines.
    Now operates on DAILY data instead of hourly.
    """
    
    def __init__(self):
        self.engine = get_engine()
        self.baseline_dir = "complaints_ai/memory/baselines"
        self.config_path = "complaints_ai/config.yaml"
        self.load_config()
        self.dimensions = {
            "Type": "sr_type",
            "Region": "region",
            "Exchange": "exc_id",
            "City": "city",
            "RCA": "rca"
        }

    def load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                self.threshold_warning = config['thresholds'].get('z_score_warning', 2.0)
                self.threshold_critical = config['thresholds'].get('z_score_critical', 3.0)
        except Exception:
            logger.warning("Could not load config.yaml, using defaults.")
            self.threshold_warning = 2.0
            self.threshold_critical = 3.0

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detects anomalies for a specific date.
        
        Args:
            context: Must contain 'target_date' (str YYYY-MM-DD).
        """
        target_date_str = context.get('target_date')
        
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
            
        logger.info(f"Running daily anomaly detection for {target_date_str}")
        
        try:
            # 1. Fetch current data for the entire day
            query = f"""
                SELECT sr_type, region, exc_id, city, rca
                FROM complaints_raw
                WHERE sr_open_dt = '{target_date_str}'
            """
            current_df = pl.read_database(query, self.engine)
            
            if current_df.is_empty():
                logger.info("No complaints found for this date. No anomalies to detect.")
                return {"status": "success", "anomalies_found": 0}

            anomalies = []
            
            # 2. Iterate dimensions
            target_dims = context.get('target_dimensions')
            
            for dim_name, dim_col in self.dimensions.items():
                # Filter by target dimensions if provided
                if target_dims and dim_name not in target_dims:
                    continue
                    
                baseline_file = f"{self.baseline_dir}/baseline_{dim_name.lower()}_daily.parquet"
                if not os.path.exists(baseline_file):
                    logger.warning(f"Baseline file not found for {dim_name}, skipping.")
                    continue
                
                # Load baseline
                baseline_df = pl.read_parquet(baseline_file)
                
                # Aggregate current data
                current_counts = current_df.group_by(dim_col).len().rename({"len": "current_count"})
                
                # Join with baseline (default 30d window for z-score)
                merged = current_counts.join(baseline_df, on=dim_col, how="left")
                
                # Calculate Z-Score
                # z = (current - avg) / std
                merged = merged.with_columns([
                    pl.col("avg_30d").fill_null(0),
                    pl.col("std_30d").fill_null(0)
                ])
                
                # Calculate Z-score with epsilon to avoid division by zero
                epsilon = 0.001
                merged = merged.with_columns([
                     ((pl.col("current_count") - pl.col("avg_30d")) / (pl.col("std_30d") + epsilon)).alias("z_score")
                ])
                
                # Filter for anomalies
                detected = merged.filter(pl.col("z_score") > self.threshold_warning)
                
                if detected.is_empty():
                    continue
                
                # Convert to dictionaries and prepare for DB insert
                rows = detected.to_dicts()
                for row in rows:
                    severity = "WARNING"
                    if row['z_score'] > self.threshold_critical:
                        severity = "CRITICAL"
                    
                    anomalies.append({
                        "anomaly_date": target_date_str,
                        "dimension": dim_name,
                        "dimension_key": str(row[dim_col]),
                        "metric_value": row['current_count'],
                        "baseline_avg": row['avg_30d'],
                        "baseline_std": row['std_30d'],
                        "z_score": row['z_score'],
                        "severity": severity,
                        "rca_context": "" # To be filled by RCA agent or Correlation
                    })

            # 3. Store Anomalies
            if anomalies:
                session = get_session()
                # Remove existing anomalies for this date to allow re-runs (idempotency)
                session.query(DailyAnomalies).filter(
                    DailyAnomalies.anomaly_date == target_date_str
                ).delete()
                
                db_objects = [DailyAnomalies(**a) for a in anomalies]
                session.add_all(db_objects)
                session.commit()
                session.close()
                
                logger.info(f"Detected and stored {len(anomalies)} daily anomalies.")
            else:
                logger.info("No anomalies detected.")

            return {
                "status": "success", 
                "anomalies_found": len(anomalies),
                "anomalies": anomalies
            }

        except Exception as e:
            logger.exception("Anomaly detection failed")
            return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    agent = AnomalyAgent()
    # agent.run({"target_date": "2023-10-01"})
