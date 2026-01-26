import polars as pl
import os
import logging
import yaml
from datetime import datetime, timedelta
from typing import Dict, Any, List
from scipy import stats
import numpy as np

from ..db.mysql import get_engine, get_session
from ..db.models import DailyTrends

logger = logging.getLogger(__name__)

class TrendAgent:
    """
    Agent responsible for detecting and analyzing daily trends.
    Calculates trend direction, strength, and significance over multiple time windows.
    """
    
    def __init__(self):
        self.engine = get_engine()
        self.config_path = "complaints_ai/config.yaml"
        self.load_config()
        self.dimensions = {
            "Type": "sr_type",
            "Region": "region",
            "Exchange": "exc_id",
            "City": "city",
            "RCA": "rca"
        }
        self.windows = [7, 14, 30]

    def load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                self.significance_threshold = config['thresholds'].get('trend_significance', 0.05)
        except Exception:
            logger.warning("Could not load config.yaml, using defaults.")
            self.significance_threshold = 0.05

    def calculate_trend(self, values: List[float]) -> Dict[str, Any]:
        """
        Calculate trend direction, strength, and significance using linear regression.
        
        Returns:
            dict with 'direction', 'strength', 'significance'
        """
        if len(values) < 3:
            return {"direction": "STABLE", "strength": 0.0, "significance": 1.0}
        
        # Prepare data for linear regression
        x = np.arange(len(values))
        y = np.array(values)
        
        # Perform linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        
        # Calculate percentage change over the period
        if y[0] != 0:
            strength = ((y[-1] - y[0]) / y[0]) * 100
        else:
            strength = 0.0
        
        # Determine direction
        if p_value < self.significance_threshold:
            if slope > 0:
                direction = "UP"
            elif slope < 0:
                direction = "DOWN"
            else:
                direction = "STABLE"
        else:
            direction = "STABLE"
        
        return {
            "direction": direction,
            "strength": strength,
            "significance": p_value
        }

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyzes trends for a specific date.
        
        Args:
            context: Must contain 'target_date' (str YYYY-MM-DD).
        """
        target_date_str = context.get('target_date')
        
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
            
        logger.info(f"Running trend analysis for {target_date_str}")
        
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
            
            all_trends = []
            target_dims = context.get('target_dimensions')
            
            for dim_name, dim_col in self.dimensions.items():
                # Filter by target dimensions if provided
                if target_dims and dim_name not in target_dims:
                    continue
                    
                logger.info(f"Analyzing trends for dimension: {dim_name}")
                
                for window in self.windows:
                    start_date = target_date - timedelta(days=window)
                    
                    # Fetch historical data for the window
                    query = f"""
                        SELECT sr_open_dt, {dim_col}
                        FROM complaints_raw
                        WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{target_date.date()}'
                    """
                    
                    df = pl.read_database(query, self.engine)
                    
                    if df.is_empty():
                        continue
                    
                    # Group by dimension key and date
                    daily_counts = df.group_by([dim_col, "sr_open_dt"]).len().rename({"len": "count"})
                    
                    # Get unique dimension keys
                    dim_keys = daily_counts[dim_col].unique().to_list()
                    
                    for key in dim_keys:
                        # Filter for this specific key
                        key_data = daily_counts.filter(pl.col(dim_col) == key).sort("sr_open_dt")
                        
                        if len(key_data) < 3:
                            continue
                        
                        values = key_data["count"].to_list()
                        current_value = values[-1] if values else 0
                        
                        # Calculate trend
                        trend_result = self.calculate_trend(values)
                        
                        # Handle NaN significance (MySQL doesn't like NaN)
                        significance = trend_result["significance"]
                        if np.isnan(significance):
                            significance = None
                        
                        all_trends.append({
                            "trend_date": target_date_str,
                            "dimension": dim_name,
                            "dimension_key": str(key),
                            "metric_value": current_value,
                            "trend_direction": trend_result["direction"],
                            "trend_strength": trend_result["strength"],
                            "window_days": window,
                            "significance": significance
                        })
            
            # Store trends in database
            if all_trends:
                session = get_session()
                # Remove existing trends for this date to allow re-runs
                session.query(DailyTrends).filter(
                    DailyTrends.trend_date == target_date_str
                ).delete()
                
                db_objects = [DailyTrends(**t) for t in all_trends]
                session.add_all(db_objects)
                session.commit()
                session.close()
                
                logger.info(f"Stored {len(all_trends)} trend records.")
            else:
                logger.info("No trends calculated.")
            
            return {
                "status": "success",
                "trends_calculated": len(all_trends)
            }

        except Exception as e:
            logger.exception("Trend analysis failed")
            return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    agent = TrendAgent()
    # agent.run({"target_date": "2026-01-23"})
