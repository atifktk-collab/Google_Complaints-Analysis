import polars as pl
import os
import logging
import yaml
from datetime import datetime, timedelta
from typing import Dict, Any, List

from ..db.mysql import get_engine, get_session
from ..db.models import DailyVariations

logger = logging.getLogger(__name__)

class VariationAgent:
    """
    Agent responsible for tracking day-over-day, week-over-week, and month-over-month variations.
    Identifies significant changes in complaint volumes.
    """
    
    def __init__(self):
        self.engine = get_engine()
        self.config_path = "complaints_ai/config.yaml"
        self.load_config()
        self.dimensions = {
            "Type": "sr_type",
            "Region": "region",
            "Exchange": "exc_id",
            "OLT": "olt_id",
            "RCA": "rca"
        }

    def load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                self.variation_threshold = config['thresholds'].get('variation_threshold_percent', 15.0)
        except Exception:
            logger.warning("Could not load config.yaml, using defaults.")
            self.variation_threshold = 15.0

    def calculate_variation(self, current: float, previous: float) -> Dict[str, Any]:
        """
        Calculate percentage variation and determine if it's significant.
        
        Returns:
            dict with 'variation_percent' and 'is_significant'
        """
        if previous == 0:
            if current > 0:
                return {"variation_percent": 100.0, "is_significant": True}
            else:
                return {"variation_percent": 0.0, "is_significant": False}
        
        variation_percent = ((current - previous) / previous) * 100
        is_significant = abs(variation_percent) >= self.variation_threshold
        
        return {
            "variation_percent": variation_percent,
            "is_significant": is_significant
        }

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculates variations for a specific date.
        
        Args:
            context: Must contain 'target_date' (str YYYY-MM-DD).
        """
        target_date_str = context.get('target_date')
        
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
            
        logger.info(f"Running variation analysis for {target_date_str}")
        
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
            
            # Define comparison periods
            comparisons = {
                "DOD": timedelta(days=1),      # Day over Day
                "WOW": timedelta(days=7),      # Week over Week
                "MOM": timedelta(days=30)      # Month over Month
            }
            
            all_variations = []
            
            for dim_name, dim_col in self.dimensions.items():
                logger.info(f"Analyzing variations for dimension: {dim_name}")
                
                # Fetch current day data
                current_query = f"""
                    SELECT {dim_col}, COUNT(*) as count
                    FROM complaints_raw
                    WHERE sr_open_dt = '{target_date.date()}'
                    GROUP BY {dim_col}
                """
                current_df = pl.read_database(current_query, self.engine)
                
                if current_df.is_empty():
                    continue
                
                for var_type, delta in comparisons.items():
                    comparison_date = target_date - delta
                    
                    # Fetch comparison period data
                    comparison_query = f"""
                        SELECT {dim_col}, COUNT(*) as count
                        FROM complaints_raw
                        WHERE sr_open_dt = '{comparison_date.date()}'
                        GROUP BY {dim_col}
                    """
                    comparison_df = pl.read_database(comparison_query, self.engine)
                    
                    # Join current and comparison data
                    merged = current_df.join(
                        comparison_df, 
                        on=dim_col, 
                        how="outer",
                        suffix="_prev"
                    )
                    
                    # Fill nulls with 0
                    merged = merged.with_columns([
                        pl.col("count").fill_null(0),
                        pl.col("count_prev").fill_null(0)
                    ])
                    
                    # Calculate variations
                    for row in merged.to_dicts():
                        current_val = row.get("count", 0)
                        previous_val = row.get("count_prev", 0)
                        
                        variation = self.calculate_variation(current_val, previous_val)
                        
                        all_variations.append({
                            "variation_date": target_date_str,
                            "dimension": dim_name,
                            "dimension_key": str(row[dim_col]),
                            "current_value": current_val,
                            "previous_value": previous_val,
                            "variation_type": var_type,
                            "variation_percent": variation["variation_percent"],
                            "is_significant": 1 if variation["is_significant"] else 0
                        })
            
            # Store variations in database
            if all_variations:
                session = get_session()
                # Remove existing variations for this date to allow re-runs
                session.query(DailyVariations).filter(
                    DailyVariations.variation_date == target_date_str
                ).delete()
                
                db_objects = [DailyVariations(**v) for v in all_variations]
                session.add_all(db_objects)
                session.commit()
                session.close()
                
                logger.info(f"Stored {len(all_variations)} variation records.")
            else:
                logger.info("No variations calculated.")
            
            return {
                "status": "success",
                "variations_calculated": len(all_variations)
            }

        except Exception as e:
            logger.exception("Variation analysis failed")
            return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    agent = VariationAgent()
    # agent.run({"target_date": "2026-01-23"})
