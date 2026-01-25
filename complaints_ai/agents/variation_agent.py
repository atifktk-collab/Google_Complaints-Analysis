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
            "City": "city",
            "RCA": "rca",
            "Total": "pl.lit('Total')"
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
        Calculates variations for a specific date using new redefined logic:
        - DOD: Target Date vs Same Day Last Week
        - WOW: WTD average (Mon-Sun) vs Previous Week same days average
        - MOM: Current Month average vs Last Month same relative days average
        
        Args:
            context: Must contain 'target_date' (str YYYY-MM-DD).
        """
        target_date_str = context.get('target_date')
        
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
            
        logger.info(f"Running redefined variation analysis for {target_date_str}")
        
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
            
            # --- DOD Logic: Target Date vs Same Day Last Week ---
            # Using timedelta(days=7)
            dod_prev_date = target_date - timedelta(days=7)
            
            # --- WOW Logic: WTD Average vs Previous Week Average ---
            # Current Week start (Monday)
            current_week_start = target_date - timedelta(days=target_date.weekday())
            # Previous Week range (same relative offset)
            prev_week_start = current_week_start - timedelta(days=7)
            prev_week_end = target_date - timedelta(days=7)
            
            # --- MOM Logic: Monthly average comparison ---
            current_month_start = target_date.replace(day=1)
            # Find last month same relative days
            if target_date.month == 1:
                prev_month_start = target_date.replace(year=target_date.year-1, month=12, day=1)
            else:
                prev_month_start = target_date.replace(month=target_date.month-1, day=1)
                
            prev_month_end = prev_month_start + (target_date - current_month_start)

            all_variations = []
            
            for dim_name, dim_col in self.dimensions.items():
                logger.info(f"Analyzing variations for dimension: {dim_name}")
                
                # Fetch all necessary dates in range efficiently (Max buffer needed: month + buffer)
                fetch_start = prev_month_start
                # Fetch daily counts for the entire period
                if dim_col.startswith("pl.lit"):
                    query = f"""
                        SELECT sr_open_dt, COUNT(*) as count
                        FROM complaints_raw
                        WHERE sr_open_dt BETWEEN '{fetch_start.date()}' AND '{target_date.date()}'
                        GROUP BY sr_open_dt
                    """
                    df = pl.read_database(query, self.engine)
                    df = df.with_columns(pl.lit("Total").alias("total_col"))
                    dim_col_effective = "total_col"
                else:
                    query = f"""
                        SELECT sr_open_dt, {dim_col}, COUNT(*) as count
                        FROM complaints_raw
                        WHERE sr_open_dt BETWEEN '{fetch_start.date()}' AND '{target_date.date()}'
                        GROUP BY sr_open_dt, {dim_col}
                    """
                    df = pl.read_database(query, self.engine)
                    dim_col_effective = dim_col
                
                if df.is_empty():
                    continue

                # Get unique dimension keys found for target date
                active_keys = df.filter(pl.col("sr_open_dt") == pl.lit(target_date.date()))[dim_col_effective].unique().to_list()
                
                for key in active_keys:
                    key_df = df.filter(pl.col(dim_col_effective) == key)
                    
                    # 1. DOD (Single Day vs Same Day Last week)
                    curr_val = key_df.filter(pl.col("sr_open_dt") == pl.lit(target_date.date()))["count"].sum()
                    prev_val_dod = key_df.filter(pl.col("sr_open_dt") == pl.lit(dod_prev_date.date()))["count"].sum()
                    dod_var = self.calculate_variation(curr_val, prev_val_dod)
                    
                    all_variations.append({
                        "variation_date": target_date_str, "dimension": dim_name, "dimension_key": str(key),
                        "current_value": curr_val, "previous_value": prev_val_dod,
                        "variation_type": "DOD", "variation_percent": dod_var["variation_percent"],
                        "is_significant": 1 if dod_var["is_significant"] else 0
                    })
                    
                    # 2. WOW (WTD Average)
                    wtd_curr_avg = key_df.filter(
                        (pl.col("sr_open_dt") >= pl.lit(current_week_start.date())) & 
                        (pl.col("sr_open_dt") <= pl.lit(target_date.date()))
                    )["count"].mean() or 0
                    
                    wtd_prev_avg = key_df.filter(
                        (pl.col("sr_open_dt") >= pl.lit(prev_week_start.date())) & 
                        (pl.col("sr_open_dt") <= pl.lit(prev_week_end.date()))
                    )["count"].mean() or 0
                    
                    wow_var = self.calculate_variation(wtd_curr_avg, wtd_prev_avg)
                    
                    all_variations.append({
                        "variation_date": target_date_str, "dimension": dim_name, "dimension_key": str(key),
                        "current_value": wtd_curr_avg, "previous_value": wtd_prev_avg,
                        "variation_type": "WOW", "variation_percent": wow_var["variation_percent"],
                        "is_significant": 1 if wow_var["is_significant"] else 0
                    })
                    
                    # 3. MOM (MTD Average)
                    mtd_curr_avg = key_df.filter(
                        (pl.col("sr_open_dt") >= pl.lit(current_month_start.date())) & 
                        (pl.col("sr_open_dt") <= pl.lit(target_date.date()))
                    )["count"].mean() or 0
                    
                    mtd_prev_avg = key_df.filter(
                        (pl.col("sr_open_dt") >= pl.lit(prev_month_start.date())) & 
                        (pl.col("sr_open_dt") <= pl.lit(prev_month_end.date()))
                    )["count"].mean() or 0
                    
                    mom_var = self.calculate_variation(mtd_curr_avg, mtd_prev_avg)
                    
                    all_variations.append({
                        "variation_date": target_date_str, "dimension": dim_name, "dimension_key": str(key),
                        "current_value": mtd_curr_avg, "previous_value": mtd_prev_avg,
                        "variation_type": "MOM", "variation_percent": mom_var["variation_percent"],
                        "is_significant": 1 if mom_var["is_significant"] else 0
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
