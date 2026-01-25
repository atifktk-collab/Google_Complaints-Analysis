import polars as pl
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

from ..db.mysql import get_engine

logger = logging.getLogger(__name__)

class BaselineAgent:
    """
    Agent responsible for calculating and updating historical baselines.
    Stores results in Parquet format in memory/baselines/.
    Now operates on DAILY data instead of hourly.
    """
    
    def __init__(self):
        self.engine = get_engine()
        self.baseline_dir = "complaints_ai/memory/baselines"
        self.dimensions = {
            "Type": "sr_type",
            "Region": "region",
            "Exchange": "exc_id",
            "City": "city",
            "RCA": "rca"
        }
        self.windows = [7, 14, 30]

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates baselines for daily data.
        Context can specify 'target_date' to calculate baselines up to that date.
        If not provided, uses current date (yesterday as last full day).
        """
        target_date_str = context.get('target_date', datetime.now().strftime("%Y-%m-%d"))
        logger.info(f"Updating daily baselines relative to {target_date_str}")
        
        try:
            # We need enough history for the max window (30 days)
            end_date = datetime.strptime(target_date_str, "%Y-%m-%d")
            start_date = end_date - timedelta(days=35) # Buffer
            
            query = f"""
                SELECT sr_open_dt, sr_type, region, exc_id, olt_id, rca
                FROM complaints_raw
                WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            """
            
            logger.info("Fetching data for baseline calculation...")
            df = pl.read_database(query, self.engine)
            
            if df.is_empty():
                logger.warning("No data found for baseline calculation.")
                return {"status": "warning", "message": "No data"}
            
            results = {}
            
            for dim_name, dim_col in self.dimensions.items():
                logger.info(f"Processing dimension: {dim_name}")
                
                # Group by Dimension + Date to get DAILY counts
                daily_counts = df.group_by([dim_col, "sr_open_dt"]).len().rename({"len": "count"})
                
                # Filter out the target_date itself from baseline calculation (avoid leakage)
                history_df = daily_counts.filter(pl.col("sr_open_dt") < pl.lit(end_date.date()))
                
                # Calculate statistics over different windows
                stats_dfs = []
                
                for window in self.windows:
                    cutoff_date = end_date - timedelta(days=window)
                    
                    window_df = history_df.filter(pl.col("sr_open_dt") >= pl.lit(cutoff_date.date()))
                    
                    # Group by dimension key and aggregate
                    agg_df = window_df.group_by([dim_col]).agg([
                        pl.col("count").mean().alias(f"avg_{window}d"),
                        pl.col("count").std().alias(f"std_{window}d"),
                        pl.col("count").count().alias(f"samples_{window}d") # How many days had data
                    ])
                    
                    stats_dfs.append(agg_df)
                
                # Join all windows
                final_dim_df = stats_dfs[0]
                for i in range(1, len(stats_dfs)):
                    final_dim_df = final_dim_df.join(stats_dfs[i], on=[dim_col], how="outer", coalesce=True)
                
                # Fill nulls (std might be null if 1 sample)
                final_dim_df = final_dim_df.fill_nan(0).fill_null(0)
                
                # Save to Parquet
                filename = f"{self.baseline_dir}/baseline_{dim_name.lower()}_daily.parquet"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                final_dim_df.write_parquet(filename)
                
                results[dim_name] = len(final_dim_df)
            
            logger.info("Daily baseline calculation complete.")
            return {"status": "success", "counts": results}

        except Exception as e:
            logger.exception("Baseline calculation failed")
            return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    agent = BaselineAgent()
    # agent.run({})
