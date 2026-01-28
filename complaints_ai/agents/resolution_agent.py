import polars as pl
import logging
from datetime import datetime, date
from typing import Dict, Any, List
from sqlalchemy import text

from ..db.mysql import get_engine, get_session
from ..db.models import DailyMTTR, DailyAging

logger = logging.getLogger(__name__)

class ResolutionAgent:
    """
    Agent responsible for analyzing resolution times (MTTR) and open complaint aging.
    """
    
    def __init__(self):
        self.engine = get_engine()
        self.dimensions = {
            "Region": "region",
            "City": "city",
            "Exchange": "exc_id"
        }

    def _calculate_mttr(self, target_date_str: str) -> List[Dict[str, Any]]:
        """Calculates Mean Time To Resolution for the given date."""
        query = f"""
            SELECT sr_open_dttm, sr_close_dttm, region, city, exc_id
            FROM complaints_raw
            WHERE DATE(sr_close_dttm) = '{target_date_str}'
        """
        df = pl.read_database(query, self.engine)
        
        if df.is_empty():
            return []

        # Convert to datetime and calculate duration in hours
        df = df.with_columns([
            (pl.col("sr_close_dttm") - pl.col("sr_open_dttm")).dt.total_seconds().alias("seconds")
        ])
        
        # Filter: Exclude if resolution is less than 5 minutes (300 seconds)
        df = df.filter(pl.col("seconds") >= 300)
        
        if df.is_empty():
            return []
            
        df = df.with_columns([
            (pl.col("seconds") / 3600).alias("duration_hours")
        ])
        
        results = []
        
        # 1. Overall MTTR
        total_mttr = df["duration_hours"].mean()
        results.append({
            "date": target_date_str,
            "dimension": "Total",
            "dimension_key": "All",
            "avg_mttr_hours": round(total_mttr, 2),
            "total_resolved_count": len(df)
        })
        
        # 2. Dimensional MTTR
        for dim_name, dim_col in self.dimensions.items():
            dim_df = df.group_by(dim_col).agg([
                pl.col("duration_hours").mean().alias("avg_mttr"),
                pl.len().alias("count")
            ])
            
            for row in dim_df.to_dicts():
                if row[dim_col]:
                    results.append({
                        "date": target_date_str,
                        "dimension": dim_name,
                        "dimension_key": str(row[dim_col]),
                        "avg_mttr_hours": round(row["avg_mttr"], 2),
                        "total_resolved_count": row["count"]
                    })
        
        return results

    def _calculate_aging(self, target_date_str: str) -> List[Dict[str, Any]]:
        """Calculates aging slabs for open complaints as of the target date."""
        # Use target_date end of day as reference for 'current age'
        ref_time = datetime.strptime(target_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        
        query = f"""
            SELECT sr_open_dttm, region, city, exc_id
            FROM complaints_raw
            WHERE sr_open_dt <= '{target_date_str}'
            AND (sr_close_dttm IS NULL OR sr_close_dttm > '{ref_time}')
            AND sr_status != 'Closed'
        """
        df = pl.read_database(query, self.engine)
        
        if df.is_empty():
            return []

        # Calculate age in hours
        df = df.with_columns([
            ((pl.lit(ref_time) - pl.col("sr_open_dttm")).dt.total_seconds() / 3600).alias("age_hours")
        ])
        
        # Define slabs
        def get_slab(hours):
            days = hours / 24
            if days > 60: return "> 60 Days"
            if days > 30: return "> 30 Days"
            if days > 10: return "> 10 Days"
            if days > 6:  return "> 6 Days"
            if hours > 72: return "> 72 Hours"
            if hours > 48: return "> 48 Hours"
            if hours > 24: return "> 24 Hours"
            return "Within 24 Hours"

        df = df.with_columns([
            pl.col("age_hours").map_elements(get_slab, return_dtype=pl.Utf8).alias("slab")
        ])
        
        # Filter out 'Within 24 Hours' if only 'greater than' requested, but keep for completeness
        # We will filter in results
        
        results = []
        
        target_slabs = ["> 24 Hours", "> 48 Hours", "> 72 Hours", "> 6 Days", "> 10 Days", "> 30 Days", "> 60 Days"]

        def process_dim(data_df, dim_name, dim_key_col):
            agg = data_df.group_by([dim_key_col, "slab"]).len()
            for row in agg.to_dicts():
                if row["slab"] in target_slabs and row[dim_key_col]:
                    results.append({
                        "date": target_date_str,
                        "dimension": dim_name,
                        "dimension_key": str(row[dim_key_col]),
                        "slab": row["slab"],
                        "count": row["len"]
                    })

        # Total Aging
        total_agg = df.group_by("slab").len()
        for row in total_agg.to_dicts():
            if row["slab"] in target_slabs:
                results.append({
                    "date": target_date_str,
                    "dimension": "Total",
                    "dimension_key": "All",
                    "slab": row["slab"],
                    "count": row["len"]
                })

        # Dimensional Aging
        for dim_name, dim_col in self.dimensions.items():
            process_dim(df, dim_name, dim_col)
            
        return results

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        target_date_str = context.get('target_date')
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
            
        logger.info(f"Running resolution and aging analysis for {target_date_str}")
        
        try:
            # 1. MTTR
            mttr_records = self._calculate_mttr(target_date_str)
            
            # 2. Aging
            aging_records = self._calculate_aging(target_date_str)
            
            # 3. Store in DB
            session = get_session()
            
            # MTTR Storage
            session.query(DailyMTTR).filter_by(date=target_date_str).delete()
            if mttr_records:
                session.add_all([DailyMTTR(**r) for r in mttr_records])
                
            # Aging Storage
            session.query(DailyAging).filter_by(date=target_date_str).delete()
            if aging_records:
                session.add_all([DailyAging(**r) for r in aging_records])
                
            session.commit()
            session.close()
            
            logger.info(f"Analysis complete for {target_date_str}. Stored {len(mttr_records)} MTTR and {len(aging_records)} Aging records.")
            
            return {
                "status": "success",
                "mttr_stored": len(mttr_records),
                "aging_stored": len(aging_records)
            }

        except Exception as e:
            logger.exception("Resolution analysis failed")
            return {"status": "error", "message": str(e)}
