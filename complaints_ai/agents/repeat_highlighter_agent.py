import polars as pl
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

from ..db.mysql import get_engine

logger = logging.getLogger(__name__)

class RepeatHighlighterAgent:
    """
    Agent responsible for detecting repeat complainers (MDNs) within the last 30 days.
    Categorizes repeats by frequency:
    - Alarming: > 3 repeats
    - Critical: > 6 repeats
    - Very Alarming: > 10 repeats
    """
    
    def __init__(self):
        self.engine = get_engine()
        self.thresholds = {
            "alarming": 3,
            "critical": 6,
            "very_alarming": 10
        }

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyzes MDN repetitions for the 30 days preceding the target date.
        
        Args:
            context: Must contain 'target_date' (str YYYY-MM-DD).
        
        Returns:
            Dictionary with repeat analysis results.
        """
        target_date_str = context.get('target_date')
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
        
        logger.info(f"Running repeat analysis for {target_date_str}")
        
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
            start_date = target_date - timedelta(days=30)
            
            # 1. Fetch data for the last 30 days
            # Now including sr_sub_type
            query = f"""
                SELECT mdn, region, exc_id, city, sr_open_dt, sr_sub_type
                FROM complaints_raw
                WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{target_date.date()}'
                AND mdn IS NOT NULL AND mdn != ''
            """
            
            df = pl.read_database(query, self.engine)
            
            if df.is_empty():
                return {
                    "status": "success",
                    "total_repeaters": 0,
                    "period": f"{start_date.date()} to {target_date.date()}",
                    "summaries": {
                        "regional": [],
                        "regional_subtype": [],
                        "exchange": [],
                        "exchange_subtype": [],
                        "city": [],
                        "city_subtype": [],
                        "severity": [],
                        "subtype_overall": []
                    },
                    "top_repeaters": []
                }
            
            # 2. Identify Repeaters (Total)
            # Group by MDN to get count of SRs
            mdn_counts = df.group_by("mdn").agg([
                pl.len().alias("repeat_count"),
                pl.col("region").first().alias("region"),
                pl.col("exc_id").first().alias("exc_id"),
                pl.col("city").first().alias("city"),
                pl.col("sr_sub_type").mode().first().alias("SR_Sub_Type")
            ]).filter(pl.col("repeat_count") > 1)
            
            # 3. Apply Severity Frequency Thresholds
            mdn_counts = mdn_counts.with_columns(
                pl.when(pl.col("repeat_count") > self.thresholds["very_alarming"]).then(pl.lit("VERY ALARMING"))
                .when(pl.col("repeat_count") > self.thresholds["critical"]).then(pl.lit("CRITICAL"))
                .when(pl.col("repeat_count") > self.thresholds["alarming"]).then(pl.lit("ALARMING"))
                .otherwise(pl.lit("NORMAL REPEAT")).alias("severity")
            )
            
            # 4. Aggregations
            total_repeat_count = len(mdn_counts)
            
            # Regional breakdown
            regional_summary = mdn_counts.group_by("region").agg(pl.len().alias("count")).to_dicts()
            
            # Regional Sub-Type breakdown
            regional_subtype = mdn_counts.group_by(["region", "SR_Sub_Type"]).agg(pl.len().alias("count")).to_dicts()
            
            # Exchange breakdown
            exchange_summary = mdn_counts.group_by("exc_id").agg(pl.len().alias("count")).to_dicts()
            
            # Exchange Sub-Type breakdown
            exchange_subtype = mdn_counts.group_by(["exc_id", "SR_Sub_Type"]).agg(pl.len().alias("count")).to_dicts()
            
            # City breakdown
            city_summary = mdn_counts.group_by("city").agg(pl.len().alias("count")).to_dicts()
            
            # City Sub-Type breakdown
            city_subtype = mdn_counts.group_by(["city", "SR_Sub_Type"]).agg(pl.len().alias("count")).to_dicts()
            
            # Severity breakdown
            severity_summary = mdn_counts.group_by("severity").agg(pl.len().alias("count")).to_dicts()
            
            # Top SR Sub-Types overall among repeaters
            subtype_summary = mdn_counts.group_by("SR_Sub_Type").agg(pl.len().alias("count")).sort("count", descending=True).to_dicts()
            
            result = {
                "status": "success",
                "target_date": target_date_str,
                "period": f"{start_date.date()} to {target_date.date()}",
                "total_repeaters": total_repeat_count,
                "summaries": {
                    "regional": regional_summary,
                    "regional_subtype": regional_subtype,
                    "exchange": exchange_summary,
                    "exchange_subtype": exchange_subtype,
                    "city": city_summary,
                    "city_subtype": city_subtype,
                    "severity": severity_summary,
                    "subtype_overall": subtype_summary
                },
                "top_repeaters": mdn_counts.sort("repeat_count", descending=True).head(100).to_dicts()
            }
            
            logger.info(f"Repeat analysis complete. Found {total_repeat_count} repeaters.")
            return result
            
        except Exception as e:
            logger.exception("Repeat highlighter failed")
            return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    agent = RepeatHighlighterAgent()
    # test
    # print(agent.run({"target_date": "2026-01-24"}))
