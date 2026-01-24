import polars as pl
from sqlalchemy import text
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

from ..db.mysql import get_engine

logger = logging.getLogger(__name__)

class ValidationAgent:
    """
    Agent responsible for validating data quality and detecting simple anomalies like missing hours.
    """
    
    def __init__(self):
        self.engine = get_engine()

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates data for the given time range.
        
        Args:
            context: Must contain 'start_date' and 'end_date' (str or date objects).
        """
        start_date = context.get('start_date')
        end_date = context.get('end_date')
        
        if not start_date or not end_date:
            logger.error("Missing start_date or end_date in context")
            return {"status": "error", "message": "Missing date range"}
            
        logger.info(f"Running validation for {start_date} to {end_date}")
        
        try:
            # Load data for the period
            query = f"""
                SELECT sr_row_id, sr_open_dttm, region, sr_type, rca, status
                FROM complaints_raw
                WHERE sr_open_dt BETWEEN '{start_date}' AND '{end_date}'
            """
            
            df = pl.read_database(query, self.engine)
            
            if df.is_empty():
                logger.warning("No data found for validation period")
                return {"status": "warning", "message": "No data found", "issues": []}

            issues = []
            
            # 1. Missing Value Checks
            for col in ['region', 'sr_type', 'rca']:
                null_count = df[col].null_count()
                if null_count > 0:
                    issues.append(f"Found {null_count} rows with missing {col}")

            # 2. Ingestion Gaps (Missing Hours)
            # Generate expected hourly range
            # Ensure start_date/end_date are datetime for range generation
            # (Assuming context passes strings for now, converting)
            s_dt = datetime.strptime(str(start_date), "%Y-%m-%d")
            e_dt = datetime.strptime(str(end_date), "%Y-%m-%d") + timedelta(days=1) # inclusive of end date
            
            # Simple check: Group by hour and check uniqueness
            # Actually, let's just check if we have data for every hour in the range?
            # Or just check if there are significant gaps.
            # Let's count rows per hour.
            
            # Cast to datetime if needed (read_database usually handles it)
            # df = df.with_columns(pl.col("sr_open_dttm").cast(pl.Datetime))
            
            hourly_counts = df.group_by(pl.col("sr_open_dttm").dt.truncate("1h")).len()
            
            # Check for zero counts? 
            # If we expect data every hour, we can generate a complete range and join.
            # detailed gap detection might be complex, let's stick to reporting low volume hours as a proxy or just 'missing hours'
            
            # For this MVP, let's report hours with 0 records if we expected them?
            # Actually, `hourly_counts` only has hours with data.
            # We can find hours present and compare set.
            
            present_hours = set(hourly_counts["sr_open_dttm"].dt.strftime("%Y-%m-%d %H:00:00").to_list())
            
            # Generate expected
            total_hours = int((e_dt - s_dt).total_seconds() / 3600)
            expected_hours = {
                (s_dt + timedelta(hours=i)).strftime("%Y-%m-%d %H:00:00") 
                for i in range(total_hours)
            }
            
            # Filter expected hours to only those within the requested dates (strictly)
            # (The simple range above covers start to end+1 day start)
            
            missing_hours = expected_hours - present_hours
            if missing_hours:
                # Limit output
                missing_list = sorted(list(missing_hours))
                issues.append(f"Missing data for {len(missing_list)} hours: {missing_list[:5]}...")

            validation_result = {
                "status": "success",
                "issues_found": len(issues),
                "issues": issues,
                "row_count": len(df)
            }
            
            if issues:
                logger.warning(f"Validation issues found: {issues}")
            else:
                logger.info("Validation passed with no major issues.")
                
            return validation_result

        except Exception as e:
            logger.exception("Validation failed")
            return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    # Test
    agent = ValidationAgent()
    # agent.run({"start_date": "2023-10-01", "end_date": "2023-10-01"})
