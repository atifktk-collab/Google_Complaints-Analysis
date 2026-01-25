import polars as pl
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from calendar import monthrange

from ..db.mysql import get_engine

logger = logging.getLogger(__name__)

class SurgeHighlighterAgent:
    """
    Agent responsible for detecting complaint surges by comparing a specific date with:
    1. Month-to-Date (MTD) average
    2. Same day of last week
    
    Highlights surges based on configurable thresholds (alarming and critical).
    """
    
    def __init__(self):
        self.engine = get_engine()

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detects surges for a specific date.
        
        Args:
            context: Must contain:
                - 'target_date' (str YYYY-MM-DD): Date to analyze
                - 'alarming_threshold' (float): % increase for alarming (e.g., 20.0 for 20%)
                - 'critical_threshold' (float): % increase for critical (e.g., 50.0 for 50%)
        
        Returns:
            Dictionary containing surge analysis results.
        """
        target_date_str = context.get('target_date')
        alarming_threshold = context.get('alarming_threshold', 20.0)
        critical_threshold = context.get('critical_threshold', 50.0)
        
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
        
        logger.info(f"Running surge analysis for {target_date_str}")
        logger.info(f"Thresholds - Alarming: {alarming_threshold}%, Critical: {critical_threshold}%")
        
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
            
            # Calculate comparison dates
            same_day_last_week = target_date - timedelta(days=7)
            
            # MTD calculation: from 1st of month to target date
            mtd_start = target_date.replace(day=1)
            mtd_end = target_date - timedelta(days=1)  # Exclude target date from MTD
            
            # 1. Get target date data
            target_data = self._get_date_data(target_date)
            
            # 2. Get same day last week data
            last_week_data = self._get_date_data(same_day_last_week)
            
            # 3. Get MTD average
            mtd_avg_data = self._get_mtd_average(mtd_start, mtd_end)
            
            # 4. Compare and detect surges
            surges = self._detect_surges(
                target_data,
                last_week_data,
                mtd_avg_data,
                alarming_threshold,
                critical_threshold
            )
            
            result = {
                "status": "success",
                "target_date": target_date_str,
                "same_day_last_week": same_day_last_week.strftime("%Y-%m-%d"),
                "mtd_period": f"{mtd_start.strftime('%Y-%m-%d')} to {mtd_end.strftime('%Y-%m-%d')}",
                "alarming_threshold": alarming_threshold,
                "critical_threshold": critical_threshold,
                "surges": surges
            }
            
            logger.info(f"Surge analysis complete. Found {len(surges['total'])} total surges, "
                       f"{len(surges['regions'])} regional surges, "
                       f"{len(surges['exchanges'])} exchange surges, "
                       f"{len(surges['cities'])} city surges")
            
            return result
            
        except Exception as e:
            logger.exception("Surge highlighter failed")
            return {"status": "error", "message": str(e)}

    def _get_date_data(self, date: datetime) -> Dict[str, Any]:
        """Get complaint counts for a specific date at all levels."""
        date_str = date.strftime("%Y-%m-%d")
        
        # Total count
        total_query = f"""
            SELECT COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt = '{date_str}'
        """
        total_df = pl.read_database(total_query, self.engine)
        total_count = total_df['count'][0] if not total_df.is_empty() else 0
        
        # Region counts
        region_query = f"""
            SELECT region, COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt = '{date_str}' AND region IS NOT NULL
            GROUP BY region
        """
        region_df = pl.read_database(region_query, self.engine)
        regions = {row['region']: row['count'] for row in region_df.to_dicts()} if not region_df.is_empty() else {}
        
        # Exchange counts
        exchange_query = f"""
            SELECT region, exc_id, COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt = '{date_str}' AND region IS NOT NULL AND exc_id IS NOT NULL
            GROUP BY region, exc_id
        """
        exchange_df = pl.read_database(exchange_query, self.engine)
        exchanges = {f"{row['region']}|{row['exc_id']}": row['count'] 
                    for row in exchange_df.to_dicts()} if not exchange_df.is_empty() else {}
        
        # City counts
        city_query = f"""
            SELECT region, exc_id, city, COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt = '{date_str}' 
            AND region IS NOT NULL AND exc_id IS NOT NULL AND city IS NOT NULL
            GROUP BY region, exc_id, city
        """
        city_df = pl.read_database(city_query, self.engine)
        cities = {f"{row['region']}|{row['exc_id']}|{row['city']}": row['count'] 
              for row in city_df.to_dicts()} if not city_df.is_empty() else {}
        
        return {
            "total": total_count,
            "regions": regions,
            "exchanges": exchanges,
            "cities": cities
        }

    def _get_mtd_average(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Get MTD average counts at all levels."""
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        # Calculate number of days
        days_count = (end_date - start_date).days + 1
        
        if days_count <= 0:
            # If target date is 1st of month, no MTD average available
            return {
                "total": 0,
                "regions": {},
                "exchanges": {},
                "nes": {}
            }
        
        # Total average
        total_query = f"""
            SELECT COUNT(*) / {days_count} as avg_count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_str}' AND '{end_str}'
        """
        total_df = pl.read_database(total_query, self.engine)
        total_avg = total_df['avg_count'][0] if not total_df.is_empty() else 0
        
        # Region averages
        region_query = f"""
            SELECT region, COUNT(*) / {days_count} as avg_count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_str}' AND '{end_str}' AND region IS NOT NULL
            GROUP BY region
        """
        region_df = pl.read_database(region_query, self.engine)
        regions = {row['region']: row['avg_count'] for row in region_df.to_dicts()} if not region_df.is_empty() else {}
        
        # Exchange averages
        exchange_query = f"""
            SELECT region, exc_id, COUNT(*) / {days_count} as avg_count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_str}' AND '{end_str}' 
            AND region IS NOT NULL AND exc_id IS NOT NULL
            GROUP BY region, exc_id
        """
        exchange_df = pl.read_database(exchange_query, self.engine)
        exchanges = {f"{row['region']}|{row['exc_id']}": row['avg_count'] 
                    for row in exchange_df.to_dicts()} if not exchange_df.is_empty() else {}
        
        # City averages
        city_query = f"""
            SELECT region, exc_id, city, COUNT(*) / {days_count} as avg_count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_str}' AND '{end_str}' 
            AND region IS NOT NULL AND exc_id IS NOT NULL AND city IS NOT NULL
            GROUP BY region, exc_id, city
        """
        city_df = pl.read_database(city_query, self.engine)
        cities = {f"{row['region']}|{row['exc_id']}|{row['city']}": row['avg_count'] 
              for row in city_df.to_dicts()} if not city_df.is_empty() else {}
        
        return {
            "total": total_avg,
            "regions": regions,
            "exchanges": exchanges,
            "cities": cities
        }

    def _calculate_surge(self, current: float, comparison: float) -> Dict[str, Any]:
        """Calculate surge percentage and determine severity."""
        if comparison == 0:
            if current > 0:
                return {"percent": 999.9, "increase": current}  # Infinite increase
            else:
                return {"percent": 0.0, "increase": 0}
        
        increase = current - comparison
        percent = (increase / comparison) * 100
        
        return {
            "percent": round(percent, 1),
            "increase": round(increase, 1)
        }

    def _detect_surges(self, target_data: Dict, last_week_data: Dict, 
                      mtd_avg_data: Dict, alarming_threshold: float, 
                      critical_threshold: float) -> Dict[str, List]:
        """Detect surges across all levels."""
        surges = {
            "total": [],
            "regions": [],
            "exchanges": [],
            "cities": []
        }
        
        # Check total
        total_surge = self._check_surge(
            "Total",
            target_data['total'],
            last_week_data['total'],
            mtd_avg_data['total'],
            alarming_threshold,
            critical_threshold
        )
        if total_surge:
            surges['total'].append(total_surge)
        
        # Check regions
        for region, count in target_data['regions'].items():
            if count < 15:
                continue
            last_week_count = last_week_data['regions'].get(region, 0)
            mtd_avg_count = mtd_avg_data['regions'].get(region, 0)
            
            surge = self._check_surge(
                region,
                count,
                last_week_count,
                mtd_avg_count,
                alarming_threshold,
                critical_threshold,
                level="Region"
            )
            if surge:
                surges['regions'].append(surge)
        
        # Check exchanges
        for exchange_key, count in target_data['exchanges'].items():
            if count < 10:
                continue
            region, exc_id = exchange_key.split('|')
            last_week_count = last_week_data['exchanges'].get(exchange_key, 0)
            mtd_avg_count = mtd_avg_data['exchanges'].get(exchange_key, 0)
            
            surge = self._check_surge(
                exc_id,
                count,
                last_week_count,
                mtd_avg_count,
                alarming_threshold,
                critical_threshold,
                level="Exchange",
                parent=region
            )
            if surge:
                surges['exchanges'].append(surge)
        
        # Check Cities
        for city_key, count in target_data['cities'].items():
            if count < 5:
                continue
            region, exc_id, city = city_key.split('|')
            last_week_count = last_week_data['cities'].get(city_key, 0)
            mtd_avg_count = mtd_avg_data['cities'].get(city_key, 0)
            
            surge = self._check_surge(
                city,
                count,
                last_week_count,
                mtd_avg_count,
                alarming_threshold,
                critical_threshold,
                level="City",
                parent=f"{region} > {exc_id}"
            )
            if surge:
                surges['cities'].append(surge)
        
        return surges

    def _check_surge(self, name: str, current: float, last_week: float, 
                    mtd_avg: float, alarming_threshold: float, 
                    critical_threshold: float, level: str = "Total", 
                    parent: str = None) -> Dict[str, Any]:
        """Check if a surge exists and determine its severity."""
        
        # Calculate surges vs MTD and last week
        mtd_surge = self._calculate_surge(current, mtd_avg)
        wow_surge = self._calculate_surge(current, last_week)
        
        # Determine if it crosses thresholds
        max_percent = max(mtd_surge['percent'], wow_surge['percent'])
        
        if max_percent < alarming_threshold:
            return None  # No surge
        
        # Determine severity
        if max_percent >= critical_threshold:
            severity = "CRITICAL"
        else:
            severity = "ALARMING"
        
        result = {
            "level": level,
            "name": name,
            "current_count": int(current),
            "mtd_avg": round(mtd_avg, 1),
            "last_week_count": int(last_week),
            "mtd_surge_percent": mtd_surge['percent'],
            "mtd_surge_increase": mtd_surge['increase'],
            "wow_surge_percent": wow_surge['percent'],
            "wow_surge_increase": wow_surge['increase'],
            "max_surge_percent": max_percent,
            "severity": severity
        }
        
        if parent:
            result["parent"] = parent
        
        return result

if __name__ == "__main__":
    agent = SurgeHighlighterAgent()
    result = agent.run({
        "target_date": "2026-01-23",
        "alarming_threshold": 20.0,
        "critical_threshold": 50.0
    })
    print(f"Status: {result['status']}")
