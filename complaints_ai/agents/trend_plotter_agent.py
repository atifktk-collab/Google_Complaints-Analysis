import polars as pl
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json

from ..db.mysql import get_engine

logger = logging.getLogger(__name__)

class TrendPlotterAgent:
    """
    Agent responsible for generating date-wise trend data for the last 30 days.
    Provides hierarchical drill-down: Total -> Region -> Exchange -> NE
    Also provides SR Sub-type and RCA trend analysis.
    """
    
    def __init__(self):
        self.engine = get_engine()
        self.default_days = 30

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generates trend data for visualization.
        
        Args:
            context: Can contain 'target_date' (str YYYY-MM-DD) and 'days_back' (int).
        
        Returns:
            Dictionary containing trend data for various dimensions.
        """
        target_date_str = context.get('target_date', datetime.now().strftime("%Y-%m-%d"))
        days_back = context.get('days_back', self.default_days)
        
        logger.info(f"Generating trend plot data for {days_back} days ending {target_date_str}")
        
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
            start_date = target_date - timedelta(days=days_back)
            
            # 1. Total Complaints Count (Daily)
            total_trend = self._get_total_trend(start_date, target_date)
            
            # 2. Region-wise Count
            region_trend = self._get_region_trend(start_date, target_date)
            
            # 3. Exchange-wise Count (per region)
            exchange_trend = self._get_exchange_trend(start_date, target_date)
            
            # 4. NE (Network Element) Count (per exchange)
            ne_trend = self._get_ne_trend(start_date, target_date)
            
            # 5. SR Sub-type wise Count
            sr_subtype_trend = self._get_sr_subtype_trend(start_date, target_date)
            
            # 6. RCA wise Count
            rca_trend = self._get_rca_trend(start_date, target_date)
            
            result = {
                "status": "success",
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": target_date.strftime("%Y-%m-%d"),
                "total_trend": total_trend,
                "region_trend": region_trend,
                "exchange_trend": exchange_trend,
                "ne_trend": ne_trend,
                "sr_subtype_trend": sr_subtype_trend,
                "rca_trend": rca_trend
            }
            
            logger.info("Trend plot data generated successfully")
            return result
            
        except Exception as e:
            logger.exception("Trend plotter failed")
            return {"status": "error", "message": str(e)}

    def _get_total_trend(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get daily total complaint counts."""
        query = f"""
            SELECT sr_open_dt as date, COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            GROUP BY sr_open_dt
            ORDER BY sr_open_dt
        """
        df = pl.read_database(query, self.engine)
        return df.to_dicts() if not df.is_empty() else []

    def _get_region_trend(self, start_date: datetime, end_date: datetime) -> Dict[str, List[Dict]]:
        """Get daily counts per region."""
        query = f"""
            SELECT sr_open_dt as date, region, COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            AND region IS NOT NULL
            GROUP BY sr_open_dt, region
            ORDER BY sr_open_dt, region
        """
        df = pl.read_database(query, self.engine)
        
        if df.is_empty():
            return {}
        
        # Group by region
        result = {}
        for region in df['region'].unique().to_list():
            region_data = df.filter(pl.col('region') == region)
            result[region] = region_data.select(['date', 'count']).to_dicts()
        
        return result

    def _get_exchange_trend(self, start_date: datetime, end_date: datetime) -> Dict[str, Dict[str, List[Dict]]]:
        """Get daily counts per exchange, grouped by region."""
        query = f"""
            SELECT sr_open_dt as date, region, exc_id, COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            AND region IS NOT NULL AND exc_id IS NOT NULL
            GROUP BY sr_open_dt, region, exc_id
            ORDER BY sr_open_dt, region, exc_id
        """
        df = pl.read_database(query, self.engine)
        
        if df.is_empty():
            return {}
        
        # Group by region -> exchange
        result = {}
        for region in df['region'].unique().to_list():
            result[region] = {}
            region_df = df.filter(pl.col('region') == region)
            
            for exchange in region_df['exc_id'].unique().to_list():
                exchange_data = region_df.filter(pl.col('exc_id') == exchange)
                result[region][exchange] = exchange_data.select(['date', 'count']).to_dicts()
        
        return result

    def _get_ne_trend(self, start_date: datetime, end_date: datetime) -> Dict[str, Dict[str, Dict[str, List[Dict]]]]:
        """Get daily counts per NE (cabinet_id), grouped by exchange and region."""
        query = f"""
            SELECT sr_open_dt as date, region, exc_id, cabinet_id, COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            AND region IS NOT NULL AND exc_id IS NOT NULL AND cabinet_id IS NOT NULL
            GROUP BY sr_open_dt, region, exc_id, cabinet_id
            ORDER BY sr_open_dt, region, exc_id, cabinet_id
        """
        df = pl.read_database(query, self.engine)
        
        if df.is_empty():
            return {}
        
        # Group by region -> exchange -> NE
        result = {}
        for region in df['region'].unique().to_list():
            result[region] = {}
            region_df = df.filter(pl.col('region') == region)
            
            for exchange in region_df['exc_id'].unique().to_list():
                result[region][exchange] = {}
                exchange_df = region_df.filter(pl.col('exc_id') == exchange)
                
                for ne in exchange_df['cabinet_id'].unique().to_list():
                    ne_data = exchange_df.filter(pl.col('cabinet_id') == ne)
                    result[region][exchange][ne] = ne_data.select(['date', 'count']).to_dicts()
        
        return result

    def _get_sr_subtype_trend(self, start_date: datetime, end_date: datetime) -> Dict[str, List[Dict]]:
        """Get daily counts per SR sub-type."""
        query = f"""
            SELECT sr_open_dt as date, sr_sub_type, COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            AND sr_sub_type IS NOT NULL
            GROUP BY sr_open_dt, sr_sub_type
            ORDER BY sr_open_dt, sr_sub_type
        """
        df = pl.read_database(query, self.engine)
        
        if df.is_empty():
            return {}
        
        # Group by SR sub-type
        result = {}
        for subtype in df['sr_sub_type'].unique().to_list():
            subtype_data = df.filter(pl.col('sr_sub_type') == subtype)
            result[subtype] = subtype_data.select(['date', 'count']).to_dicts()
        
        return result

    def _get_rca_trend(self, start_date: datetime, end_date: datetime) -> Dict[str, List[Dict]]:
        """Get daily counts per RCA."""
        query = f"""
            SELECT sr_open_dt as date, rca, COUNT(*) as count
            FROM complaints_raw
            WHERE sr_open_dt BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            AND rca IS NOT NULL
            GROUP BY sr_open_dt, rca
            ORDER BY sr_open_dt, rca
        """
        df = pl.read_database(query, self.engine)
        
        if df.is_empty():
            return {}
        
        # Group by RCA
        result = {}
        for rca in df['rca'].unique().to_list():
            rca_data = df.filter(pl.col('rca') == rca)
            result[rca] = rca_data.select(['date', 'count']).to_dicts()
        
        return result

if __name__ == "__main__":
    agent = TrendPlotterAgent()
    result = agent.run({"target_date": "2026-01-24", "days_back": 30})
    print(f"Status: {result['status']}")
    if result['status'] == 'success':
        print(f"Total trend points: {len(result['total_trend'])}")
        print(f"Regions: {len(result['region_trend'])}")
