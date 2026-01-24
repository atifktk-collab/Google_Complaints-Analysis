import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from ..db.mysql import get_session
from ..db.models import DailyAnomalies

logger = logging.getLogger(__name__)

class SeverityAgent:
    """
    Agent responsible for refining the severity of anomalies based on persistence and spread.
    """
    
    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Refines severity for anomalies in the target hour.
        """
        target_date_str = context.get('target_date')
        
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
            
        logger.info("Refining severity...")
        
        try:
            session = get_session()
            anomalies = session.query(DailyAnomalies).filter_by(
                anomaly_date=target_date_str
            ).all()
            
            updates = 0
            
            # 1. Check Persistence
            # For each anomaly, check if it existed in the previous day
            target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
            prev_day_dt = target_dt - timedelta(days=1)
            
            for anomaly in anomalies:
                # Check previous day
                prev_anomaly = session.query(DailyAnomalies).filter_by(
                    anomaly_date=prev_day_dt.date(),
                    dimension=anomaly.dimension,
                    dimension_key=anomaly.dimension_key
                ).first()
                
                is_persistent = prev_anomaly is not None
                
                # Check Spread (Regional Impact)
                # If Type anomaly (e.g., Internet DSL), is it affecting multiple regions?
                region_anomalies_count = session.query(DailyAnomalies).filter_by(
                    anomaly_date=target_date_str,
                    dimension='Region'
                ).count()
                
                is_widespread = region_anomalies_count > 3 # arbitrary threshold for "Multi-region"
                
                original_severity = anomaly.severity
                new_severity = original_severity
                
                # Logic:
                # If WARNING + Persistent -> CRITICAL
                # If WARNING + Widespread -> CRITICAL
                
                if original_severity == 'WARNING':
                    if is_persistent or (is_widespread and anomaly.dimension == 'Type'):
                         new_severity = 'CRITICAL'
                
                if new_severity != original_severity:
                    anomaly.severity = new_severity
                    updates += 1
            
            if updates:
                session.commit()
                logger.info(f"Upgraded severity for {updates} anomalies.")
                
            session.close()
            return {"status": "success", "updates": updates}

        except Exception as e:
            logger.exception("Severity refinement failed")
            return {"status": "error", "message": str(e)}
