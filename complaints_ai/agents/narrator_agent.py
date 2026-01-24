import logging
from datetime import datetime
from typing import Dict, Any

from ..db.mysql import get_session
from ..db.models import DailyAnomalies, ExecInsights

logger = logging.getLogger(__name__)

class NarratorAgent:
    """
    Agent responsible for generating human-readable insights from technical anomalies.
    """
    
    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generates insights for the target hour.
        """
        target_date_str = context.get('target_date')
        
        if not target_date_str:
            return {"status": "error", "message": "Missing target_date"}
            
        logger.info("Generating insights...")
        
        try:
            session = get_session()
            anomalies = session.query(DailyAnomalies).filter_by(
                anomaly_date=target_date_str
            ).order_by(DailyAnomalies.z_score.desc()).all()
            
            insights_generated = 0
            
            for anomaly in anomalies:
                # We typically generate insight for CRITICAL or high-value WARNINGs
                if anomaly.severity == 'INFO':
                    continue
                
                # Construct Title
                # "Spike in [DimensionKey] ([Dimension])"
                title = f"Spike in {anomaly.dimension_key} ({anomaly.dimension})"
                
                # Construct Summary
                # "Detected {value} complaints (Baseline: {avg}). Z-Score: {z}. Impact: {severity}."
                # "Context: {rca}"
                
                summary = (
                    f"On {target_date_str}, detected {int(anomaly.metric_value)} complaints "
                    f"(Baseline: {anomaly.baseline_avg:.1f}). "
                    f"Deviation: {anomaly.z_score:.1f}σ. "
                    f"Severity: {anomaly.severity}. "
                )
                
                if anomaly.rca_context:
                    summary += f"\nContext: {anomaly.rca_context}"
                
                # Save to ExecInsights
                formatted_ts = datetime.strptime(target_date_str, "%Y-%m-%d")
                
                insight = ExecInsights(
                    created_at=formatted_ts,
                    title=title,
                    summary=summary,
                    severity=anomaly.severity
                )
                
                session.add(insight)
                insights_generated += 1
            
            if insights_generated:
                session.commit()
                logger.info(f"Generated {insights_generated} insights.")
                
            session.close()
            return {"status": "success", "insights": insights_generated}

        except Exception as e:
            logger.exception("Insight generation failed")
            return {"status": "error", "message": str(e)}
