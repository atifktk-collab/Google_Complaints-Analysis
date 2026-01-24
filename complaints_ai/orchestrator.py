import logging
import argparse
from datetime import datetime
from typing import Optional

# Import Agents
from complaints_ai.agents.ingestion_agent import IngestionAgent
from complaints_ai.agents.validation_agent import ValidationAgent
from complaints_ai.agents.baseline_agent import BaselineAgent
from complaints_ai.agents.anomaly_agent import AnomalyAgent
from complaints_ai.agents.trend_agent import TrendAgent
from complaints_ai.agents.variation_agent import VariationAgent
from complaints_ai.agents.correlation_agent import CorrelationAgent
from complaints_ai.agents.rca_agent import RCAAgent
from complaints_ai.agents.severity_agent import SeverityAgent
from complaints_ai.agents.narrator_agent import NarratorAgent
from complaints_ai.db.mysql import init_db

# Configure global logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Orchestrator")

class Orchestrator:
    def __init__(self):
        self.ingestion_agent = IngestionAgent()
        self.validation_agent = ValidationAgent()
        self.baseline_agent = BaselineAgent()
        self.anomaly_agent = AnomalyAgent()
        self.trend_agent = TrendAgent()
        self.variation_agent = VariationAgent()
        self.correlation_agent = CorrelationAgent()
        self.rca_agent = RCAAgent()
        self.severity_agent = SeverityAgent()
        self.narrator_agent = NarratorAgent()
        
        # Ensure DB is ready
        init_db()

    def run_pipeline(self, 
                     file_path: Optional[str] = None,
                     target_date: Optional[str] = None,
                     run_ingestion: bool = True,
                     run_baseline: bool = False):
        
        logger.info("Starting Daily Pipeline Execution")
        
        # 1. Ingestion
        if run_ingestion and file_path:
            logger.info("Step 1: Ingestion")
            result = self.ingestion_agent.run({"file_path": file_path})
            if result['status'] == 'error':
                logger.error(f"Ingestion failed: {result['message']}")
                return result
        
        # Determine date if not provided (default to yesterday for complete data)
        if not target_date:
            from datetime import timedelta
            yesterday = datetime.now() - timedelta(days=1)
            target_date = yesterday.strftime("%Y-%m-%d")
        
        logger.info(f"Running analysis for date: {target_date}")
        
        # 2. Validation
        logger.info("Step 2: Validation")
        self.validation_agent.run({
            "start_date": target_date, 
            "end_date": target_date
        })
        
        # 3. Baseline Update
        # Typically run daily, or if requested
        if run_baseline:
            logger.info("Step 3: Baseline Calculation")
            self.baseline_agent.run({"target_date": target_date})
            
        # 4. Anomaly Detection
        logger.info(f"Step 4: Daily Anomaly Detection for {target_date}")
        anom_res = self.anomaly_agent.run({"target_date": target_date})
        
        if anom_res['status'] == 'success':
            # 5. Trend Analysis
            logger.info("Step 5: Trend Analysis")
            self.trend_agent.run({"target_date": target_date})
            
            # 6. Variation Analysis
            logger.info("Step 6: Variation Analysis")
            self.variation_agent.run({"target_date": target_date})
            
            if anom_res.get('anomalies_found', 0) > 0:
                # 7. Correlation
                logger.info("Step 7: Correlation Analysis")
                self.correlation_agent.run({"target_date": target_date})
                
                # 8. RCA
                logger.info("Step 8: RCA Identification")
                self.rca_agent.run({"target_date": target_date})
                
                # 9. Severity
                logger.info("Step 9: Severity Refinement")
                self.severity_agent.run({"target_date": target_date})
                
                # 10. Narrator
                logger.info("Step 10: Narrative Generation")
                self.narrator_agent.run({"target_date": target_date})
            else:
                logger.info("No anomalies detected, skipping correlation/RCA/severity/narrator agents.")
        
        logger.info("Daily Pipeline Execution Complete")
        
        final_result = {"status": "success", "message": "Pipeline completed successfully"}
        if run_ingestion and 'result' in locals():
             # pass ingestion diagnostics up
             if 'diagnostics' in result:
                 final_result['diagnostics'] = result['diagnostics']
                 
        return final_result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telecom Complaints AI Orchestrator - Daily Analysis")
    parser.add_argument("--file", help="Path to CSV file for ingestion")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD)")
    parser.add_argument("--baseline", action="store_true", help="Force baseline recalculation")
    parser.add_argument("--no-ingest", action="store_true", help="Skip ingestion even if file is provided")
    
    args = parser.parse_args()
    
    orchestrator = Orchestrator()
    orchestrator.run_pipeline(
        file_path=args.file,
        target_date=args.date,
        run_ingestion=not args.no_ingest,
        run_baseline=args.baseline
    )
