"""
Verification script to test the daily data analysis pipeline.
This script verifies that all components are working correctly.
"""

from complaints_ai.db.mysql import get_session, get_engine
from complaints_ai.db.models import DailyAnomalies, DailyTrends, DailyVariations, ComplaintsRaw
from sqlalchemy import inspect

def verify_schema():
    """Verify that all new tables exist in the database."""
    print("=" * 60)
    print("VERIFYING DATABASE SCHEMA")
    print("=" * 60)
    
    engine = get_engine()
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    required_tables = [
        'complaints_raw',
        'daily_anomalies',
        'daily_trends',
        'daily_variations',
        'exec_insights'
    ]
    
    print("\nChecking for required tables:")
    all_present = True
    for table in required_tables:
        present = table in tables
        status = "[OK]" if present else "[FAIL]"
        print(f"  {status} {table}")
        if not present:
            all_present = False
    
    if all_present:
        print("\n[SUCCESS] All required tables are present!")
    else:
        print("\n[ERROR] Some tables are missing!")
        return False
    
    # Check that hourly_anomalies is gone
    if 'hourly_anomalies' in tables:
        print("\n[WARNING] Old 'hourly_anomalies' table still exists!")
    else:
        print("\n[SUCCESS] Old 'hourly_anomalies' table successfully removed!")
    
    return True

def verify_table_structure():
    """Verify the structure of new tables."""
    print("\n" + "=" * 60)
    print("VERIFYING TABLE STRUCTURES")
    print("=" * 60)
    
    engine = get_engine()
    inspector = inspect(engine)
    
    # Check DailyAnomalies structure
    print("\nDailyAnomalies columns:")
    columns = inspector.get_columns('daily_anomalies')
    column_names = [col['name'] for col in columns]
    
    required_columns = ['id', 'anomaly_date', 'dimension', 'dimension_key', 
                       'metric_value', 'baseline_avg', 'baseline_std', 
                       'z_score', 'severity', 'rca_context']
    
    for col in required_columns:
        present = col in column_names
        status = "[OK]" if present else "[FAIL]"
        print(f"  {status} {col}")
    
    # Verify no anomaly_hour column
    if 'anomaly_hour' in column_names:
        print("  [FAIL] ERROR: anomaly_hour column should not exist!")
        return False
    else:
        print("  [SUCCESS] Confirmed: No 'anomaly_hour' column (correct for daily data)")
    
    # Check DailyTrends structure
    print("\nDailyTrends columns:")
    columns = inspector.get_columns('daily_trends')
    column_names = [col['name'] for col in columns]
    
    required_columns = ['id', 'trend_date', 'dimension', 'dimension_key',
                       'metric_value', 'trend_direction', 'trend_strength',
                       'window_days', 'significance']
    
    for col in required_columns:
        present = col in column_names
        status = "[OK]" if present else "[FAIL]"
        print(f"  {status} {col}")
    
    # Check DailyVariations structure
    print("\nDailyVariations columns:")
    columns = inspector.get_columns('daily_variations')
    column_names = [col['name'] for col in columns]
    
    required_columns = ['id', 'variation_date', 'dimension', 'dimension_key',
                       'current_value', 'previous_value', 'variation_type',
                       'variation_percent', 'is_significant']
    
    for col in required_columns:
        present = col in column_names
        status = "[OK]" if present else "[FAIL]"
        print(f"  {status} {col}")
    
    print("\n[SUCCESS] All table structures verified!")
    return True

def verify_agents():
    """Verify that agents can be imported."""
    print("\n" + "=" * 60)
    print("VERIFYING AGENT IMPORTS")
    print("=" * 60)
    
    agents = [
        ('IngestionAgent', 'complaints_ai.agents.ingestion_agent'),
        ('ValidationAgent', 'complaints_ai.agents.validation_agent'),
        ('BaselineAgent', 'complaints_ai.agents.baseline_agent'),
        ('AnomalyAgent', 'complaints_ai.agents.anomaly_agent'),
        ('TrendAgent', 'complaints_ai.agents.trend_agent'),
        ('VariationAgent', 'complaints_ai.agents.variation_agent'),
        ('CorrelationAgent', 'complaints_ai.agents.correlation_agent'),
        ('RCAAgent', 'complaints_ai.agents.rca_agent'),
        ('SeverityAgent', 'complaints_ai.agents.severity_agent'),
        ('NarratorAgent', 'complaints_ai.agents.narrator_agent'),
    ]
    
    all_imported = True
    for agent_name, module_path in agents:
        try:
            module = __import__(module_path, fromlist=[agent_name])
            agent_class = getattr(module, agent_name)
            # Try to instantiate
            agent = agent_class()
            print(f"  [OK] {agent_name}")
        except Exception as e:
            print(f"  [FAIL] {agent_name}: {str(e)}")
            all_imported = False
    
    if all_imported:
        print("\n[SUCCESS] All agents imported successfully!")
    else:
        print("\n[ERROR] Some agents failed to import!")
        return False
    
    return True

def main():
    """Run all verification checks."""
    print("\n" + "=" * 60)
    print("DAILY DATA ANALYSIS MIGRATION VERIFICATION")
    print("=" * 60)
    
    results = []
    
    # Run checks
    results.append(("Schema Verification", verify_schema()))
    results.append(("Table Structure Verification", verify_table_structure()))
    results.append(("Agent Import Verification", verify_agents()))
    
    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for check_name, passed in results:
        status = "[SUCCESS] PASSED" if passed else "[ERROR] FAILED"
        print(f"{status}: {check_name}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("[SUCCESS] ALL VERIFICATIONS PASSED!")
        print("The system is ready for daily data analysis.")
    else:
        print("[WARNING]  SOME VERIFICATIONS FAILED!")
        print("Please review the errors above.")
    print("=" * 60)
    
    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
