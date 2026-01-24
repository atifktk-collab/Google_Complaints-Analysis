"""
Helper utility functions
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


def load_json(file_path: str) -> Dict[str, Any]:
    """
    Load JSON file
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        Dictionary with JSON content
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON from {file_path}: {str(e)}")
        raise


def save_json(data: Dict[str, Any], file_path: str, indent: int = 2):
    """
    Save data to JSON file
    
    Args:
        data: Dictionary to save
        file_path: Output file path
        indent: JSON indentation
    """
    try:
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent)
        logger.info(f"Saved JSON to {file_path}")
    except Exception as e:
        logger.error(f"Error saving JSON to {file_path}: {str(e)}")
        raise


def create_summary_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Create summary statistics from analyzed complaints
    
    Args:
        df: DataFrame with analyzed complaints
        
    Returns:
        Dictionary with summary statistics
    """
    try:
        summary = {
            "total_complaints": len(df),
            "sentiment_distribution": df['sentiment'].value_counts().to_dict(),
            "category_distribution": df['category'].value_counts().to_dict(),
            "priority_distribution": df['priority'].value_counts().to_dict(),
            "average_sentiment_score": float(df['sentiment_score'].mean()) if 'sentiment_score' in df else 0,
            "negative_percentage": float((df['sentiment'] == 'negative').sum() / len(df) * 100),
            "high_priority_count": int((df['priority'].isin(['high', 'critical'])).sum())
        }
        return summary
    except Exception as e:
        logger.error(f"Error creating summary statistics: {str(e)}")
        return {}


def format_complaint_report(analysis: Dict[str, Any]) -> str:
    """
    Format analysis results into a readable report
    
    Args:
        analysis: Analysis results dictionary
        
    Returns:
        Formatted report string
    """
    report = f"""
    ==========================================
    COMPLAINT ANALYSIS REPORT
    ==========================================
    
    Sentiment:        {analysis.get('sentiment', 'N/A').upper()}
    Sentiment Score:  {analysis.get('sentiment_score', 0):.3f}
    Category:         {analysis.get('category', 'N/A')}
    Priority Level:   {analysis.get('priority', 'N/A').upper()}
    
    Key Topics:
    {', '.join(analysis.get('keywords', [])) if analysis.get('keywords') else 'None identified'}
    
    ==========================================
    """
    return report


def validate_dataframe(df: pd.DataFrame, required_columns: list) -> bool:
    """
    Validate DataFrame has required columns
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        True if valid, False otherwise
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logger.error(f"Missing required columns: {missing}")
        return False
    return True


def get_file_size(file_path: str) -> str:
    """
    Get human-readable file size
    
    Args:
        file_path: Path to file
        
    Returns:
        Formatted file size string
    """
    try:
        size_bytes = Path(file_path).stat().st_size
        
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        
        return f"{size_bytes:.2f} TB"
    except Exception as e:
        logger.error(f"Error getting file size: {str(e)}")
        return "Unknown"

