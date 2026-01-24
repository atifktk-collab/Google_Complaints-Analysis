"""
Data loading utilities
"""
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def load_complaints_data(file_path, required_columns=None):
    """
    Load complaints data from CSV file
    
    Args:
        file_path: Path to the CSV file
        required_columns: List of required column names
        
    Returns:
        pandas.DataFrame: Loaded complaints data
    """
    if required_columns is None:
        required_columns = ['complaint_text']
    
    try:
        # Convert to Path object
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Load data based on file extension
        if file_path.suffix == '.csv':
            df = pd.read_csv(file_path)
        elif file_path.suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        elif file_path.suffix == '.json':
            df = pd.read_json(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
        
        # Check for required columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing columns: {missing_cols}")
            # If complaint_text is missing but there's a text column, use it
            if 'complaint_text' in missing_cols and 'text' in df.columns:
                df['complaint_text'] = df['text']
                logger.info("Using 'text' column as 'complaint_text'")
        
        # Add complaint_id if not present
        if 'complaint_id' not in df.columns:
            df['complaint_id'] = range(1, len(df) + 1)
        
        # Remove duplicates
        initial_len = len(df)
        df = df.drop_duplicates(subset=['complaint_text'])
        if len(df) < initial_len:
            logger.info(f"Removed {initial_len - len(df)} duplicate complaints")
        
        # Remove null values
        df = df.dropna(subset=['complaint_text'])
        
        logger.info(f"Successfully loaded {len(df)} complaints from {file_path}")
        return df
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise


def save_complaints_data(df, file_path, format='csv'):
    """
    Save complaints data to file
    
    Args:
        df: pandas.DataFrame to save
        file_path: Output file path
        format: Output format ('csv', 'xlsx', 'json')
    """
    try:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format == 'csv':
            df.to_csv(file_path, index=False)
        elif format == 'xlsx':
            df.to_excel(file_path, index=False)
        elif format == 'json':
            df.to_json(file_path, orient='records', indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Successfully saved {len(df)} complaints to {file_path}")
        
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        raise

