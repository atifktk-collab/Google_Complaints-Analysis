"""
Main entry point for Complaints Analysis System
"""
import argparse
import logging
import logging.config
import pandas as pd
from pathlib import Path

from config import LOGGING_CONFIG, RAW_DATA_DIR, PROCESSED_DATA_DIR
from src.data.data_loader import load_complaints_data
from src.data.preprocessor import ComplaintPreprocessor
from src.models.complaint_analyzer import ComplaintAnalyzer
from src.visualization.dashboard import generate_dashboard_report

# Configure logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def main():
    """Main function to run the complaints analysis pipeline"""
    parser = argparse.ArgumentParser(description='Analyze customer complaints')
    parser.add_argument('--input', type=str, help='Path to input CSV file')
    parser.add_argument('--output', type=str, help='Path to output CSV file')
    parser.add_argument('--generate-report', action='store_true', 
                        help='Generate visualization report')
    parser.add_argument('--train', action='store_true', 
                        help='Train new models')
    
    args = parser.parse_args()
    
    try:
        # Set default paths if not provided
        input_path = args.input or RAW_DATA_DIR / "complaints.csv"
        output_path = args.output or PROCESSED_DATA_DIR / "analyzed_complaints.csv"
        
        logger.info("Starting Complaints Analysis System...")
        logger.info(f"Input file: {input_path}")
        logger.info(f"Output file: {output_path}")
        
        # Load data
        logger.info("Loading complaint data...")
        df = load_complaints_data(input_path)
        logger.info(f"Loaded {len(df)} complaints")
        
        # Preprocess data
        logger.info("Preprocessing complaints...")
        preprocessor = ComplaintPreprocessor()
        df['cleaned_text'] = df['complaint_text'].apply(preprocessor.preprocess)
        
        # Initialize analyzer
        logger.info("Initializing complaint analyzer...")
        analyzer = ComplaintAnalyzer()
        
        # Analyze complaints
        logger.info("Analyzing complaints...")
        results = []
        for idx, row in df.iterrows():
            try:
                analysis = analyzer.analyze(row['complaint_text'])
                results.append({
                    'complaint_id': row.get('complaint_id', idx),
                    'original_text': row['complaint_text'],
                    'cleaned_text': row['cleaned_text'],
                    'sentiment': analysis['sentiment'],
                    'sentiment_score': analysis['sentiment_score'],
                    'category': analysis['category'],
                    'priority': analysis['priority'],
                    'keywords': ', '.join(analysis.get('keywords', []))
                })
            except Exception as e:
                logger.error(f"Error analyzing complaint {idx}: {str(e)}")
                continue
        
        # Create results DataFrame
        results_df = pd.DataFrame(results)
        
        # Save results
        logger.info(f"Saving results to {output_path}...")
        results_df.to_csv(output_path, index=False)
        logger.info(f"Successfully saved {len(results_df)} analyzed complaints")
        
        # Generate report if requested
        if args.generate_report:
            logger.info("Generating visualization report...")
            generate_dashboard_report(results_df)
            logger.info("Report generated successfully!")
        
        # Print summary
        print("\n" + "="*60)
        print("ANALYSIS SUMMARY")
        print("="*60)
        print(f"Total Complaints Analyzed: {len(results_df)}")
        print(f"\nSentiment Distribution:")
        print(results_df['sentiment'].value_counts())
        print(f"\nCategory Distribution:")
        print(results_df['category'].value_counts())
        print(f"\nPriority Distribution:")
        print(results_df['priority'].value_counts())
        print("="*60)
        
        logger.info("Complaints analysis completed successfully!")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        print(f"Error: Input file not found. Please check the file path.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

