"""
Quick test script to verify installation
"""
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    print("Testing imports...")
    from src.models.complaint_analyzer import ComplaintAnalyzer
    from src.data.data_loader import load_complaints_data
    from src.data.preprocessor import ComplaintPreprocessor
    from config import RAW_DATA_DIR, PROCESSED_DATA_DIR
    
    print("[OK] All imports successful!")
    
    # Test analyzer
    print("\nTesting ComplaintAnalyzer...")
    analyzer = ComplaintAnalyzer()
    result = analyzer.analyze("This product is terrible and broken. Very disappointed!")
    
    print(f"[OK] ComplaintAnalyzer working!")
    print(f"   Sentiment: {result['sentiment']}")
    print(f"   Category: {result['category']}")
    print(f"   Priority: {result['priority']}")
    print(f"   Keywords: {result['keywords']}")
    
    # Test preprocessor
    print("\nTesting ComplaintPreprocessor...")
    preprocessor = ComplaintPreprocessor()
    cleaned = preprocessor.preprocess("This is a TEST complaint!!!")
    print(f"[OK] Preprocessor working!")
    print(f"   Original: 'This is a TEST complaint!!!'")
    print(f"   Cleaned: '{cleaned}'")
    
    # Check sample data
    print("\nChecking sample data...")
    sample_file = RAW_DATA_DIR / "sample_complaints.csv"
    if sample_file.exists():
        print(f"[OK] Sample data file exists: {sample_file}")
        try:
            df = load_complaints_data(sample_file)
            print(f"[OK] Data loader working! Loaded {len(df)} complaints")
        except Exception as e:
            print(f"[WARNING] Error loading data: {e}")
    else:
        print(f"[WARNING] Sample data file not found: {sample_file}")
    
    print("\n" + "="*60)
    print("[OK] SETUP VERIFICATION COMPLETE!")
    print("="*60)
    print("\nYou can now:")
    print("1. Run: python main.py --input data/raw/sample_complaints.csv")
    print("2. Run: streamlit run app.py")
    print("="*60)
    
except ImportError as e:
    print(f"[ERROR] Import error: {e}")
    print("Please install dependencies: pip install -r requirements.txt")
    sys.exit(1)
except Exception as e:
    print(f"[ERROR] Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

