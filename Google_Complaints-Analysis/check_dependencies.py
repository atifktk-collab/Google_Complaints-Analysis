"""
Comprehensive dependency and readiness checker
"""
import sys
import importlib
from pathlib import Path

# Color codes for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def check_package(package_name, import_name=None):
    """Check if a package is installed"""
    if import_name is None:
        import_name = package_name
    
    try:
        module = importlib.import_module(import_name)
        version = getattr(module, '__version__', 'unknown')
        return True, version
    except ImportError:
        return False, None

def check_nltk_data():
    """Check if NLTK data is downloaded"""
    try:
        import nltk
        data_required = {
            'punkt': 'tokenizers/punkt',
            'stopwords': 'corpora/stopwords',
            'wordnet': 'corpora/wordnet',
            'vader_lexicon': 'sentiment/vader_lexicon'
        }
        missing = []
        for data_name, data_path in data_required.items():
            try:
                nltk.data.find(data_path)
            except LookupError:
                missing.append(data_name)
        return len(missing) == 0, missing
    except:
        return False, ['nltk not installed']

def check_project_files():
    """Check if all required project files exist"""
    required_files = [
        'main.py',
        'app.py',
        'config.py',
        'src/models/complaint_analyzer.py',
        'src/data/data_loader.py',
        'src/data/preprocessor.py',
        'src/utils/helpers.py',
        'src/visualization/dashboard.py',
        'data/raw/sample_complaints.csv'
    ]
    
    missing = []
    for file in required_files:
        if not Path(file).exists():
            missing.append(file)
    
    return len(missing) == 0, missing

def check_modules():
    """Check if all project modules can be imported"""
    # Add current directory to path for config import
    import sys
    if str(Path.cwd()) not in sys.path:
        sys.path.insert(0, str(Path.cwd()))
    
    modules_to_check = [
        ('config', 'BASE_DIR'),
        ('src.models.complaint_analyzer', 'ComplaintAnalyzer'),
        ('src.data.data_loader', 'load_complaints_data'),
        ('src.data.preprocessor', 'ComplaintPreprocessor'),
        ('src.utils.helpers', 'create_summary_statistics'),
        ('src.visualization.dashboard', 'generate_dashboard_report'),
    ]
    
    results = []
    for module_name, item_name in modules_to_check:
        try:
            module = importlib.import_module(module_name)
            if hasattr(module, item_name):
                results.append((module_name, True, None))
            else:
                results.append((module_name, False, f"{item_name} not found"))
        except Exception as e:
            results.append((module_name, False, str(e)))
    
    return results

def test_functionality():
    """Test core functionality"""
    tests = []
    
    try:
        from src.models.complaint_analyzer import ComplaintAnalyzer
        analyzer = ComplaintAnalyzer()
        result = analyzer.analyze("Test complaint: product is broken")
        tests.append(("ComplaintAnalyzer.analyze()", True, None))
    except Exception as e:
        tests.append(("ComplaintAnalyzer.analyze()", False, str(e)))
    
    try:
        from src.data.preprocessor import ComplaintPreprocessor
        preprocessor = ComplaintPreprocessor()
        cleaned = preprocessor.preprocess("Test text!!!")
        tests.append(("ComplaintPreprocessor.preprocess()", True, None))
    except Exception as e:
        tests.append(("ComplaintPreprocessor.preprocess()", False, str(e)))
    
    try:
        from src.data.data_loader import load_complaints_data
        sample_file = Path('data/raw/sample_complaints.csv')
        if sample_file.exists():
            df = load_complaints_data(sample_file)
            tests.append(("load_complaints_data()", True, f"Loaded {len(df)} rows"))
        else:
            tests.append(("load_complaints_data()", False, "Sample file not found"))
    except Exception as e:
        tests.append(("load_complaints_data()", False, str(e)))
    
    return tests

def main():
    print("="*70)
    print("COMPREHENSIVE DEPENDENCY AND READINESS CHECK")
    print("="*70)
    
    # Required packages from requirements.txt
    required_packages = {
        'pandas': 'pandas',
        'numpy': 'numpy',
        'scikit-learn': 'sklearn',
        'nltk': 'nltk',
        'textblob': 'textblob',
        'matplotlib': 'matplotlib',
        'seaborn': 'seaborn',
        'plotly': 'plotly',
        'streamlit': 'streamlit',
        'python-dotenv': 'dotenv',
        'requests': 'requests',
        'tqdm': 'tqdm',
        'openpyxl': 'openpyxl',
    }
    
    # Optional packages
    optional_packages = {
        'spacy': 'spacy',
        'transformers': 'transformers',
        'torch': 'torch',
        'flask': 'flask',
        'wordcloud': 'wordcloud',
        'sqlalchemy': 'sqlalchemy',
        'pymongo': 'pymongo',
        'pytest': 'pytest',
    }
    
    print("\n[1] CHECKING REQUIRED PACKAGES")
    print("-" * 70)
    required_status = []
    for package, import_name in required_packages.items():
        installed, version = check_package(package, import_name)
        status = f"{GREEN}[OK]{RESET}" if installed else f"{RED}[MISSING]{RESET}"
        version_str = f" (v{version})" if version else ""
        print(f"{status} {package:20s} {version_str}")
        required_status.append(installed)
    
    print("\n[2] CHECKING OPTIONAL PACKAGES")
    print("-" * 70)
    for package, import_name in optional_packages.items():
        installed, version = check_package(package, import_name)
        status = f"{BLUE}[OPTIONAL]{RESET}" if installed else f"{YELLOW}[NOT INSTALLED]{RESET}"
        version_str = f" (v{version})" if version else ""
        print(f"{status} {package:20s} {version_str}")
    
    print("\n[3] CHECKING NLTK DATA")
    print("-" * 70)
    nltk_ok, missing_nltk = check_nltk_data()
    if nltk_ok:
        print(f"{GREEN}[OK]{RESET} All required NLTK data downloaded")
    else:
        print(f"{RED}[MISSING]{RESET} NLTK data missing: {', '.join(missing_nltk)}")
        print(f"  Run: python -c \"import nltk; [nltk.download(d) for d in {missing_nltk}]\"")
    
    print("\n[4] CHECKING PROJECT FILES")
    print("-" * 70)
    files_ok, missing_files = check_project_files()
    if files_ok:
        print(f"{GREEN}[OK]{RESET} All required project files exist")
    else:
        print(f"{RED}[MISSING]{RESET} Missing files:")
        for file in missing_files:
            print(f"  - {file}")
    
    print("\n[5] CHECKING MODULE IMPORTS")
    print("-" * 70)
    module_results = check_modules()
    all_modules_ok = True
    for module_name, ok, error in module_results:
        status = f"{GREEN}[OK]{RESET}" if ok else f"{RED}[FAIL]{RESET}"
        print(f"{status} {module_name:40s}", end="")
        if not ok:
            print(f" - {error}")
            all_modules_ok = False
        else:
            print()
    
    print("\n[6] TESTING FUNCTIONALITY")
    print("-" * 70)
    test_results = test_functionality()
    all_tests_ok = True
    for test_name, ok, info in test_results:
        status = f"{GREEN}[PASS]{RESET}" if ok else f"{RED}[FAIL]{RESET}"
        print(f"{status} {test_name:40s}", end="")
        if info:
            print(f" - {info}")
        else:
            print()
        if not ok:
            all_tests_ok = False
    
    # Final summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    all_required_installed = all(required_status)
    overall_status = all_required_installed and nltk_ok and files_ok and all_modules_ok and all_tests_ok
    
    if overall_status:
        print(f"{GREEN}[READY]{RESET} System is fully operational and ready to use!")
        print("\nYou can now:")
        print("  1. Run: python main.py --input data/raw/sample_complaints.csv")
        print("  2. Run: streamlit run app.py")
    else:
        print(f"{YELLOW}[NOT READY]{RESET} Some components need attention:")
        if not all_required_installed:
            print("  - Install missing required packages")
        if not nltk_ok:
            print("  - Download missing NLTK data")
        if not files_ok:
            print("  - Check missing project files")
        if not all_modules_ok:
            print("  - Fix module import errors")
        if not all_tests_ok:
            print("  - Fix functionality test failures")
    
    print("="*70)
    
    return 0 if overall_status else 1

if __name__ == "__main__":
    sys.exit(main())

