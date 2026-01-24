# Quick Start Guide

Get up and running with Google Complaints Analysis in minutes!

## Prerequisites

- Python 3.8 or higher
- pip package manager
- Git

## Installation

1. **Clone the repository:**
```bash
git clone https://github.com/atifktk-collab/Google_Complaints-Analysis.git
cd Google_Complaints-Analysis
```

2. **Create and activate virtual environment:**

On Windows:
```bash
python -m venv venv
venv\Scripts\activate
```

On macOS/Linux:
```bash
python -m venv venv
source venv/bin/activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Download NLP models:**
```bash
python -m spacy download en_core_web_sm
python -c "import nltk; nltk.download('vader_lexicon'); nltk.download('stopwords'); nltk.download('punkt')"
```

## Quick Test with Sample Data

We've included sample data to help you get started quickly!

1. **Run analysis on sample data:**
```bash
python main.py --input data/raw/sample_complaints.csv --output data/processed/results.csv
```

2. **Generate visualizations:**
```bash
python main.py --input data/raw/sample_complaints.csv --generate-report
```

3. **Launch the dashboard:**
```bash
streamlit run app.py
```

The dashboard will open in your browser at `http://localhost:8501`

## Analyze Your Own Data

### Option 1: Using the Command Line

1. Place your CSV file in `data/raw/` (must have a `complaint_text` column)
2. Run analysis:
```bash
python main.py --input data/raw/your_file.csv --output data/processed/analyzed.csv
```

### Option 2: Using the Interactive Dashboard

1. Launch the dashboard:
```bash
streamlit run app.py
```

2. Navigate to "Analyze Single Complaint"
3. Paste your complaint text and click "Analyze"

### Option 3: Using Python Code

```python
from src.models.complaint_analyzer import ComplaintAnalyzer

# Initialize analyzer
analyzer = ComplaintAnalyzer()

# Analyze a complaint
complaint = "Your complaint text here..."
result = analyzer.analyze(complaint)

print(f"Sentiment: {result['sentiment']}")
print(f"Category: {result['category']}")
print(f"Priority: {result['priority']}")
```

## Expected Data Format

Your CSV file should have at least these columns:
- `complaint_text` (required): The actual complaint text
- `complaint_id` (optional): Unique identifier
- `date` (optional): Date of complaint
- `customer_id` (optional): Customer identifier

Example:
```csv
complaint_id,complaint_text,date
1,"The product is broken",2026-01-24
2,"Great customer service!",2026-01-24
```

## Understanding the Results

The analysis provides:

- **Sentiment**: positive, neutral, or negative
- **Sentiment Score**: -1.0 (very negative) to +1.0 (very positive)
- **Category**: Classified complaint type (e.g., Product Quality, Customer Service)
- **Priority**: low, medium, high, or critical
- **Keywords**: Key topics extracted from the complaint

## Next Steps

- Check out the Jupyter notebooks in `notebooks/` for detailed analysis examples
- Read the full [README.md](README.md) for comprehensive documentation
- See [CONTRIBUTING.md](CONTRIBUTING.md) to contribute to the project

## Troubleshooting

**Issue: "ModuleNotFoundError"**
- Solution: Make sure you've installed all requirements: `pip install -r requirements.txt`

**Issue: "File not found"**
- Solution: Ensure your CSV file is in the correct directory and the path is correct

**Issue: "NLTK data not found"**
- Solution: Run the NLTK download commands from step 4 of Installation

## Support

If you encounter any issues:
1. Check the error message carefully
2. Look for similar issues in the GitHub Issues section
3. Open a new issue with details about your problem

Happy analyzing! 📊

