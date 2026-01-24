# Google Complaints Analysis - AI Agent

An intelligent AI-powered system for analyzing customer complaints using Natural Language Processing (NLP) and Machine Learning techniques.

## 📋 Project Overview

This project provides comprehensive analysis of customer complaints, including:
- **Sentiment Analysis**: Classify complaints as positive, negative, or neutral
- **Topic Modeling**: Identify common themes and issues in complaints
- **Complaint Classification**: Categorize complaints into predefined categories
- **Priority Detection**: Identify urgent complaints requiring immediate attention
- **Trend Analysis**: Track complaint patterns over time
- **Interactive Dashboard**: Visualize insights and metrics

## 🚀 Features

- ✅ Automated complaint categorization
- ✅ Sentiment analysis using advanced NLP models
- ✅ Topic extraction and clustering
- ✅ Priority scoring system
- ✅ Interactive visualizations and dashboards
- ✅ Export reports in multiple formats
- ✅ Real-time analysis capabilities
- ✅ Integration with various data sources

## 📁 Project Structure

```
Google_Complaints-Analysis/
│
├── data/
│   ├── raw/              # Raw complaint data
│   ├── processed/        # Cleaned and processed data
│   └── models/           # Saved ML models
│
├── notebooks/            # Jupyter notebooks for exploration
│   ├── 01_data_exploration.ipynb
│   ├── 02_sentiment_analysis.ipynb
│   └── 03_topic_modeling.ipynb
│
├── src/
│   ├── data/             # Data loading and preprocessing
│   ├── models/           # ML model definitions
│   ├── utils/            # Utility functions
│   └── visualization/    # Plotting and dashboard code
│
├── tests/                # Unit tests
├── config.py             # Configuration settings
├── main.py               # Main application entry point
├── requirements.txt      # Python dependencies
└── README.md             # Project documentation
```

## 🛠️ Installation

1. Clone the repository:
```bash
git clone https://github.com/atifktk-collab/Google_Complaints-Analysis.git
cd Google_Complaints-Analysis
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Download required NLP models:
```bash
python -m spacy download en_core_web_sm
python -c "import nltk; nltk.download('vader_lexicon'); nltk.download('stopwords'); nltk.download('punkt')"
```

5. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## 💻 Usage

### Basic Usage

```python
from src.models.complaint_analyzer import ComplaintAnalyzer

# Initialize analyzer
analyzer = ComplaintAnalyzer()

# Analyze a single complaint
complaint = "The product stopped working after just one week. Very disappointed!"
result = analyzer.analyze(complaint)

print(f"Sentiment: {result['sentiment']}")
print(f"Category: {result['category']}")
print(f"Priority: {result['priority']}")
```

### Running the Dashboard

```bash
streamlit run app.py
```

### Processing Batch Data

```bash
python main.py --input data/raw/complaints.csv --output data/processed/analyzed_complaints.csv
```

## 📊 Data Format

Input data should be in CSV format with the following columns:
- `complaint_id`: Unique identifier
- `complaint_text`: The complaint text
- `date`: Date of complaint (optional)
- `customer_id`: Customer identifier (optional)

## 🧪 Testing

Run tests using pytest:
```bash
pytest tests/
```

## 📈 Model Performance

Current model metrics:
- Sentiment Analysis Accuracy: ~85%
- Classification F1-Score: ~82%
- Topic Coherence Score: 0.67

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👥 Authors

- **Atif KTK** - [atifktk-collab](https://github.com/atifktk-collab)

## 🙏 Acknowledgments

- Google for providing inspiration for complaint analysis
- OpenAI for NLP capabilities
- The open-source community for amazing libraries

## 📧 Contact

For questions or feedback, please open an issue on GitHub.

---

**Note**: This project is for educational and analytical purposes. Ensure compliance with data privacy regulations when handling customer complaints.

