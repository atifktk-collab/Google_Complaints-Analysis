"""
Configuration file for Complaints Analysis Project
"""
import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).resolve().parent

# Data directories
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
MODELS_DIR = DATA_DIR / "models"

# Create directories if they don't exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, MODELS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Model configurations
MODEL_CONFIG = {
    "sentiment_model": "distilbert-base-uncased-finetuned-sst-2-english",
    "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
    "max_length": 512,
    "batch_size": 32,
}

# Categories for complaint classification
COMPLAINT_CATEGORIES = [
    "Product Quality",
    "Customer Service",
    "Delivery Issues",
    "Billing/Payment",
    "Technical Issues",
    "Refund/Return",
    "Account Management",
    "Other"
]

# Priority levels
PRIORITY_LEVELS = {
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4
}

# Sentiment thresholds
SENTIMENT_THRESHOLDS = {
    "positive": 0.6,
    "negative": -0.6
}

# Text preprocessing config
PREPROCESSING_CONFIG = {
    "lowercase": True,
    "remove_urls": True,
    "remove_emails": True,
    "remove_special_chars": True,
    "remove_stopwords": True,
    "lemmatize": True,
    "min_length": 10
}

# Visualization settings
VIZ_CONFIG = {
    "figure_size": (12, 6),
    "color_palette": "viridis",
    "font_size": 12
}

# API Configuration (load from environment variables)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///complaints.db")

# Logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "filename": "complaints_analysis.log",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["console", "file"]
    }
}

