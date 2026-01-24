"""
Main complaint analyzer using NLP and ML techniques
"""
import logging
from typing import Dict, List
import re

try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False
    logging.warning("TextBlob not available. Using basic sentiment analysis.")

from config import COMPLAINT_CATEGORIES, PRIORITY_LEVELS

logger = logging.getLogger(__name__)


class ComplaintAnalyzer:
    """Analyzer for customer complaints"""
    
    def __init__(self):
        """Initialize the complaint analyzer"""
        self.categories = COMPLAINT_CATEGORIES
        self.priority_levels = PRIORITY_LEVELS
        
        # Keywords for category classification
        self.category_keywords = {
            "Product Quality": ["defect", "broken", "damaged", "poor quality", "faulty", 
                               "not working", "stopped working", "malfunctioning"],
            "Customer Service": ["rude", "unprofessional", "unhelpful", "customer service",
                                "support", "representative", "staff", "employee"],
            "Delivery Issues": ["delivery", "shipping", "arrived late", "never arrived",
                              "lost package", "wrong address", "delayed"],
            "Billing/Payment": ["charged", "payment", "billing", "refund", "overcharged",
                               "wrong amount", "invoice", "credit card"],
            "Technical Issues": ["technical", "bug", "error", "crash", "not loading",
                                "glitch", "software", "app", "website"],
            "Refund/Return": ["refund", "return", "money back", "exchange", "replace"],
            "Account Management": ["account", "login", "password", "access", "locked out",
                                  "username", "registration"],
            "Other": []
        }
        
        # Keywords for priority detection
        self.urgent_keywords = ["urgent", "immediately", "asap", "emergency", "critical",
                               "serious", "dangerous", "unsafe", "lawyer", "legal"]
        
        logger.info("ComplaintAnalyzer initialized successfully")
    
    def analyze_sentiment(self, text: str) -> Dict[str, any]:
        """
        Analyze sentiment of the complaint
        
        Args:
            text: Complaint text
            
        Returns:
            Dictionary with sentiment and score
        """
        if TEXTBLOB_AVAILABLE:
            try:
                blob = TextBlob(text)
                polarity = blob.sentiment.polarity
                
                # Classify sentiment based on polarity
                if polarity > 0.1:
                    sentiment = "positive"
                elif polarity < -0.1:
                    sentiment = "negative"
                else:
                    sentiment = "neutral"
                
                return {
                    "sentiment": sentiment,
                    "sentiment_score": round(polarity, 3)
                }
            except Exception as e:
                logger.error(f"Error in sentiment analysis: {str(e)}")
        
        # Fallback: simple keyword-based sentiment
        negative_words = ["bad", "terrible", "awful", "poor", "worst", "hate", 
                         "disappointed", "frustrated", "angry", "horrible"]
        positive_words = ["good", "great", "excellent", "love", "best", "happy",
                         "satisfied", "pleased", "wonderful"]
        
        text_lower = text.lower()
        neg_count = sum(1 for word in negative_words if word in text_lower)
        pos_count = sum(1 for word in positive_words if word in text_lower)
        
        if neg_count > pos_count:
            sentiment = "negative"
            score = -0.5
        elif pos_count > neg_count:
            sentiment = "positive"
            score = 0.5
        else:
            sentiment = "neutral"
            score = 0.0
        
        return {
            "sentiment": sentiment,
            "sentiment_score": score
        }
    
    def classify_category(self, text: str) -> str:
        """
        Classify complaint into a category
        
        Args:
            text: Complaint text
            
        Returns:
            Category string
        """
        text_lower = text.lower()
        category_scores = {}
        
        for category, keywords in self.category_keywords.items():
            if category == "Other":
                continue
            
            score = sum(1 for keyword in keywords if keyword in text_lower)
            if score > 0:
                category_scores[category] = score
        
        if category_scores:
            return max(category_scores, key=category_scores.get)
        else:
            return "Other"
    
    def determine_priority(self, text: str, sentiment_score: float) -> str:
        """
        Determine priority level of the complaint
        
        Args:
            text: Complaint text
            sentiment_score: Sentiment score (-1 to 1)
            
        Returns:
            Priority level string
        """
        text_lower = text.lower()
        
        # Check for urgent keywords
        has_urgent = any(keyword in text_lower for keyword in self.urgent_keywords)
        
        # Very negative sentiment + urgent keywords = critical
        if sentiment_score < -0.6 and has_urgent:
            return "critical"
        
        # Urgent keywords or very negative = high priority
        if has_urgent or sentiment_score < -0.5:
            return "high"
        
        # Moderately negative = medium priority
        if sentiment_score < -0.2:
            return "medium"
        
        # Everything else = low priority
        return "low"
    
    def extract_keywords(self, text: str, top_n: int = 5) -> List[str]:
        """
        Extract key topics/keywords from complaint
        
        Args:
            text: Complaint text
            top_n: Number of keywords to extract
            
        Returns:
            List of keywords
        """
        # Simple keyword extraction (can be enhanced with TF-IDF or other methods)
        # Remove common words and extract nouns
        text_lower = text.lower()
        
        # Remove punctuation
        text_clean = re.sub(r'[^\w\s]', '', text_lower)
        
        # Split into words
        words = text_clean.split()
        
        # Filter out common words (simple stopwords)
        common_words = {'the', 'is', 'at', 'which', 'on', 'a', 'an', 'and', 'or', 
                       'but', 'in', 'with', 'to', 'for', 'of', 'as', 'by', 'this',
                       'that', 'it', 'from', 'are', 'was', 'were', 'been', 'be',
                       'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
                       'could', 'should', 'my', 'your', 'their', 'our'}
        
        # Get word frequency
        word_freq = {}
        for word in words:
            if word not in common_words and len(word) > 3:
                word_freq[word] = word_freq.get(word, 0) + 1
        
        # Get top N keywords
        if word_freq:
            sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
            keywords = [word for word, freq in sorted_words[:top_n]]
            return keywords
        
        return []
    
    def analyze(self, text: str) -> Dict[str, any]:
        """
        Perform complete analysis on a complaint
        
        Args:
            text: Complaint text
            
        Returns:
            Dictionary with all analysis results
        """
        try:
            # Sentiment analysis
            sentiment_result = self.analyze_sentiment(text)
            
            # Category classification
            category = self.classify_category(text)
            
            # Priority determination
            priority = self.determine_priority(text, sentiment_result['sentiment_score'])
            
            # Keyword extraction
            keywords = self.extract_keywords(text)
            
            return {
                "sentiment": sentiment_result['sentiment'],
                "sentiment_score": sentiment_result['sentiment_score'],
                "category": category,
                "priority": priority,
                "keywords": keywords
            }
            
        except Exception as e:
            logger.error(f"Error analyzing complaint: {str(e)}")
            return {
                "sentiment": "unknown",
                "sentiment_score": 0.0,
                "category": "Other",
                "priority": "low",
                "keywords": [],
                "error": str(e)
            }

