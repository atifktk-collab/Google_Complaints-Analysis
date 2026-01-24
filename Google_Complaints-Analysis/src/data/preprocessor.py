"""
Text preprocessing utilities for complaints analysis
"""
import re
import string
import logging
from typing import List

try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    from nltk.stem import WordNetLemmatizer
    
    # Download required NLTK data (only on first run)
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        nltk.download('punkt', quiet=True)
    
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords', quiet=True)
    
    try:
        nltk.data.find('corpora/wordnet')
    except LookupError:
        nltk.download('wordnet', quiet=True)
        
except ImportError:
    pass

from config import PREPROCESSING_CONFIG

logger = logging.getLogger(__name__)


class ComplaintPreprocessor:
    """Preprocessor for complaint text data"""
    
    def __init__(self, config=None):
        """
        Initialize preprocessor
        
        Args:
            config: Configuration dictionary for preprocessing options
        """
        self.config = config or PREPROCESSING_CONFIG
        
        try:
            self.stop_words = set(stopwords.words('english'))
            self.lemmatizer = WordNetLemmatizer()
        except:
            logger.warning("NLTK resources not available. Using basic preprocessing.")
            self.stop_words = set()
            self.lemmatizer = None
    
    def remove_urls(self, text: str) -> str:
        """Remove URLs from text"""
        url_pattern = re.compile(r'https?://\S+|www\.\S+')
        return url_pattern.sub('', text)
    
    def remove_emails(self, text: str) -> str:
        """Remove email addresses from text"""
        email_pattern = re.compile(r'\S+@\S+')
        return email_pattern.sub('', text)
    
    def remove_special_chars(self, text: str) -> str:
        """Remove special characters but keep basic punctuation"""
        # Keep letters, numbers, spaces, and basic punctuation
        text = re.sub(r'[^a-zA-Z0-9\s.,!?]', '', text)
        return text
    
    def remove_extra_spaces(self, text: str) -> str:
        """Remove extra whitespace"""
        return ' '.join(text.split())
    
    def remove_stopwords(self, text: str) -> str:
        """Remove stopwords from text"""
        if not self.stop_words:
            return text
        
        try:
            tokens = word_tokenize(text)
            filtered_tokens = [w for w in tokens if w.lower() not in self.stop_words]
            return ' '.join(filtered_tokens)
        except:
            # Fallback to simple splitting
            words = text.split()
            filtered_words = [w for w in words if w.lower() not in self.stop_words]
            return ' '.join(filtered_words)
    
    def lemmatize_text(self, text: str) -> str:
        """Lemmatize text"""
        if not self.lemmatizer:
            return text
        
        try:
            tokens = word_tokenize(text)
            lemmatized = [self.lemmatizer.lemmatize(w) for w in tokens]
            return ' '.join(lemmatized)
        except:
            return text
    
    def preprocess(self, text: str) -> str:
        """
        Apply all preprocessing steps to text
        
        Args:
            text: Input text string
            
        Returns:
            Preprocessed text string
        """
        if not isinstance(text, str):
            return ""
        
        # Convert to lowercase
        if self.config.get('lowercase', True):
            text = text.lower()
        
        # Remove URLs
        if self.config.get('remove_urls', True):
            text = self.remove_urls(text)
        
        # Remove emails
        if self.config.get('remove_emails', True):
            text = self.remove_emails(text)
        
        # Remove special characters
        if self.config.get('remove_special_chars', True):
            text = self.remove_special_chars(text)
        
        # Remove extra spaces
        text = self.remove_extra_spaces(text)
        
        # Remove stopwords
        if self.config.get('remove_stopwords', True):
            text = self.remove_stopwords(text)
        
        # Lemmatize
        if self.config.get('lemmatize', True):
            text = self.lemmatize_text(text)
        
        # Final cleanup
        text = self.remove_extra_spaces(text)
        
        # Check minimum length
        min_length = self.config.get('min_length', 10)
        if len(text) < min_length:
            logger.debug(f"Text too short after preprocessing: {len(text)} chars")
        
        return text
    
    def preprocess_batch(self, texts: List[str]) -> List[str]:
        """
        Preprocess a batch of texts
        
        Args:
            texts: List of text strings
            
        Returns:
            List of preprocessed text strings
        """
        return [self.preprocess(text) for text in texts]

