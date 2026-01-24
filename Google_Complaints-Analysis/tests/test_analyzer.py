"""
Unit tests for ComplaintAnalyzer
"""
import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models.complaint_analyzer import ComplaintAnalyzer


class TestComplaintAnalyzer(unittest.TestCase):
    """Test cases for ComplaintAnalyzer class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.analyzer = ComplaintAnalyzer()
        
        # Test complaints
        self.negative_complaint = "This product is terrible and broke after one day. Very disappointed!"
        self.positive_complaint = "Excellent service! The team was very helpful and resolved my issue quickly."
        self.neutral_complaint = "I received the product. It works as described in the manual."
    
    def test_analyzer_initialization(self):
        """Test that analyzer initializes correctly"""
        self.assertIsNotNone(self.analyzer)
        self.assertIsInstance(self.analyzer.categories, list)
        self.assertGreater(len(self.analyzer.categories), 0)
    
    def test_sentiment_analysis_negative(self):
        """Test sentiment analysis on negative complaint"""
        result = self.analyzer.analyze_sentiment(self.negative_complaint)
        
        self.assertIn('sentiment', result)
        self.assertIn('sentiment_score', result)
        self.assertEqual(result['sentiment'], 'negative')
        self.assertLess(result['sentiment_score'], 0)
    
    def test_sentiment_analysis_positive(self):
        """Test sentiment analysis on positive complaint"""
        result = self.analyzer.analyze_sentiment(self.positive_complaint)
        
        self.assertIn('sentiment', result)
        self.assertIn('sentiment_score', result)
        self.assertEqual(result['sentiment'], 'positive')
        self.assertGreater(result['sentiment_score'], 0)
    
    def test_category_classification(self):
        """Test category classification"""
        product_complaint = "The product quality is very poor and it's defective."
        category = self.analyzer.classify_category(product_complaint)
        
        self.assertIsInstance(category, str)
        self.assertIn(category, self.analyzer.categories)
    
    def test_priority_determination(self):
        """Test priority level determination"""
        urgent_complaint = "This is urgent! The product is dangerous and needs immediate attention!"
        priority = self.analyzer.determine_priority(urgent_complaint, -0.8)
        
        self.assertIn(priority, ['low', 'medium', 'high', 'critical'])
        self.assertIn(priority, ['high', 'critical'])  # Should be high priority
    
    def test_keyword_extraction(self):
        """Test keyword extraction"""
        text = "The product quality is poor and customer service was unhelpful"
        keywords = self.analyzer.extract_keywords(text)
        
        self.assertIsInstance(keywords, list)
        # Should extract meaningful words
        self.assertTrue(any(keyword in ['product', 'quality', 'customer', 'service'] 
                          for keyword in keywords))
    
    def test_full_analysis(self):
        """Test complete analysis pipeline"""
        result = self.analyzer.analyze(self.negative_complaint)
        
        # Check all required fields are present
        required_fields = ['sentiment', 'sentiment_score', 'category', 'priority', 'keywords']
        for field in required_fields:
            self.assertIn(field, result)
        
        # Validate types
        self.assertIsInstance(result['sentiment'], str)
        self.assertIsInstance(result['sentiment_score'], (int, float))
        self.assertIsInstance(result['category'], str)
        self.assertIsInstance(result['priority'], str)
        self.assertIsInstance(result['keywords'], list)
    
    def test_empty_text(self):
        """Test handling of empty text"""
        result = self.analyzer.analyze("")
        self.assertIn('sentiment', result)


if __name__ == '__main__':
    unittest.main()

