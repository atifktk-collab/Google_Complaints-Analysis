"""
Function status checker for all analyzer functions
"""
import inspect
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class FunctionStatusChecker:
    """Check status of all analyzer functions"""
    
    def __init__(self):
        self.functions_status = {}
    
    def check_function(self, func, test_input=None):
        """
        Check if a function is working
        
        Args:
            func: Function to check
            test_input: Test input for the function
            
        Returns:
            Dict with status information
        """
        func_name = func.__name__
        try:
            # Get function signature
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            
            # Try to call function if test input provided
            if test_input is not None:
                try:
                    if len(params) == 1:
                        result = func(test_input)
                    elif len(params) == 0:
                        result = func()
                    else:
                        result = func(test_input, **{p: None for p in params[1:]})
                    
                    return {
                        "status": "working",
                        "error": None,
                        "has_test": True,
                        "test_result": "success",
                        "parameters": params
                    }
                except Exception as e:
                    return {
                        "status": "error",
                        "error": str(e),
                        "has_test": True,
                        "test_result": "failed",
                        "parameters": params
                    }
            else:
                return {
                    "status": "available",
                    "error": None,
                    "has_test": False,
                    "test_result": None,
                    "parameters": params
                }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "has_test": False,
                "test_result": None,
                "parameters": []
            }
    
    def check_analyzer_functions(self):
        """Check all functions in ComplaintAnalyzer"""
        from src.models.complaint_analyzer import ComplaintAnalyzer
        
        analyzer = ComplaintAnalyzer()
        functions_info = {}
        
        # Get all methods from the instance
        methods = inspect.getmembers(analyzer, predicate=inspect.ismethod)
        
        test_complaint = "This product is broken and terrible. I need a refund immediately!"
        
        for name, method in methods:
            if not name.startswith('_'):
                try:
                    # Test the method with appropriate inputs
                    if name == 'analyze':
                        result = method(test_complaint)
                        status = {"status": "working", "error": None, "has_test": True, 
                                 "test_result": "success", "parameters": ["text"]}
                    elif name == 'analyze_sentiment':
                        result = method(test_complaint)
                        status = {"status": "working", "error": None, "has_test": True,
                                 "test_result": "success", "parameters": ["text"]}
                    elif name == 'classify_category':
                        result = method(test_complaint)
                        status = {"status": "working", "error": None, "has_test": True,
                                 "test_result": "success", "parameters": ["text"]}
                    elif name == 'determine_priority':
                        result = method(test_complaint, -0.7)
                        status = {"status": "working", "error": None, "has_test": True,
                                 "test_result": "success", "parameters": ["text", "sentiment_score"]}
                    elif name == 'extract_keywords':
                        result = method(test_complaint)
                        status = {"status": "working", "error": None, "has_test": True,
                                 "test_result": "success", "parameters": ["text", "top_n"]}
                    else:
                        # For other methods, just check if they exist
                        sig = inspect.signature(method)
                        status = {"status": "available", "error": None, "has_test": False,
                                 "test_result": None, "parameters": list(sig.parameters.keys())}
                    
                    functions_info[name] = {
                        "name": name,
                        "status": status["status"],
                        "error": status.get("error"),
                        "tested": status.get("has_test", False),
                        "parameters": status.get("parameters", []),
                        "docstring": inspect.getdoc(method) or "No documentation"
                    }
                except Exception as e:
                    functions_info[name] = {
                        "name": name,
                        "status": "error",
                        "error": str(e),
                        "tested": True,
                        "parameters": [],
                        "docstring": "Error checking function"
                    }
        
        return functions_info
    
    def check_data_functions(self):
        """Check all functions in data modules"""
        from src.data.data_loader import load_complaints_data, save_complaints_data
        from src.data.preprocessor import ComplaintPreprocessor
        from pathlib import Path
        
        functions_info = {}
        
        # Check load_complaints_data
        try:
            sample_file = Path('data/raw/sample_complaints.csv')
            if sample_file.exists():
                df = load_complaints_data(sample_file)
                functions_info['load_complaints_data'] = {
                    "name": "load_complaints_data",
                    "status": "working",
                    "error": None,
                    "tested": True,
                    "parameters": ["file_path", "required_columns"],
                    "docstring": inspect.getdoc(load_complaints_data) or "Load complaints data"
                }
            else:
                functions_info['load_complaints_data'] = {
                    "name": "load_complaints_data",
                    "status": "available",
                    "error": "Sample file not found for testing",
                    "tested": False,
                    "parameters": ["file_path", "required_columns"],
                    "docstring": inspect.getdoc(load_complaints_data) or "Load complaints data"
                }
        except Exception as e:
            functions_info['load_complaints_data'] = {
                "name": "load_complaints_data",
                "status": "error",
                "error": str(e),
                "tested": True,
                "parameters": ["file_path", "required_columns"],
                "docstring": "Load complaints data"
            }
        
        # Check preprocessor
        try:
            preprocessor = ComplaintPreprocessor()
            test_text = "This is a TEST!!!"
            cleaned = preprocessor.preprocess(test_text)
            functions_info['preprocess'] = {
                "name": "preprocess",
                "status": "working",
                "error": None,
                "tested": True,
                "parameters": ["text"],
                "docstring": inspect.getdoc(preprocessor.preprocess) or "Preprocess text"
            }
        except Exception as e:
            functions_info['preprocess'] = {
                "name": "preprocess",
                "status": "error",
                "error": str(e),
                "tested": True,
                "parameters": ["text"],
                "docstring": "Preprocess text"
            }
        
        return functions_info
    
    def check_visualization_functions(self):
        """Check visualization functions"""
        from src.visualization.dashboard import (
            plot_sentiment_distribution,
            plot_category_distribution,
            plot_priority_distribution,
            plot_sentiment_by_category,
            generate_dashboard_report
        )
        
        functions_info = {}
        functions = [
            plot_sentiment_distribution,
            plot_category_distribution,
            plot_priority_distribution,
            plot_sentiment_by_category,
            generate_dashboard_report
        ]
        
        for func in functions:
            try:
                sig = inspect.signature(func)
                functions_info[func.__name__] = {
                    "name": func.__name__,
                    "status": "available",
                    "error": None,
                    "tested": False,
                    "parameters": list(sig.parameters.keys()),
                    "docstring": inspect.getdoc(func) or "No documentation"
                }
            except Exception as e:
                functions_info[func.__name__] = {
                    "name": func.__name__,
                    "status": "error",
                    "error": str(e),
                    "tested": False,
                    "parameters": [],
                    "docstring": "Error checking function"
                }
        
        return functions_info
    
    def get_all_functions_status(self) -> Dict[str, Any]:
        """Get status of all functions"""
        all_status = {
            "analyzer": self.check_analyzer_functions(),
            "data": self.check_data_functions(),
            "visualization": self.check_visualization_functions()
        }
        
        # Calculate summary
        total_functions = 0
        working_functions = 0
        available_functions = 0
        error_functions = 0
        
        for category, functions in all_status.items():
            for func_name, func_info in functions.items():
                total_functions += 1
                if func_info["status"] == "working":
                    working_functions += 1
                elif func_info["status"] == "available":
                    available_functions += 1
                elif func_info["status"] == "error":
                    error_functions += 1
        
        all_status["summary"] = {
            "total": total_functions,
            "working": working_functions,
            "available": available_functions,
            "error": error_functions,
            "health_percentage": round((working_functions / total_functions * 100) if total_functions > 0 else 0, 1)
        }
        
        return all_status

