"""Test function status checker"""
from src.utils.function_status import FunctionStatusChecker

checker = FunctionStatusChecker()
status = checker.get_all_functions_status()

print("Status check completed")
print(f"Total functions: {status['summary']['total']}")
print(f"Working: {status['summary']['working']}")
print(f"Available: {status['summary']['available']}")
print(f"Errors: {status['summary']['error']}")
print(f"Health: {status['summary']['health_percentage']}%")


