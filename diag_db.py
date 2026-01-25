import os
import sys
import logging

# Set up logging to see what load_dotenv is doing
logging.basicConfig(level=logging.INFO)

# 1. Check if we can find mysql.py
try:
    from complaints_ai.db import mysql
    print("Successfully imported complaints_ai.db.mysql")
    print(f"mysql.py file: {mysql.__file__}")
except ImportError as e:
    print(f"Failed to import mysql: {e}")
    sys.exit(1)

# 2. Check the URL it generates
url = mysql.get_db_url()
# Mask password for safety in display but check if it's there
parts = url.split(":")
print(f"Generated DB URL (masked): {parts[0]}:{parts[1]}:***@{parts[2].split('@')[-1]}")

if ":@" in url.replace("mysql+pymysql://", ""):
    print("CRITICAL: Password appears to be EMPTY in the generated URL.")
else:
    print("SUCCESS: Password appears to be present in the generated URL.")

# 3. Check environment variables directly
print(f"OS Env DB_USER: {os.environ.get('DB_USER')}")
print(f"OS Env DB_PASSWORD exists: {bool(os.environ.get('DB_PASSWORD'))}")

# 4. Try to connect
try:
    engine = mysql.get_engine()
    with engine.connect() as conn:
        print("Successfully connected to the database!")
except Exception as e:
    print(f"Connection failed: {e}")
