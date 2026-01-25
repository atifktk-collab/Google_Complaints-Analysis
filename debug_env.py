import os
import sys

# Simulation of what mysql.py does
mysql_file = os.path.join(os.getcwd(), 'complaints_ai', 'db', 'mysql.py')
dir_name = os.path.dirname(mysql_file)
env_path = os.path.abspath(os.path.join(dir_name, '..', '.env'))

print(f"Current Working Dir: {os.getcwd()}")
print(f"mysql.py Dir: {dir_name}")
print(f"Calculated .env Path: {env_path}")
print(f"Does it exist? {os.path.exists(env_path)}")

if os.path.exists(env_path):
    from dotenv import load_dotenv
    load_dotenv(env_path)
    print(f"DB_USER: {os.getenv('DB_USER')}")
    print(f"DB_PASSWORD is set? {'Yes' if os.getenv('DB_PASSWORD') else 'No'}")
