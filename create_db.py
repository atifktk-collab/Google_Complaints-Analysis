import pymysql
import os
from dotenv import load_dotenv

load_dotenv("complaints_ai/.env")

def create_database():
    host = os.getenv("DB_HOST", "localhost")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "khan4346")
    port = int(os.getenv("DB_PORT", 3306))
    db_name = os.getenv("DB_NAME", "complaints_db")

    print(f"Connecting to MySQL at {host} as {user}...")
    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=port
        )
        cursor = conn.cursor()
        print(f"Creating database '{db_name}' if not exists...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print("Database created successfully.")
        conn.close()
    except Exception as e:
        print(f"Failed to create database: {e}")

if __name__ == "__main__":
    create_database()
