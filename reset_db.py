from complaints_ai.db.models import Base
from complaints_ai.db.mysql import get_engine
import os
from dotenv import load_dotenv

load_dotenv("complaints_ai/.env")

def reset_database():
    print("Connecting to database...")
    engine = get_engine()
    
    print("Dropping all tables...")
    Base.metadata.drop_all(engine)
    
    print("Creating all tables...")
    Base.metadata.create_all(engine)
    
    print("Database reset successfully with new schema.")

if __name__ == "__main__":
    reset_database()
