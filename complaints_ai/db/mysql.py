import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from .models import Base

load_dotenv()

def get_db_url():
    user = os.getenv('DB_USER', 'root')
    password = os.getenv('DB_PASSWORD', '')
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'complaints_db')
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            get_db_url(),
            pool_size=5,
            pool_recycle=3600
        )
    return _engine

def init_db():
    engine = get_engine()
    Base.metadata.create_all(engine)

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()
