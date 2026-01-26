from sqlalchemy import Column, Integer, String, DateTime, Date, Enum, Text, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ComplaintsRaw(Base):
    __tablename__ = 'complaints_raw'
    
    sr_number = Column(String(50), primary_key=True)
    sr_row_id = Column(String(50), index=True)
    mdn = Column(String(50))
    region_id = Column(String(50))
    sr_open_dt = Column(Date)
    sr_open_dttm = Column(DateTime, index=True)
    sr_close_dttm = Column(DateTime)
    sr_duration = Column(String(50)) # Kept as string for now to avoid parsing issues, or Float if cleaned
    
    # Classifications
    sr_type = Column(String(100), index=True)
    sr_sub_type = Column(String(100))
    sr_status = Column(String(50), index=True) # Mapped from SR_STATUS
    sr_sub_status = Column(String(50))
    
    # Text Fields
    rca = Column(String(255), index=True) # RCA
    desc_text = Column(Text)
    fault_type = Column(String(100))
    department = Column(String(100))
    
    # Location / Network Elements
    region = Column(String(50), index=True)
    city = Column(String(50), index=True)
    exc_id = Column(String(50), index=True)
    cabinet_id = Column(String(50), index=True)
    dp_id = Column(String(50))
    switch_id = Column(String(50))
    
    # Product / Customer
    product = Column(String(50))
    sub_product = Column(String(50))
    product_id = Column(String(50))
    cust_seg = Column(String(50))
    service_type = Column(String(50))
    
    # Misc
    priority = Column(String(50)) # SR_PRIO_CD

class DailyAnomalies(Base):
    __tablename__ = 'daily_anomalies'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    anomaly_date = Column(Date, nullable=False, index=True)
    dimension = Column(Enum('Type', 'Region', 'Exchange', 'City', 'RCA', 'Total', name='dim_enum'), nullable=False)
    dimension_key = Column(String(200), nullable=False)
    metric_value = Column(Float, nullable=False)
    baseline_avg = Column(Float, nullable=False)
    baseline_std = Column(Float, nullable=False)
    z_score = Column(Float, nullable=False)
    severity = Column(Enum('INFO', 'WARNING', 'CRITICAL', name='sev_enum'), nullable=False)
    rca_context = Column(Text) # JSON string of probable RCAs

class DailyTrends(Base):
    __tablename__ = 'daily_trends'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    trend_date = Column(Date, nullable=False, index=True)
    dimension = Column(Enum('Type', 'Region', 'Exchange', 'City', 'RCA', 'Total', name='dim_enum'), nullable=False)
    dimension_key = Column(String(200), nullable=False)
    metric_value = Column(Float, nullable=False)
    trend_direction = Column(Enum('UP', 'DOWN', 'STABLE', name='trend_dir_enum'), nullable=False)
    trend_strength = Column(Float, nullable=False) # Percentage change or slope
    window_days = Column(Integer, nullable=False) # 7, 14, or 30
    significance = Column(Float) # p-value or confidence score

class DailyVariations(Base):
    __tablename__ = 'daily_variations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    variation_date = Column(Date, nullable=False, index=True)
    dimension = Column(Enum('Type', 'Region', 'Exchange', 'City', 'RCA', 'Total', name='dim_enum'), nullable=False)
    dimension_key = Column(String(200), nullable=False)
    current_value = Column(Float, nullable=False)
    previous_value = Column(Float, nullable=False)
    variation_type = Column(Enum('DOD', 'WOW', 'MOM', name='var_type_enum'), nullable=False) # Day/Week/Month over Day/Week/Month
    variation_percent = Column(Float, nullable=False)
    is_significant = Column(Integer, nullable=False, default=0) # 0 or 1 (boolean)

class ExecInsights(Base):
    __tablename__ = 'exec_insights'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False)
    title = Column(String(255), nullable=False)
    summary = Column(Text, nullable=False)
    severity = Column(Enum('INFO', 'WARNING', 'CRITICAL', name='insight_sev_enum'), nullable=False)

class DailyMTTR(Base):
    __tablename__ = 'daily_mttr'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, index=True)
    dimension = Column(Enum('Region', 'City', 'Exchange', 'Total', name='dim_mttr_enum'), nullable=False)
    dimension_key = Column(String(200), nullable=False)
    avg_mttr_hours = Column(Float, nullable=False)
    total_resolved_count = Column(Integer, nullable=False)

class DailyAging(Base):
    __tablename__ = 'daily_aging'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, index=True)
    dimension = Column(Enum('Region', 'City', 'Exchange', 'Total', name='dim_mttr_enum'), nullable=False)
    dimension_key = Column(String(200), nullable=False)
    slab = Column(Enum('> 24 Hours', '> 48 Hours', '> 72 Hours', '> 6 Days', '> 10 Days', '> 30 Days', '> 60 Days', name='aging_slab_enum'), nullable=False)
    count = Column(Integer, nullable=False)
