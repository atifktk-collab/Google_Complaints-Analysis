import polars as pl
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import IntegrityError
import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

from ..db.mysql import get_engine, get_session
from ..db.models import ComplaintsRaw

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IngestionAgent:
    """
    Agent responsible for parsing CSV complaint data and ingesting it into the MySQL database.
    """
    
    def __init__(self):
        self.engine = get_engine()
        # Updated required columns: sr_number is now the primary key
        self.required_columns = [
            "sr_number", "sr_open_dttm", "sr_type", "region", 
            "exc_id"
        ]

    def validate_schema(self, df: pl.DataFrame) -> tuple[bool, list, list]:
        """
        Validates that the provided DataFrame contains all required columns.
        """
        missing_cols = [col for col in self.required_columns if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns in CSV: {missing_cols}")
            logger.info(f"Found columns: {df.columns}")
            return False, missing_cols, df.columns
        return True, [], df.columns

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution method for the agent.
        
        Args:
            context: Dictionary containing input parameters. 
                     Must contain 'file_path' (str) to the CSV file.
                     
        Returns:
            Updated context with execution statistics.
        """
        file_path = context.get('file_path')
        if not file_path or not os.path.exists(file_path):
            logger.error(f"Invalid or missing file path: {file_path}")
            return {"status": "error", "message": "Invalid file path"}

        logger.info(f"Starting ingestion for file: {file_path}")
        
        try:
            # 1. Parse CSV (Try multiple strategies and encodings)
            logger.info(f"Reading file: {file_path}")
            
            # Common encodings to try
            encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252', 'utf-16']
            
            df = pl.DataFrame()
            read_success = False
            last_enc = 'utf-8'
            
            # Strategy A: Polars with encoding trial
            for enc in encodings:
                try:
                    logger.info(f"Attempting Polars read with encoding: {enc}")
                    df = pl.read_csv(file_path, encoding=enc, ignore_errors=True)
                    if not df.is_empty():
                        read_success = True
                        last_enc = enc
                        break
                except Exception as e:
                    logger.debug(f"Polars read failed for {enc}: {e}")
            
            # Strategy B: Pandas fallback with encoding trial
            if not read_success:
                logger.info("Polars failed or returned empty. Trying Pandas fallback with encoding trial.")
                import pandas as pd
                for enc in encodings:
                    try:
                        logger.info(f"Attempting Pandas read with encoding: {enc}")
                        pdf = pd.read_csv(file_path, encoding=enc, on_bad_lines='skip', sep=None, engine='python')
                        if not pdf.empty:
                            df = pl.from_pandas(pdf)
                            read_success = True
                            last_enc = enc
                            break
                    except Exception as e:
                        logger.debug(f"Pandas read failed for {enc}: {e}")

            if not read_success or df.is_empty():
                logger.error("Dataframe is empty after all reading attempts.")
                return {"status": "error", "message": "CSV file appears to be empty, unreadable, or has invalid encoding."}

            # If it only has 1 column, it might be wrong delimiter (Check after successful encoding read)
            if df.width <= 1 and len(df) > 0:
                for sep in [';', '\t', '|']:
                    logger.info(f"Retrying with delimiter: '{sep}' using encoding: {last_enc}")
                    try:
                        # Re-read with same success encoding but new separator
                        temp_df = pl.read_csv(file_path, separator=sep, encoding=last_enc, ignore_errors=True)
                        if temp_df.width > df.width:
                            df = temp_df
                            break
                    except: continue

            logger.info(f"Read {len(df)} rows with columns: {df.columns}")
            
            # Normalize Headers: Lowercase, strip, replace space with underscore
            df.columns = [str(col).lower().strip().replace(' ', '_') for col in df.columns]
            
            # --- Synonym Mapping ---
            synonyms = {
                "sr_row_id": ["id", "row_id", "record_id", "row", "sr_id", "sr_row", "rowid"],
                "sr_open_dttm": ["date", "time", "open_date", "opened", "timestamp", "created_at", "open_dttm", "occurrence_time"],
                "sr_type": ["type", "complaint_type", "category", "sr_type", "order_type"],
                "region": ["location", "zone", "area", "region_name"],
                "exc_id": ["exchange", "exc", "exchange_id", "excid"]
            }
            
            for target, matches in synonyms.items():
                if target not in df.columns:
                    for match in matches:
                        if match in df.columns:
                            logger.info(f"Mapping synonym '{match}' to '{target}'")
                            df = df.rename({match: target})
                            break
            # -----------------------
            
            # Column Mappings / Corrections
            if "sr_status" in df.columns and "status" not in df.columns:
                # We have sr_status in model now, so strictly speaking we don't *need* to rename 
                # if the model column is sr_status. But let's ensure we match model fields.
                # Model has `sr_status`. CSV has `sr_status`. No rename needed.
                pass
            
            # Map 'priority' from 'sr_prio_cd' if exists
            if "sr_prio_cd" in df.columns:
                 df = df.rename({"sr_prio_cd": "priority"})

            # Fill missing text fields/optional fields with None
            expected_fields = [
                "rca", "desc_text", "sr_sub_type", "sr_sub_status", 
                "city", "cabinet_id", "dp_id", "switch_id", 
                "mdn", "region_id", "product", "sub_product", "cust_seg", "sr_duration",
                "sr_number", "product_id"
            ]
            for col in expected_fields:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            
            # 2. Validate Schema
            is_valid, missing, found = self.validate_schema(df)
            if not is_valid:
                return {
                    "status": "error", 
                    "message": f"Schema validation failed. Missing: {missing}",
                    "diagnostics": {
                        "missing": missing,
                        "found": found,
                        "required": self.required_columns
                    }
                }

            # 3. Data Transformation
            
            # Log sample raw date before parsing
            if not df.is_empty() and "sr_open_dttm" in df.columns and len(df["sr_open_dttm"]) > 0:
                # Store original string for diagnostics if parsing fails
                df = df.with_columns(pl.col("sr_open_dttm").alias("raw_sr_open_dttm"))
                raw_sample = df["sr_open_dttm"][0]
                logger.info(f"Sample raw date from CSV: {raw_sample}")
            else:
                logger.warning("No sr_open_dttm column found or dataframe is empty")

            # Date Parsing (Try multiple common formats)
            formats = [
                "%d-%m-%Y %H:%M:%S",
                "%d-%m-%Y %H:%M",
                "%d/%m/%Y %H:%M",
                "%m/%d/%Y %H:%M",
                "%d-%m-%y %H:%M:%S",
                "%d-%m-%y %H:%M",
                "%d-%b-%y %H:%M:%S",
                "%d-%b-%y %H:%M",
                "%Y-%m-%d %H:%M:%S",
                "%m/%d/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
                "%Y/%m/%d %H:%M:%S",
                "%m/%d/%y %H:%M:%S",
                "%d/%m/%y %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S", # ISO
                "%d-%b-%Y %H:%M:%S",
                "%m/%d/%Y %I:%M:%S %p", # 12-hour format
                "%d/%m/%Y %I:%M:%S %p",
                "%Y-%m-%d %I:%M:%S %p"
            ]
            
            try:
                # Attempt parsing with multiple formats
                parsed_dttm = None
                for fmt in formats:
                    try:
                        temp_parsed = df["sr_open_dttm"].str.strptime(pl.Datetime, format=fmt, strict=False)
                        if parsed_dttm is None:
                            parsed_dttm = temp_parsed
                        else:
                            parsed_dttm = pl.coalesce([parsed_dttm, temp_parsed])
                    except:
                        continue
                
                if parsed_dttm is not None:
                    df = df.with_columns(parsed_dttm.alias("sr_open_dttm"))

                # Also parse close date and open date (date only) IF they exist
                if "sr_close_dttm" in df.columns:
                     parsed_close = None
                     for fmt in formats:
                         try:
                             temp_p = df["sr_close_dttm"].str.strptime(pl.Datetime, format=fmt, strict=False)
                             parsed_close = temp_p if parsed_close is None else pl.coalesce([parsed_close, temp_p])
                         except: continue
                     df = df.with_columns(parsed_close.alias("sr_close_dttm"))
                
                if "sr_open_dt" in df.columns:
                     # Date only formats
                     for fmt in ["%d-%m-%Y", "%d-%m-%y", "%d-%b-%y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]:
                         try:
                             temp_d = df["sr_open_dt"].str.strptime(pl.Date, format=fmt, strict=False)
                             df = df.with_columns(temp_d.alias("sr_open_dt"))
                             if df["sr_open_dt"].null_count() < len(df):
                                 break
                         except: continue
                             
            except Exception as e:
                logger.warning(f"Date parsing logic encountered an error: {e}")
            
            # Fallback for sr_open_dt: extract from sr_open_dttm if it's now a datetime
            if "sr_open_dttm" in df.columns and df["sr_open_dttm"].dtype == pl.Datetime:
                 if "sr_open_dt" not in df.columns or df["sr_open_dt"].null_count() == len(df):
                    df = df.with_columns(pl.col("sr_open_dttm").dt.date().alias("sr_open_dt"))

            # Map 'status' -> 'sr_status' if needed
            if "status" in df.columns and "sr_status" not in df.columns:
                 df = df.rename({"status": "sr_status"})
            
            # Ensure proper types for insertions
            if "sr_duration" in df.columns:
                df = df.with_columns(pl.col("sr_duration").cast(pl.Utf8))

            # Drop rows where primary timestamps are missing
            rows_before_drop = len(df)
            
            # Store a sample raw value for diagnostics before dropping
            sample_raw_date = "N/A"
            if rows_before_drop > 0 and "raw_sr_open_dttm" in df.columns:
                sample_raw_date = str(df["raw_sr_open_dttm"][0])

            df = df.drop_nulls(subset=["sr_open_dttm"])
            rows_after_drop = len(df)
            
            if rows_after_drop == 0:
                  msg = f"No valid rows after date parsing (Sample raw date: '{sample_raw_date}'). Please check file date format."
                  logger.error(msg)
                  return {
                      "status": "error", 
                      "message": msg,
                      "diagnostics": {
                          "columns_found": df.columns,
                          "raw_sample": sample_raw_date,
                          "total_rows_read": rows_before_drop
                      }
                  }

            # 4. Upsert (Insert or Update on Duplicate Key)
            records = df.to_dicts()
            
            if not records:
                logger.warning("No records found to ingest.")
                return {"status": "success", "processed_rows": 0, "inserted_rows": 0}

            session = get_session()
            upserted_count = 0
            
            model_columns = ComplaintsRaw.__table__.columns.keys()
            
            # Filter records to only include columns present in the model
            clean_records = []
            for rec in records:
                if rec.get('sr_number'):
                    filtered_rec = {k: v for k, v in rec.items() if k in model_columns}
                    clean_records.append(filtered_rec)
            
            if clean_records:
                # Use MySQL-specific insert for ON DUPLICATE KEY UPDATE
                stmt = insert(ComplaintsRaw).values(clean_records)
                
                # Identify columns actually present in the input data (take from first record)
                input_cols = clean_records[0].keys()
                
                # Define columns to update on conflict (all provided columns except the primary key)
                update_dict = {
                    col: stmt.inserted[col]
                    for col in input_cols
                    if col != 'sr_number'
                }
                
                if update_dict:
                    upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
                else:
                    # If only sr_number is provided, just do a normal insert that ignores duplicates
                    upsert_stmt = stmt.prefix_with("IGNORE")
                
                result = session.execute(upsert_stmt)
                session.commit()
                # rowcount for upsert is: 1 for insert, 2 for update
                upserted_count = result.rowcount
                logger.info(f"Upsert operation completed. Rowcount affected: {upserted_count}")
            else:
                logger.info("No valid records found in batch.")
                
            session.close()

            return {
                "status": "success",
                "processed_rows": len(df),
                "upserted_rowcount": upserted_count,
                "diagnostics": {
                    "columns_found": df.columns,
                    "rows_read": len(df),
                    "rows_after_drop": len(df) if len(df) > 0 else 0,
                    "sample_row": df.head(1).to_dicts()[0] if len(df) > 0 else None
                }
            }

        except Exception as e:
            logger.exception("Ingestion failed")
            return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    # Test execution
    agent = IngestionAgent()
    # Dummy context for testing
    # agent.run({"file_path": "test_data.csv"})
