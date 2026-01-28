
import polars as pl
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from complaints_ai.agents.ingestion_agent import IngestionAgent

def debug_file(file_path):
    print(f"--- Checking File: {file_path} ---")
    if not os.path.exists(file_path):
        print("File does not exist.")
        return

    agent = IngestionAgent()
    context = {"file_path": file_path}
    
    # We won't run the full ingestion to avoid DB changes, 
    # but we will check the dataframe processing part.
    # I'll manually run the parsing logic from IngestionAgent.run
    
    encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252', 'utf-16']
    df = pl.DataFrame()
    read_success = False
    last_enc = 'utf-8'
    
    for enc in encodings:
        try:
            print(f"Attempting Polars read with encoding: {enc}")
            df = pl.read_csv(file_path, encoding=enc, ignore_errors=True, n_rows=100)
            if not df.is_empty():
                read_success = True
                last_enc = enc
                print(f"Success with {enc}")
                break
        except Exception as e:
            print(f"Failed {enc}: {e}")

    if not read_success:
        print("Could not read file with any encoding.")
        return

    print(f"Columns found: {df.columns}")
    
    # Normalize Headers
    df.columns = [str(col).lower().strip().replace(' ', '_') for col in df.columns]
    
    # Synonym Mapping check
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
                    print(f"Mapping synonym '{match}' to '{target}'")
                    df = df.rename({match: target})
                    break

    # Required Columns check
    required = ["sr_number", "sr_open_dttm", "sr_type", "region", "exc_id"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        print(f"MISSING REQUIRED COLUMNS: {missing}")
    else:
        print("All required columns mapped successfully.")

    # Date parsing check
    if "sr_open_dttm" in df.columns:
        raw_sample = df["sr_open_dttm"][0]
        print(f"Raw Date Sample: {raw_sample}")
        
        formats = [
            "%d-%b-%y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%d-%m-%y %H:%M:%S",
            "%d-%m-%y %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%m/%d/%y %H:%M:%S",
            "%d/%m/%y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%d-%b-%Y %H:%M:%S",
            "%m/%d/%Y %I:%M:%S %p",
            "%d/%m/%Y %I:%M:%S %p",
            "%Y-%m-%d %I:%M:%S %p"
        ]
        
        parsed = None
        for fmt in formats:
            try:
                temp_parsed = df["sr_open_dttm"].str.strptime(pl.Datetime, format=fmt, strict=False)
                count = temp_parsed.null_count()
                if count < len(temp_parsed):
                    print(f"Date Format Success: {fmt} (Null count: {count}/{len(df)})")
                    parsed = temp_parsed
                    break
            except:
                continue
        
        if parsed is None:
            print("FAILED TO PARSE DATE with any format.")
        else:
            print(f"Sample Parsed Date: {parsed[0]}")

if __name__ == "__main__":
    target_files = [
        r"C:\Users\PTCL\Downloads\Complaints data.csv",
        r"C:\Users\PTCL\Downloads\Complaints data Jan-26.csv",
        r"C:\Users\PTCL\Downloads\Complaints data Dec-25.csv"
    ]
    for f in target_files:
        debug_file(f)
