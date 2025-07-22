import glob
import re
import traceback
import duckdb
import fastbt
# print(dir(fastbt))

import pandas as pd
import numpy as np
import httpx
import time

import fastbt.urlpatterns as patterns
import logging
import os

import zipfile
"""
This script downloads historical data files from the NSE website.
It supports downloading files for multiple keys, such as 'bhav', 'sec_del', etc.
You can specify a date range and an output directory for the downloaded files.
It also handles downloading all keys or a specific key based on the DOWNLOAD_ALL_KEYS flag.
"""
# patterns.file_patterns.keys()


    
def download_and_save_file(url: str, filename: str) -> bool:
    """
    Download the file from the given url and save it in the given filename
    url
        valid url to download
    filename
    filename as str, can include entire path
    returns True if the file is downloaded and saved else returns False
    """
    try:
        client = httpx.Client()
        req = client.get(url, timeout=3)
        if req.status_code == 200:
            with open(filename, "wb") as f:
                f.write(req.content)
            return True
        else:
            return False
    except Exception as e:
        logging.error(e)
        return False

def retrieve_bhav_data(
    dictKeys: list = ['bhav','sec_del','bhav_sec'], 
    output_directory: str = "./data/",
    dates = pd.bdate_range(
        start='2016-01-01', 
        end=(pd.Timestamp.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')),
    sleep: float = 0.5
    ):
    """
    Main method to retrieve data based on the specified key and date range.
    It downloads files for the specified key or all keys if DOWNLOAD_ALL_KEYS is True.
    """
    for key in dictKeys:
        key_folder = os.path.join(output_directory, key)
        os.makedirs(key_folder, exist_ok=True)

        pat, func = patterns.file_patterns[key]
        missed_dates, skipped, downloaded = [], 0, 0

        try:
            for dt in dates:
                # --- catch URL‐formatting errors and bail out on this key ---
                try:
                    url = pat.format(**func(dt))
                except Exception:
                    print(f"\n[ERROR]: building URL for key '{key}' on date {dt}:")
                    traceback.print_exc()
                    # skip this entire key
                    raise

                name = url.split("/")[-1]
                filepath = os.path.join(key_folder, name)

                if os.path.exists(filepath):
                    logging.info(f"[{key}] File exists for {dt}")
                    skipped += 1
                    continue

                if download_and_save_file(url, filepath):
                    downloaded += 1
                    # --- attempt unzip only if it really is a zip ---
                    try:
                        with zipfile.ZipFile(filepath, 'r') as z:
                            z.extractall(key_folder)
                            print(f"[INFO]: [{key}] Extracted {name}")
                    except zipfile.BadZipFile:
                        # not a zip, leave the file as-is
                        print(f"[INFO]: [{key}] '{name}' is not a ZIP archive, saved without extraction.")
                else:
                    missed_dates.append(dt)

                time.sleep(sleep)

            # only prints if no exception was raised above
            print(f"[INFO]: [{key}] Total dates={len(dates)}, missed={len(missed_dates)}, "
                f"skipped={skipped}, downloaded={downloaded}")

        except Exception:
            # we already printed the traceback — move on to the next key
            continue
    
    print(f"[INFO]: Total number of dates = {len(dates)}")
    print(f"[INFO]: Number of missed dates = {len(missed_dates)}")
    print(f"[INFO]: Number of skipped dates = {skipped}")
    print(f"[INFO]: Number of downloaded dates = {downloaded}")

def create_oldBhav():
    # Go through data/bhav/cm*bhav.csv files. 
    old_files = glob.glob('data/bhav/cm*bhav.csv')
    df_list = []
    
    for fp in old_files:
        try:
            # Read CSV with keep_default_na=False to prevent 'NA' from being converted to NaN
            df = pd.read_csv(fp, keep_default_na=False)
            # print(f"[INFO]: Processing file: {fp}")
            df = df.drop(columns=[col for col in df.columns if col.startswith('Unnamed')], errors='ignore')
            
            # Handle timestamp format - it can be in format '01-APR-2016' or '01-APR-20' format
            if 'TIMESTAMP' in df.columns:
                try:
                    # Try 4-digit year first
                    df['DATE1'] = pd.to_datetime(df['TIMESTAMP'], format='%d-%b-%Y').dt.date
                except ValueError:
                    try:
                        # Try 2-digit year
                        df['DATE1'] = pd.to_datetime(df['TIMESTAMP'], format='%d-%b-%y').dt.date
                    except ValueError:
                        # Let pandas infer the format
                        df['DATE1'] = pd.to_datetime(df['TIMESTAMP'], infer_datetime_format=True).dt.date
            else:
                print(f"[WARNING]: TIMESTAMP column not found in {fp}")
                continue
            
            # Map columns to match the bhav_old table structure
            df_mapped = pd.DataFrame()
            df_mapped['SYMBOL'] = df['SYMBOL'].str.strip().fillna('')
            df_mapped['SERIES'] = df['SERIES'].str.strip().fillna('')
            df_mapped['DATE1'] = df['DATE1']
            df_mapped['PREV_CLOSE'] = df['PREVCLOSE']
            df_mapped['OPEN_PRICE'] = df['OPEN']
            df_mapped['HIGH_PRICE'] = df['HIGH']
            df_mapped['LOW_PRICE'] = df['LOW']
            df_mapped['LAST_PRICE'] = df['LAST']
            df_mapped['CLOSE_PRICE'] = df['CLOSE']
            df_mapped['AVG_PRICE'] = (df['TOTTRDVAL'] / df['TOTTRDQTY'])  # Calculate average price
            df_mapped['TTL_TRD_QNTY'] = df['TOTTRDQTY']
            df_mapped['TURNOVER_LACS'] = df['TOTTRDVAL'] / 100000.0  # Convert to lakhs
            df_mapped['NO_OF_TRADES'] = df['TOTALTRADES']
            # df_mapped['DELIV_QTY'] = 0  # Default to 0, will be updated from sec_del data
            # df_mapped['DELIV_PER'] = 0.0  # Default to 0, will be updated from sec_del data
            
            # Filter out rows with empty SYMBOL or SERIES
            df_mapped = df_mapped[(df_mapped['SYMBOL'] != '') & (df_mapped['SERIES'] != '')]
            df_list.append(df_mapped)
            
        except Exception as e:
            print(f"[ERROR]: processing file {fp}: {e}")
    
    # Combine all dataframes
    if df_list:
        bhav_old_df = pd.concat(df_list, ignore_index=True)
        bhav_old_df = bhav_old_df.drop_duplicates()
        
        print(f"[DEBUG]: bhav_old data created successfully. Shape: {bhav_old_df.shape}")
        print(f"[DEBUG]: Columns in bhav_old_df: {bhav_old_df.columns.tolist()}")
        # Print date range of bhav_old_df
        if not bhav_old_df.empty:
            date_range = bhav_old_df['DATE1'].min(), bhav_old_df['DATE1'].max()
            print(f"[INFO]: Date range of bhav_old data: {date_range[0]} to {date_range[1]}")
        
        return bhav_old_df

    else:
        print("[WARNING]: No bhav_old data files found.")
        return
    
def create_secDel():
    sec_frames = []
    for fp in glob.glob('data/sec_del/MTO_*.DAT'):
        df = pd.read_csv(fp, skiprows=4, header=None)
        df.columns = [
            'RECORD_TYPE','SR_NO','SYMBOL','SERIES',
            'QUANTITY_TRADED','DELIV_QTY','DELIV_PER'
        ]
        date_str = os.path.basename(fp)[4:12]  
        date_obj = pd.to_datetime(date_str, format='%d%m%Y').date()
        df['DATE1'] = date_obj

        df = df[['SYMBOL','SERIES','DELIV_QTY','DELIV_PER','DATE1']]
        # Fix formatting issues by trimming SYMBOL and SERIES columns
        df['SYMBOL'] = df['SYMBOL'].str.strip()
        df['SERIES'] = df['SERIES'].str.strip()
        # Handle NULL/empty SERIES values
        df['SERIES'] = df['SERIES'].fillna('').replace('', 'UNKNOWN')
        # Handle NULL values in other columns
        df['DELIV_QTY'] = df['DELIV_QTY'].fillna(0)
        df['DELIV_PER'] = df['DELIV_PER'].fillna(0.0)
        sec_frames.append(df)
    
    if sec_frames:
        sec_del_df = pd.concat(sec_frames, ignore_index=True)
        # Remove duplicates based on primary key columns
        sec_del_df = sec_del_df.drop_duplicates()
        print(f"[INFO]: SEC_DEL data created successfully. Shape: {sec_del_df.shape}")
        # Print date range of sec_del data
        if not sec_del_df.empty:
            date_range = sec_del_df['DATE1'].min(), sec_del_df['DATE1'].max()
            print(f"[INFO]: Date range of sec_del data: {date_range[0]} to {date_range[1]}")
        print(f"[DEBUG]: Columns in sec_del_df: {sec_del_df.columns.tolist()}")
        return sec_del_df
    else:
        print("[WARNING]: No sec_del data files found.")
        return

def merge_oldBhav_secDel():
    print("[INFO]: Merging old_bhav and sec_del data into eod_cash table...")
    bhav_old_df = create_oldBhav()
    sec_del_df = create_secDel()
    print(f"[DEBUG]: bhav_old_df shape: {bhav_old_df.shape}, sec_del_df shape: {sec_del_df.shape}")
    if bhav_old_df is None or sec_del_df is None:
        print("[WARNING]: Cannot merge data as one of the required datasets is empty.")
        return
    # merge both dataframes on SYMBOL, SERIES, and DATE1
    merged_df = pd.merge(
        bhav_old_df, 
        sec_del_df, 
        on=['SYMBOL', 'SERIES', 'DATE1'], 
        how='left', 
        # suffixes=('', '_sec')
    )
    print(f"[DEBUG]: Merged DataFrame shape: {merged_df.shape}")
    print(f"[DEBUG]: Columns in merged_df: {merged_df.columns.tolist()}")
    if not merged_df.empty:
        date_range = merged_df['DATE1'].min(), merged_df['DATE1'].max()
        print(f"[INFO]: Date range of merged data: {date_range[0]} to {date_range[1]}")
        return merged_df
    else:
        print("[WARNING]: Merged data is empty. No records found.")
        return 

def create_newBhav():
    print("[INFO]: Processing new_bhav csv data...")
    new_files = [f for f in os.listdir('data/bhav_sec/') if f.endswith('.csv')]
    main_df = []
    manual_rows = 0
    
    for file in new_files:
        df = pd.read_csv(f"data/bhav_sec/{file}")
        main_df.append(df)
        manual_rows += len(df)
    if not main_df:
        print("[INFO]: No new_bhav data files found.")
        return
        
    # Concatenate all dataframes
    df_new = pd.concat(main_df, ignore_index=True)
    print(f"[INFO]: Length of concatenated file: {len(df_new)} vs manual length: {manual_rows}")
    
    # Check for duplicates after concatenation
    total_duplicates = df_new.duplicated().sum()
    print(f"[INFO]: Total duplicate rows after concatenation: {total_duplicates}")
    if total_duplicates > 0:
        print(f"[WARNING]: Found {total_duplicates} duplicate rows in new_bhav data. These will be removed.")
    df_new = df_new.drop_duplicates()  
    print(f"[INFO]: Total rows after removing duplicates: {len(df_new)}")
    
    # Fix formatting issues by trimming columns
    df_new.columns = df_new.columns.str.strip()
    df_new['SYMBOL'] = df_new['SYMBOL'].str.strip().fillna('')
    df_new['SERIES'] = df_new['SERIES'].str.strip().fillna('')
    
    # Filter out rows with empty SYMBOL or SERIES
    before_filter = len(df_new)
    df_new = df_new[(df_new['SYMBOL'] != '') & (df_new['SERIES'] != '')]
    after_filter = len(df_new)
    print(f"[INFO]: Filtered out {before_filter - after_filter} rows with empty SYMBOL/SERIES")
    
    # Convert DATE1 to proper date format
    df_new['DATE1'] = pd.to_datetime(
        df_new['DATE1'].str.strip(),      
        format='%d-%b-%Y',                
        dayfirst=True                     
    ).dt.date
    
    # Convert numeric columns
    df_new['LAST_PRICE'] = pd.to_numeric(df_new['LAST_PRICE'], errors='coerce')
    df_new['DELIV_QTY'] = pd.to_numeric(df_new['DELIV_QTY'], downcast='integer', errors='coerce')
    df_new['DELIV_PER'] = pd.to_numeric(df_new['DELIV_PER'], errors='coerce')
    df_new['PREV_CLOSE'] = pd.to_numeric(df_new['PREV_CLOSE'], errors='coerce')
    df_new['OPEN_PRICE'] = pd.to_numeric(df_new['OPEN_PRICE'], errors='coerce')
    df_new['HIGH_PRICE'] = pd.to_numeric(df_new['HIGH_PRICE'], errors='coerce')
    df_new['LOW_PRICE'] = pd.to_numeric(df_new['LOW_PRICE'], errors='coerce')
    df_new['CLOSE_PRICE'] = pd.to_numeric(df_new['CLOSE_PRICE'], errors='coerce')
    df_new['AVG_PRICE'] = pd.to_numeric(df_new['AVG_PRICE'], errors='coerce')
    df_new['TTL_TRD_QNTY'] = pd.to_numeric(df_new['TTL_TRD_QNTY'], downcast='integer', errors='coerce')
    df_new['TURNOVER_LACS'] = pd.to_numeric(df_new['TURNOVER_LACS'], errors='coerce')
    df_new['NO_OF_TRADES'] = pd.to_numeric(df_new['NO_OF_TRADES'], downcast='integer', errors='coerce')
    
    if not df_new.empty:
        date_range = df_new['DATE1'].min(), df_new['DATE1'].max()
        print(f"[INFO]: Date range of bhav new data: {date_range[0]} to {date_range[1]}")
        return df_new
    else:
        print("[WARNING]: Merged data is empty. No records found.")
        return 


def create_finalDB(con):
    # Create final comprehensive dataset
    # Priority: new_bhav data first, then eod_cash data for missing records
    print("[INFO]: Creating final comprehensive dataset...")

    con.execute("""
    CREATE TABLE IF NOT EXISTS bhav_complete_data (
        SYMBOL         VARCHAR,
        SERIES         VARCHAR,
        DATE1          DATE,
        PREV_CLOSE     DOUBLE,
        OPEN_PRICE     DOUBLE,
        HIGH_PRICE     DOUBLE,
        LOW_PRICE      DOUBLE,
        LAST_PRICE     DOUBLE,
        CLOSE_PRICE    DOUBLE,
        AVG_PRICE      DOUBLE,
        TTL_TRD_QNTY  BIGINT,
        TURNOVER_LACS  DOUBLE,
        NO_OF_TRADES   BIGINT,
        DELIV_QTY     BIGINT,
        DELIV_PER     DOUBLE,
        PRIMARY KEY (SYMBOL, SERIES, DATE1)
    );
    """)
    
    merged_df = merge_oldBhav_secDel()
    if merged_df is None:
        print("[WARNING]: No data to merge from old_bhav and sec_del. Skipping this step.")
        merged_df = pd.DataFrame()
    new_bhav_df = create_newBhav()
    if new_bhav_df is None:
        print("[WARNING]: No new_bhav data found. Skipping this step.")
        new_bhav_df = pd.DataFrame()
    # Insert new_bhav data first
    combined_df = pd.concat([new_bhav_df, merged_df], ignore_index=True)
    final_df = combined_df.drop_duplicates(
        subset=['SYMBOL', 'SERIES', 'DATE1'],
        keep='first'
    ).reset_index(drop=True)
    final_df = final_df.sort_values(by=['DATE1', 'SYMBOL']).reset_index(drop=True)
    print(f"[DEBUG]: Columns in final_df ({len(final_df.columns)}): {list(final_df.columns)}")

    con.register('tmp_bhav_complete_data', final_df)
    # insert into bhav_complete_data table from tmp_bhav_complete_data
    con.execute("""
    INSERT INTO bhav_complete_data
    SELECT * FROM tmp_bhav_complete_data
    -- WHERE (SYMBOL, SERIES, DATE1) NOT IN (
    --     SELECT SYMBOL, SERIES, DATE1 FROM bhav_complete_data
    -- );
    """)
    print("[INFO]: Final comprehensive dataset created successfully.")
    # Overall statistics
    total_stats = con.execute("""
    SELECT 
        COUNT(*) as total_records,
        MIN(DATE1) as earliest_date,
        MAX(DATE1) as latest_date,
        COUNT(DISTINCT SYMBOL) as unique_symbols,
        COUNT(DISTINCT SERIES) as unique_series
    FROM bhav_complete_data;
    """).fetchone()

    print("OVERALL DATASET:")
    print(f"  Total Records: {total_stats[0]:,}")
    print(f"  Date Range: {total_stats[1]} to {total_stats[2]}")
    print(f"  Unique Symbols: {total_stats[3]:,}")
    print(f"  Unique Series: {total_stats[4]:,}")

    # Export to CSV
    print("\n[INFO]: Exporting final dataset to CSV...")
    final_df.to_csv('data/bhav_complete_data.csv', index=False)
    print(f"[INFO]: Final dataset exported to: data/bhav_complete_data.csv")
    print(f"[INFO]: File size: {len(final_df):,} records")

    
if __name__ == "__main__":
    start_time = time.time()
    logging.basicConfig(level=logging.INFO)
    
    # Initialize database connection (still needed for some fallback operations)
    con = duckdb.connect(database='data/eod.duckdb', read_only=False)
    
    # retrieve_bhav_data()
    create_finalDB(con)
    
    print("[INFO]: Data retrieval completed.")
    con.close()
    end_time = time.time()
    print(f"[INFO]: Total time taken: {end_time - start_time:.2f} seconds")
    print("[INFO]: All operations completed successfully.")
    
 