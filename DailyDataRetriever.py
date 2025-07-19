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
    dictKeys: list = ['bhav_sec'], 
    output_directory: str = "./data/",
    dates = pd.bdate_range(
        start='2024-01-01', 
        end=(pd.Timestamp.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    ),
    sleep: float = 0.5,
    con: duckdb.DuckDBPyConnection = None
    ):
    """
    Main method to retrieve data based on the specified key and date range.
    It downloads files for the specified key or all keys if DOWNLOAD_ALL_KEYS is True.
    """
    from_date = None
    try:
        print("[INFO]: Checking latest date in existing data...")
        result = con.execute("SELECT MAX(DATE1) FROM bhav_complete_data").fetchone()
        if result and result[0]:
            from_date = str(result[0] + pd.Timedelta(days=1))
            print(f"[INFO]: Latest date in existing data: {from_date}")
    except Exception as e:
        print(f"[WARNING]: Error checking latest date in existing data: {e}. Using original from_date.")
            
    if from_date:
        dates = pd.bdate_range(
            start=from_date, 
            end=(pd.Timestamp.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        )
    
    for key in dictKeys:
        key_folder = os.path.join(output_directory, key)
        os.makedirs(key_folder, exist_ok=True)

        pat, func = patterns.file_patterns[key]
        missed_dates, skipped, downloaded = [], 0, 0
        names = []
        
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
                    names.append(name)
                    continue

                if download_and_save_file(url, filepath):
                    downloaded += 1
                    names.append(name)
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
    
    return names

def create_newBhav(con, names : list = None):
    # print current bhav shape and date range
    print("\n[INFO]: Checking existing bhav_complete_data table...")
    result = con.execute("SELECT COUNT(*), COUNT(DISTINCT SYMBOL), COUNT(DISTINCT SERIES) FROM bhav_complete_data").fetchone()
    print(f"[INFO]: bhav_complete_data table shape: {result[0]} rows, {result[1]} unique SYMBOLs, {result[2]} unique SERIESs.")
    date_range = con.execute("SELECT MIN(DATE1), MAX(DATE1) FROM bhav_complete_data").fetchone()
    print(f"[INFO]: Date range of bhav_complete_data: {date_range[0]} to {date_range[1]}")
    if names is None or len(names) == 0:
        print("[INFO]: No bhav_complete_data files found. Skipping processing.")
        return
    print("[INFO]: Processing new_bhav csv data...")
    new_files = names
    main_df = []
    manual_rows = 0
    print(f"[INFO]: New files to process: {len(new_files)}")
    print(f"[INFO]: New files: {new_files}")
    for file in new_files:
        print(f"[INFO]: Processing file: {file}")
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
        print(f"[INFO]: Date range of extracted bhav new data: {date_range[0]} to {date_range[1]}")
        # insert df_new into the database
        con.register('bhav_new_data', df_new)
        # insert into existing bhav_complete_data table
        con.execute("""
                    insert into bhav_complete_data
                    select * from bhav_new_data
                    where (SYMBOL, SERIES, DATE1) not in (select SYMBOL, SERIES, DATE1 from bhav_complete_data)
        """)
        # print new bhav shape and date range
        print("\n[INFO]: Checking updated bhav_complete_data table...")
        result = con.execute("SELECT COUNT(*), COUNT(DISTINCT SYMBOL), COUNT(DISTINCT SERIES) FROM bhav_complete_data").fetchone()
        print(f"[INFO]: Updated bhav_complete_data table shape: {result[0]} rows, {result[1]} unique SYMBOLs, {result[2]} unique SERIESs.")
        date_range = con.execute("SELECT MIN(DATE1), MAX(DATE1) FROM bhav_complete_data").fetchone()
        print(f"[INFO]: Date range of updated bhav_complete_data: {date_range[0]} to {date_range[1]}")
        
        return df_new
    else:
        print("[WARNING]: Merged data is empty. No records found.")
        return 

    
if __name__ == "__main__":
    start_time = time.time()
    logging.basicConfig(level=logging.INFO)
    
    # Initialize database connection (still needed for some fallback operations)
    con = duckdb.connect(database='data/eod.duckdb', read_only=False)
    
    files = retrieve_bhav_data(con=con)
    create_newBhav(con, files)

    print("[INFO]: Data retrieval completed.")
    con.close()
    end_time = time.time()
    print(f"[INFO]: Total time taken: {end_time - start_time:.2f} seconds")
    print("[INFO]: All operations completed successfully.")
    
 