import glob
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


class DataRetriever:
    """
    DataRetriever class to download historical data files from NSE website.
    It supports downloading files for multiple keys, such as 'bhav', 'sec_del',
    etc. You can specify a date range and an output directory for the downloaded files.
    It also handles downloading all keys or a specific key based on the DOWNLOAD_ALL_KEYS flag.
    """
    def __init__(self, fromDate, toDate, con):
        ## Parameters
        self.dictKeys = ['sec_del', 'bhav', 'bhav_sec'] #list(patterns.file_patterns.keys()) #equity_info, hist_data, ipo_eq, nse_oi, trade_info
        self.key: str = "bhav_sec"  # Should be one of the available keys
        self.from_date: str = fromDate #"2024-01-01"
        self.to_date: str = toDate #"2025-01-01"
        self.output_directory: str = "./data/"
        self.sleep: float = 0.5
        self.DOWNLOAD_ALL_KEYS = True  # Set to True if you want to download all keys from dictKeys
        self.client = httpx.Client()
        self.con = con
        self.calculate_from_date()  # Calculate from_date based on existing data in the database
        self.dates = pd.bdate_range(start=self.from_date, end=self.to_date)
        if len(self.dates) == 0:
            raise ValueError("[ERROR]: No business dates found in the specified range.")
        print(f"[INFO]: Dates to download: {self.dates}")
    def calculate_from_date(self):
        # Check the database for the latest date, if the table exists
        try:
            result = self.con.execute("SELECT MAX(DATE1) FROM bhav_complete_data").fetchone()
            if result and result[0]:
                self.from_date = str(result[0] + pd.Timedelta(days=1))
        except Exception as e:
            logging.error(f"Error checking latest date in database: {e}")
        print(f"[INFO]: Using from_date: {self.from_date} for downloading data.")

    def download_and_save_file(self, url: str, filename: str) -> bool:
        """
        Download the file from the given url and save it in the given filename
        url
            valid url to download
        filename
        filename as str, can include entire path
        returns True if the file is downloaded and saved else returns False
        """
        try:
            req = self.client.get(url, timeout=3)
            if req.status_code == 200:
                with open(filename, "wb") as f:
                    f.write(req.content)
                return True
            else:
                return False
        except Exception as e:
            logging.error(e)
            return False

    def retrieve_bhav_data(self):
        """
        Main method to retrieve data based on the specified key and date range.
        It downloads files for the specified key or all keys if DOWNLOAD_ALL_KEYS is True.
        """
        # Looped download for all keys
        
        # If fromDate is after 4th july 2024, ignore the 'bhav_sec' and 'bhav' key
        if self.from_date > "2024-07-04":
            self.dictKeys = [key for key in self.dictKeys if key not in ['sec_del', 'bhav']]
            print(f"[INFO]: Skipping 'sec_del' and 'bhav' keys as fromDate is after 4th July 2024. Remaining keys: {self.dictKeys}")
        
        if self.DOWNLOAD_ALL_KEYS:
            for key in self.dictKeys:
                key_folder = os.path.join(self.output_directory, key)
                os.makedirs(key_folder, exist_ok=True)

                pat, func = patterns.file_patterns[key]
                missed_dates, skipped, downloaded = [], 0, 0

                try:
                    for dt in self.dates:
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

                        if self.download_and_save_file(url, filepath):
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

                        time.sleep(self.sleep)

                    # only prints if no exception was raised above
                    print(f"[INFO]: [{key}] Total dates={len(self.dates)}, missed={len(missed_dates)}, "
                        f"skipped={skipped}, downloaded={downloaded}")

                except Exception:
                    # we already printed the traceback — move on to the next key
                    continue
        else:
            pat, func = patterns.file_patterns[key]
            missed_dates = []
            skipped = 0
            downloaded = 0
            for dt in self.dates:
                url = pat.format(**func(dt))
                # print(url)
                name = url.split("/")[-1]
                filename = os.path.join(self.output_directory, name)
                if os.path.exists(filename):
                    logging.info(f"File already exists for date {dt}")
                    skipped += 1
                else:
                    status = self.download_and_save_file(url, filename)
                    time.sleep(self.sleep)
                    if status:
                        downloaded += 1
                    else:
                        missed_dates.append(dt)
        print(f"[INFO]: Total number of dates = {len(self.dates)}")
        print(f"[INFO]: Number of missed dates = {len(missed_dates)}")
        print(f"[INFO]: Number of skipped dates = {skipped}")
        print(f"[INFO]: Number of downloaded dates = {downloaded}")

    def create_oldBhav(self):
        # Create or replace the bhav_old table
        # This table will hold the old bhav data from cm*bhav.csv files
        if self.from_date > "2024-07-04":
            print("[INFO]: Skipping old_bhav data as fromDate is after 4th July 2024.")
            return
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS bhav_old (
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

        # Go through data/bhav/cm*bhav.csv files. Take each csv, and insert into bhav_old table

        old_files = glob.glob('data/bhav/cm*bhav.csv')
        for fp in old_files:
            try:
                # Read CSV with keep_default_na=False to prevent 'NA' from being converted to NaN
                df = pd.read_csv(fp, keep_default_na=False)
                print(f"[INFO]: Processing file: {fp}")
                # print(f"Columns: {df.columns.tolist()}")
                
                # Drop unnamed columns
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
                df_mapped['AVG_PRICE'] = (df['TOTTRDVAL'] / df['TOTTRDQTY']) #.round(2)  # Calculate average price and round to 2 decimals - shows some error still so handled in sql
                df_mapped['TTL_TRD_QNTY'] = df['TOTTRDQTY']
                df_mapped['TURNOVER_LACS'] = df['TOTTRDVAL'] / 100000.0  # Convert to lakhs
                df_mapped['NO_OF_TRADES'] = df['TOTALTRADES']
                df_mapped['DELIV_QTY'] = 0  # Default to 0, will be updated from sec_del data
                df_mapped['DELIV_PER'] = 0.0  # Default to 0, will be updated from sec_del data
                
                # Filter out rows with empty SYMBOL or SERIES
                df_mapped = df_mapped[(df_mapped['SYMBOL'] != '') & (df_mapped['SERIES'] != '')]
                
                self.con.register('tmp_bhav_old', df_mapped)
                self.con.execute("""
                INSERT OR IGNORE INTO bhav_old
                SELECT * FROM tmp_bhav_old;
                """)
            except Exception as e:
                print(f"[ERROR]: processing file {fp}: {e}")
        print("[INFO]: bhav_old data created successfully.")
        # # Save the bhav_old table to a CSV file
        # bhav_old_df = self.con.execute("SELECT * FROM bhav_old").df()
        # bhav_old_df.to_csv('data/bhav_old.csv', index=False)
        
    def create_secDel(self):
        if self.from_date > "2024-07-04":
            print("[INFO]: Skipping sec_del data as fromDate is after 4th July 2024.")
            return
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
        df_sec = pd.concat(sec_frames, ignore_index=True)
        print("[INFO]: SEC_DEL data shape:", df_sec.shape)
        self.con.register('tmp_sec_del', df_sec)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS sec_del (
          SYMBOL         VARCHAR,
          SERIES         VARCHAR,
          DELIV_QTY      BIGINT,
          DELIV_PER      DOUBLE,
          DATE1          DATE,
          PRIMARY KEY (SYMBOL, SERIES, DATE1)
        );
        """)
        self.con.execute("""
        INSERT OR IGNORE INTO sec_del
        SELECT * FROM tmp_sec_del;
        """)
        print("[INFO]: SEC_DEL data created successfully.")
        
    def merge_oldBhav_secDel(self):
        if self.from_date > "2024-07-04":
            print("[INFO]: Skipping merge of old_bhav and sec_del data as fromDate is after 4th July 2024.")
            return
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS eod_cash (
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

        # 2) Insert merged bhav_old + sec_del (using cleaned data)
        self.con.execute(r"""
        INSERT OR IGNORE INTO eod_cash
        SELECT
          o.SYMBOL                                       AS SYMBOL,
          o.SERIES                                       AS SERIES,
          o.DATE1                                        AS DATE1,
          o.PREV_CLOSE                                   AS PREV_CLOSE,
          o.OPEN_PRICE                                   AS OPEN_PRICE,
          o.HIGH_PRICE                                   AS HIGH_PRICE,
          o.LOW_PRICE                                    AS LOW_PRICE,
          o.LAST_PRICE                                   AS LAST_PRICE,
          o.CLOSE_PRICE                                  AS CLOSE_PRICE,
          o.AVG_PRICE                                    AS AVG_PRICE,
          o.TTL_TRD_QNTY                                 AS TTL_TRD_QNTY,
          o.TURNOVER_LACS                                AS TURNOVER_LACS,
          o.NO_OF_TRADES                                 AS NO_OF_TRADES,
          COALESCE(s.DELIV_QTY, 0)                       AS DELIV_QTY,
          COALESCE(s.DELIV_PER, 0.0)                     AS DELIV_PER
        FROM bhav_old o
        LEFT JOIN sec_del s
          ON o.SYMBOL = s.SYMBOL
        AND o.SERIES = s.SERIES
        AND o.DATE1 = s.DATE1;
        """)
        
        # download eod_cash
        # eod_cash = self.con.execute("SELECT * FROM eod_cash").fetchall()
        # eod_cash_df = pd.DataFrame(eod_cash, columns=[
        #     'SYMBOL', 'SERIES', 'DATE1', 'PREV_CLOSE',
        #     'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE',
        #     'LAST_PRICE', 'CLOSE_PRICE', 'AVG_PRICE',
        #     'TTL_TRD_QNTY', 'TURNOVER_LACS', 'NO_OF_TRADES',
        #     'DELIV_QTY', 'DELIV_PER'
        # ])
        # eod_cash_df.to_csv('data/eod_cash.csv', index=False)
        
        print("[INFO]: Merged old_bhav and sec_del data into eod_cash table successfully.")
        
    def create_newBhav(self):
        print("[INFO]: Processing new_bhav csv data...")
        new_files = [fp for fp in glob.glob('data/bhav_sec/sec_bhavdata_full_*.csv') if fp.lower().endswith('.csv')]
        df_list = []
        for fp in new_files:
            # print(fp)
            df = pd.read_csv(fp, )
            df_list.append(df)
        df_new = pd.concat(df_list, ignore_index=True)
        df_new.columns = df_new.columns.str.strip()
        # Fix formatting issues by trimming SYMBOL and SERIES columns
        df_new['SYMBOL'] = df_new['SYMBOL'].str.strip().fillna('')
        df_new['SERIES'] = df_new['SERIES'].str.strip().fillna('')
        # Filter out rows with empty SYMBOL or SERIES
        df_new = df_new[(df_new['SYMBOL'] != '') & (df_new['SERIES'] != '')]
        df_new['DATE1'] = pd.to_datetime(
            df_new['DATE1'].str.strip(),      
            format='%d-%b-%Y',                
            dayfirst=True                     
        ).dt.date
        df_new['LAST_PRICE'] = pd.to_numeric(df_new['LAST_PRICE'], errors='coerce')
        df_new['DELIV_QTY']  = pd.to_numeric(df_new['DELIV_QTY'], downcast='integer', errors='coerce')
        df_new['DELIV_PER']  = pd.to_numeric(df_new['DELIV_PER'], errors='coerce')
        # print("New BHAV data shape:", df_new.shape)
        self.con.register('tmp_new_bhav', df_new)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS new_bhav (
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
        self.con.execute("""
        INSERT OR IGNORE INTO new_bhav
        SELECT * FROM tmp_new_bhav;
        """)
        print("[INFO]: new_bhav data table created successfully.")
    
    def create_finalDB(self):
        # Create final comprehensive dataset
        # Priority: new_bhav data first, then eod_cash data for missing records
        print("[INFO]: Creating final comprehensive dataset...")

        self.con.execute("""
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
          DATA_SOURCE    VARCHAR,  -- Track which source the data came from
          PRIMARY KEY (SYMBOL, SERIES, DATE1)
        );
        """)

        # First, insert all new_bhav data (prioritized)
        self.con.execute("""
        INSERT OR IGNORE INTO bhav_complete_data
        SELECT
          SYMBOL,
          SERIES,
          DATE1,
          PREV_CLOSE,
          OPEN_PRICE,
          HIGH_PRICE,
          LOW_PRICE,
          LAST_PRICE,
          CLOSE_PRICE,
          AVG_PRICE,
          TTL_TRD_QNTY,
          TURNOVER_LACS,
          NO_OF_TRADES,
          DELIV_QTY,
          DELIV_PER,
          'new_bhav' as DATA_SOURCE
        FROM new_bhav;
        """)

        # Then, insert eod_cash data for records NOT already in new_bhav
        if self.from_date <= "2024-07-04":
            self.con.execute("""
            INSERT OR IGNORE INTO bhav_complete_data
            SELECT
            e.SYMBOL,
            e.SERIES,
            e.DATE1,
            e.PREV_CLOSE,
            e.OPEN_PRICE,
            e.HIGH_PRICE,
            e.LOW_PRICE,
            e.LAST_PRICE,
            e.CLOSE_PRICE,
            e.AVG_PRICE,
            e.TTL_TRD_QNTY,
            e.TURNOVER_LACS,
            e.NO_OF_TRADES,
            e.DELIV_QTY,
            e.DELIV_PER,
            'eod_cash' as DATA_SOURCE
            FROM eod_cash e
            WHERE NOT EXISTS (
            SELECT 1 FROM new_bhav n
            WHERE n.SYMBOL = e.SYMBOL
                AND n.SERIES = e.SERIES
                AND n.DATE1 = e.DATE1
            );
            """)

        print("[INFO]: Final comprehensive dataset created successfully.")
        # Get statistics about the final dataset
        final_stats = self.con.execute("""
        SELECT 
          DATA_SOURCE,
          COUNT(*) as record_count,
          MIN(DATE1) as earliest_date,
          MAX(DATE1) as latest_date,
          COUNT(DISTINCT SYMBOL) as unique_symbols
        FROM bhav_complete_data
        GROUP BY DATA_SOURCE
        ORDER BY DATA_SOURCE;
        """).fetchall()

        print("\nFinal Comprehensive Dataset Statistics:")
        print("=" * 50)
        for stat in final_stats:
            source, count, min_date, max_date, symbols = stat
            print(f"{source.upper()}:")
            print(f"  Records: {count:,}")
            print(f"  Date Range: {min_date} to {max_date}")
            print(f"  Unique Symbols: {symbols:,}")
            print()

        # Overall statistics
        total_stats = self.con.execute("""
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
        final_data = self.con.execute("SELECT * FROM bhav_complete_data ORDER BY DATE1, SYMBOL").df()
        final_data.to_csv('data/bhav_complete_data.csv', index=False)
        print(f"[INFO]: Final dataset exported to: data/bhav_complete_data.csv")
        print(f"[INFO]: File size: {len(final_data):,} records")

    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    con = duckdb.connect(database='data/eod.duckdb', read_only=False)
    fromDate = '2016-01-01'
    toDate = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d') #'2025-07-01'
    start_time = time.time()
    retriever = DataRetriever(fromDate, toDate, con)
    retriever.retrieve_bhav_data()
    retriever.create_oldBhav()
    retriever.create_secDel()
    retriever.merge_oldBhav_secDel()
    retriever.create_newBhav()
    retriever.create_finalDB()
    print("[INFO]: Data retrieval completed.")
    con.close()
    end_time = time.time()
    print(f"[INFO]: Total time taken: {end_time - start_time:.2f} seconds")
    print("[INFO]: All operations completed successfully.")
    