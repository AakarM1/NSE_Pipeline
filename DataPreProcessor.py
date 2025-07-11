import traceback
import duckdb
import pandas as pd
import glob
import os
import re
from datetime import datetime
import requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import calendar
"""
This file is to preprocess the entire data
Required:
    data/bhav/cm*bhav.csv files
    data/sec_del/MTO_*.DAT files
    data/bhav_sec/sec_bhavdata_full_*.csv files
    data/bonus_*.csv files
    data/dividends_*.csv files
    data/splits_*.csv files
Actions:
    Saves bhav_old to a db table
    Saves sec_del to a db table
    Saves bhav_sec to a db table
    Saves corporate actions to a db table
    Creates adjusted prices table
"""

class DataPreProcessor:
    def __init__(self, startDate=None, endDate=None, tickerDict=None, con=None):
        # Ensure the data directory exists
        os.makedirs('data', exist_ok=True)
        # Connect (or create) your DuckDB database under data/
        self.con = con if con else duckdb.connect(database='data/eod.duckdb', read_only=False)
        self.startDate = startDate
        self.endDate = endDate
        self.tickerDict = tickerDict


    def fetch_corporate_actions(self) -> pd.DataFrame:
        df = pd.read_csv('data/CF-CA-equities.csv')
        # print(df)
        return df
        start_date = self.startDate
        end_date = self.endDate

        try:
            # 1. Parse input strings into date objects
            final_start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            final_end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        except ValueError:
            print("Error: Invalid date format. Please use 'YYYY-MM-DD'.")
            return pd.DataFrame()

        print(f"Fetching corporate actions from {final_start_date} to {final_end_date}.")

        # 2. Prepare for fetching (session and headers)
        base_url = "https://www.nseindia.com/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        session = requests.Session()
        try:
            # Perform a single "warm-up" request to get necessary cookies for the session
            session.get(base_url, headers=headers, timeout=10)
        except requests.exceptions.RequestException as e:
            print(f"Error establishing session with NSE: {e}")
            return pd.DataFrame()

        # 3. Loop through the date range month by month
        all_data = []
        current_start_date = final_start_date

        while current_start_date <= final_end_date:
            # Determine the end date for the current monthly chunk
            month_end_day = calendar.monthrange(current_start_date.year, current_start_date.month)[1]
            month_end_date = date(current_start_date.year, current_start_date.month, month_end_day)
            
            # The end date for this chunk is the earlier of the month-end or the final end date
            current_end_date = min(month_end_date, final_end_date)
            
            # Format dates for the API URL (DD-MM-YYYY)
            start_str = current_start_date.strftime('%d-%m-%Y')
            end_str = current_end_date.strftime('%d-%m-%Y')

            print(f"  > Fetching chunk: {start_str} to {end_str}")
            api_url = f"https://www.nseindia.com/api/corporate-actions?index=equities&from_date={start_str}&to_date={end_str}"
            
            try:
                response = session.get(api_url, headers=headers, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                if 'data' in data and data['data']:
                    all_data.extend(data['data'])

            except requests.exceptions.RequestException as e:
                print(f"[ERROR]: An error occurred while fetching chunk {start_str} to {end_str}: {e}")
            
            # Move to the first day of the next month
            current_start_date = month_end_date + relativedelta(days=1)
        
        if not all_data:
            print("[WARNING]: No automated corporate actions extracted. Attempting to read from local CSV file.")
            try:
                return pd.read_csv('data/CF-CA-equities.csv')
            except Exception as e:
                print(f"[ERROR]: An error occurred while reading CSV files: {e}")
                return pd.DataFrame()

        # 4. Consolidate all fetched data into a single DataFrame
        df = pd.DataFrame(all_data)
        # Remove duplicates that might arise from overlapping day fetches (unlikely but safe)
        df = df.drop_duplicates(subset=['symbol', 'series', 'exDate', 'purpose'])
        print(f"Successfully fetched a total of {len(df)} unique corporate action records.")
        return df

    def preprocess_ca(self,):
        df = pd.DataFrame()
        startDate = self.startDate
        endDate = self.endDate
        if startDate and endDate:
            print(f"--- Fetching Live Corporate Actions from {startDate} to {endDate} ---")
            raw_df = self.fetch_corporate_actions()
            # print("THE DATAFRAME IS",raw_df.head())
            if raw_df.empty:
                print("[INFO]: Aborting corporate action processing as no data was fetched.")
                return

            # Map the columns from the API response to the names your code expects
            df['SYMBOL'] = raw_df['SYMBOL']
            df['EX-DATE'] = raw_df['EX-DATE']
            df['PURPOSE'] = raw_df['PURPOSE']
            df['SERIES'] = raw_df['SERIES']

        else:
            print("--- Reading Corporate Actions from Local CSV File ---")
            ca_files = glob.glob('data/CF-CA-equities-*.csv')
            if not ca_files:
                print("[WARNING]: No corporate action file found and no date specified. Skipping CA processing.")
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS corporate_actions (
                  symbol VARCHAR, ex_date DATE, action_type VARCHAR, dividend_amount DOUBLE,
                  bonus_ratio_from DOUBLE, bonus_ratio_to DOUBLE, split_ratio_from DOUBLE,
                  split_ratio_to DOUBLE, PRIMARY KEY (symbol, ex_date, action_type)
                );
                """)
                return
            df = pd.read_csv(ca_files[0])
            
        # Print column names to debug
        print("[INFO]: Available columns:", df.columns.tolist())

        # Parse the ex-date column
        df['ex_date'] = pd.to_datetime(df['EX-DATE'], format='%d-%b-%Y', errors='coerce').dt.date
        
        # Extract action type from PURPOSE column
        df['action_type'] = df['PURPOSE'].str.strip().str.lower()
        
        # Clean symbol column
        df['symbol'] = df['SYMBOL'].str.strip().str.upper()
        
        # Initialize ratio and amount columns with default values
        df['bonus_ratio_from'] = 0.0
        df['bonus_ratio_to'] = 0.0
        df['split_ratio_from'] = 1.0
        df['split_ratio_to'] = 1.0
        df['dividend_amount'] = 0.0
        
        # Parse PURPOSE column to extract specific action details using REGEX
        import re
        for idx, row in df.iterrows():
            purpose = row['PURPOSE'].lower()
            if 'dividend' in purpose:
                df.at[idx, 'action_type'] = 'dividend'
                # Extract dividend amount 
                match = re.search(r'rs?\s*(\d+(?:\.\d+)?)', purpose)
                if match:
                    df.at[idx, 'dividend_amount'] = float(match.group(1))
            elif 'bonus' in purpose:
                df.at[idx, 'action_type'] = 'bonus'
                # Extract bonus ratio like "1:1" or "1:2"
                match = re.search(r'(\d+):(\d+)', purpose)
                if match:
                    df.at[idx, 'bonus_ratio_from'] = float(match.group(1))
                    df.at[idx, 'bonus_ratio_to'] = float(match.group(2))
            elif 'split' in purpose:
                df.at[idx, 'action_type'] = 'split'
                match = re.search(r'(\d+):(\d+)', purpose)
                if match:
                    df.at[idx, 'split_ratio_from'] = float(match.group(1))
                    df.at[idx, 'split_ratio_to'] = float(match.group(2))
            elif 'rights' in purpose:
                df.at[idx, 'action_type'] = 'rights'
        
        # Select and rename columns
        df = df[['symbol', 'ex_date', 'action_type', 'dividend_amount', 
                 'bonus_ratio_from', 'bonus_ratio_to', 
                 'split_ratio_from', 'split_ratio_to']]
        df.columns = [
            'symbol', 'ex_date', 'action_type', 'dividend_amount',
            'bonus_ratio_from', 'bonus_ratio_to',
            'split_ratio_from', 'split_ratio_to'
        ]
        # using regex find out the action type, and fill in data accordingly
        df['action_type'] = df['action_type'].str.replace(r'\s+', ' ', regex=True).str.strip()
        df['action_type'] = df['action_type'].str.lower()
        df['action_type'] = df['action_type'].replace({
            'bonus issue': 'bonus',
            'stock split': 'split',
            'dividend': 'dividend',
            'rights issue': 'rights'
        })
        df.to_csv('data/corporate_actions_processed.csv', index=False)
        # Create corporate_actions table
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS corporate_actions (
          symbol VARCHAR,
          ex_date DATE,
          action_type VARCHAR,
          dividend_amount DOUBLE,
          bonus_ratio_from DOUBLE,
          bonus_ratio_to DOUBLE,
          split_ratio_from DOUBLE,
          split_ratio_to DOUBLE,
          PRIMARY KEY (symbol, ex_date, action_type)
        );
        """)
        # Insert data into corporate_actions table
        self.con.register('tmp_corporate_actions', df)
        self.con.execute("""
        INSERT OR IGNORE INTO corporate_actions
        SELECT * FROM tmp_corporate_actions;
        """)
        
    def calculate_adjusted_prices(self):
        """
        Calculates the adjusted closing price for all stocks in bhav_complete_data.
        This function accounts for dividends, stock splits, and bonus issues by
        calculating a cumulative adjustment factor.
        It creates a new table 'bhav_adjusted_prices' with the results.
        """
        print("[INFO]: Starting adjusted price calculation...")

        self.con.execute("""
        CREATE TABLE IF NOT EXISTS bhav_adjusted_prices (
            SYMBOL VARCHAR,
            SERIES VARCHAR,
            DATE1 DATE,
            PREV_CLOSE DOUBLE,
            OPEN_PRICE DOUBLE,
            HIGH_PRICE DOUBLE,
            LOW_PRICE DOUBLE,   
            LAST_PRICE DOUBLE,
            CLOSE_PRICE DOUBLE,
            ADJ_CLOSE_PRICE DOUBLE,
            AVG_PRICE DOUBLE,
            TTL_TRD_QNTY BIGINT,
            TURNOVER_LACS DOUBLE,
            NO_OF_TRADES BIGINT,
            DELIV_QTY BIGINT,
            DELIV_PER DOUBLE,
            PRIMARY KEY (SYMBOL, SERIES, DATE1)
        );
        """)

        print("[INFO]: Fetching price and corporate action data...")
        prices_df = self.con.execute("""
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
            DELIV_PER
            FROM bhav_complete_data
            ORDER BY SYMBOL, DATE1
        """).df()

        ca_df = self.con.execute("""
            SELECT symbol, ex_date, action_type, dividend_amount,
                   bonus_ratio_from, bonus_ratio_to,
                   split_ratio_from, split_ratio_to
            FROM corporate_actions
            WHERE action_type IN ('dividend', 'split', 'bonus')
            ORDER BY symbol, ex_date
        """).df()

        if ca_df.empty:
            print("[WARNING]: No corporate actions found to process. Adjusted prices will equal close prices.")

        prices_df['DATE1'] = pd.to_datetime(prices_df['DATE1'])
        ca_df['ex_date'] = pd.to_datetime(ca_df['ex_date'])

        # 4. Process data symbol by symbol
        all_adjusted_data = []
        unique_symbols = prices_df['SYMBOL'].unique()
        total_symbols = len(unique_symbols)
        print(f"[INFO]: Processing {total_symbols} unique symbols...")

        for i, symbol in enumerate(unique_symbols):
            # Print progress every 250 symbols just to check if it is running or not
            # if (i + 1) % 250 == 0 or i == total_symbols - 1:
            #     print(f"[INFO]: Processed {i + 1}/{total_symbols} symbols...")

            group = prices_df[prices_df['SYMBOL'] == symbol].copy()
            if group.empty:
                continue
            
            group = group.set_index('DATE1').sort_index()
            symbol_ca = ca_df[ca_df['symbol'] == symbol].copy()

            # column for the cumulative adjustment factor
            group['CUMULATIVE_FACTOR'] = 1.0

            if symbol_ca.empty:
                # If no CAs, adjusted price is the same as close price
                all_adjusted_data.append(group.reset_index())
                continue
            
            # Iterate through corporate actions for the symbol chronologically
            # print(symbol_ca)
            for x , action in symbol_ca.iterrows():
                ex_date = action['ex_date']
                
                # The adjustment affects all prices *before* the ex-date.
                # We need the close price of the last trading day right before the ex-date.
                prev_days_mask = group.index < ex_date
                if not prev_days_mask.any():
                    continue # Skip if CA is before any trading data for this stock
                
                prev_trade_date = group[prev_days_mask].index.max()
                prev_close_price = group.loc[prev_trade_date, 'CLOSE_PRICE']

                multiplier = 1.0
                action_type = action['action_type']

                if action_type == 'dividend':
                    dividend = action['dividend_amount']
                    if prev_close_price > 0 and dividend > 0:
                        # Factor = (PrevClose - Dividend) / PrevClose
                        multiplier = (prev_close_price - dividend) / prev_close_price
                
                elif action_type == 'split':
                    # Factor = New / Old
                    split_from = action['split_ratio_from'] 
                    split_to = action['split_ratio_to']     
                    if split_from > 0:
                         multiplier = split_to / split_from

                elif action_type == 'bonus':
                    # Factor = Existing / (Existing + New)
                    bonus_from = action['bonus_ratio_from'] 
                    bonus_to = action['bonus_ratio_to']     
                    denominator = bonus_from + bonus_to
                    if denominator > 0:
                        multiplier = bonus_to / denominator
                
                if multiplier > 0 and multiplier != 1.0:
                    # Apply this adjustment factor to all prices on and before the last trade date
                    group.loc[group.index <= prev_trade_date, 'CUMULATIVE_FACTOR'] *= multiplier

            all_adjusted_data.append(group.reset_index())

        print("[INFO]: Consolidating and saving adjusted price data...")
        if all_adjusted_data:
            final_df = pd.concat(all_adjusted_data, ignore_index=True)
            
            # Calculate final adjusted price
            final_df['ADJ_CLOSE_PRICE'] = final_df['CLOSE_PRICE'] * final_df['CUMULATIVE_FACTOR']
            
            target_column_order = [
                'SYMBOL', 'SERIES', 'DATE1', 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE',
                'LOW_PRICE', 'LAST_PRICE', 'CLOSE_PRICE', 'ADJ_CLOSE_PRICE', 'AVG_PRICE',
                'TTL_TRD_QNTY', 'TURNOVER_LACS', 'NO_OF_TRADES', 'DELIV_QTY',
                'DELIV_PER',
            ]
            
            final_df = final_df[target_column_order]
            
            self.con.register('tmp_adjusted_prices', final_df)
            self.con.execute("""
                INSERT OR IGNORE INTO bhav_adjusted_prices
                SELECT *
                FROM tmp_adjusted_prices;
            """)
            self.con.unregister('tmp_adjusted_prices')
            print(f"[INFO]: Successfully created 'bhav_adjusted_prices' table with {len(final_df):,} records.")

            # Optional: Export to CSV for verification
            final_df.to_csv('data/bhav_adjusted_prices.csv', index=False)
            print(f"[INFO]: Adjusted price data also exported to: data/bhav_adjusted_prices.csv")
        else:
            print("[INFO]: No data was processed for adjusted prices.")

    def process_bulk_deals(self):
        """
        Process bulk deals data and join with adjusted prices
        """
        print("[INFO]: Processing bulk deals data...")
        
        try:
            bulk_deals_df = pd.read_csv('data/Bulk-Deals.csv')
            print(f"[INFO]: Loaded {len(bulk_deals_df)} bulk deal records")
            
            # Remove spaces from column headers
            bulk_deals_df.columns = bulk_deals_df.columns.str.strip()
            print("[INFO]: Column headers:", bulk_deals_df.columns.tolist())
            
            # Convert date format from DD-MMM-YYYY to YYYY-MM-DD
            if 'Date' in bulk_deals_df.columns:
                bulk_deals_df['DATE1'] = pd.to_datetime(bulk_deals_df['Date'], format='%d-%b-%Y').dt.date
                print(f"[INFO]: Date conversion completed")
            
            print(f"[INFO]: DataFrame shape: {bulk_deals_df.shape}")
            print(f"[INFO]: DataFrame columns: {list(bulk_deals_df.columns)}")
            
            self.con.register('tmp_bulk_deals', bulk_deals_df)
            self.con.execute("""
            CREATE OR REPLACE TABLE bulk_deals AS 
            SELECT * FROM tmp_bulk_deals;
            """)
            self.con.unregister('tmp_bulk_deals')
            
            # Join with bhav_adjusted_prices and calculate is_greater column
            print("[INFO]: Joining bulk deals with adjusted prices...")
            result_df = self.con.execute("""
                SELECT
                bd.Symbol,
                bd.Date,
                AVG(bap.PREV_CLOSE)      AS PREV_CLOSE,
                AVG(bap.OPEN_PRICE)      AS OPEN_PRICE,
                AVG(bap.HIGH_PRICE)      AS HIGH_PRICE,
                AVG(bap.LOW_PRICE)       AS LOW_PRICE,
                AVG(bap.LAST_PRICE)      AS LAST_PRICE,
                AVG(bap.CLOSE_PRICE)     AS CLOSE_PRICE,
                AVG(bap.ADJ_CLOSE_PRICE) AS ADJ_CLOSE_PRICE,
                AVG(bap.AVG_PRICE)       AS AVG_PRICE,
                AVG(bap.TTL_TRD_QNTY)    AS TTL_TRD_QNTY,
                AVG(bap.TURNOVER_LACS)   AS TURNOVER_LACS,
                AVG(bap.NO_OF_TRADES)    AS NO_OF_TRADES,
                AVG(bap.DELIV_QTY)       AS DELIV_QTY,
                AVG(bap.DELIV_PER)       AS DELIV_PER,
                AVG(
                    CASE 
                    WHEN bap.CLOSE_PRICE > bap.OPEN_PRICE THEN 1 
                    ELSE 0 
                    END
                )                         AS is_greater
                FROM bulk_deals bd
                LEFT JOIN bhav_adjusted_prices bap
                ON bd.Symbol = bap.SYMBOL
                AND bd.DATE1  = bap.DATE1
                -- AND bap.SERIES = 'EQ'
                GROUP BY
                bd.Symbol,
                bd.Date
                ORDER BY
                bd.Date,
                bd.Symbol;
                """).df()
            
            # Save to CSV
            output_file = 'data/bulk_deals_with_prices.csv'
            result_df.to_csv(output_file, index=False)
            print(f"[INFO]: Bulk deals analysis saved to {output_file}")
            print(f"[INFO]: Total records in output: {len(result_df)}")
            
        except FileNotFoundError:
            print("[ERROR]: data/Bulk-Deals.csv file not found!")
        except Exception as e:
            print(f"[ERROR]: Error processing bulk deals: {e}")
            traceback.print_exc()
    def preprocess_data(self):
        self.preprocess_ca()
        self.calculate_adjusted_prices()

if __name__ == "__main__":
    pre_processor = DataPreProcessor()
    # pre_processor.preprocess_data('2025-04-01', '2025-04-30')
    pre_processor.process_bulk_deals()
    print("Data preprocessing completed successfully!")






