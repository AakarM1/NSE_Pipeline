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

import yfinance
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


    def fetch_corporate_actions(self, corp_actions_csv='data/CF-CA-equities.csv') -> pd.DataFrame:
        df = pd.read_csv(corp_actions_csv)
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

    def preprocess_ca(self, corp_actions_csv='data/CF-CA-equities.csv'):
        df = pd.DataFrame()
        startDate = self.startDate
        endDate = self.endDate
        if startDate and endDate:
            print(f"[INFO]: Fetching Corporate Actions from {startDate} to {endDate}")
            raw_df = self.fetch_corporate_actions(corp_actions_csv)
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
                if CREATE_TABLES:
                    # Create an empty corporate_actions table if no file is found
                    self.con.execute("""
                    CREATE TABLE IF NOT EXISTS corporate_actions (
                    symbol VARCHAR, ex_date DATE, action_type VARCHAR, dividend_amount DOUBLE,
                    bonus_ratio_from DOUBLE, bonus_ratio_to DOUBLE, split_ratio_from DOUBLE,
                    split_ratio_to DOUBLE, PRIMARY KEY (symbol, ex_date, action_type)
                    );
                    """)
                print("[INFO]: Created corporate action table strcuture.")
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
        print("[INFO]: Successfully processed corporate actions data.")
        if CREATE_TABLES:
            # Create corporate_actions table if it doesn't exist
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
            print(f"[INFO]: Successfully created 'corporate_actions' table with {len(df):,} records.")
            self.con.unregister('tmp_corporate_actions')
        
    def calculate_adjusted_prices(self):
        """
        Calculates the adjusted closing price for all stocks in bhav_complete_data.
        This function accounts for dividends, stock splits, and bonus issues by
        calculating a cumulative adjustment factor.
        It creates a new table 'bhav_adjusted_prices' with the results.
        """
        print("[INFO]: Starting adjusted price calculation...")

        if CREATE_TABLES:
            # Create bhav_complete_data table if it doesn't exist
            self.con.execute("""
            CREATE TABLE IF NOT EXISTS bhav_complete_data (
                SYMBOL VARCHAR,
                SERIES VARCHAR,
                DATE1 DATE,
                PREV_CLOSE DOUBLE,
                OPEN_PRICE DOUBLE,
                HIGH_PRICE DOUBLE,
                LOW_PRICE DOUBLE,   
                LAST_PRICE DOUBLE,
                CLOSE_PRICE DOUBLE,
                AVG_PRICE DOUBLE,
                TTL_TRD_QNTY BIGINT,
                TURNOVER_LACS DOUBLE,
                NO_OF_TRADES BIGINT,
                DELIV_QTY BIGINT,
                DELIV_PER DOUBLE,
                PRIMARY KEY (SYMBOL, SERIES, DATE1)
            );
            """)
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
        # filter and use tickerDict to only process relevant symbols
        if self.tickerDict:
            prices_df = prices_df[prices_df['SYMBOL'].isin(self.tickerDict.keys())]
            ca_df = ca_df[ca_df['symbol'].isin(self.tickerDict.keys())]
            print(f"[INFO]: Filtering data to only include symbols in tickerDict: {self.tickerDict.keys()}")
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
            
            # Export to CSV for verification
            final_df.to_csv('data/bhav_adjusted_prices.csv', index=False)
            print(f"[INFO]: Adjusted price data exported to: data/bhav_adjusted_prices.csv")
            
            if CREATE_TABLES:
                self.con.register('tmp_adjusted_prices', final_df)
                self.con.execute("""
                    INSERT OR IGNORE INTO bhav_adjusted_prices
                    SELECT *
                    FROM tmp_adjusted_prices;
                """)
                self.con.unregister('tmp_adjusted_prices')
                print(f"[INFO]: Successfully created 'bhav_adjusted_prices' table with {len(final_df):,} records.")

        else:
            print("[INFO]: No data was processed for adjusted prices.")

    def compare_adj_close(self):
        """
        Downloads data from Yahoo Finance for a symbol(s),
        compares it against the local bhav_adjusted_prices table, and
        calculates the accuracy of Close and Adjusted Close prices.
        """
        print("\n" + "="*60)
        print("=== ADJUSTED CLOSE PRICE VALIDATION vs. YAHOO FINANCE ===")
        print("="*60)
        
        if not self.startDate or not self.endDate:
            print("Error: Start date and end date must be provided for comparison.")
            return
        
        # Convert string dates to datetime objects if needed
        if isinstance(self.startDate, str):
            start_date = datetime.strptime(self.startDate, '%Y-%m-%d')
        else:
            start_date = self.startDate
            
        if isinstance(self.endDate, str):
            end_date = datetime.strptime(self.endDate, '%Y-%m-%d')
        else:
            end_date = self.endDate

        # 1. Define the symbol mapping
        symbols = self.tickerDict
        symbol_list = list(symbols.keys())
        ticker_list = list(symbols.values())
        
        # 2. Fetch data from Yahoo Finance
        print(f"Fetching data from Yahoo Finance for {len(ticker_list)} ticker(s)...")
        yfin_df = yfinance.download(ticker_list, start=start_date, end=end_date, auto_adjust=False, group_by='ticker')
        
        if yfin_df.empty:
            print("Could not download any data from Yahoo Finance. Aborting comparison.")
            return
        
        # Reformat the multi-index yfinance DataFrame into a clean, long-format DataFrame
        yfin_cleaned_list = []
        for ticker in ticker_list:
            if ticker in yfin_df.columns:
                # Get the symbol for the current ticker
                symbol_name = [s for s, t in symbols.items() if t == ticker][0]
                temp_df = yfin_df[ticker].copy()
                temp_df = temp_df.dropna(subset=['Close']) # Drop rows where there was no trading
                temp_df['Symbol'] = symbol_name
                yfin_cleaned_list.append(temp_df)
        
        yfin_df_long = pd.concat(yfin_cleaned_list).reset_index()
        yfin_df_long = yfin_df_long.rename(columns={'Date': 'DATE1', 'Symbol': 'SYMBOL'})
        yfin_df_long['DATE1'] = pd.to_datetime(yfin_df_long['DATE1']).dt.date
        print(f"Successfully processed {len(yfin_df_long)} records from Yahoo Finance.")

        if CREATE_TABLES:
            # Fetch your calculated data from the database
            print("Fetching data from local 'bhav_adjusted_prices' table...")
            symbols_tuple = tuple(symbol_list)
            if self.con is None:
                self.con = duckdb.connect(database='data/eod.duckdb', read_only=False)
            bhav_adj_df = self.con.execute(f"""
                SELECT SYMBOL, DATE1, CLOSE_PRICE, ADJ_CLOSE_PRICE
                FROM bhav_adjusted_prices
                WHERE SYMBOL IN {symbols_tuple}
                AND DATE1 BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
                ORDER BY SYMBOL, DATE1
            """).df()
            bhav_adj_df['DATE1'] = pd.to_datetime(bhav_adj_df['DATE1']).dt.date
            print(f"Successfully fetched {len(bhav_adj_df)} records from local database.")
        else:
            # print("No data found in local 'bhav_adjusted_prices' table for the specified symbols and date range.")
            # Fetch data from csv file if available
            bhav_adj_files = glob.glob('data/bhav_adjusted_prices.csv')
            if bhav_adj_files:
                print("Reading data from local CSV file...")
                bhav_adj_df = pd.read_csv(bhav_adj_files[0])
                bhav_adj_df['DATE1'] = pd.to_datetime(bhav_adj_df['DATE1']).dt.date
                bhav_adj_df = bhav_adj_df[bhav_adj_df['SYMBOL'].isin(symbol_list)]
                print(f"Successfully read {len(bhav_adj_df)} records from CSV file.")
            else:
                print("No data found in local 'bhav_adjusted_prices' table or CSV file for the specified symbols and date range.")
                return
        # 4. Merge the two DataFrames for direct comparison
        merged_df = pd.merge(
            yfin_df_long,
            bhav_adj_df,
            on=['SYMBOL', 'DATE1'],
            how='inner', # Use 'inner' to only compare dates where both sources have data
            suffixes=('_yfin', '_bhav')
        )
        
        if merged_df.empty:
            print("No common records found between Yahoo Finance and local data for the given symbols and dates.")
            return

        # 5. Calculate differences and round values
        merged_df['CLOSE_PRICE_yfin_rounded'] = merged_df['Close'].round(2)
        merged_df['CLOSE_PRICE_bhav_rounded'] = merged_df['CLOSE_PRICE'].round(2)
        merged_df['ADJ_CLOSE_yfin_rounded'] = merged_df['Adj Close'].round(2)
        merged_df['ADJ_CLOSE_bhav_rounded'] = merged_df['ADJ_CLOSE_PRICE'].round(2)
        
        merged_df['close_diff'] = (merged_df['CLOSE_PRICE_yfin_rounded'] - merged_df['CLOSE_PRICE_bhav_rounded']).abs()
        merged_df['adj_close_diff'] = (merged_df['ADJ_CLOSE_yfin_rounded'] - merged_df['ADJ_CLOSE_bhav_rounded']).abs()

        # 6. Calculate accuracy percentages
        total_records = len(merged_df)
        close_matches = (merged_df['close_diff'] == 0).sum()
        adj_close_matches = (merged_df['adj_close_diff'] == 0).sum()
        
        close_accuracy = (close_matches / total_records) * 100
        adj_close_accuracy = (adj_close_matches / total_records) * 100
        
        print("\n--- COMPARISON SUMMARY ---")
        print(f"Total Common Records Analyzed: {total_records}")
        print(f"Close Price Accuracy (rounded to 2dp):   {close_accuracy:.2f}% ({close_matches}/{total_records} matches)")
        print(f"Adj Close Price Accuracy (rounded to 2dp): {adj_close_accuracy:.2f}% ({adj_close_matches}/{total_records} matches)")

        # 7. Display sample data
        print("\n--- SAMPLE COMPARISON DATA ---")
        # Select relevant columns for display
        display_cols = [
            'SYMBOL', 'DATE1',
            'CLOSE_PRICE_yfin_rounded', 'CLOSE_PRICE_bhav_rounded', 'close_diff',
            'ADJ_CLOSE_yfin_rounded', 'ADJ_CLOSE_bhav_rounded', 'adj_close_diff'
        ]
        
        # Show some of the largest differences first to identify issues
        sample_diff_df = merged_df[merged_df['adj_close_diff'] > 0].sort_values(by='adj_close_diff', ascending=False)
        
        if not sample_diff_df.empty:
            print("\nSample of Mismatched Adjusted Close Prices:")
            print(sample_diff_df[display_cols].head(10).to_string(index=False))
        else:
            print("\nNo mismatches found in Adjusted Close Prices!")

        # Show a sample of matching records
        sample_match_df = merged_df[merged_df['adj_close_diff'] == 0]
        if not sample_match_df.empty:
            print("\nSample of Matching Adjusted Close Prices:")
            print(sample_match_df[display_cols].head(10).to_string(index=False))

        # 8. Save the detailed comparison to a CSV file
        output_path = f"data/yfin_vs_bhav_comparison_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv"
        # Add the names of symbols to the output path
        output_path = output_path.replace("yfin_vs_bhav", f"yfin_vs_bhav_{'_'.join(self.tickerDict.keys())}")
        merged_df.to_csv(output_path, index=False)
        print(f"\nDetailed comparison saved to: {output_path}")
 
    def preprocess_data(self, corp_actions_csv='data/CF-CA-equities.csv'):
        self.preprocess_ca(corp_actions_csv)
        self.calculate_adjusted_prices()
        self.compare_adj_close()
        
        

if __name__ == "__main__":
    con = duckdb.connect(database='data/eod.duckdb', read_only=False)
    fromDate = '2025-01-01'
    toDate = '2025-07-01'
    tickerDict = {
        '360ONE': '360ONE.NS',
        # 'CIEINDIA': 'CIEINDIA.NS',
        # 'CRISIL': 'CRISIL.NS',
        # 'DCMSRIND': 'DCMSRIND.NS',
    }
    corp_actions_csv = 'data/CF-CA-equities.csv'
    CREATE_TABLES = False
    pre_processor = DataPreProcessor(startDate=fromDate, endDate=toDate, tickerDict=tickerDict, con=con)
    pre_processor.preprocess_data(corp_actions_csv)
    print("Data preprocessing completed successfully!")
    con.close()






