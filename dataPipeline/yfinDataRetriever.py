#!/usr/bin/env python3
"""
Simple Yahoo Finance Data Downloader
===================================

Downloads OHLCV data and dividend/stock split data for NSE and BSE stocks.
Simple and compact.
"""

import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime

class SimpleDataDownloader:
    def __init__(self, securities_file: str, data_folder: str = "data", **config):
        self.securities_file = securities_file
        self.data_folder = data_folder
        self.failed_downloads = []
        
        # Set configuration parameters
        self.min_data_rows = config.get('min_data_rows', 30)
        self.nse_delay = config.get('nse_delay', 3.0)
        self.bse_delay = config.get('bse_delay', 5.0)
        self.break_interval = config.get('break_interval', 20)
        self.break_duration = config.get('break_duration', 30)
        self.failed_file = config.get('failed_file', os.path.join(data_folder, 'failed_downloads.csv'))
        
        # Setup log file
        os.makedirs(os.path.join(data_folder, 'logs'), exist_ok=True)
        self.log_file = f"{data_folder}/logs/downloader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Setup skipped stocks CSV file
        self.skipped_file = f"{data_folder}/skipped_stocks.csv"
        self.init_skipped_csv()
        
        # Create folders
        os.makedirs(os.path.join(self.data_folder, 'nse', 'ohlcv'), exist_ok=True)
        os.makedirs(os.path.join(self.data_folder, 'nse', 'div_stock_split'), exist_ok=True)
        os.makedirs(os.path.join(self.data_folder, 'bse', 'ohlcv'), exist_ok=True)
        os.makedirs(os.path.join(self.data_folder, 'bse', 'div_stock_split'), exist_ok=True)
        
        # Load securities
        self.securities_df = pd.read_csv(securities_file)
        self.log("INFO", f"Loaded {len(self.securities_df)} securities from {securities_file}")
    
    def init_skipped_csv(self):
        """Initialize skipped stocks CSV with headers if it doesn't exist."""
        if not os.path.exists(self.skipped_file):
            headers_df = pd.DataFrame(columns=['timestamp', 'symbol', 'exchange', 'reason', 'details'])
            headers_df.to_csv(self.skipped_file, index=False)
    
    def add_skipped_stock(self, symbol: str, exchange: str, reason: str, details: str = ""):
        """Add a skipped stock immediately to CSV file."""
        skipped_entry = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'exchange': exchange,
            'reason': reason,
            'details': details
        }
        
        # Append to CSV immediately
        skipped_df = pd.DataFrame([skipped_entry])
        skipped_df.to_csv(self.skipped_file, mode='a', header=False, index=False)
    
    def log(self, level: str, message: str):
        """Simple logging with timestamp and level to both console and file."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        full_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"{timestamp} [{level}] {message}"
        file_log_message = f"{full_timestamp} [{level}] {message}"
        
        # Print to console
        print(log_message)
        
        # Write to log file
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(file_log_message + '\n')
    
    def is_data_complete(self, symbol: str, exchange: str) -> bool:
        """Check if existing data for a symbol is complete and valid."""
        try:
            # Determine file paths
            if exchange.upper() == 'NSE':
                ohlcv_file = os.path.join(self.data_folder, 'nse', 'ohlcv', f"{symbol}.csv")
            else:  # BSE
                ohlcv_file = os.path.join(self.data_folder, 'bse', 'ohlcv', f"{symbol}.csv")
            
            # Check if OHLCV file exists
            if not os.path.exists(ohlcv_file):
                return False
            
            # Validate OHLCV file
            try:
                ohlcv_data = pd.read_csv(ohlcv_file)
                
                # Check if file has required columns
                required_columns = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
                if not all(col in ohlcv_data.columns for col in required_columns):
                    self.log("WARNING", f"Invalid columns in {symbol} OHLCV file")
                    self.add_skipped_stock(symbol, exchange, "invalid_columns", f"Missing required columns")
                    time.sleep(1.0) 
                    return False
                
                # Check if file has reasonable amount of data (at least 30 days)
                if len(ohlcv_data) < self.min_data_rows:
                    self.log("WARNING", f"Insufficient data in {symbol} OHLCV file ({len(ohlcv_data)} rows)")
                    self.add_skipped_stock(symbol, exchange, "insufficient_data", f"Only {len(ohlcv_data)} rows")
                    time.sleep(1.0) 
                    return False
                
                # Check for valid date format
                pd.to_datetime(ohlcv_data['date'].iloc[0])
                
                return True
                
            except Exception as e:
                self.log("ERROR", f"Error reading {symbol} OHLCV file: {e}")
                self.add_skipped_stock(symbol, exchange, "file_read_error", str(e))
                return False
                
        except Exception as e:
            self.log("ERROR", f"Error checking {symbol}: {e}")
            self.add_skipped_stock(symbol, exchange, "validation_error", str(e))
            return False
    
    def download_stock(self, symbol: str, company_name: str, exchange: str) -> bool:
        """Download OHLCV and dividend/split data for one stock."""
        try:
            # Check if data already exists and is complete
            if self.is_data_complete(symbol, exchange):
                self.log("INFO", f"{symbol} - data already exists, skipping")
                return True
            
            # Format symbol for Yahoo Finance
            if exchange.upper() == 'NSE':
                yahoo_symbol = f"{symbol}.NS"
                ohlcv_folder = os.path.join(self.data_folder, 'nse', 'ohlcv')
                div_folder = os.path.join(self.data_folder, 'nse', 'div_stock_split')
            else:  # BSE
                yahoo_symbol = f"{symbol}.BO"
                ohlcv_folder = os.path.join(self.data_folder, 'bse', 'ohlcv')
                div_folder = os.path.join(self.data_folder, 'bse', 'div_stock_split')
            
            # Download data
            ticker = yf.Ticker(yahoo_symbol)
            data = ticker.history(period="max", auto_adjust=False)
            
            if data.empty:
                self.log("WARNING", f"No data available for {symbol} - possibly delisted")
                self.add_skipped_stock(symbol, exchange, "delisted_or_no_data", "Yahoo Finance returned empty dataset")
                time.sleep(1.0) 
                return False
            
            # Clean column names and reset index
            data.reset_index(inplace=True)
            data.columns = data.columns.str.replace(' ', '_').str.replace('-', '_').str.lower()
            data.rename(columns={'Date': 'date'}, inplace=True)
            
            # 1. Save OHLCV data
            ohlcv_data = data[['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']].copy()
            ohlcv_file = os.path.join(ohlcv_folder, f"{symbol}.csv")
            ohlcv_data.to_csv(ohlcv_file, index=False)
            
            # 2. Save dividend/split data
            div_split_data = data[['date', 'dividends', 'stock_splits']].copy()
            # Only keep rows with actual dividends or splits
            div_split_data = div_split_data[
                (div_split_data['dividends'] != 0) | (div_split_data['stock_splits'] != 0)
            ]
            
            if not div_split_data.empty:
                div_file = os.path.join(div_folder, f"{symbol}.csv")
                div_split_data.to_csv(div_file, index=False)
                self.log("INFO", f"{symbol} - {len(ohlcv_data)} OHLCV, {len(div_split_data)} div/split records saved")
            else:
                self.log("INFO", f"{symbol} - {len(ohlcv_data)} OHLCV records saved, no div/split data")
            
            return True
            
        except Exception as e:
            self.log("ERROR", f"Failed to download {symbol}: {e}")
            # Don't add rate limiting errors to skipped stocks (temporary issues)
            if "Too Many Requests" not in str(e):
                self.add_skipped_stock(symbol, exchange, "download_error", str(e))
            self.failed_downloads.append({'symbol': symbol, 'exchange': exchange, 'error': str(e)})
            return False
    
    def download_nse_stocks(self, max_stocks=None):
        """Download NSE stocks."""
        nse_stocks = self.securities_df[self.securities_df['NSE_SYMBOL'].notna() & 
                                       (self.securities_df['NSE_SYMBOL'] != '')]
        
        if max_stocks:
            nse_stocks = nse_stocks.head(max_stocks)
        
        total = len(nse_stocks)
        self.log("INFO", f"Starting NSE download - {total} stocks to process")
        
        for count, (idx, row) in enumerate(nse_stocks.iterrows(), 1):
            symbol = row['NSE_SYMBOL'].strip()
            
            # Skip empty or invalid symbols
            if not symbol or len(symbol) < 2:
                self.log("WARNING", f"NSE {count}/{total}: Skipping empty/invalid symbol")
                time.sleep(1.0) 
                continue
                
            company_name = row.get('COMPANY_NAME', 'Unknown')
            
            self.log("INFO", f"NSE {count}/{total}: Processing {symbol}")
            self.download_stock(symbol, company_name, 'NSE')
            
            # Rate limiting
            time.sleep(self.nse_delay)
            
            # Break every N stocks
            if count % self.break_interval == 0:
                self.log("INFO", f"Taking a {self.break_duration}-second break...")
                time.sleep(self.break_duration)
    
    def download_bse_stocks(self, max_stocks=None):
        """Download BSE stocks."""
        bse_stocks = self.securities_df[self.securities_df['BSE_SYMBOL'].notna() & 
                                       (self.securities_df['BSE_SYMBOL'] != '')]
        
        if max_stocks:
            bse_stocks = bse_stocks.head(max_stocks)
        
        total = len(bse_stocks)
        self.log("INFO", f"Starting BSE download - {total} stocks to process")
        
        for count, (idx, row) in enumerate(bse_stocks.iterrows(), 1):
            symbol = row['BSE_SYMBOL'].strip()
            
            # Skip empty or invalid symbols
            if not symbol or len(symbol) < 2:
                self.log("WARNING", f"BSE {count}/{total}: Skipping empty/invalid symbol")
                time.sleep(1.0) 
                continue
                
            company_name = row.get('COMPANY_NAME', 'Unknown')
            
            self.log("INFO", f"BSE {count}/{total}: Processing {symbol}")
            self.download_stock(symbol, company_name, 'BSE')
            
            # Rate limiting
            time.sleep(self.bse_delay)
            
            # Break every N stocks
            if count % self.break_interval == 0:
                self.log("INFO", f"Taking a {self.break_duration}-second break...")
                time.sleep(self.break_duration)
    
    def save_failed_report(self):
        """Save failed downloads report."""
        if self.failed_downloads:
            failed_df = pd.DataFrame(self.failed_downloads)
            failed_df.to_csv(self.failed_file, index=False)
            self.log("INFO", f"Failed downloads saved to {self.failed_file}")
        
        if not self.failed_downloads:
            self.log("INFO", "No failed downloads!")
        
        self.log("INFO", f"Skipped stocks are being logged to {self.skipped_file} in real-time")
    
    def show_summary(self):
        """Show download summary."""
        try:
            nse_ohlcv_count = len([f for f in os.listdir(os.path.join(self.data_folder, 'nse', 'ohlcv')) if f.endswith('.csv')])
            nse_div_count = len([f for f in os.listdir(os.path.join(self.data_folder, 'nse', 'div_stock_split')) if f.endswith('.csv')])
            bse_ohlcv_count = len([f for f in os.listdir(os.path.join(self.data_folder, 'bse', 'ohlcv')) if f.endswith('.csv')])
            bse_div_count = len([f for f in os.listdir(os.path.join(self.data_folder, 'bse', 'div_stock_split')) if f.endswith('.csv')])
            
            # Count skipped stocks from CSV file
            skipped_count = 0
            if os.path.exists(self.skipped_file):
                try:
                    skipped_df = pd.read_csv(self.skipped_file)
                    skipped_count = len(skipped_df)
                except:
                    skipped_count = 0
            
            self.log("INFO", "="*50)
            self.log("INFO", "DOWNLOAD SUMMARY")
            self.log("INFO", "="*50)
            self.log("INFO", f"NSE OHLCV files: {nse_ohlcv_count}")
            self.log("INFO", f"NSE Dividend/Split files: {nse_div_count}")
            self.log("INFO", f"BSE OHLCV files: {bse_ohlcv_count}")
            self.log("INFO", f"BSE Dividend/Split files: {bse_div_count}")
            self.log("INFO", f"Total files: {nse_ohlcv_count + nse_div_count + bse_ohlcv_count + bse_div_count}")
            self.log("INFO", f"Skipped stocks: {skipped_count}")
            self.log("INFO", f"Failed downloads: {len(self.failed_downloads)}")
            self.log("INFO", "="*50)
        except Exception as e:
            self.log("ERROR", f"Could not generate summary: {e}")


def main():
    """Main function - simple and focused."""
    
    # ============================================
    # CONFIGURATION PARAMETERS - EDIT THESE
    # ============================================
    securities_file = "data/bse_nse_securities.csv"  # Input securities file
    data_folder = "./data"  # Where to save/read all data files
    
    # Download limits (set to None for no limit)
    max_nse_stocks = None  # Set to a number like 50 for testing
    max_bse_stocks = None  # Set to a number like 50 for testing
    
    # Rate limiting (seconds)
    nse_delay = 0.5      # Delay between NSE stock downloads
    bse_delay = 0.5      # Delay between BSE stock downloads
    break_interval = 50   # Take break every N stocks
    break_duration = 5   # Break duration in seconds
    
    # Data validation
    min_data_rows = 30    # Minimum rows required for valid data
    
    # File paths (relative to data_folder)
    failed_file = "failed_downloads.csv"
    # ============================================
    
    if not os.path.exists(securities_file):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"{timestamp} [ERROR] {securities_file} not found!")
        return
    
    try:
        # Create configuration dictionary
        config = {
            'min_data_rows': min_data_rows,
            'nse_delay': nse_delay,
            'bse_delay': bse_delay,
            'break_interval': break_interval,
            'break_duration': break_duration,
            'failed_file': os.path.join(data_folder, failed_file)
        }
        
        downloader = SimpleDataDownloader(securities_file, data_folder, **config)
        
        # Download NSE stocks
        downloader.download_nse_stocks(max_stocks=max_nse_stocks)
        
        # Download BSE stocks
        downloader.download_bse_stocks(max_stocks=max_bse_stocks)
        
        # Generate reports
        downloader.save_failed_report()
        downloader.show_summary()
        
        downloader.log("INFO", f"Log file saved as: {downloader.log_file}")
        downloader.log("INFO", "Simple data download completed!")
        
    except Exception as e:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"{timestamp} [ERROR] {e}")


if __name__ == "__main__":
    main()
