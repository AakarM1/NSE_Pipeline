import os
import re
import time
import zipfile
import logging
import pandas as pd
import httpx
import fastbt.urlpatterns as patterns
from datetime import datetime
from typing import Optional

"""
NSE_BSE.py - Simplified module for retrieving NSE/BSE data
Downloads and processes NSE bhav_sec and BSE equity data.
"""

def download_file(url: str, filepath: str) -> bool:
    """Download file from URL and save to filepath."""
    try:
        with httpx.Client(timeout=30) as client:  # Increased timeout
            response = client.get(url)
            if response.status_code == 200:
                with open(filepath, "wb") as f:
                    f.write(response.content)
                return True
    except Exception as e:
        logging.error(f"Download failed for {url}: {e}")
    return False

def download_data(source: str, output_dir: str, dates: pd.DatetimeIndex, sleep: float = 0.5) -> dict:
    """Generic download function for NSE/BSE data."""
    folder = os.path.join(output_dir, source)
    os.makedirs(folder, exist_ok=True)
    
    downloaded = skipped = 0
    missed_dates = []
    
    for date in dates:
        # Generate URL based on source
        if source == 'nse_bhav_sec':
            pat, func = patterns.file_patterns['bhav_sec']
            url = pat.format(**func(date))
        elif source == 'bse_equity':
            date_str = date.strftime('%d%m%y')
            url = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{date_str}_T0.CSV"
        else:
            continue
            
        filename = url.split("/")[-1]
        filepath = os.path.join(folder, filename)
        
        if os.path.exists(filepath):
            skipped += 1
            continue
            
        if download_file(url, filepath):
            downloaded += 1
            # Handle ZIP extraction for NSE
            if source == 'nse_bhav_sec' and filename.endswith('.zip'):
                try:
                    with zipfile.ZipFile(filepath, 'r') as z:
                        z.extractall(folder)
                except zipfile.BadZipFile:
                    pass
        else:
            missed_dates.append(date)
            
        time.sleep(sleep)
    
    print(f"[{source}] Downloaded: {downloaded}, Skipped: {skipped}, Missed: {len(missed_dates)}")
    return {'downloaded': downloaded, 'skipped': skipped, 'missed': len(missed_dates)}

def clean_data(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Clean and format data based on source."""
    df.columns = df.columns.str.strip()
    
    if source == 'nse_bhav_sec':
        # Clean NSE data
        df['SYMBOL'] = df['SYMBOL'].str.strip().fillna('')
        df['SERIES'] = df['SERIES'].str.strip().fillna('')
        df = df[(df['SYMBOL'] != '') & (df['SERIES'] != '')]
        
        # Convert date
        df['DATE1'] = pd.to_datetime(df['DATE1'].str.strip(), format='%d-%b-%Y', dayfirst=True).dt.date
        
        # Convert numeric columns
        numeric_cols = ['LAST_PRICE', 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 
                       'CLOSE_PRICE', 'AVG_PRICE', 'TURNOVER_LACS', 'DELIV_PER']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        integer_cols = ['DELIV_QTY', 'TTL_TRD_QNTY', 'NO_OF_TRADES']
        for col in integer_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    elif source == 'bse_equity':
        # Clean BSE data
        if 'SC_CODE' in df.columns:
            df['SYMBOL'] = df['SC_CODE'].astype(str).str.strip()
        if 'SC_NAME' in df.columns:
            df['COMPANY_NAME'] = df['SC_NAME'].str.strip()
            
        # Convert numeric columns
        numeric_cols = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'NET_TURNOV']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        integer_cols = ['NO_TRADES', 'NO_OF_SHRS']
        for col in integer_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    return df

def process_data(data_dir: str, source: str) -> Optional[pd.DataFrame]:
    """Process downloaded files into consolidated DataFrame."""
    if not os.path.exists(data_dir):
        print(f"[WARNING]: Directory {data_dir} does not exist.")
        return None
    
    # Get CSV files
    if source == 'nse_bhav_sec':
        files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    else:  # BSE
        files = [f for f in os.listdir(data_dir) if f.endswith('.CSV')]
    
    if not files:
        print(f"[INFO]: No {source} files found.")
        return None
    
    dataframes = []
    for file in files:
        try:
            df = pd.read_csv(os.path.join(data_dir, file))
            
            # Add date for BSE files (extract from filename)
            if source == 'bse_equity':
                date_match = re.search(r'EQ_ISINCODE_(\d{6})_T0\.CSV', file)
                if date_match:
                    date_obj = datetime.strptime(date_match.group(1), '%d%m%y').date()
                    df['DATE1'] = date_obj
            
            dataframes.append(df)
        except Exception as e:
            print(f"[ERROR]: Error processing {file}: {e}")
    
    if not dataframes:
        return None
    
    # Combine and clean data
    combined_df = pd.concat(dataframes, ignore_index=True).drop_duplicates()
    cleaned_df = clean_data(combined_df, source)
    
    if not cleaned_df.empty:
        date_range = cleaned_df['DATE1'].min(), cleaned_df['DATE1'].max()
        print(f"[{source}] Processed {len(cleaned_df):,} records from {date_range[0]} to {date_range[1]}")
        return cleaned_df
    
    return None

def get_data(source: str, start_date: str = '2024-01-01', end_date: Optional[str] = None, 
             output_dir: str = "./data/", export_csv: bool = True) -> Optional[pd.DataFrame]:
    """Complete workflow to download and process data."""
    if end_date is None:
        end_date = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    
    dates = pd.bdate_range(start=start_date, end=end_date)
    print(f"[{source}] Processing {len(dates)} business days from {start_date} to {end_date}")
    
    # Download data
    download_data(source, output_dir, dates)
    
    # Process data
    data_dir = os.path.join(output_dir, source)
    df = process_data(data_dir, source)
    
    # Export to CSV
    if df is not None and export_csv:
        csv_path = f"./data/{source}_processed.csv"
        df.to_csv(csv_path, index=False)
        print(f"[{source}] Exported to {csv_path}")
    
    return df

def get_nse_equity_list(output_dir: str = "./data/") -> Optional[pd.DataFrame]:
    """Download and process NSE equity list with ISIN codes."""
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    filepath = os.path.join(output_dir, "nse_equity_list.csv")
    
    print("[NSE_EQUITY_LIST] Downloading NSE equity list...")
    
    # Try alternative URLs if main URL fails
    alternative_urls = [
        "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
        "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
        # Add more alternative URLs if needed
    ]
    
    success = False
    for url in alternative_urls:
        print(f"[NSE_EQUITY_LIST] Trying URL: {url}")
        if download_file(url, filepath):
            success = True
            break
        print(f"[NSE_EQUITY_LIST] Failed, trying next URL...")
    
    if success:
        print(f"[NSE_EQUITY_LIST] Downloaded successfully")
        try:
            df = pd.read_csv(filepath)
            # Clean column names
            df.columns = df.columns.str.strip()
            print(f"[NSE_EQUITY_LIST] Loaded {len(df)} records")
            print(f"[NSE_EQUITY_LIST] Columns: {df.columns.tolist()}")
            return df
        except Exception as e:
            print(f"[ERROR] Failed to process NSE equity list: {e}")
            return None
    else:
        print(f"[ERROR] Failed to download NSE equity list from all URLs")
        return None


def create_master(output_dir: str = "./data/") -> Optional[pd.DataFrame]:
    """Create comprehensive BSE-NSE mapping using online NSE equity list with local CSV fallback."""
    print("[MAPPING] Creating comprehensive BSE-NSE symbol mapping...")
    
    # Try to get NSE equity list from online source first
    nse_df = get_nse_equity_list(output_dir)
    
    if nse_df is None:
        print("[WARNING] Failed to get NSE equity list from online source, falling back to local CSV...")
        # Fallback to local NSE file
        nse_file = os.path.join(output_dir, "EQUITY_L.csv")
        
        if not os.path.exists(nse_file):
            print(f"[ERROR] NSE file not found: {nse_file}")
            return None
        
        try:
            nse_df = pd.read_csv(nse_file)
            nse_df.columns = nse_df.columns.str.strip()
            print(f"[INFO]: Loaded NSE data from local file with {len(nse_df)} records")
        except Exception as e:
            print(f"[ERROR] Failed to load local NSE file: {e}")
            return None
    else:
        print(f"[INFO]: Successfully retrieved NSE data online with {len(nse_df)} records")
    
    # Load BSE data
    bse_file = os.path.join(output_dir, "BSE Equity.csv")
    
    if not os.path.exists(bse_file):
        print(f"[ERROR] BSE file not found: {bse_file}")
        return None
    
    try:
        bse_df = pd.read_csv(bse_file)
        bse_df.columns = bse_df.columns.str.strip()
        print(f"[INFO]: Loaded BSE data with {len(bse_df)} records")

        print(f"[MAPPING] Loaded NSE: {len(nse_df)} records, BSE: {len(bse_df)} records")
        
    except Exception as e:
        print(f"[ERROR] Failed to load BSE data file: {e}")
        return None
    
    # Clean and prepare NSE data
    def clean_symbol(symbol):
        """Remove # and . from symbols."""
        if pd.isna(symbol):
            return symbol
        return str(symbol).replace('#', '').replace('.', '').strip()
    
    # Prepare NSE mapping
    nse_mapping = nse_df[['SYMBOL', 'NAME OF COMPANY', 'ISIN NUMBER']].copy()
    nse_mapping.columns = ['NSE_SYMBOL', 'NSE_COMPANY_NAME', 'ISIN']
    nse_mapping['NSE_SYMBOL'] = nse_mapping['NSE_SYMBOL'].apply(clean_symbol)
    nse_mapping['ISIN'] = nse_mapping['ISIN'].str.strip()
    nse_mapping = nse_mapping[nse_mapping['ISIN'].notna() & (nse_mapping['ISIN'] != '')]
    nse_mapping = nse_mapping.drop_duplicates(subset=['ISIN'])
    
    # Prepare BSE mapping
    bse_mapping = bse_df[['Security Id', 'Security Name', 'ISIN No']].copy()
    bse_mapping.columns = ['BSE_SYMBOL', 'BSE_COMPANY_NAME', 'ISIN']
    bse_mapping['BSE_SYMBOL'] = bse_mapping['BSE_SYMBOL'].apply(clean_symbol)
    bse_mapping['ISIN'] = bse_mapping['ISIN'].str.strip()
    bse_mapping = bse_mapping[bse_mapping['ISIN'].notna() & (bse_mapping['ISIN'] != '')]
    bse_mapping = bse_mapping.drop_duplicates(subset=['ISIN'])
    
    print(f"[MAPPING] After cleaning - NSE: {len(nse_mapping)} records, BSE: {len(bse_mapping)} records")
    
    # Get all unique ISINs from both exchanges
    all_isins = set(nse_mapping['ISIN'].tolist() + bse_mapping['ISIN'].tolist())
    
    mapping_list = []
    
    for isin in all_isins:
        nse_record = nse_mapping[nse_mapping['ISIN'] == isin]
        bse_record = bse_mapping[bse_mapping['ISIN'] == isin]
        
        # Get NSE info or default to " "
        if not nse_record.empty:
            nse_symbol = nse_record.iloc[0]['NSE_SYMBOL']
            nse_company = nse_record.iloc[0]['NSE_COMPANY_NAME']
        else:
            nse_symbol = " "
            nse_company = ""
        
        # Get BSE info or default to " "
        if not bse_record.empty:
            bse_symbol = bse_record.iloc[0]['BSE_SYMBOL']
            bse_company = bse_record.iloc[0]['BSE_COMPANY_NAME']
        else:
            bse_symbol = " "
            bse_company = ""
        
        # Use the available company name (prefer NSE, fallback to BSE)
        company_name = nse_company if nse_company else bse_company
        
        mapping_list.append({
            'BSE_SYMBOL': bse_symbol,
            'NSE_SYMBOL': nse_symbol,
            'COMPANY_NAME': company_name,
            'ISIN': isin
        })
    
    final_mapping = pd.DataFrame(mapping_list)
    
    if len(final_mapping) == 0:
        print("[ERROR] No mapping data could be created")
        return None
    
    # Sort by NSE symbol (putting spaces at the end)
    final_mapping['sort_key'] = final_mapping['NSE_SYMBOL'].apply(lambda x: 'zzz' if x == ' ' else x)
    final_mapping = final_mapping.sort_values('sort_key')
    if 'sort_key' in final_mapping.columns:
        final_mapping = final_mapping.drop('sort_key', axis=1)
    final_mapping = final_mapping.reset_index(drop=True)
    
    print(f"[MAPPING] Created comprehensive mapping for {len(final_mapping)} securities")
    
    # Count statistics (using spaces)
    both_exchanges = len(final_mapping[(final_mapping['BSE_SYMBOL'] != ' ') & (final_mapping['NSE_SYMBOL'] != ' ')])
    nse_only = len(final_mapping[(final_mapping['BSE_SYMBOL'] == ' ') & (final_mapping['NSE_SYMBOL'] != ' ')])
    bse_only = len(final_mapping[(final_mapping['BSE_SYMBOL'] != ' ') & (final_mapping['NSE_SYMBOL'] == ' ')])
    
    print(f"[MAPPING] Statistics:")
    print(f"  - Both exchanges: {both_exchanges}")
    print(f"  - NSE only: {nse_only}")
    print(f"  - BSE only: {bse_only}")
    
    # Export mapping
    mapping_path = os.path.join(output_dir, "bse_nse_securities.csv")
    final_mapping.to_csv(mapping_path, index=False)
    print(f"[MAPPING] Exported to {mapping_path}")
    
    return final_mapping

if __name__ == "__main__":
    start_time = time.time()
    logging.basicConfig(level=logging.INFO)
    
    # Configuration
    DOWNLOAD_NSE = False
    DOWNLOAD_BSE = False
    CREATE_MAPPING = True
    start_date = '2024-01-01'
    
    print("Starting NSE/BSE data retrieval...")
    
    nse_df = None
    bse_df = None
    
    if DOWNLOAD_NSE:
        print("\n" + "="*50)
        print("NSE BHAV_SEC DATA")
        print("="*50)
        nse_df = get_data('nse_bhav_sec', start_date)
        if nse_df is not None:
            print(f"NSE: {len(nse_df):,} records processed")
    
    if DOWNLOAD_BSE:
        print("\n" + "="*50)
        print("BSE EQUITY DATA")
        print("="*50)
        bse_df = get_data('bse_equity', start_date)
        if bse_df is not None:
            print(f"BSE: {len(bse_df):,} records processed")
    
    if CREATE_MAPPING:
        print("\n" + "="*50)
        print("CREATING COMPREHENSIVE BSE-NSE MAPPING")
        print("="*50)
        
        # Create comprehensive mapping using local files
        mapping_df = create_master("./data/")
        if mapping_df is not None:
            print(f"COMPREHENSIVE MAPPING: {len(mapping_df):,} securities mapped")
            print(f"\nFirst 10 mappings:")
            print(mapping_df.head(10).to_string(index=False))
            print(f"\nLast 10 mappings:")
            print(mapping_df.tail(10).to_string(index=False))
        
       
    print(f"\nCompleted in {time.time() - start_time:.2f} seconds")
