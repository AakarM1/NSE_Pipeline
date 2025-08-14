# Data Pipeline Modules

This directory contains two main data retrieval and processing modules for Indian stock market data:

1. **`yfinDataRetriever.py`** - Yahoo Finance data downloader for NSE/BSE stocks
2. **`NSE_BSE_masterCreator.py`** - NSE and BSE data retriever with symbol mapping

## Table of Contents

- [yfinDataRetriever.py](#yfinDataRetrieverpy)
- [NSE_BSE_masterCreator.py](#nse_bse_mastercreatorpy)
- [Setup Requirements](#setup-requirements)
- [Quick Start](#quick-start)

---

## yfinDataRetriever.py

### Overview
A comprehensive Yahoo Finance data downloader specifically designed for Indian stock markets (NSE and BSE). Downloads historical OHLCV data and dividend/stock split information with robust error handling and rate limiting.

### Key Features

- **Dual Exchange Support**: Downloads data from both NSE (.NS) and BSE (.BO) exchanges
- **Complete Data Retrieval**: Gets OHLCV (Open, High, Low, Close, Volume) and adjusted close prices
- **Corporate Actions**: Downloads dividend and stock split data separately
- **Smart Skip Logic**: Automatically skips stocks with existing complete data
- **Rate Limiting**: Configurable delays to respect Yahoo Finance API limits
- **Comprehensive Logging**: Real-time logging with timestamps and detailed error tracking
- **Failure Recovery**: Tracks and reports failed downloads for manual review
- **Data Validation**: Validates downloaded data for completeness and integrity

### Usage

#### Basic Usage
```python
from yfinDataRetriever import SimpleDataDownloader

# Initialize downloader
downloader = SimpleDataDownloader(
    securities_file="data/bse_nse_securities.csv",
    data_folder="./data"
)

# Download NSE stocks
downloader.download_nse_stocks()

# Download BSE stocks  
downloader.download_bse_stocks()

# Generate summary
downloader.show_summary()
```

#### Configuration Options
```python
config = {
    'min_data_rows': 30,      # Minimum rows for valid data
    'nse_delay': 0.5,         # Delay between NSE downloads (seconds)
    'bse_delay': 0.5,         # Delay between BSE downloads (seconds)
    'break_interval': 50,     # Take break every N stocks
    'break_duration': 5,      # Break duration (seconds)
}

downloader = SimpleDataDownloader(securities_file, data_folder, **config)
```

#### Command Line Execution
```bash
python yfinDataRetriever.py
```

### Input Requirements

The module expects a CSV file with the following columns:
- `NSE_SYMBOL`: NSE stock symbol (e.g., "RELIANCE")
- `BSE_SYMBOL`: BSE stock symbol (e.g., "500325")
- `COMPANY_NAME`: Company name (optional)

### Output Structure

```
data/
├── nse/
│   ├── ohlcv/              # NSE OHLCV data
│   │   ├── RELIANCE.csv
│   │   └── TCS.csv
│   └── div_stock_split/    # NSE dividend/split data
│       ├── RELIANCE.csv
│       └── TCS.csv
├── bse/
│   ├── ohlcv/              # BSE OHLCV data
│   │   ├── ADANIENT.csv
│   │   └── AARTECH.csv
│   └── div_stock_split/    # BSE dividend/split data
│       ├── ADANIENT.csv
│       └── AARTECH.csv
├── skipped_stocks.csv      # Real-time skipped stocks log
├── failed_downloads.csv    # Failed download summary
└── logs/                   # Timestamped log files
    └── downloader_20250814_224457.log
```

### Data Format

#### OHLCV Files
```csv
date,open,high,low,close,adj_close,volume
2024-01-01,2500.0,2550.0,2480.0,2520.0,2520.0,1000000
```

#### Dividend/Split Files
```csv
date,dividends,stock_splits
2024-03-15,10.0,0.0
2024-06-15,0.0,2.0
```

---

## NSE_BSE_masterCreator.py

### Overview
A comprehensive module for downloading official NSE and BSE market data and creating unified symbol mappings. Handles bhav copy data from both exchanges and generates master mapping files using ISIN codes.

### Key Features

- **Official Data Sources**: Downloads directly from NSE and BSE official websites
- **Bhav Copy Processing**: Handles NSE bhav_sec and BSE equity files
- **Master Mapping Creation**: Creates comprehensive BSE-NSE symbol mapping using ISIN codes
- **Automatic Extraction**: Handles ZIP file extraction for NSE data
- **Data Cleaning**: Standardizes column names and data types
- **Flexible Date Ranges**: Supports custom date range downloads
- **Fallback Mechanisms**: Multiple URL sources for reliability

### Main Functions

#### 1. Download Market Data
```python
# Download NSE bhav_sec data
nse_df = get_data('nse_bhav_sec', start_date='2024-01-01', end_date='2024-12-31')

# Download BSE equity data  
bse_df = get_data('bse_equity', start_date='2024-01-01', end_date='2024-12-31')
```

#### 2. Create Symbol Mapping
```python
# Create comprehensive BSE-NSE mapping
mapping_df = create_master("./data/")
```

#### 3. Get NSE Equity List
```python
# Download current NSE equity list with ISIN codes
nse_equity_df = get_nse_equity_list("./data/")
```

### Usage Examples

#### Complete Workflow
```python
import NSE_BSE_masterCreator as nse_bse

# Download and process NSE data
nse_data = nse_bse.get_data(
    source='nse_bhav_sec', 
    start_date='2024-01-01',
    output_dir='./data/',
    export_csv=True
)

# Download and process BSE data
bse_data = nse_bse.get_data(
    source='bse_equity',
    start_date='2024-01-01', 
    output_dir='./data/',
    export_csv=True
)

# Create unified mapping
mapping = nse_bse.create_master('./data/')
```

#### Command Line Execution
```bash
python NSE_BSE_masterCreator.py
```

### Configuration Options

Edit the configuration section in the `__main__` block:

```python
# Configuration
DOWNLOAD_NSE = True      # Download NSE bhav_sec data
DOWNLOAD_BSE = True      # Download BSE equity data  
CREATE_MAPPING = True    # Create symbol mapping
start_date = '2024-01-01'
```

### Input Files for Mapping

Required files in `./data/` directory:
- `EQUITY_L.csv` - NSE equity list (auto-downloaded or manual)
- `BSE Equity.csv` - BSE equity list (manual download required)

### Output Files

```
data/
├── nse_bhav_sec/              # NSE bhav_sec files
│   ├── bhav_sec_20240101.csv
│   └── bhav_sec_20240102.csv
├── bse_equity/                # BSE equity files  
│   ├── EQ_ISINCODE_010124_T0.CSV
│   └── EQ_ISINCODE_020124_T0.CSV
├── nse_bhav_sec_processed.csv # Consolidated NSE data
├── bse_equity_processed.csv   # Consolidated BSE data
├── nse_equity_list.csv        # NSE equity list with ISIN
└── bse_nse_securities.csv     # Master mapping file
```

### Master Mapping Format

The `bse_nse_securities.csv` file contains:

```csv
BSE_SYMBOL,NSE_SYMBOL,COMPANY_NAME,ISIN
500325,RELIANCE,Reliance Industries Limited,INE002A01018
532540,TCS,Tata Consultancy Services Limited,INE467B01029
```

---

## Setup Requirements

### Dependencies

Install required packages:

```bash
pip install pandas yfinance httpx fastbt
```

### For yfinDataRetriever.py:
```bash
pip install yfinance pandas
```

### For NSE_BSE_masterCreator.py:
```bash
pip install pandas httpx fastbt
```

### Directory Structure

Create the following directory structure:

```
data/
├── bse_nse_securities.csv  # Input file for yfinDataRetriever
├── EQUITY_L.csv           # NSE equity list (optional - auto-downloaded)
└── BSE Equity.csv         # BSE equity list (manual download required)
```

---

## Quick Start

### 1. Setup Environment
```bash
# Install dependencies
pip install pandas yfinance httpx fastbt

# Create data directory
mkdir data
```

### 2. Create Symbol Mapping (First Time)
```bash
# Download BSE equity list manually from BSE website
# Place it as data/BSE Equity.csv

# Run master creator to generate mapping
python NSE_BSE_masterCreator.py
```

### 3. Download Yahoo Finance Data
```bash
# Configure yfinDataRetriever.py settings
# Run Yahoo Finance downloader
python yfinDataRetriever.py
```

### 4. Download Official Market Data (Optional)
```bash
# Configure NSE_BSE_masterCreator.py for data download
# Set DOWNLOAD_NSE = True, DOWNLOAD_BSE = True
python NSE_BSE_masterCreator.py
```

---

## Best Practices

### Rate Limiting
- Use appropriate delays (0.5-3 seconds) for Yahoo Finance
- Take breaks every 20-50 stocks for large datasets
- Monitor for "Too Many Requests" errors

### Data Validation
- Check `skipped_stocks.csv` for data quality issues
- Verify minimum data requirements (30+ days)
- Review failed downloads in logs

### Error Handling
- Both modules include comprehensive error handling
- Check log files for detailed error information
- Skipped stocks are tracked separately from failures

### File Management
- Use absolute paths when possible
- Organize data by exchange and data type
- Regular cleanup of temporary files

---

## Troubleshooting

### Common Issues

1. **Yahoo Finance Rate Limiting**
   - Increase delays between requests
   - Reduce batch sizes
   - Use break intervals

2. **Missing Input Files**
   - Ensure `bse_nse_securities.csv` exists for yfinDataRetriever
   - Download `BSE Equity.csv` manually for mapping creation

3. **Network Issues**
   - Check internet connectivity
   - Verify firewall settings
   - Use alternative URLs in NSE_BSE_masterCreator

4. **Data Quality Issues**
   - Review `skipped_stocks.csv` for validation failures
   - Check minimum data requirements
   - Verify ISIN code matching for mapping

### Logging

Both modules provide comprehensive logging:
- Console output with timestamps
- Detailed log files in `data/logs/`
- Real-time error tracking
- Summary statistics

---

