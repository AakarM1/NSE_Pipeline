from time import time
import pandas as pd
import traceback
import duckdb
def process_bulk_deals(bulk_deals_csv, con):
        """
        Process bulk deals data and join with adjusted prices
        """
        print("[INFO]: Processing bulk deals data...")
        
        try:
            bulk_deals_df = pd.read_csv(bulk_deals_csv)
            if len(bulk_deals_df) == 0:
                print("[WARNING]: No records found in bulk deals data.")
                return
            print(f"[INFO]: Loaded {len(bulk_deals_df)} bulk deal records")
            
            # Remove spaces from column headers
            bulk_deals_df.columns = bulk_deals_df.columns.str.strip()
            print("[INFO]: Column headers:", bulk_deals_df.columns.tolist())
            
            # Convert date format from DD-MMM-YYYY to YYYY-MM-DD
            if 'Date' in bulk_deals_df.columns:
                bulk_deals_df['Date'] = pd.to_datetime(bulk_deals_df['Date'], format='%d-%b-%Y').dt.date
                print(f"[INFO]: Date conversion completed")
            # remove space from column names
            bulk_deals_df.columns = bulk_deals_df.columns.str.replace(' ', '_')
            bulk_deals_df.columns = bulk_deals_df.columns.str.replace('/', '')
            bulk_deals_df.columns = bulk_deals_df.columns.str.replace('.', '')
            
            # Convert 'Quantity' to numeric, handling errors
            if 'Quantity_Traded' in bulk_deals_df.columns:
                # Replace commas with empty string and convert to numeric
                bulk_deals_df['Quantity_Traded'] = bulk_deals_df['Quantity_Traded'].str.replace(',', '', regex=False)
                bulk_deals_df['Quantity_Traded'] = pd.to_numeric(bulk_deals_df['Quantity_Traded'], errors='coerce')
                print(f"[INFO]: Converted 'Quantity_Traded' to numeric")
            else:
                print("[WARNING]: 'Quantity_Traded' column not found in bulk deals data.")
            
            print(f"[INFO]: DataFrame shape: {bulk_deals_df.shape}")
            print(f"[INFO]: DataFrame columns: {list(bulk_deals_df.columns)}")
            
            # Convert trade_price to numeric, handling errors
            if 'Trade_Price__Wght_Avg_Price' in bulk_deals_df.columns:
                bulk_deals_df['Trade_Price__Wght_Avg_Price'] = pd.to_numeric(bulk_deals_df['Trade_Price__Wght_Avg_Price'], errors='coerce')
                print(f"[INFO]: Converted 'Trade_Price__Wght_Avg_Price' to numeric")
            else:
                print("[WARNING]: 'Trade_Price__Wght_Avg_Price' column not found in bulk deals data.")
                
                    
            con.register('tmp_bulk_deals', bulk_deals_df)
            con.execute("""
            CREATE OR REPLACE TABLE bulk_deals AS 
            SELECT * FROM tmp_bulk_deals;
            """)
            con.unregister('tmp_bulk_deals')
            
            # Join with bhav_adjusted_prices and calculate is_greater column
            print("[INFO]: Joining bulk deals with bhav_data...")
            result_df = con.execute("""
                SELECT
                bd.Symbol,
                bd.Date,
                SUM(bd.Quantity_Traded)        AS Quantity_Traded,
                AVG(bd.Trade_Price__Wght_Avg_Price) AS Trade_Price_Avg_Price,
                AVG(bcd.PREV_CLOSE)      AS PREV_CLOSE,
                AVG(bcd.OPEN_PRICE)      AS OPEN_PRICE,
                AVG(bcd.HIGH_PRICE)      AS HIGH_PRICE,
                AVG(bcd.LOW_PRICE)       AS LOW_PRICE,
                AVG(bcd.LAST_PRICE)      AS LAST_PRICE,
                AVG(bcd.CLOSE_PRICE)     AS CLOSE_PRICE,
                AVG(bcd.AVG_PRICE)       AS AVG_PRICE,
                AVG(bcd.TTL_TRD_QNTY)    AS TTL_TRD_QNTY,
                AVG(bcd.TURNOVER_LACS)   AS TURNOVER_LACS,
                AVG(bcd.NO_OF_TRADES)    AS NO_OF_TRADES,
                AVG(bcd.DELIV_QTY)       AS DELIV_QTY,
                AVG(bcd.DELIV_PER)       AS DELIV_PER,
                AVG(
                    CASE
                    WHEN bcd.CLOSE_PRICE > bcd.OPEN_PRICE THEN 1
                    ELSE 0
                    END
                )                         AS is_greater,
                AVG(
                    CASE
                    WHEN bcd.OPEN_PRICE = bcd.LOW_PRICE THEN 1
                    ELSE 0
                    END
                )                         AS is_open_equal_low
                FROM bulk_deals bd
                LEFT JOIN bhav_complete_data bcd
                ON bd.Symbol = bcd.SYMBOL
                AND bd.DATE  = bcd.DATE1
                -- AND bcd.SERIES = 'EQ'
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
            

if __name__ == "__main__":
    bulk_deals_csv = 'data/Bulk-Deals.csv'
    con = duckdb.connect(database='data/eod.duckdb', read_only=False)
    start_time = time()
    process_bulk_deals(bulk_deals_csv, con)
    con.close()
    end_time = time()
    print(f"[INFO]: Bulk deals processing completed in {end_time - start_time:.2f} seconds.")
    print("[INFO]: Process completed successfully.")