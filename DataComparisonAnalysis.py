import datetime
import duckdb
import pandas as pd
import yfinance as yf
"""
This script performs a final comparison analysis between the 
corrected EOD_CASH and NEW_BHAV datasets to check for errors
"""


class DataComparisonAnalysis:
    def __init__(self, startDate = None, endDate = None, tickerDict = None, con= None):
        if isinstance(startDate, str):
            self.startDate = datetime.datetime.strptime(startDate, '%Y-%m-%d')
        else:
            self.startDate = startDate  
        if isinstance(endDate, str):
            self.endDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
        else:
            self.endDate = endDate
        self.tickerDict = tickerDict 
        # Connect to DuckDB
        self.con = con # duckdb.connect(database='data/eod.duckdb', read_only=False)

        
        

    def get_overlap_period(self):
        # Leave as empty string '' or None to analyze all series
        self.SERIES_FILTER = 'EQ'  # Change this to filter by specific series (e.g., 'EQ', 'BE', 'SM', etc.)
        print("Series Filter:", self.SERIES_FILTER)
        print("Note: Series Filter changes only YoY analysis")
        # Create series filter condition
        self.series_condition_eod = ""
        self.series_condition_new = ""
        self.series_condition_join = ""
        if self.SERIES_FILTER and self.SERIES_FILTER.strip():
            self.series_condition_eod = f"AND TRIM(SERIES) = '{self.SERIES_FILTER.strip()}'"
            self.series_condition_new = f"AND TRIM(SERIES) = '{self.SERIES_FILTER.strip()}'"
            self.series_condition_join = f"AND TRIM(e.SERIES) = '{self.SERIES_FILTER.strip()}' AND TRIM(n.SERIES) = '{self.SERIES_FILTER.strip()}'"
            self.series_title_suffix = f" (Series: {self.SERIES_FILTER.strip()})"
        else:
            self.series_title_suffix = " (All Series)"
        print("=" * 55)
        print(f"=== CORRECTED EOD_CASH vs NEW_BHAV COMPARISON{self.series_title_suffix} ===")
        print("=" * 55)
        
        # Get overlap period
        overlap_query = """
        SELECT 
            GREATEST(
                (SELECT MIN(DATE1) FROM eod_cash),
                (SELECT MIN(DATE1) FROM new_bhav)
            ) AS overlap_start,
            LEAST(
                (SELECT MAX(DATE1) FROM eod_cash),
                (SELECT MAX(DATE1) FROM new_bhav)
            ) AS overlap_end
        """
        if self.con==None:
            self.con = duckdb.connect(database='data/eod.duckdb', read_only=False)
        self.overlap_dates = self.con.execute(overlap_query).fetchone()
        print(f"\nAnalyzing overlapping period: {self.overlap_dates[0]} to {self.overlap_dates[1]}")

    def records_analysis(self):
        # 1. CORRECTED COMMON RECORDS ANALYSIS
        print("\n1. CORRECTED COMMON RECORDS ANALYSIS")
        print("-" * 40)

        exact_matches = self.con.execute(f"""
        SELECT COUNT(*) FROM eod_cash e
        INNER JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
        {self.series_condition_join}
        """).fetchone()[0]

        print(f"Exact matches : {exact_matches:,}")

        # 2. Corrected Missing Records Analysis
        print("\n2. CORRECTED MISSING RECORDS ANALYSIS")
        print("-" * 40)

        missing_in_new = self.con.execute(f"""
        SELECT COUNT(*) FROM eod_cash e
        LEFT JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE n.SYMBOL IS NULL
        AND e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
        {self.series_condition_eod.replace('SERIES', 'e.SERIES')}
        """).fetchone()[0]

        missing_in_eod = self.con.execute(f"""
        SELECT COUNT(*) FROM new_bhav n
        LEFT JOIN eod_cash e 
        ON TRIM(n.SYMBOL) = TRIM(e.SYMBOL) 
        AND TRIM(n.SERIES) = TRIM(e.SERIES) 
        AND n.DATE1 = e.DATE1
        WHERE e.SYMBOL IS NULL
        AND n.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
        {self.series_condition_new.replace('SERIES', 'n.SERIES')}
        """).fetchone()[0]

        print(f"Records in EOD_CASH but not in NEW_BHAV: {missing_in_new:,}")
        print(f"Records in NEW_BHAV but not in EOD_CASH: {missing_in_eod:,}")

    def year_analysis(self):
        # 3. Year-by-Year Match Analysis
        print("\n3. YEAR-BY-YEAR MATCH ANALYSIS")
        print("-" * 35)

        yearly_matches = self.con.execute(f"""
        WITH new_yearly_counts AS (
            SELECT 
                EXTRACT(YEAR FROM DATE1) AS year,
                COUNT(*) AS new_total
            FROM new_bhav 
            WHERE DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
            {self.series_condition_new}
            GROUP BY EXTRACT(YEAR FROM DATE1)
        ),
        match_counts AS (
            SELECT 
                EXTRACT(YEAR FROM e.DATE1) AS year,
                COUNT(*) AS total_matches
            FROM eod_cash e
            INNER JOIN new_bhav n 
            ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
            AND TRIM(e.SERIES) = TRIM(n.SERIES) 
            AND e.DATE1 = n.DATE1
            WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
            {self.series_condition_join}
            GROUP BY EXTRACT(YEAR FROM e.DATE1)
        )
        SELECT 
            m.year,
            m.total_matches,
            m.total_matches * 100.0 / n.new_total AS match_percentage
        FROM match_counts m
        INNER JOIN new_yearly_counts n ON m.year = n.year
        ORDER BY m.year
        """).fetchall()

        print(f"{'Year':<6} {'Matches':<10} {'Match %':<10}")
        print("-" * 30)
        for row in yearly_matches:
            year, matches, match_pct = row
            print(f"{year:<6} {matches:<10,} {match_pct:<10.2f}%")

        # 4. Year-by-Year Error Percentage Analysis
        print("\n4. YEAR-BY-YEAR ERROR PERCENTAGE ANALYSIS")
        print("-" * 45)

        yearly_errors = self.con.execute(f"""
        WITH yearly_comparison AS (
            SELECT 
                EXTRACT(YEAR FROM e.DATE1) AS year,
                COUNT(*) AS total_matches,
                
                -- All column error counts
                SUM(CASE WHEN TRIM(e.SYMBOL) != TRIM(n.SYMBOL) THEN 1 ELSE 0 END) AS symbol_errors,
                SUM(CASE WHEN TRIM(e.SERIES) != TRIM(n.SERIES) THEN 1 ELSE 0 END) AS series_errors,
                SUM(CASE WHEN e.DATE1 != n.DATE1 THEN 1 ELSE 0 END) AS date_errors,
                SUM(CASE WHEN e.PREV_CLOSE != n.PREV_CLOSE THEN 1 ELSE 0 END) AS prev_close_errors,
                SUM(CASE WHEN e.OPEN_PRICE != n.OPEN_PRICE THEN 1 ELSE 0 END) AS open_errors,
                SUM(CASE WHEN e.HIGH_PRICE != n.HIGH_PRICE THEN 1 ELSE 0 END) AS high_errors,
                SUM(CASE WHEN e.LOW_PRICE != n.LOW_PRICE THEN 1 ELSE 0 END) AS low_errors,
                SUM(CASE WHEN e.LAST_PRICE != n.LAST_PRICE THEN 1 ELSE 0 END) AS last_errors,
                SUM(CASE WHEN e.CLOSE_PRICE != n.CLOSE_PRICE THEN 1 ELSE 0 END) AS close_errors,
                SUM(CASE WHEN e.AVG_PRICE != n.AVG_PRICE THEN 1 ELSE 0 END) AS avg_price_errors,
                SUM(CASE WHEN ROUND(e.AVG_PRICE, 2) != ROUND(n.AVG_PRICE, 2) THEN 1 ELSE 0 END) AS avg_price_errors_rounded,
                SUM(CASE WHEN e.TTL_TRD_QNTY != n.TTL_TRD_QNTY THEN 1 ELSE 0 END) AS volume_errors,
                SUM(CASE WHEN ABS(e.TURNOVER_LACS - n.TURNOVER_LACS) >= 0.01 THEN 1 ELSE 0 END) AS turnover_errors,
                SUM(CASE WHEN e.NO_OF_TRADES != n.NO_OF_TRADES THEN 1 ELSE 0 END) AS trades_errors,
                SUM(CASE WHEN e.DELIV_QTY != n.DELIV_QTY THEN 1 ELSE 0 END) AS deliv_qty_errors,
                SUM(CASE WHEN ABS(e.DELIV_PER - n.DELIV_PER) >= 0.01 THEN 1 ELSE 0 END) AS deliv_per_errors
                
            FROM eod_cash e
            INNER JOIN new_bhav n 
            ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
            AND TRIM(e.SERIES) = TRIM(n.SERIES) 
            AND e.DATE1 = n.DATE1
            WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
            {self.series_condition_join}
            GROUP BY EXTRACT(YEAR FROM e.DATE1)
        )
        SELECT 
            year,
            total_matches,
            
            -- Calculate error percentages for all columns
            CASE WHEN total_matches > 0 THEN symbol_errors * 100.0 / total_matches ELSE 0 END AS symbol_error_pct,
            CASE WHEN total_matches > 0 THEN series_errors * 100.0 / total_matches ELSE 0 END AS series_error_pct,
            CASE WHEN total_matches > 0 THEN date_errors * 100.0 / total_matches ELSE 0 END AS date_error_pct,
            CASE WHEN total_matches > 0 THEN prev_close_errors * 100.0 / total_matches ELSE 0 END AS prev_close_error_pct,
            CASE WHEN total_matches > 0 THEN open_errors * 100.0 / total_matches ELSE 0 END AS open_error_pct,
            CASE WHEN total_matches > 0 THEN high_errors * 100.0 / total_matches ELSE 0 END AS high_error_pct,
            CASE WHEN total_matches > 0 THEN low_errors * 100.0 / total_matches ELSE 0 END AS low_error_pct,
            CASE WHEN total_matches > 0 THEN last_errors * 100.0 / total_matches ELSE 0 END AS last_error_pct,
            CASE WHEN total_matches > 0 THEN close_errors * 100.0 / total_matches ELSE 0 END AS close_error_pct,
            CASE WHEN total_matches > 0 THEN avg_price_errors * 100.0 / total_matches ELSE 0 END AS avg_price_error_pct,
            CASE WHEN total_matches > 0 THEN avg_price_errors_rounded * 100.0 / total_matches ELSE 0 END AS avg_price_error_rounded_pct,
            CASE WHEN total_matches > 0 THEN volume_errors * 100.0 / total_matches ELSE 0 END AS volume_error_pct,
            CASE WHEN total_matches > 0 THEN turnover_errors * 100.0 / total_matches ELSE 0 END AS turnover_error_pct,
            CASE WHEN total_matches > 0 THEN trades_errors * 100.0 / total_matches ELSE 0 END AS trades_error_pct,
            CASE WHEN total_matches > 0 THEN deliv_qty_errors * 100.0 / total_matches ELSE 0 END AS deliv_qty_error_pct,
            CASE WHEN total_matches > 0 THEN deliv_per_errors * 100.0 / total_matches ELSE 0 END AS deliv_per_error_pct
            
        FROM yearly_comparison
        ORDER BY year
        """).fetchall()

        if yearly_errors:
            print("Year-by-Year Error Percentages for All Columns:")
            print()
            
            # Create column headers
            columns = ['SYMBOL', 'SERIES', 'DATE1', 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 
                    'LOW_PRICE', 'LAST_PRICE', 'CLOSE_PRICE', 'AVG_PRICE', 'AVG_PRICE_ROUNDED', 'TTL_TRD_QNTY', 
                    'TURNOVER_LACS', 'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER']
            
            # Print data in a more readable format
            for row in yearly_errors:
                year = row[0]
                total = row[1]
                errors = row[2:]  # All error percentages
                
                print(f"Year {year} (Total Matches: {total:,})")
                print("-" * 50)
                
                # Group columns for better readability
                for i, col in enumerate(columns):
                    error_pct = errors[i]
                    if error_pct > 0:
                        print(f"  {col:<15}: {error_pct:>8.2f}% ERROR")
                    else:
                        print(f"  {col:<15}: {error_pct:>8.2f}%")
                print()
            
            # Calculate overall error rates across all years
            print("=" * 60)
            print("OVERALL ERROR RATES (All Years Combined)")
            print("=" * 60)
            
            overall_errors = self.con.execute(f"""
            SELECT 
                COUNT(*) AS total_matches,
                SUM(CASE WHEN TRIM(e.SYMBOL) != TRIM(n.SYMBOL) THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS symbol_error_pct,
                SUM(CASE WHEN TRIM(e.SERIES) != TRIM(n.SERIES) THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS series_error_pct,
                SUM(CASE WHEN e.DATE1 != n.DATE1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS date_error_pct,
                SUM(CASE WHEN e.PREV_CLOSE != n.PREV_CLOSE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS prev_close_error_pct,
                SUM(CASE WHEN e.OPEN_PRICE != n.OPEN_PRICE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS open_error_pct,
                SUM(CASE WHEN e.HIGH_PRICE != n.HIGH_PRICE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS high_error_pct,
                SUM(CASE WHEN e.LOW_PRICE != n.LOW_PRICE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS low_error_pct,
                SUM(CASE WHEN e.LAST_PRICE != n.LAST_PRICE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS last_error_pct,
                SUM(CASE WHEN e.CLOSE_PRICE != n.CLOSE_PRICE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS close_error_pct,
                SUM(CASE WHEN e.AVG_PRICE != n.AVG_PRICE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS avg_price_error_pct,
                SUM(CASE WHEN ROUND(e.AVG_PRICE, 2) != ROUND(n.AVG_PRICE, 2) THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS avg_price_error_rounded_pct,
                SUM(CASE WHEN e.TTL_TRD_QNTY != n.TTL_TRD_QNTY THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS volume_error_pct,
                SUM(CASE WHEN ABS(e.TURNOVER_LACS - n.TURNOVER_LACS) >= 0.01 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS turnover_error_pct,
                SUM(CASE WHEN e.NO_OF_TRADES != n.NO_OF_TRADES THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS trades_error_pct,
                SUM(CASE WHEN e.DELIV_QTY != n.DELIV_QTY THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS deliv_qty_error_pct,
                SUM(CASE WHEN ABS(e.DELIV_PER - n.DELIV_PER) >= 0.01 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS deliv_per_error_pct
            FROM eod_cash e
            INNER JOIN new_bhav n 
            ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
            AND TRIM(e.SERIES) = TRIM(n.SERIES) 
            AND e.DATE1 = n.DATE1
            WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
            {self.series_condition_join}
            """).fetchone()
            
            if overall_errors:
                total = overall_errors[0]
                errors = overall_errors[1:]
                
                print(f"Total records analyzed: {total:,}")
                print()
                
                # Separate perfect columns from those with errors
                perfect_columns = []
                error_columns = []
                
                for i, col in enumerate(columns):
                    error_pct = errors[i]
                    if error_pct == 0:
                        perfect_columns.append(col)
                    else:
                        error_columns.append((col, error_pct))
                
                print("PERFECT COLUMNS (0.00% Error):")
                for col in perfect_columns:
                    print(f"  ✓ {col}")
                
                if error_columns:
                    print("\nCOLUMNS WITH ERRORS:")
                    for col, error_pct in error_columns:
                        print(f"  ✗ {col:<15}: {error_pct:>8.4f}%")
                
                print(f"\nSUMMARY: {len(perfect_columns)}/{len(columns)} columns are perfect, {len(error_columns)} have errors")
        else:
            print("No year-by-year error data available")
    
    def metrics_analysis(self):
        # 4A. Detailed AVG_PRICE Analysis
        print("\n4A. DETAILED AVG_PRICE ANALYSIS")
        print("-" * 40)

        avg_price_analysis = self.con.execute(f"""
        SELECT 
            COUNT(*) AS total_matches,
            
            -- Unrounded comparison
            SUM(CASE WHEN e.AVG_PRICE = n.AVG_PRICE THEN 1 ELSE 0 END) AS exact_matches_unrounded,
            SUM(CASE WHEN e.AVG_PRICE != n.AVG_PRICE THEN 1 ELSE 0 END) AS differences_unrounded,
            
            -- Rounded to 2 decimals comparison
            SUM(CASE WHEN ROUND(e.AVG_PRICE, 2) = ROUND(n.AVG_PRICE, 2) THEN 1 ELSE 0 END) AS exact_matches_rounded,
            SUM(CASE WHEN ROUND(e.AVG_PRICE, 2) != ROUND(n.AVG_PRICE, 2) THEN 1 ELSE 0 END) AS differences_rounded,
            
            -- Rounded to 4 decimals comparison
            SUM(CASE WHEN ROUND(e.AVG_PRICE, 4) = ROUND(n.AVG_PRICE, 4) THEN 1 ELSE 0 END) AS exact_matches_4dec,
            SUM(CASE WHEN ROUND(e.AVG_PRICE, 4) != ROUND(n.AVG_PRICE, 4) THEN 1 ELSE 0 END) AS differences_4dec,
            
            -- Average differences
            AVG(CASE WHEN e.AVG_PRICE != n.AVG_PRICE THEN ABS(e.AVG_PRICE - n.AVG_PRICE) END) AS avg_diff_unrounded,
            AVG(CASE WHEN ROUND(e.AVG_PRICE, 2) != ROUND(n.AVG_PRICE, 2) THEN ABS(e.AVG_PRICE - n.AVG_PRICE) END) AS avg_diff_rounded,
            
            -- Maximum differences
            MAX(CASE WHEN e.AVG_PRICE != n.AVG_PRICE THEN ABS(e.AVG_PRICE - n.AVG_PRICE) END) AS max_diff_unrounded,
            MAX(CASE WHEN ROUND(e.AVG_PRICE, 2) != ROUND(n.AVG_PRICE, 2) THEN ABS(e.AVG_PRICE - n.AVG_PRICE) END) AS max_diff_rounded
            
        FROM eod_cash e
        INNER JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
        AND e.AVG_PRICE IS NOT NULL AND n.AVG_PRICE IS NOT NULL
        {self.series_condition_join}
        """).fetchone()

        if avg_price_analysis:
            total, exact_unrounded, diff_unrounded, exact_rounded, diff_rounded, exact_4dec, diff_4dec, avg_diff_unrounded, avg_diff_rounded, max_diff_unrounded, max_diff_rounded = avg_price_analysis
            
            print(f"Total AVG_PRICE comparisons: {total:,}")
            print()
            print("PRECISION ANALYSIS:")
            print(f"  Exact matches (unrounded): {exact_unrounded:,} ({exact_unrounded/total*100:.2f}%)")
            print(f"  Differences (unrounded):   {diff_unrounded:,} ({diff_unrounded/total*100:.2f}%)")
            print()
            print(f"  Exact matches (2 decimals): {exact_rounded:,} ({exact_rounded/total*100:.2f}%)")
            print(f"  Differences (2 decimals):   {diff_rounded:,} ({diff_rounded/total*100:.2f}%)")
            print()
            print(f"  Exact matches (4 decimals): {exact_4dec:,} ({exact_4dec/total*100:.2f}%)")
            print(f"  Differences (4 decimals):   {diff_4dec:,} ({diff_4dec/total*100:.2f}%)")
            print()
            print("DIFFERENCE STATISTICS:")
            if avg_diff_unrounded:
                print(f"  Average difference (unrounded): ₹{avg_diff_unrounded:.6f}")
                print(f"  Maximum difference (unrounded): ₹{max_diff_unrounded:.6f}")
            if avg_diff_rounded:
                print(f"  Average difference (2 decimals): ₹{avg_diff_rounded:.6f}")
                print(f"  Maximum difference (2 decimals): ₹{max_diff_rounded:.6f}")

        # Sample of AVG_PRICE differences
        print("\nSAMPLE OF AVG_PRICE DIFFERENCES:")
        avg_price_sample = self.con.execute(f"""
        SELECT 
            e.SYMBOL, TRIM(e.SERIES) as series, e.DATE1,
            e.AVG_PRICE as eod_avg, n.AVG_PRICE as new_avg,
            ABS(e.AVG_PRICE - n.AVG_PRICE) as diff,
            ROUND(e.AVG_PRICE, 2) as eod_rounded, ROUND(n.AVG_PRICE, 2) as new_rounded
        FROM eod_cash e
        INNER JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
        AND e.AVG_PRICE != n.AVG_PRICE
        {self.series_condition_join}
        ORDER BY ABS(e.AVG_PRICE - n.AVG_PRICE) DESC
        LIMIT 10
        """).fetchall()

        if avg_price_sample:
            print(f"{'Symbol':<10} {'Series':<6} {'Date':<12} {'EOD_AVG':<12} {'NEW_AVG':<12} {'Diff':<12} {'EOD_R':<8} {'NEW_R':<8}")
            print("-" * 85)
            for row in avg_price_sample:
                symbol, series, date, eod_avg, new_avg, diff, eod_rounded, new_rounded = row
                print(f"{symbol:<10} {series:<6} {date} {eod_avg:<12.6f} {new_avg:<12.6f} {diff:<12.6f} {eod_rounded:<8.2f} {new_rounded:<8.2f}")
        else:
            print("No AVG_PRICE differences found!")

        # 4B. Sample DELIV_QTY Comparison
        print("\n4B. SAMPLE DELIV_QTY COMPARISON")
        print("-" * 35)

        deliv_qty_sample = self.con.execute(f"""
        SELECT 
            e.SYMBOL, TRIM(e.SERIES) as series, e.DATE1,
            e.DELIV_QTY as eod_deliv_qty, n.DELIV_QTY as new_deliv_qty,
            ABS(e.DELIV_QTY - n.DELIV_QTY) as diff,
            e.DELIV_PER as eod_deliv_per, n.DELIV_PER as new_deliv_per
        FROM eod_cash e
        INNER JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
        AND e.DELIV_QTY != n.DELIV_QTY
        {self.series_condition_join}
        ORDER BY ABS(e.DELIV_QTY - n.DELIV_QTY) DESC
        LIMIT 10
        """).fetchall()

        if deliv_qty_sample:
            print("Records with DELIV_QTY differences:")
            print(f"{'Symbol':<12} {'Series':<6} {'Date':<12} {'EOD_DELIV':<12} {'NEW_DELIV':<12} {'Diff':<12} {'EOD_PER':<8} {'NEW_PER':<8}")
            print("-" * 95)
            for row in deliv_qty_sample:
                symbol, series, date, eod_deliv, new_deliv, diff, eod_per, new_per = row
                print(f"{symbol:<12} {series:<6} {date} {eod_deliv:<12,} {new_deliv:<12,} {diff:<12,} {eod_per:<8.2f} {new_per:<8.2f}")
            # pd.DataFrame(deliv_qty_sample).to_csv('data/deliv_qty_differences.csv', index=False)
        else:
            print("No DELIV_QTY differences found!")

        # Sample of matching DELIV_QTY records
        print("\nSample of matching DELIV_QTY records:")
        deliv_qty_match_sample = self.con.execute(f"""
        SELECT 
            e.SYMBOL, TRIM(e.SERIES) as series, e.DATE1,
            e.DELIV_QTY as eod_deliv_qty, n.DELIV_QTY as new_deliv_qty,
            e.DELIV_PER as eod_deliv_per, n.DELIV_PER as new_deliv_per
        FROM eod_cash e
        INNER JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
        AND e.DELIV_QTY = n.DELIV_QTY
        AND e.DELIV_QTY > 0  -- Show only non-zero values
        {self.series_condition_join}
        ORDER BY e.DATE1 DESC, e.SYMBOL
        LIMIT 10
        """).fetchall()

        if deliv_qty_match_sample:
            print(f"{'Symbol':<12} {'Series':<6} {'Date':<12} {'EOD_DELIV':<12} {'NEW_DELIV':<12} {'EOD_PER':<8} {'NEW_PER':<8}")
            print("-" * 80)
            for row in deliv_qty_match_sample:
                symbol, series, date, eod_deliv, new_deliv, eod_per, new_per = row
                print(f"{symbol:<12} {series:<6} {date} {eod_deliv:<12,} {new_deliv:<12,} {eod_per:<8.2f} {new_per:<8.2f}")
        else:
            print("No matching DELIV_QTY records found!")

        # 5. Price Comparison Analysis
        print("\n5. PRICE COMPARISON ANALYSIS")
        print("-" * 35)

        price_comparison = self.con.execute(f"""
        SELECT 
            COUNT(*) AS total_matches,
            
            -- Count exact price matches
            SUM(CASE WHEN e.CLOSE_PRICE = n.CLOSE_PRICE THEN 1 ELSE 0 END) AS exact_close_matches,
            SUM(CASE WHEN e.OPEN_PRICE = n.OPEN_PRICE THEN 1 ELSE 0 END) AS exact_open_matches,
            SUM(CASE WHEN e.HIGH_PRICE = n.HIGH_PRICE THEN 1 ELSE 0 END) AS exact_high_matches,
            SUM(CASE WHEN e.LOW_PRICE = n.LOW_PRICE THEN 1 ELSE 0 END) AS exact_low_matches,
            SUM(CASE WHEN e.LAST_PRICE = n.LAST_PRICE THEN 1 ELSE 0 END) AS exact_last_matches,
            
            -- Average differences for non-matching prices
            AVG(CASE WHEN e.CLOSE_PRICE != n.CLOSE_PRICE THEN ABS(e.CLOSE_PRICE - n.CLOSE_PRICE) END) AS avg_close_diff,
            AVG(CASE WHEN e.OPEN_PRICE != n.OPEN_PRICE THEN ABS(e.OPEN_PRICE - n.OPEN_PRICE) END) AS avg_open_diff,
            AVG(CASE WHEN e.HIGH_PRICE != n.HIGH_PRICE THEN ABS(e.HIGH_PRICE - n.HIGH_PRICE) END) AS avg_high_diff,
            AVG(CASE WHEN e.LOW_PRICE != n.LOW_PRICE THEN ABS(e.LOW_PRICE - n.LOW_PRICE) END) AS avg_low_diff,
            AVG(CASE WHEN e.LAST_PRICE != n.LAST_PRICE THEN ABS(e.LAST_PRICE - n.LAST_PRICE) END) AS avg_last_diff,
            
            -- Count differences
            SUM(CASE WHEN e.CLOSE_PRICE != n.CLOSE_PRICE THEN 1 ELSE 0 END) AS close_diffs,
            SUM(CASE WHEN e.OPEN_PRICE != n.OPEN_PRICE THEN 1 ELSE 0 END) AS open_diffs,
            SUM(CASE WHEN e.HIGH_PRICE != n.HIGH_PRICE THEN 1 ELSE 0 END) AS high_diffs,
            SUM(CASE WHEN e.LOW_PRICE != n.LOW_PRICE THEN 1 ELSE 0 END) AS low_diffs,
            SUM(CASE WHEN e.LAST_PRICE != n.LAST_PRICE THEN 1 ELSE 0 END) AS last_diffs
            
        FROM eod_cash e
        INNER JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
        AND e.CLOSE_PRICE IS NOT NULL AND n.CLOSE_PRICE IS NOT NULL
        AND e.OPEN_PRICE IS NOT NULL AND n.OPEN_PRICE IS NOT NULL
        AND e.HIGH_PRICE IS NOT NULL AND n.HIGH_PRICE IS NOT NULL
        AND e.LOW_PRICE IS NOT NULL AND n.LOW_PRICE IS NOT NULL
        AND e.LAST_PRICE IS NOT NULL AND n.LAST_PRICE IS NOT NULL
        {self.series_condition_join}
        """).fetchone()

        if price_comparison[0] > 0:
            total = price_comparison[0]
            print(f"Total price comparisons: {total:,}")
            
            print(f"\nExact Price Matches:")
            print(f"  Close: {price_comparison[1]:,} ({price_comparison[1]/total*100:.2f}%)")
            print(f"  Open:  {price_comparison[2]:,} ({price_comparison[2]/total*100:.2f}%)")
            print(f"  High:  {price_comparison[3]:,} ({price_comparison[3]/total*100:.2f}%)")
            print(f"  Low:   {price_comparison[4]:,} ({price_comparison[4]/total*100:.2f}%)")
            print(f"  Last:  {price_comparison[5]:,} ({price_comparison[5]/total*100:.2f}%)")
            
            if price_comparison[11] > 0:  # If there are differences
                print(f"\nPrice Differences:")
                print(f"  Close: {price_comparison[11]:,} differences, avg ₹{price_comparison[6]:.4f}")
                print(f"  Open:  {price_comparison[12]:,} differences, avg ₹{price_comparison[7]:.4f}")
                print(f"  High:  {price_comparison[13]:,} differences, avg ₹{price_comparison[8]:.4f}")
                print(f"  Low:   {price_comparison[14]:,} differences, avg ₹{price_comparison[9]:.4f}")
                print(f"  Last:  {price_comparison[15]:,} differences, avg ₹{price_comparison[10]:.4f}")

        # 6. Volume Comparison Analysis
        print("\n6. VOLUME COMPARISON ANALYSIS")
        print("-" * 35)

        volume_comparison = self.con.execute(f"""
        SELECT 
            COUNT(*) AS total_matches,
            SUM(CASE WHEN e.TTL_TRD_QNTY = n.TTL_TRD_QNTY THEN 1 ELSE 0 END) AS exact_volume_matches,
            SUM(CASE WHEN e.NO_OF_TRADES = n.NO_OF_TRADES THEN 1 ELSE 0 END) AS exact_trades_matches,
            SUM(CASE WHEN ABS(e.TURNOVER_LACS - n.TURNOVER_LACS) < 0.01 THEN 1 ELSE 0 END) AS exact_turnover_matches,
            
            -- Differences
            SUM(CASE WHEN e.TTL_TRD_QNTY != n.TTL_TRD_QNTY THEN 1 ELSE 0 END) AS volume_diffs,
            SUM(CASE WHEN e.NO_OF_TRADES != n.NO_OF_TRADES THEN 1 ELSE 0 END) AS trades_diffs,
            SUM(CASE WHEN ABS(e.TURNOVER_LACS - n.TURNOVER_LACS) >= 0.01 THEN 1 ELSE 0 END) AS turnover_diffs,
            
            -- Average differences for non-matching values
            AVG(CASE WHEN e.TTL_TRD_QNTY != n.TTL_TRD_QNTY THEN ABS(e.TTL_TRD_QNTY - n.TTL_TRD_QNTY) END) AS avg_volume_diff,
            AVG(CASE WHEN e.NO_OF_TRADES != n.NO_OF_TRADES THEN ABS(e.NO_OF_TRADES - n.NO_OF_TRADES) END) AS avg_trades_diff,
            AVG(CASE WHEN ABS(e.TURNOVER_LACS - n.TURNOVER_LACS) >= 0.01 THEN ABS(e.TURNOVER_LACS - n.TURNOVER_LACS) END) AS avg_turnover_diff
            
        FROM eod_cash e
        INNER JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE e.DATE1 BETWEEN '{self.overlap_dates[0]}' AND '{self.overlap_dates[1]}'
        {self.series_condition_join}
        """).fetchone()

        if volume_comparison[0] > 0:
            total = volume_comparison[0]
            print(f"Total volume comparisons: {total:,}")
            
            print(f"\nExact Volume Matches:")
            print(f"  Volume:   {volume_comparison[1]:,} ({volume_comparison[1]/total*100:.2f}%)")
            print(f"  Trades:   {volume_comparison[2]:,} ({volume_comparison[2]/total*100:.2f}%)")
            print(f"  Turnover: {volume_comparison[3]:,} ({volume_comparison[3]/total*100:.2f}%)")
            
            if volume_comparison[4] > 0:
                print(f"\nVolume Differences:")
                print(f"  Volume:   {volume_comparison[4]:,} differences, avg {volume_comparison[7]:,.0f}")
                if volume_comparison[8] is not None:
                    print(f"  Trades:   {volume_comparison[5]:,} differences, avg {volume_comparison[8]:,.0f}")
                else:
                    print(f"  Trades:   {volume_comparison[5]:,} differences")
                if volume_comparison[9] is not None:
                    print(f"  Turnover: {volume_comparison[6]:,} differences, avg ₹{volume_comparison[9]:,.2f} lacs")
                else:
                    print(f"  Turnover: {volume_comparison[6]:,} differences")

    def sample_analysis(self):
        # 7. Sample of Exact Matches
        print("\n7. SAMPLE OF EXACT MATCHES")
        print("-" * 30)

        exact_sample = con.execute(f"""
        SELECT 
            e.SYMBOL, TRIM(e.SERIES) as series, e.DATE1,
            e.CLOSE_PRICE, e.TTL_TRD_QNTY, e.NO_OF_TRADES
        FROM eod_cash e
        INNER JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE e.DATE1 BETWEEN '{overlap_dates[0]}' AND '{overlap_dates[1]}'
        AND e.CLOSE_PRICE = n.CLOSE_PRICE
        AND e.TTL_TRD_QNTY = n.TTL_TRD_QNTY
        {series_condition_join}
        ORDER BY e.DATE1 DESC, e.SYMBOL
        LIMIT 10
        """).fetchall()

        print(f"{'Symbol':<12} {'Series':<6} {'Date':<12} {'Close':<10} {'Volume':<12} {'Trades':<8}")
        print("-" * 70)
        for row in exact_sample:
            symbol, series, date, close, volume, trades = row
            print(f"{symbol:<12} {series:<6} {date} {close:<10.2f} {volume:<12,} {trades:<8}")

        # 7. Sample of Mismatched Prices
        print("\n8. SAMPLE OF PRICE MISMATCHES")
        print("-" * 35)

        mismatch_sample = con.execute(f"""
        SELECT 
            e.SYMBOL, TRIM(e.SERIES) as series, e.DATE1,
            e.CLOSE_PRICE as eod_close, n.CLOSE_PRICE as new_close,
            ABS(e.CLOSE_PRICE - n.CLOSE_PRICE) as diff
        FROM eod_cash e
        INNER JOIN new_bhav n 
        ON TRIM(e.SYMBOL) = TRIM(n.SYMBOL) 
        AND TRIM(e.SERIES) = TRIM(n.SERIES) 
        AND e.DATE1 = n.DATE1
        WHERE e.DATE1 BETWEEN '{overlap_dates[0]}' AND '{overlap_dates[1]}'
        AND e.CLOSE_PRICE != n.CLOSE_PRICE
        {series_condition_join}
        ORDER BY ABS(e.CLOSE_PRICE - n.CLOSE_PRICE) DESC
        LIMIT 10
        """).fetchall()

        if mismatch_sample:
            print(f"{'Symbol':<12} {'Series':<6} {'Date':<12} {'EOD_Close':<10} {'NEW_Close':<10} {'Diff':<8}")
            print("-" * 70)
            for row in mismatch_sample:
                symbol, series, date, eod_close, new_close, diff = row
                print(f"{symbol:<12} {series:<6} {date} {eod_close:<10.2f} {new_close:<10.2f} {diff:<8.2f}")
        else:
            print("No price mismatches found!")

        # 9. Data Quality Score
        print("\n9. DATA QUALITY SCORE")
        print("-" * 25)

        if exact_matches > 0 and price_comparison[0] > 0:
            match_rate = exact_matches / (exact_matches + missing_in_new + missing_in_eod) * 100
            price_accuracy = price_comparison[1] / price_comparison[0] * 100
            volume_accuracy = volume_comparison[1] / volume_comparison[0] * 100
            
            print(f"Record Match Rate: {match_rate:.2f}%")
            print(f"Price Accuracy: {price_accuracy:.2f}%")
            print(f"Volume Accuracy: {volume_accuracy:.2f}%")
            print(f"Overall Quality Score: {(match_rate + price_accuracy + volume_accuracy) / 3:.2f}%")

    
    def analyze_data(self):
        self.get_overlap_period()
        self.records_analysis()
        self.year_analysis()
        self.metrics_analysis()
        self.sample_analysis()
        print("\n=== CORRECTED ANALYSIS COMPLETE ===")
        
    
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

        # 1. Define the symbol mapping
        symbols = self.tickerDict
        symbol_list = list(symbols.keys())
        ticker_list = list(symbols.values())
        
        # 2. Fetch data from Yahoo Finance
        print(f"Fetching data from Yahoo Finance for {len(ticker_list)} ticker(s)...")
        yfin_df = yf.download(ticker_list, start=self.startDate, end=self.endDate, auto_adjust=False, group_by='ticker')
        
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

        # 3. Fetch your calculated data from the database
        print("Fetching data from local 'bhav_adjusted_prices' table...")
        symbols_tuple = tuple(symbol_list)
        if self.con is None:
            self.con = duckdb.connect(database='data/eod.duckdb', read_only=False)
        bhav_adj_df = self.con.execute(f"""
            SELECT SYMBOL, DATE1, CLOSE_PRICE, ADJ_CLOSE_PRICE
            FROM bhav_adjusted_prices
            WHERE SYMBOL IN {symbols_tuple}
            AND DATE1 BETWEEN '{self.startDate.strftime('%Y-%m-%d')}' AND '{self.endDate.strftime('%Y-%m-%d')}'
            ORDER BY SYMBOL, DATE1
        """).df()
        bhav_adj_df['DATE1'] = pd.to_datetime(bhav_adj_df['DATE1']).dt.date
        print(f"Successfully fetched {len(bhav_adj_df)} records from local database.")

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
        output_path = f"data/yfin_vs_bhav_comparison_{self.startDate.strftime('%Y%m%d')}_to_{self.endDate.strftime('%Y%m%d')}.csv"
        merged_df.to_csv(output_path, index=False)
        print(f"\nDetailed comparison saved to: {output_path}")

if __name__ == "__main__":
    analysis = DataComparisonAnalysis(startDate=datetime(2020, 1, 1), endDate=datetime(2020, 12, 31))
    # analysis.analyze_data()
    analysis.compare_adj_close()