import duckdb
import DataComparisonAnalysis, DataPreProcessor, DataRetriever
import os
def main(fromDate, toDate, tickerDict, con, create_database=False):
    # # Step 1: Retrieve data
    if create_database:
        data_retriever = DataRetriever.DataRetriever(fromDate, toDate, con)
        data_retriever.retrieve_bhav_data()
        data_retriever.create_oldBhav()
        data_retriever.create_secDel()
        data_retriever.merge_oldBhav_secDel()
        data_retriever.create_newBhav()
        data_retriever.create_finalDB() #creates bhav_complete_data.csv
    
    # # Step 2: Preprocess data - corporate actions
    pre_processor = DataPreProcessor.DataPreProcessor(fromDate, toDate, tickerDict, con)
    pre_processor.preprocess_data() #creates bhav_adjusted_prices.csv 

    # Step 3: Analyze data
    # NOTE: tickerDict is used to map ticker names to their respective symbols
    #       in the database, so it should contain the correct mappings and must be initialized
    #       for this step.
    # analysis = DataComparisonAnalysis.DataComparisonAnalysis(fromDate, toDate, tickerDict, con)
    # analysis.compare_adj_close()

if __name__ == "__main__":
    # Parameters
    fromDate = '2025-01-01'
    toDate = '2025-07-01'
    tickerDict = {
        # '360ONE': '360ONE.NS',
        # 'CIEINDIA': 'CIEINDIA.NS',
        # 'CRISIL': 'CRISIL.NS',
        # 'DCMSRIND': 'DCMSRIND.NS',
    }
    create_database = True # Set to True to create a new database
    
    
    # create a data folder if it doesn't exist
    if not os.path.exists('data'):
        os.makedirs('data')
    con = duckdb.connect(database='data/eod.duckdb', read_only=False)
    main(fromDate, toDate, tickerDict, con, create_database)
    print("Data retrieval, preprocessing, and analysis completed successfully!")
    con.close()