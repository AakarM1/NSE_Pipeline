import duckdb
import DataComparisonAnalysis, DataPreProcessor, DataRetriever
def main(fromDate, toDate, tickerDict, con):
    # # Step 1: Retrieve data
    data_retriever = DataRetriever.DataRetriever(fromDate, toDate, tickerDict, con)
    data_retriever.retrieve_bhav_data()
    data_retriever.create_oldBhav()
    data_retriever.create_secDel()
    data_retriever.merge_oldBhav_secDel()
    data_retriever.create_newBhav()
    data_retriever.create_finalDB()
    
    # # Step 2: Preprocess data - corporate actions
    pre_processor = DataPreProcessor.DataPreProcessor(fromDate, toDate, tickerDict, con)
    pre_processor.preprocess_data()

    # Step 3: Analyze data
    analysis = DataComparisonAnalysis.DataComparisonAnalysis(fromDate, toDate, tickerDict, con)
    analysis.compare_adj_close()

if __name__ == "__main__":
    fromDate = '2025-01-01'
    toDate = '2025-07-01'
    tickerDict = {
        '360ONE': '360ONE.NS',
        # 'CIEINDIA': 'CIEINDIA.NS',
        # 'CRISIL': 'CRISIL.NS',
        # 'DCMSRIND': 'DCMSRIND.NS',
    }
    con = duckdb.connect(database='data/eod.duckdb', read_only=False)
    main(fromDate, toDate, tickerDict, con)
    print("Data retrieval, preprocessing, and analysis completed successfully!")
    con.close()