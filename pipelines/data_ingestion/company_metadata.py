import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import traceback
from tqdm import tqdm
from utils.helper_funcs import setup_logging
from utils.data_extraction import (get_analyst_recommendations, get_company_info,
                                   get_financial_statements, get_historical_data, scrape_nse_tickers)
from utils.data_storage import (connect_db, save_to_sqlite, initialize_table,
                                get_last_run_timestamp, update_pipeline_log)


# Setup Logging
global logger
logger = setup_logging()

# Function to execute a single company's pipeline


def execute_company_pipeline(conn, ticker, load_type='init'):
    ticker = ticker.strip()

    # Determine if it's an initial or delta load
    if load_type == 'delta':
        last_run_timestamp = get_last_run_timestamp(conn, ticker)
        if not last_run_timestamp:
            logger.info(
                f"No previous run found for {ticker}. Running full init load.")
            load_type = 'init'  # Switch to init if no previous run

    # Fetch company data
    company_info = get_company_info(ticker)
    balance_sheet, income_statement, cash_flow = get_financial_statements(
        ticker)
    historical_data = get_historical_data(ticker)
    analyst_recommendations = get_analyst_recommendations(ticker)

    # Save data to SQLite
    save_to_sqlite(company_info, 'company_info', conn,
                   ticker=ticker, id_columns=['ticker_id'])
    save_to_sqlite(balance_sheet, 'balance_sheet', conn,
                   ticker=ticker, id_columns=['ticker_id', 'period'])
    save_to_sqlite(income_statement, 'income_statement', conn,
                   ticker=ticker, id_columns=['ticker_id', 'period'])
    save_to_sqlite(cash_flow, 'cash_flow', conn, ticker=ticker,
                   id_columns=['ticker_id', 'period'])
    save_to_sqlite(analyst_recommendations, 'analyst_recommendations',
                   conn, ticker=ticker, id_columns=['ticker_id', 'period'])

    # Handle historical data for delta load
    if load_type == 'delta':
        new_historical_data = historical_data[historical_data.index >
                                              last_run_timestamp]
        save_to_sqlite(new_historical_data, 'historical_data',
                       conn, ticker=ticker, id_columns=['ticker_id', 'period'])
    else:
        save_to_sqlite(historical_data, 'historical_data', conn,
                       ticker=ticker, id_columns=['ticker_id', 'period'])

    # Update the pipeline log with the current timestamp
    update_pipeline_log(conn, ticker)

# Function to run the pipeline for all companies or specific tickers


def run_pipeline_for_companies(
        load_type='init', tickers=None, use_failed_tickers=False):

    logger.info(
        f"Running pipeline for {('all' if not tickers else 'specified')} companies as {load_type} load...")

    # Connect to SQLite3 database
    conn = connect_db(db_name='company_metadata.db', folder_path='./data/raw')

    # Initialize pipeline log for init load
    if load_type == 'init':
        initialize_table(
            conn, script_path="./metadata/company_metadata/pipeline_log.sql")

    # Get tickers either from scraped tickers, passed tickers, or failed log
    if use_failed_tickers:
        log_file = './data/logs/failed_tickers.log'
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                tickers = list(set([line.split(' - ')[0].strip()
                               for line in f.readlines()]))
            logger.info(f"Found {len(tickers)} tickers in failed log.")
        else:
            logger.info("No failed tickers log found.")
            return
    elif tickers is None:
        tickers = scrape_nse_tickers()
        logger.info(f"Found {len(tickers)} tickers.")

    failed_tickers = []
    log_dir = './data/logs'

    # Ensure the logs directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = os.path.join(log_dir, 'failed_tickers.log')

    # Process each ticker
    for ticker in tqdm(tickers):
        logger.info(f"Processing {ticker}...")
        try:
            execute_company_pipeline(conn, ticker, load_type)
        except Exception as e:
            # Log the error for this ticker
            logger.error(f"Failed to process {ticker}. Error: {e}")
            error_trace = traceback.format_exc()
            logger.error(f"Error details: {error_trace}")
            failed_tickers.append(ticker)
            # Save the failed ticker and error in the log file
            with open(log_file, 'a') as f:
                f.write(f"{ticker} - {str(e)}\n")

    # Close the database connection
    conn.close()

    logger.info("Completed loading data for companies.")

    # Log the failed tickers if any
    if failed_tickers:
        logger.info(
            f"Failed to process {len(failed_tickers)} tickers. Check 'failed_tickers.log' for details.")
    else:
        logger.info("All tickers processed successfully.")


# Main function to initiate the pipeline
if __name__ == "__main__":
    load_type = input("Enter load type ('init' or 'delta'): ").lower()
    specific_tickers = input(
        "Enter specific tickers (comma separated) or press Enter to skip: ").split(',')
    use_failed = input("Use failed tickers log? (y/n): ").lower() == 'y'

    # Run pipeline based on input
    if specific_tickers[0]:  # If specific tickers are provided
        run_pipeline_for_companies(
            load_type=load_type, tickers=specific_tickers)
    else:
        run_pipeline_for_companies(
            load_type=load_type, use_failed_tickers=use_failed)
