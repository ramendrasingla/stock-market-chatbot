"""
Data Ingestion pipeline for company metadata.
"""

import os
import sys
import traceback

import yfinance as yf
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from utils.data_extraction import (
    get_company_info,
    get_financial_statements,
    get_historical_data,
    scrape_nse_tickers,
)
from utils.data_storage import connect_db, df_to_sqlite, dict_to_sqlite
from utils.helper_funcs import generate_id, setup_logging

# Setup Logging
global logger
logger = setup_logging()

# Function to execute a single company's pipeline


def execute_company_pipeline(conn, ticker):
    ticker = ticker.strip()
    yf_ticker = yf.Ticker(ticker)
    # Fetch company data
    company_info = get_company_info(yf_ticker)
    balance_sheet, income_statement, cash_flow = get_financial_statements(yf_ticker)
    historical_data = get_historical_data(yf_ticker)
    ticker_id = generate_id(ticker)

    # Save data to SQLite
    company_info["ticker"], company_info["ticker_id"] = ticker, ticker_id
    del company_info["companyOfficers"]
    dict_to_sqlite(company_info, "company_info", conn, id_columns=["ticker_id"])

    balance_sheet["ticker"], balance_sheet["ticker_id"] = ticker, ticker_id
    df_to_sqlite(
        table_name="balance_sheet", df=balance_sheet, id_columns=["ticker_id", "period"], conn=conn
    )

    income_statement["ticker"], income_statement["ticker_id"] = ticker, ticker_id
    df_to_sqlite(
        table_name="income_statement",
        df=income_statement,
        id_columns=["ticker_id", "period"],
        conn=conn,
    )

    cash_flow["ticker"], cash_flow["ticker_id"] = ticker, ticker_id
    df_to_sqlite(
        table_name="cash_flow", df=cash_flow, id_columns=["ticker_id", "period"], conn=conn
    )

    historical_data["ticker"], historical_data["ticker_id"] = ticker, ticker_id
    df_to_sqlite(
        table_name="historical_data",
        df=historical_data,
        id_columns=["ticker_id", "period"],
        conn=conn,
    )


# Function to run the pipeline for all companies or specific tickers


def run_pipeline_for_companies(tickers=None, use_failed_tickers=False):

    logger.info(
        f"""Running pipeline for {('all' if not tickers else 'specified')} 
        companies"""
    )

    # Connect to SQLite3 database
    conn = connect_db(db_name="company_metadata.db", folder_path="./data/raw")

    # Get tickers either from scraped tickers, passed tickers, or failed log
    if use_failed_tickers:
        log_file = "./data/logs/failed_tickers.log"
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                tickers = list(set([line.split(" - ")[0].strip() for line in f.readlines()]))
            logger.info(f"Found {len(tickers)} tickers in failed log.")
        else:
            logger.info("No failed tickers log found.")
            return
    elif tickers is None:
        tickers = scrape_nse_tickers()
        logger.info(f"Found {len(tickers)} tickers.")

    failed_tickers = []
    log_dir = "./data/logs"

    # Ensure the logs directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = os.path.join(log_dir, "failed_tickers.log")

    # Process each ticker
    for ticker in tqdm(tickers):
        logger.info(f"Processing {ticker}...")
        try:
            execute_company_pipeline(conn, ticker)
        except Exception as e:
            # Log the error for this ticker
            logger.error(f"Failed to process {ticker}. Error: {e}")
            error_trace = traceback.format_exc()
            logger.error(f"Error details: {error_trace}")
            failed_tickers.append(ticker)
            # Save the failed ticker and error in the log file
            with open(log_file, "a") as f:
                f.write(f"{ticker} - {str(e)}\n")

    # Close the database connection
    conn.close()

    logger.info("Completed loading data for companies.")

    # Log the failed tickers if any
    if failed_tickers:
        logger.info(
            f"""Failed to process {len(failed_tickers)} tickers. 
            Check 'failed_tickers.log' for details."""
        )
    else:
        logger.info("All tickers processed successfully.")


# Main function to initiate the pipeline
if __name__ == "__main__":

    specific_tickers = input(
        "Enter specific tickers (comma separated) or press Enter to skip: "
    ).split(",")
    use_failed = input("Use failed tickers log? (y/n): ").lower() == "y"
    # Run pipeline based on input
    if specific_tickers[0]:  # If specific tickers are provided
        run_pipeline_for_companies(tickers=specific_tickers)
    else:
        run_pipeline_for_companies(use_failed_tickers=use_failed)
