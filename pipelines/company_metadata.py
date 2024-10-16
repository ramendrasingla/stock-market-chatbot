import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from utils.data_extraction import (get_analyst_recommendations, get_company_info, 
                                   get_financial_statements, get_historical_data)
from utils.data_storage import (connect_db, save_to_sqlite, initialize_pipeline_log, 
                                get_last_run_timestamp, update_pipeline_log)
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import traceback

# Function to scrape Indian stock market tickers
def scrape_nse_tickers(folder_path = "./data"):

    # Set up the Chrome WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    # Navigate to the page
    url = "https://www.nseindia.com/regulations/listing-compliance/nse-market-capitalisation-all-companies"
    driver.get(url)

    # Add a wait to ensure page loads completely (you can adjust the time if necessary)
    time.sleep(5)

    # Find the first row and extract the href from the download link
    first_row = driver.find_element(By.XPATH, '//tbody/tr[1]')
    excel_url = first_row.find_element(By.TAG_NAME, 'a').get_attribute('href')
    print("URL: {}".format(excel_url))
    driver.quit()

    # Save excel file and extract tickers
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(excel_url, headers=headers, allow_redirects=True, verify=False, timeout=10)
    file_path = os.path.join(folder_path, 'temp.xlsx')
    # Save the content as an .xlsx file
    with open(file_path, 'wb') as file:
        file.write(response.content)

    df = pd.read_excel('./data/temp.xlsx')
    nse_tickers = [ix + ".NS" for ix in df['Symbol'][:-2]]

    os.remove(file_path)

    return nse_tickers

# Function to execute a single company's pipeline
def execute_company_pipeline(conn, ticker, load_type='init'):
    ticker = ticker.strip()

    # Determine if it's an initial or delta load
    if load_type == 'delta':
        last_run_timestamp = get_last_run_timestamp(conn, ticker)
        if not last_run_timestamp:
            print(f"No previous run found for {ticker}. Running full init load.")
            load_type = 'init'  # Switch to init if no previous run

    # Fetch company data
    company_info = get_company_info(ticker)
    balance_sheet, income_statement, cash_flow = get_financial_statements(ticker)
    historical_data = get_historical_data(ticker)
    analyst_recommendations = get_analyst_recommendations(ticker)
    
    # Save data to SQLite
    save_to_sqlite(company_info, 'company_info', conn, ticker=ticker, id_columns=['ticker_id'])
    save_to_sqlite(balance_sheet, 'balance_sheet', conn, ticker=ticker, id_columns=['ticker_id', 'period'])
    save_to_sqlite(income_statement, 'income_statement', conn, ticker=ticker, id_columns=['ticker_id', 'period'])
    save_to_sqlite(cash_flow, 'cash_flow', conn, ticker=ticker, id_columns=['ticker_id', 'period'])
    save_to_sqlite(analyst_recommendations, 'analyst_recommendations', conn, ticker=ticker, id_columns=['ticker_id', 'period'])

    # Handle historical data for delta load
    if load_type == 'delta':
        new_historical_data = historical_data[historical_data.index > last_run_timestamp]
        save_to_sqlite(new_historical_data, 'historical_data', conn, ticker=ticker, id_columns=['ticker_id', 'period'])
    else:
        save_to_sqlite(historical_data, 'historical_data', conn, ticker=ticker, id_columns=['ticker_id', 'period'])

    # Update the pipeline log with the current timestamp
    update_pipeline_log(conn, ticker)

# Function to run the pipeline for all companies or specific tickers
def run_pipeline_for_companies(load_type='init', tickers=None, use_failed_tickers=False):
    print(f"Running pipeline for {('all' if not tickers else 'specified')} companies as {load_type} load...")

    conn = connect_db(db_name='company_metadata.db', folder_path='./data/raw')  # Connect to SQLite3 database
    
    # Initialize pipeline log for init load
    if load_type == 'init':
        initialize_pipeline_log(conn, script_path="./metadata/company_metadata/pipeline_log.sql")
    
    # Get tickers either from scraped tickers, passed tickers, or failed log
    if use_failed_tickers:
        log_file = './data/logs/failed_tickers.log'
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                tickers = [line.split(' - ')[0].strip() for line in f.readlines()]
            print(f"Found {len(tickers)} tickers in failed log.")
        else:
            print("No failed tickers log found.")
            return
    elif tickers is None:
        tickers = scrape_nse_tickers()
        print(f"Found {len(tickers)} tickers.")
    
    failed_tickers = []
    log_dir = './data/logs'

    # Ensure the logs directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = os.path.join(log_dir, 'failed_tickers.log')

    # Process each ticker
    for ticker in tqdm(tickers):
        print(f"Processing {ticker}...")
        try:
            execute_company_pipeline(conn, ticker, load_type)
        except Exception as e:
            # Log the error for this ticker
            print(f"Failed to process {ticker}. Error: {e}")
            error_trace = traceback.format_exc()
            print(f"Error details: {error_trace}")
            failed_tickers.append(ticker)
            # Save the failed ticker and error in the log file
            with open(log_file, 'a') as f:
                f.write(f"{ticker} - {str(e)}\n")
    
    # Close the database connection
    conn.close()

    print("Completed loading data for companies.")

    # Log the failed tickers if any
    if failed_tickers:
        print(f"Failed to process {len(failed_tickers)} tickers. Check 'failed_tickers.log' for details.")
    else:
        print("All tickers processed successfully.")

# Main function to initiate the pipeline
if __name__ == "__main__":
    load_type = input("Enter load type ('init' or 'delta'): ").lower()
    specific_tickers = input("Enter specific tickers (comma separated) or press Enter to skip: ").split(',')
    use_failed = input("Use failed tickers log? (y/n): ").lower() == 'y'
    
    # Run pipeline based on input
    if specific_tickers[0]:  # If specific tickers are provided
        run_pipeline_for_companies(load_type=load_type, tickers=specific_tickers)
    else:
        run_pipeline_for_companies(load_type=load_type, use_failed_tickers=use_failed)