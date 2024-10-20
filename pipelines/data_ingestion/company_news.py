import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import traceback
from tqdm import tqdm
from datetime import datetime
from utils.helper_funcs import setup_logging, to_utc
from utils.constants import START_DATE
from utils.data_extraction import scrape_nse_tickers, fetch_articles
from utils.data_storage import (connect_db, initialize_table,
                                save_articles, log_published_dates)

# Setup Logging
global logger
logger = setup_logging()

# Function to execute a single company's pipeline
# TODO: Need to adjust the functions and the nested functions to accomodate ticker and company_name
def execute_company_pipeline(conn, ticker, load_type='init'):
    """
    Processes news articles for a given company (ticker). Depending on the load type, it fetches
    all articles from START_DATE to the current date for the initial load or fetches only
    new articles (delta load) based on the latest published date in the log.

    Args:
    conn: sqlite3.Connection: The SQLite database connection.
    ticker (str): The ticker symbol of the company to process.
    load_type (str): The type of load to perform ('init' for initial load, 'delta' for delta load).
    """
    cursor = conn.cursor()
    all_articles = []

    if load_type == 'init':
        from_date = START_DATE  # Use the START_DATE from the .env file
        to_date = datetime.utcnow()  # Fetch until the current date

        logger.info(f"Initiating full load for {ticker} from {from_date} to {to_date}.")
        
        # Fetch all articles from START_DATE to the current date
        while True:
            articles, _, oldest_published_date = fetch_articles(ticker, from_date, to_date)
            if not articles:
                logger.info(f"No more articles found for {ticker} during the initial load.")
                break  # No more articles to fetch

            all_articles.extend(articles)
            logger.info(f"Fetched {len(articles)} articles for {ticker}.")
            from_date = to_utc(oldest_published_date)  # Update from_date to fetch older articles
            
    else:
        # Delta Load - Fetch from the latest date to current date
        cursor.execute('SELECT latest_published_date, oldest_published_date FROM pipeline_log WHERE ticker = ?', (ticker,))
        result = cursor.fetchone()

        if result:
            latest_published_date = datetime.fromisoformat(result[0])
            oldest_published_date = datetime.fromisoformat(result[1])

            logger.info(f"Performing delta load for {ticker}. Latest published date: {latest_published_date}, Oldest published date: {oldest_published_date}")

            # Fetch articles from latest published date to current date (newer articles)
            from_date = to_utc(latest_published_date)
            to_date = to_utc(datetime.utcnow())

            while True:
                articles, last_latest_date, _ = fetch_articles(ticker, from_date, to_date)
                if not articles:
                    logger.info(f"No more new articles found for {ticker} in the delta load.")
                    break
                all_articles.extend(articles)
                logger.info(f"Fetched {len(articles)} new articles for {ticker}.")
                from_date = to_utc(last_latest_date)  # Move from_date forward for newer articles

            # Fetch articles from START_DATE to the oldest published date in the log (older articles)
            if oldest_published_date:
                from_date = START_DATE
                to_date = oldest_published_date

                while True:
                    articles_old, last_latest_date_old, _ = fetch_articles(ticker, from_date, to_date)
                    if not articles_old:
                        logger.info(f"No more older articles found for {ticker} up to {oldest_published_date}.")
                        break
                    all_articles.extend(articles_old)
                    logger.info(f"Fetched {len(articles_old)} older articles for {ticker}.")
                    from_date = to_utc(last_latest_date_old)  # Continue fetching older articles
        else:
            logger.error(f"No pipeline log found for {ticker}. Please run the initial load first.")

    # Save articles after all fetching is done
    if all_articles:
        save_articles(conn, all_articles)  # Make sure to pass the connection to save_articles

        # Log the published dates after articles are saved
        oldest_date = min(article['published_date'] for article in all_articles)
        latest_date = max(article['published_date'] for article in all_articles)
        log_published_dates(ticker, oldest_date, latest_date)
        logger.info(f"Saved {len(all_articles)} articles for {ticker}. Oldest date: {oldest_date}, Latest date: {latest_date}.")
    else:
        logger.info(f"No articles to save for {ticker}.")

# Function to run the pipeline for all companies or specific tickers
def run_pipeline_for_companies(load_type='init', tickers=None, use_failed_tickers=False):

    logger.info(f"Running pipeline for {('all' if not tickers else 'specified')} companies as {load_type} load...")

    conn = connect_db(db_name='news_articles.db', folder_path='./data/raw')  # Connect to SQLite3 database
    
    # Initialize pipeline log for init load
    if load_type == 'init':
        initialize_table(conn, script_path="./metadata/news_articles/company_news.sql")
        initialize_table(conn, script_path="./metadata/news_articles/pipeline_log.sql")
    
    # Get tickers either from scraped tickers, passed tickers, or failed log
    if use_failed_tickers:
        log_file = './data/logs/failed_tickers.log'
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                tickers = list(set([line.split(' - ')[0].strip() for line in f.readlines()]))
            logger.info(f"Found {len(tickers)} tickers in failed log.")
            load_type = 'delta'
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
        logger.info(f"Failed to process {len(failed_tickers)} tickers. Check 'failed_tickers.log' for details.")
    else:
        logger.info("All tickers processed successfully.")

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
