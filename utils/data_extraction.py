import os
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time

from utils.helper_funcs import setup_logging

# Setup Logging
global logger
logger = setup_logging()

# Function to scrape Indian stock market tickers


def scrape_nse_tickers(folder_path="./data", output_col='Symbol'):

    # Set up the Chrome WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    # Navigate to the page
    # TODO: Need to add it to constants
    url = "https://www.nseindia.com/regulations/listing-compliance/nse-market-capitalisation-all-companies"
    driver.get(url)

    # Add a wait to ensure page loads completely (you can adjust the time if necessary)
    time.sleep(5)

    # Find the first row and extract the href from the download link
    first_row = driver.find_element(By.XPATH, '//tbody/tr[1]')
    excel_url = first_row.find_element(By.TAG_NAME, 'a').get_attribute('href')
    logger.info("URL: {}".format(excel_url))
    driver.quit()

    # Save excel file and extract tickers
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(excel_url, headers=headers,
                            allow_redirects=True, verify=False, timeout=10)
    file_path = os.path.join(folder_path, 'temp.xlsx')
    # Save the content as an .xlsx file
    with open(file_path, 'wb') as file:
        file.write(response.content)

    df = pd.read_excel('./data/temp.xlsx')
    if output_col == 'Symbol':
        nse_tickers = [ix + ".NS" for ix in df['Symbol'][:-2]]
    elif output_col == 'Company Name':
        nse_tickers = [ix for ix in df['Company Name'][:-2]]

    os.remove(file_path)

    return nse_tickers


def get_company_info(ticker):
    company = yf.Ticker(ticker)
    info = company.info
    return info


def get_financial_statements(ticker):
    company = yf.Ticker(ticker)
    balance_sheet = company.balance_sheet
    income_statement = company.financials
    cash_flow = company.cashflow

    # Transpose and clean up date columns
    balance_sheet = balance_sheet.T
    income_statement = income_statement.T
    cash_flow = cash_flow.T

    # Adding period
    balance_sheet['period'] = pd.to_datetime(
        balance_sheet.index, errors='coerce')
    income_statement['period'] = pd.to_datetime(
        income_statement.index, errors='coerce')
    cash_flow['period'] = pd.to_datetime(cash_flow.index, errors='coerce')

    return balance_sheet, income_statement, cash_flow


def get_historical_data(ticker, period='max'):
    company = yf.Ticker(ticker)
    hist_data = company.history(period=period)
    hist_data['period'] = pd.to_datetime(hist_data.index, errors='coerce')
    return hist_data


def get_analyst_recommendations(ticker):
    company = yf.Ticker(ticker)
    recommendations = company.recommendations
    return recommendations
