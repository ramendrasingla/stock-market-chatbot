import yfinance as yf
import pandas as pd

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
    balance_sheet['period'] = pd.to_datetime(balance_sheet.index, errors='coerce')
    income_statement['period'] = pd.to_datetime(income_statement.index, errors='coerce')
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
