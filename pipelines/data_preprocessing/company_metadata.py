import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import sqlite3
import pandas as pd

from utils.data_preprocessing import DataPreprocessor
from utils.data_storage import connect_db
from utils.helper_funcs import setup_logging

# Setup Logging
global logger
logger = setup_logging()

def preprocess_table(table_name, raw_table_conn):
    """
    Preprocess numeric and datetime-like columns in a database table.

    Parameters:
        table_name (str): The name of the table to preprocess.
        db_path (str): Path to the SQLite database.

    Returns:
        None: Saves the preprocessed table back to the database.
    """
    
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", raw_table_conn)
    # Preprocess Numeric Columns
    numeric_processed_columns = []
    for col in df.columns:
        if df[col].dtype == "object" and DataPreprocessor.is_numeric_like(df[col]):
            logger.info(f"Preprocessing numeric-like column: {col}")

            if df[col].str.contains('\n', na=False).any():
                logger.info(f"Preprocessing column: {col} (contains rows with '\\n')")
                df[col] = DataPreprocessor.preprocess_latest_date_value(df[col])

            df[col] = DataPreprocessor.preprocess_nan_numeric_and_spaces(df[col])
            problematic_rows = DataPreprocessor.validate_post_preprocessing_for_numeric(df[col])
            if not problematic_rows.empty:
                logger.info(f"Dropping {len(problematic_rows)} problematic rows in column: {col}")
                df = df[~df.index.isin(problematic_rows.index)]
            numeric_processed_columns.append(col)

    # Preprocess Remaining Columns for Datetime
    remaining_columns = [col for col in df.columns if col not in numeric_processed_columns]
    for col in remaining_columns:
        if df[col].dtype == "object" and DataPreprocessor.is_datetime_like(df[col]):
            logger.info(f"Preprocessing datetime-like column: {col}")
            df[col] = DataPreprocessor.preprocess_datetime_column(df[col])
            problematic_rows = DataPreprocessor.validate_post_preprocessing_for_datetime(df[col])
            if not problematic_rows.empty:
                logger.info(f"Dropping {len(problematic_rows)} problematic rows in column: {col}")
                df = df[~df.index.isin(problematic_rows.index)]

    # Save the cleaned table back to the SQLite database
    final_conn = connect_db(db_name='company_metadata.db', folder_path='./data/preprocessed')
    df.to_sql(table_name, final_conn, if_exists="replace", index=False)
    final_conn.close()
    logger.info(f"Preprocessed and saved table: {table_name}")

# Main function to initiate the pipeline
if __name__ == "__main__":
    # Run preprocessing on all tables
    raw_table_conn = connect_db(db_name='company_metadata.db', folder_path='./data/raw')
    tables = ["company_info", "balance_sheet", "income_statement", "cash_flow", "historical_data"]

    for table in tables:
        logger.info(f"Preprocessing Table: {table}")
        preprocess_table(table, raw_table_conn)
    
    raw_table_conn.close()
