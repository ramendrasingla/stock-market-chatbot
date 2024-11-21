import os
import re
import sqlite3
import pandas as pd
from datetime import datetime
from utils.helper_funcs import generate_id, setup_logging

# Setup Logging
global logger
logger = setup_logging()

# Connect to SQLite database


def connect_db(db_name=None, folder_path='./data'):
    # Create the folder if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logger.info(f"Created folder: {folder_path}")

    # Full path to the database
    db_path = os.path.join(folder_path, db_name)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    logger.info(f"Connected to database at: {db_path}")

    return conn


def initialize_table(conn, script_path="pipeline_log.sql"):

    with open(script_path, 'r') as file:
        sql_script = file.read()
        try:
            # Execute the SQL script
            conn.executescript(sql_script)
            logger.info(f'Successfully executed: {script_path.split("/")[-1]}')
        except sqlite3.Error as e:
            logger.error(f'Error executing {script_path.split("/")[-1]}: {e}')

    conn.commit()
    return

# Get the last run timestamp for a given ticker


def get_last_run_timestamp(conn, ticker, timestamp_col='last_run'):
    query = f"SELECT {timestamp_col} FROM pipeline_log WHERE ticker = ? ORDER BY {timestamp_col} DESC LIMIT 1;"
    cursor = conn.execute(query, (ticker,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None


def handle_sql_error(e, df):
    """
    Handle SQLite error by extracting the problematic parameter index and identifying the column causing the issue.

    Args:
    - e: Exception raised by the SQLite save operation.
    - df: The DataFrame being saved to SQLite.

    Returns:
    - None, but logs the problematic column name and sample values.
    """
    logger.error(f"Error saving to SQLite: {e}")

    # Regex to find the problematic index from the error message
    match = re.search(r'parameter (\d+)', str(e))

    if match:
        # Extract the problematic parameter index (SQLite uses 1-based index,
        # Python uses 0-based)
        problematic_index = int(match.group(1)) - 1

        # Ensure index is within DataFrame column range
        if problematic_index < len(df.columns):
            # Get the problematic column name
            problematic_col_name = df.columns[problematic_index]

            # Log the column name and sample values
            logger.error(f"Problematic Column: {problematic_col_name}")
            logger.error(
                f"Sample Values: {df.iloc[:, problematic_index].head()}")

            # Optionally, log the unique values in the column
            logger.error(
                f"Unique Values: {df.iloc[:, problematic_index].unique()}")
        else:
            logger.error(
                f"Problematic index {problematic_index} is out of bounds for the DataFrame columns.")
    else:
        logger.error("Could not extract column index from the error message.")

# Helper function to preprocess the DataFrame


def preprocess_dataframe(df):
    """
    Preprocess the DataFrame: format datetime columns, adjust numeric types,
    and handle object and boolean columns for SQLite compatibility.
    """
    if 'period' in df.columns and pd.api.types.is_datetime64_any_dtype(
            df['period']):
        df['period'] = df['period'].dt.strftime('%Y-%m-%d %H:%M:%S')

    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = handle_numeric_columns(df[col])
        elif pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str)
        elif pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].astype(int)

    return df


def handle_numeric_columns(column):
    """
    Adjust numeric column types for SQLite compatibility.
    Convert int64 to float64 if necessary.
    """
    if column.dtype == 'int64':
        return column.astype('float64')
    elif column.dtype == 'float64':
        return column.astype('float64')
    return column

# Function to create a table in SQLite if it doesn't exist


def create_table_if_not_exists(df, table_name, conn, id_columns):
    """
    Create a table if it does not already exist, with the appropriate columns
    and constraints, based on the DataFrame schema.
    """
    create_columns = []
    for col in df.columns:
        sqlite_type = map_dtype_to_sqlite(df[col].dtype)
        if col not in id_columns:
            create_columns.append(f'"{col}" {sqlite_type}')
        else:
            create_columns.append(f'"{col}" {sqlite_type} NOT NULL')

    constraints = []
    if id_columns:
        if len(id_columns) == 1:
            pk_column = id_columns[0]
            constraints.append(f'PRIMARY KEY ("{pk_column}")')
        else:
            unique_constraint = ', '.join([f'"{col}"' for col in id_columns])
            constraints.append(f'UNIQUE ({unique_constraint})')

    create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(create_columns + constraints)});'

    cursor = conn.cursor()
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info(f"Table '{table_name}' created successfully.")
    except Exception as e:
        handle_sql_error(e, df)
    finally:
        cursor.close()

# Function to check if the table exists


def table_exists(conn, table_name):
    """
    Check if a table exists in the SQLite database.
    """
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    exists = cursor.fetchone() is not None
    cursor.close()
    return exists

# Function to add missing columns if needed


def add_missing_columns(df, table_name, conn):
    """
    Add any missing columns to the table that are present in the DataFrame.
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [row[1] for row in cursor.fetchall()]
    cursor.close()

    missing_columns = set(df.columns) - set(existing_columns)
    for col in missing_columns:
        sqlite_type = map_dtype_to_sqlite(df[col].dtype)
        alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {sqlite_type};'
        cursor = conn.cursor()
        try:
            cursor.execute(alter_sql)
            conn.commit()
            logger.info(f"Added column '{col}' to table '{table_name}'.")
        except Exception as e:
            handle_sql_error(e, df)
        finally:
            cursor.close()

# Function to map Pandas data types to SQLite data types


def map_dtype_to_sqlite(dtype):
    """
    Map Pandas data types to SQLite compatible types.
    """
    if 'int' in str(dtype):
        return 'INTEGER'
    elif 'float' in str(dtype):
        return 'REAL'
    return 'TEXT'

# Function to insert or upsert data


def upsert_data(df, table_name, conn, id_columns):
    """
    Insert or upsert data into the SQLite table, handling conflicts on primary key or unique constraints.
    """
    columns = ', '.join([f'"{col}"' for col in df.columns])
    placeholders = ', '.join(['?'] * len(df.columns))
    update_clause = ', '.join(
        [f'"{col}" = excluded."{col}"' for col in df.columns if col not in id_columns])

    upsert_sql = f'''
    INSERT INTO "{table_name}" ({columns})
    VALUES ({placeholders})
    ON CONFLICT({', '.join([f'"{col}"' for col in id_columns])}) DO UPDATE SET {update_clause};
    '''

    try:
        conn.executemany(upsert_sql, df.values.tolist())
        conn.commit()
        logger.info(f"Data successfully upserted to '{table_name}'.")
    except Exception as e:
        handle_sql_error(e, df)

# Main function to save the DataFrame to SQLite


def save_to_sqlite(data, table_name, conn, ticker,
                   id_columns=None, mode='replace'):
    """
    Save the DataFrame to an SQLite table, creating it if it doesn't exist or adding missing columns if needed.
    """

    try:
        wrapped_data = {k: v if isinstance(
            v, list) else [v] for k, v in data.items()}
        df = pd.DataFrame(wrapped_data)

        df['ticker'] = ticker
        df['ticker_id'] = str(generate_id(ticker))

        df = preprocess_dataframe(df)

        if not table_exists(conn, table_name):
            create_table_if_not_exists(df, table_name, conn, id_columns)
        else:
            add_missing_columns(df, table_name, conn)

        df = df.where(pd.notnull(df), None)
        upsert_data(df, table_name, conn, id_columns)

    except Exception as e:
        # Debugging DF
        logger.error(data)
        raise e

# Function to update the pipeline log


def update_pipeline_log(
        conn, ticker, timestamp_col='last_run', latest_timestamp=None):
    """
    Insert or update the pipeline log with the current timestamp.
    """
    if not latest_timestamp:
        latest_timestamp = datetime.now()

    query = f"INSERT INTO pipeline_log (ticker, {timestamp_col}) VALUES (?, ?)"

    try:
        conn.execute(query, (ticker, latest_timestamp))
        conn.commit()
    except sqlite3.IntegrityError as e:
        logger.error(f"Error updating pipeline log: {e}")


def log_published_dates(conn, ticker, oldest_date, latest_date):
    """Logs the oldest and latest published dates for a specific company in the pipeline_log table."""
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO pipeline_log (ticker, oldest_published_date, latest_published_date)
        VALUES (?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
        oldest_published_date=excluded.oldest_published_date,
        latest_published_date=excluded.latest_published_date
    ''', (ticker, oldest_date.isoformat(), latest_date.isoformat()))
    conn.commit()
