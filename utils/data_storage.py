"""
Data Storage
"""

import json
import os
import re
import sqlite3
import sys

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from utils.helper_funcs import generate_id, setup_logging

# Setup Logging
global logger
logger = setup_logging()

# Connect to SQLite database


def connect_db(db_name=None, folder_path="./data"):
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


# Function to handle SQL errors(identifying problematic column(s))


def handle_sql_error(e, df):
    """
    Handle SQLite error by extracting the problematic parameter index
    and identifying the column causing the issue.

    Args:
    - e: Exception raised by the SQLite save operation.
    - df: The DataFrame being saved to SQLite.

    Returns:
    - None, but logs the problematic column name and sample values.
    """
    logger.error(f"Error saving to SQLite: {e}")

    # Regex to find the problematic index from the error message
    match = re.search(r"parameter (\d+)", str(e))

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
            logger.error(f"Sample Values: {df.iloc[:, problematic_index].head()}")

            # Optionally, log the unique values in the column
            logger.error(f"Unique Values: {df.iloc[:, problematic_index].unique()}")
        else:
            logger.error(
                f"Problematic index {problematic_index} is out of bounds for the DataFrame columns."
            )
    else:
        logger.error("Could not extract column index from the error message.")


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
            unique_constraint = ", ".join([f'"{col}"' for col in id_columns])
            constraints.append(f"UNIQUE ({unique_constraint})")

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
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
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

    new_columns = set(existing_columns) - set(df.columns)
    return set(existing_columns), new_columns


# Function to map Pandas data types to SQLite data types


def map_dtype_to_sqlite(dtype):
    """
    Map Pandas data types to SQLite compatible types.
    """
    if "int" in str(dtype):
        return "INTEGER"
    if "float" in str(dtype):
        return "REAL"
    return "TEXT"


# Main function to save the DataFrame to SQLite


def dict_to_sqlite(data, table_name, conn, id_columns=None):
    """
    Save the DataFrame to an SQLite table,
    creating it if it doesn't exist or adding missing columns if needed.
    """

    try:

        wrapped_data = {k: v if isinstance(v, list) else [v] for k, v in data.items()}
        wrapped_data = {k: v for k, v in wrapped_data.items() if len(v) == 1}

        df = pd.DataFrame(wrapped_data)

        if not table_exists(conn, table_name):
            create_table_if_not_exists(df, table_name, conn, id_columns)
        else:
            _ = add_missing_columns(df, table_name, conn)

        df = df.where(pd.notnull(df), None)

        # Insert/append rows into the SQLite table
        columns = ", ".join([f'"{col}"' for col in df.columns])
        placeholders = ", ".join(["?"] * len(df.columns))
        insert_sql = f"""
        INSERT INTO "{table_name}" ({columns})
        VALUES ({placeholders});
        """

        conn.executemany(insert_sql, df.values.tolist())
        conn.commit()
        logger.info(f"Data successfully inserted into '{table_name}'.")

    except Exception as e:
        # Debugging the DataFrame in case of failure
        logger.error(wrapped_data)
        raise e


def df_to_sqlite(df, table_name, id_columns, conn):
    """
    Append a DataFrame to an SQLite table, handling schema differences by
    altering the table for new columns and adding NULL values for missing columns.

    Parameters:
        df (pd.DataFrame): The DataFrame to append.
        table_name (str): The name of the SQLite table.
        db_path (str): The path to the SQLite database file.
    """
    # Get DataFrame columns
    df_columns = set(df.columns)

    if not table_exists(conn, table_name):
        create_table_if_not_exists(df, table_name, conn, id_columns)
        existing_columns = df_columns
        new_columns = set([])
    else:
        existing_columns, new_columns = add_missing_columns(df, table_name, conn)

    # Columns to add to the DataFrame
    missing_columns = existing_columns - df_columns
    for col in missing_columns:
        # Add missing columns to the DataFrame with NULL values
        df[col] = None

    # Ensure the column order matches between DataFrame and table
    df = df[[col for col in existing_columns | new_columns]]

    # Append the DataFrame to the table
    df.to_sql(table_name, conn, if_exists="append", index=False)
    logger.info(f"Data appended to table '{table_name}'.")

    # Commit the connection
    conn.commit()
