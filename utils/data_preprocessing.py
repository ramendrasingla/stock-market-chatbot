import re
import pandas as pd
import numpy as np


class DataPreprocessor:
    @staticmethod
    def is_numeric_like(column):
        """
        Determine if a column is numeric-like based on its values.

        Parameters:
            column (pd.Series): Input column.

        Returns:
            bool: True if the column is numeric-like, False otherwise.
        """
        non_null_values = column[column.notnull()]
        numeric_count = sum(
            isinstance(val, (int, float)) or
            str(val).replace('.', '', 1).replace('-', '', 1).isdigit()
            for val in non_null_values
        )
        total_count = len(non_null_values)
        return numeric_count / total_count > 0.5 if total_count > 0 else False

    @staticmethod
    def is_datetime_like(column, threshold=0.8, date_format=None):
        """
        Check if a column is datetime-like based on its values.

        Parameters:
            column (pd.Series): Input column.
            threshold (float): Proportion of valid datetime values required to classify as datetime-like.
            date_format (str, optional): Specific datetime format for parsing. If None, attempts to infer.

        Returns:
            bool: True if the column is datetime-like, False otherwise.
        """
        def is_valid_datetime(value):
            try:
                if pd.isna(value):
                    return False
                if date_format:
                    pd.to_datetime(value, format=date_format, errors="raise")
                else:
                    pd.to_datetime(value, errors="raise")
                return True
            except Exception:
                return False

        valid_count = column.apply(is_valid_datetime).sum()
        total_count = column.notnull().sum()
        if total_count == 0:
            return False
        return valid_count / total_count >= threshold

    @staticmethod
    def preprocess_latest_date_value(column):
        """
        Extract the value corresponding to the latest date from newline-separated entries.
        If no valid date-value pairs are found, return the original entry.

        Parameters:
            column (pd.Series): Input column with newline-separated date-value pairs.

        Returns:
            pd.Series: Cleaned column with values corresponding to the latest date.
        """

        def extract_latest(entry):
            if not isinstance(entry, str):
                return entry  # Return original value for non-strings

            # Split the entry by newline and process each line
            lines = entry.split('\n')
            date_value_pairs = []

            for line in lines:
                # Match date and numeric value pairs (e.g., "2024-03-31    12345.67")
                match = re.match(
                    r"(\d{4}-\d{2}-\d{2})\s+([+-]?\d*\.?\d+|NaN)", line.strip())
                if match:
                    date, value = match.groups()
                    if value.lower() != "nan":  # Exclude NaN values
                        date_value_pairs.append((date, float(value)))

            # Sort pairs by date (latest first)
            date_value_pairs.sort(key=lambda x: x[0], reverse=True)

            # Return the value for the latest date, or original entry if no valid pairs
            return date_value_pairs[0][1] if date_value_pairs else entry

        # Apply the extraction logic to the entire column
        return column.apply(extract_latest)

    @staticmethod
    def preprocess_nan_numeric_and_spaces(column):
        """
        Preprocess a column to handle:
        1. NaN values (preserve as NaN).
        2. Numeric strings (convert to int if whole number, otherwise float).
        3. Strings with spaces in numeric values (e.g., '400 001' -> 400001).
        4. Preserve non-numeric strings as is.

        Parameters:
            column (pd.Series): Input column.

        Returns:
            pd.Series: Preprocessed column.
        """
        def clean_value(value):
            if pd.isna(value):
                return np.nan  # Preserve NaN
            if isinstance(value, str):
                # Remove spaces and check if numeric
                cleaned_value = value.replace(" ", "")
                try:
                    numeric_value = float(cleaned_value)
                    if numeric_value.is_integer():
                        return int(numeric_value)
                    return numeric_value  # Return as float otherwise
                except ValueError:
                    return value  # Preserve original if not numeric
            return value  # Return original value if already numeric

        return column.apply(clean_value)

    @staticmethod
    def preprocess_datetime_column(column, date_format=None):
        """
        Preprocess a column to convert it into a proper datetime type.

        Parameters:
            column (pd.Series): Input column.
            date_format (str, optional): Date format for parsing.

        Returns:
            pd.Series: Preprocessed column with datetime or NaT.
        """
        def convert_to_datetime(value):
            try:
                if date_format:
                    return pd.to_datetime(value, format=date_format, errors="coerce")
                else:
                    return pd.to_datetime(value, errors="coerce")
            except Exception:
                return pd.NaT  # Return NaT for invalid entries

        return column.apply(convert_to_datetime)

    @staticmethod
    def validate_post_preprocessing_for_numeric(column):
        """
        Validate a column to ensure no non-numeric values remain.

        Parameters:
            column (pd.Series): Input column.

        Returns:
            pd.DataFrame: Rows that are still problematic (non-numeric).
        """
        return column[~column.apply(lambda x: isinstance(x, (int, float)) or pd.isnull(x))]

    @staticmethod
    def validate_post_preprocessing_for_datetime(column):
        """
        Validate a column to ensure all values are valid datetime objects or NaT.

        Parameters:
            column (pd.Series): Input column.

        Returns:
            pd.DataFrame: Rows that are still problematic (non-datetime).
        """
        return column[~column.apply(lambda x: pd.isnull(x) or isinstance(x, pd.Timestamp))]
