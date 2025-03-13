import pandas as pd
from datetime import datetime
from data_loader import DataLoader
from data_validator import DataValidator


class DataCleaningPipeline:
    """
    A modular pipeline for cleaning data with customizable steps.
    """

    def __init__(self):
        self.steps = []

    def add_step(self, name, function):
        """Add a cleaning step."""
        self.steps.append({'name': name, 'function': function})

    def execute(self, df):
        """Execute all cleaning steps in order."""
        results = []
        current_df = df.copy()

        for step in self.steps:
            try:
                current_df = step['function'](current_df)
                results.append({
                    'step': step['name'],
                    'status': 'success',
                    'rows_affected': len(current_df)
                })
            except Exception as e:
                results.append({
                    'step': step['name'],
                    'status': 'failed',
                    'error': str(e)
                })
                break

        return current_df, results


def remove_duplicates(df):
    return df.drop_duplicates()


def standardize_dates(df):
    date_columns = df.select_dtypes(include=['datetime64']).columns
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def fill_missing_values(df):
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col].fillna(df[col].mean(), inplace=True)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col].fillna(pd.Timestamp.now(), inplace=True)
        else:
            df[col].fillna('unknown', inplace=True)
    return df


def remove_outliers(df):
    for col in df.select_dtypes(include=['int64', 'float64']).columns:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
    return df


def convert_to_lowercase(df):
    df = df.map(lambda x: x.lower() if isinstance(x, str) else x)
    return df


def clean_text_columns(df, columns=None):
    """
    Apply standardized text cleaning to specified columns.

    Args:
        df (pd.DataFrame): Input dataframe
        columns (list): List of columns to clean. If None, clean all object columns

    Returns:
        pd.DataFrame: Dataframe with cleaned text columns
    """
    if columns is None:
        columns = df.select_dtypes(include=['object']).columns

    df = df.copy()

    for column in columns:
        if column not in df.columns:
            continue

        # Apply string cleaning operations
        df[column] = (df[column]
                      .astype(str)
                      .str.strip()
                      .str.lower()
                      # Replace multiple spaces
                      .replace(r'\s+', ' ', regex=True)
                      .replace(r'[^\w\s]', '', regex=True))  # Remove special characters

    return df


# Example usage
data = DataLoader()
dataframes = data.load_data()

pipeline = DataCleaningPipeline()
pipeline.add_step('remove_duplicates', remove_duplicates)
pipeline.add_step('standardize_dates', standardize_dates)
pipeline.add_step('fill_missing_values', fill_missing_values)
pipeline.add_step('remove_outliers', remove_outliers)
pipeline.add_step('convert_to_lowercase', convert_to_lowercase)
pipeline.add_step('clean_text_columns', clean_text_columns)

cleaned_dataframes = {}
for df_name, df in dataframes.items():
    cleaned_df, results = pipeline.execute(df)
    cleaned_dataframes[df_name] = cleaned_df
    print(f"Cleaning results for {df_name}: {results}")

validator = DataValidator(cleaned_dataframes)
validator.validate_dataframes()

# Access the cleaned DataFrames by their file names
for df_name, df in cleaned_dataframes.items():
    print(f"DataFrame name: {df_name}")
    print(df.head())  # Print the first few rows of each DataFrame
