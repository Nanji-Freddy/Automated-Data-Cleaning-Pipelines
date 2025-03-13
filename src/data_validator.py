import pandas as pd
from data_loader import DataLoader


class DataValidator:
    def __init__(self, dataframes):
        self.dataframes = dataframes

    def _check_numerical_columns(self, df):
        numerical_columns = df.select_dtypes(
            include=['int64', 'float64']).columns
        for col in numerical_columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                raise ValueError(f"Column {col} is not of numerical type.")
        return numerical_columns

    def _check_date_columns(self, df):
        date_columns = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in date_columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                raise ValueError(f"Column {col} is not of date type.")
        return date_columns

    def _check_missing_values(self, df):
        missing_values = df.isna().sum()
        if missing_values.any():
            return f"Missing values found in columns: {missing_values[missing_values > 0].index.tolist()}"
        return "No missing data"

    def validate_dataframes(self):
        for df_name, df in self.dataframes.items():
            print(f"Validating DataFrame: {df_name}")
            self._check_numerical_columns(df)
            self._check_date_columns(df)
            print(self._check_missing_values(df))
            print(f"DataFrame {df_name} is valid.")


# Example usage
data = DataLoader()
dataframes = data.load_data()

validator = DataValidator(dataframes)
validator.validate_dataframes()
