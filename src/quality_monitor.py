from data_loader import DataLoader
from data_validator import DataValidator
from data_cleaning import *


class DataMetrics:
    def __init__(self, dataframes):
        self.dataframes = dataframes

    def summary_statistics(self, df):
        return df.describe(include='all')

    def correlation_matrix(self, df):
        numerical_columns = df.select_dtypes(
            include=['int64', 'float64']).columns
        return df[numerical_columns].corr()

    def missing_values_count(self, df):
        return df.isna().sum()

    def calculate_metrics(self):
        metrics = {}
        for df_name, df in self.dataframes.items():
            metrics[df_name] = {
                'summary_statistics': self.summary_statistics(df),
                'correlation_matrix': self.correlation_matrix(df),
                'missing_values_count': self.missing_values_count(df)
            }
        return metrics


data = DataLoader()
dataframes = data.load_data()

pipeline = DataCleaningPipeline()
pipeline.add_step('remove_duplicates', remove_duplicates)
pipeline.add_step('standardize_dates', standardize_dates)
pipeline.add_step('fill_missing_values', fill_missing_values)
pipeline.add_step('remove_outliers', remove_outliers)
pipeline.add_step('convert_to_lowercase', convert_to_lowercase)
pipeline.add_step('clean_text_columns', clean_text_columns)
validator = DataValidator(cleaned_dataframes)
validator.validate_dataframes()

metrics = DataMetrics(cleaned_dataframes)
metrics_results = metrics.calculate_metrics()

# Access the cleaned DataFrames and their metrics by their file names
for df_name, df in cleaned_dataframes.items():
    print(f"DataFrame name: {df_name}")
    print(df.head())  # Print the first few rows of each DataFrame
    print(f"Metrics for {df_name}:")
    print(metrics_results[df_name]['summary_statistics'])
    print(metrics_results[df_name]['correlation_matrix'])
    print(metrics_results[df_name]['missing_values_count'])
