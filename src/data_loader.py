import pandas as pd
from pathlib import Path
import ntpath
import os


class DataLoader:
    # Get the directory of the script
    project_directory = Path(__file__).parent.parent
    file_path = project_directory / "data" / "raw"

    def __init__(self):
        self.file_name = 'all'

    def _get_file_type(self, file_name):
        self.file_name = file_name
        file_info = {}

        if self.file_name != 'all':
            file_path = f'../data/raw/{file_name}'
            file_type = Path(file_path).suffix.lower()
            file_info.update({file_path: file_type})
        else:
            files = Path(self.file_path).iterdir()
            for filename in files:
                file_path = str(filename)
                file_type = Path(file_path).suffix.lower()
                file_info.update({file_path: file_type})

        return file_info

    def _file_handling(self, file_type):
        handlers = {
            '.csv': pd.read_csv,
            '.xlsx': pd.read_excel,
            '.json': pd.read_json,
            '.parquet': pd.read_parquet
        }

        reader = handlers.get(file_type)
        if reader is None:
            raise ValueError(f"Unsupported file type: {file_type}")

        return reader

    def _extract_file_name(self, file_path):
        self.file_path = file_path
        head, tail = ntpath.split(file_path)
        tail = tail.replace('.', '_')
        return tail

    def _basic_cleaning(self, df):
        df.columns = df.columns.str.strip().str.lower()  # Standardize column names
        df = df.replace('', pd.NA)  # Convert empty strings to NA

        return df

    def load_data(self, file_name='all'):
        file_info = self._get_file_type(file_name)
        dataframes = {}

        for key, value in file_info.items():
            file_path = key
            file_type = value
            file_name = self._extract_file_name(key)
            reader = self._file_handling(file_type)
            df = reader(file_path)
            df = self._basic_cleaning(df)
            dataframes[file_name] = df

        return dataframes


data = DataLoader()

print(data.load_data())
