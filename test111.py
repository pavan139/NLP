import pandas as pd
import os
import logging
from pathlib import Path
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CSVAggregator:
    def __init__(self, directory_path, colmap):
        self.directory_path = directory_path
        self.colmap = pd.read_csv(colmap).set_index('DB_COLUMNS')

    def get_report_column(self, db_column):
        return self.colmap.at[db_column, 'REPORT_COLUMNS'] if db_column in self.colmap.index else None

    def read_csvs(self):
        return [pd.read_csv(file) for file in Path(self.directory_path).glob('*.csv')]

    def join_dataframes(self, dataframes):
        return pd.concat(dataframes, ignore_index=True)

    def check_ssn_validity(self, ssn):
        return pd.Series(ssn).str.match(r'^\d{3}-\d{2}-\d{4}$')

    def check_date_format(self, date_column, treat_null_as_invalid=False):
        valid_dates = pd.to_datetime(date_column, format='%m/%d/%Y', errors='coerce').notna()
        if treat_null_as_invalid:
            return valid_dates
        return valid_dates | date_column.isna()

    def save_to_csv(self, dataframe, filename):
        if not dataframe.empty:
            output_filename = f"{filename}_{datetime.now().strftime('%Y%m%d')}.csv"
            dataframe.to_csv(os.path.join(self.directory_path, output_filename), index=False)

    def process_csvs(self):
        dfs = self.read_csvs()
        full_df = self.join_dataframes(dfs).drop_duplicates()

        ssn_column_name = self.get_report_column("SSN")
        full_df = full_df[full_df[ssn_column_name].notna()]
        duplicates_df = full_df[full_df.duplicated(subset=ssn_column_name, keep='first')]
        self.save_to_csv(duplicates_df, 'duplicate_ssns')
        
        full_df = full_df.drop_duplicates(subset=ssn_column_name)
        full_df['SSN_valid'] = self.check_ssn_validity(full_df[ssn_column_name])
        invalid_ssn_df = full_df[~full_df['SSN_valid']]
        self.save_to_csv(invalid_ssn_df, 'invalid_ssn')

        specific_date_columns = {'HIRE_DT': True, 'TERM_DT': True}
        other_date_columns = ['REHIRE_DT', 'FLEX_1_DT', 'FLEX_2_DT', 'FLEX_3_DT', 'FLEX_4_DT', 'FLEX_5_DT']
        
        for col, treat_null_as_invalid in specific_date_columns.items():
            date_column_name = self.get_report_column(col)
            if date_column_name:
                full_df[date_column_name + '_valid'] = self.check_date_format(full_df[date_column_name], treat_null_as_invalid)
                invalid_date_df = full_df[~full_df[date_column_name + '_valid']]
                self.save_to_csv(invalid_date_df, f'{date_column_name}_invalid')
                full_df = full_df[full_df[date_column_name + '_valid']]
                full_df[date_column_name] = pd.to_datetime(full_df[date_column_name], errors='coerce').dt.date

        for col in other_date_columns:
            date_column_name = self.get_report_column(col)
            if date_column_name:
                full_df[date_column_name + '_valid'] = self.check_date_format(full_df[date_column_name])
                invalid_date_df = full_df[~full_df[date_column_name + '_valid']]
                self.save_to_csv(invalid_date_df, f'{date_column_name}_invalid')
                full_df = full_df[full_df[date_column_name + '_valid']]
                full_df[date_column_name] = pd.to_datetime(full_df[date_column_name], errors='coerce').dt.date

        self.save_to_csv(full_df, 'final_cleaned_data')
        return full_df

if __name__ == '__main__':
    directory_path = 'path_to_your_directory'
    colmap = 'path_to_your_colmap_csv'
    aggregator = CSVAggregator(directory_path, colmap)
    final_df = aggregator.process_csvs()
