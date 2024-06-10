import pandas as pd
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CSVAggregator:
    def __init__(self, directory_path, colmap):
        self.directory_path = Path(directory_path)
        self.colmap_file = colmap
        self.logger = logging.getLogger(__name__)

        # Load column mappings and validate directory path
        try:
            self.colmap = pd.read_csv(colmap).set_index('DB_COLUMNS')
        except Exception as e:
            self.logger.error(f"Failed to load column mapping file: {e}")
            sys.exit(1)

        if not self.directory_path.exists() or not self.directory_path.is_dir():
            self.logger.error(f"Provided directory path does not exist or is not a directory: {directory_path}")
            sys.exit(1)

    def get_report_column(self, db_column):
        try:
            return self.colmap.at[db_column, 'REPORT_COLUMNS']
        except KeyError:
            self.logger.error(f"Column mapping not found for {db_column}")
            return None

    def read_csvs(self):
        try:
            return [pd.read_csv(file) for file in self.directory_path.glob('*.csv')]
        except Exception as e:
            self.logger.error(f"Error reading CSV files: {e}")
            sys.exit(1)

    def join_dataframes(self, dataframes):
        try:
            return pd.concat(dataframes, ignore_index=True)
        except Exception as e:
            self.logger.error(f"Error joining dataframes: {e}")
            sys.exit(1)

    def check_ssn_validity(self, ssn):
        return pd.Series(ssn).str.match(r'^\d{3}-\d{2}-\d{4}$')

    def check_date_format(self, date_column, treat_null_as_invalid=False):
        valid_dates = pd.to_datetime(date_column, format='%m/%d/%Y', errors='coerce').notna()
        if treat_null_as_invalid:
            return valid_dates
        return valid_dates | date_column.isna()

    def save_to_csv(self, dataframe, filename):
        try:
            output_dir = self.directory_path / 'review'
            output_dir.mkdir(exist_ok=True)
            output_filename = output_dir / f"{filename}_{datetime.now().strftime('%Y%m%d')}.csv"
            dataframe.to_csv(output_filename, index=False)
        except Exception as e:
            self.logger.error(f"Failed to save dataframe to CSV: {e}")
            sys.exit(1)
    
    def process_csvs(self):
        try:
            dfs = self.read_csvs()
            if not dfs:
                self.logger.error("No CSV files found in the directory.")
                sys.exit(1)
            full_df = self.join_dataframes(dfs)
        except Exception as e:
            self.logger.error(f"Failed to read or join CSV files: {e}")
            sys.exit(1)
    
        full_df.drop_duplicates(inplace=True)
        essential_columns = ["SSN", "HIRE_DT", "REHIRE_DT"]
        columns = {col: self.get_report_column(col) for col in essential_columns}
    
        for col, name in columns.items():
            if name is None:
                self.logger.error(f"Essential column {col} name could not be retrieved. Exiting.")
                sys.exit(1)
    
        ssn_column_name = columns.get("SSN")
        if ssn_column_name:
            full_df = full_df[full_df[ssn_column_name].notna()]
            duplicates_mask = full_df.duplicated(subset=ssn_column_name, keep=False)
            duplicates_df = full_df[duplicates_mask]
            self.save_to_csv(duplicates_df, 'duplicate_ssns')
    
            full_df = full_df[~duplicates_mask]
            full_df['SSN_valid'] = self.check_ssn_validity(full_df[ssn_column_name])
            invalid_ssn_df = full_df[~full_df['SSN_valid']]
            self.save_to_csv(invalid_ssn_df, 'invalid_ssn')
        else:
            self.logger.error("SSN column mapping not found, unable to process SSNs.")
            sys.exit(1)
    
        date_column_processing = {
            'HIRE_DT': True,
            'TERM_DT': True,
            'REHIRE_DT': False,
            'FLEX_1_DT': False,
            'FLEX_2_DT': False,
            'FLEX_3_DT': False,
            'FLEX_4_DT': False,
            'FLEX_5_DT': False
        }
        for col, treat_null_as_invalid in date_column_processing.items():
            date_column_name = columns.get(col)
            if date_column_name:
                full_df[date_column_name + '_valid'] = self.check_date_format(full_df[date_column_name], treat_null_as_invalid)
                invalid_date_df = full_df[~full_df[date_column_name + '_valid']]
                self.save_to_csv(invalid_date_df, f'{date_column_name}_invalid')
                full_df = full_df[full_df[date_column_name + '_valid']]
                full_df[date_column_name] = pd.to_datetime(full_df[date_column_name], errors='coerce').dt.date
            else:
                self.logger.warning(f"Date column {col} not found in mappings, skipping validation.")
    
        full_df = full_df.convert_dtypes()
        full_df.applymap(lambda x: None if pd.isna(x) else x)
        for col in list(full_df.columns):
            if '_valid' in col:
                full_df.drop(col, axis=1, inplace=True)
        
        try:
            self.save_to_csv(full_df, 'final_cleaned_data')
        except Exception as e:
            self.logger.error(f"Error saving final cleaned data: {e}")
            sys.exit(1)
        
        return full_df


if __name__ == "__main__":
    aggregator = CSVAggregator("path/to/directory", "path/to/colmap.csv")
    df = aggregator.process_csvs()
    if df is None:
        logging.error("Data processing failed.")
        sys.exit(1)
