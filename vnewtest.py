import pandas as pd
import logging
from datetime import datetime
import os
import glob

# Configure logging
log_filename = datetime.now().strftime('log_%Y_%m_%d_%H_%M_%S.log')
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_filename),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

def load_data_from_folder(folder_path):
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    df_list = []
    for filename in all_files:
        try:
            df = pd.read_csv(filename, keep_default_na=False)
            df_list.append(df)
            logger.info(f"Loaded file: {filename}")
        except pd.errors.EmptyDataError:
            logger.warning(f"File {filename} is empty and will be skipped.")
        except pd.errors.ParserError as e:
            logger.warning(f"Parsing error in file {filename}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error loading file {filename}: {str(e)}")
    
    if not df_list:
        error_msg = "No CSV files found or successfully loaded in the specified folder"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    combined_df = pd.concat(df_list, ignore_index=True)
    logger.info(f"Combined {len(df_list)} CSV files. Total records: {len(combined_df)}")
    return combined_df

def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, '%m/%d/%Y')
        return True
    except ValueError:
        logger.debug(f"Invalid date format for {date_str}")
        return False

def process_group(group):
    logger = logging.getLogger(__name__)
    try:
        result = []
        for ssn in group['SSN'].unique():
            ssn_group = group[group['SSN'] == ssn]
            logger.debug(f"Processing SSN: {ssn}, Number of records: {len(ssn_group)}")
            processed = ssn_group.iloc[0].copy()

            # Process date fields
            date_columns = ['Date of Hire', 'Date of Termination', 'Date of Rehire', 'Match Eligibility Date']
            for col in date_columns:
                if col in ssn_group.columns:
                    try:
                        valid_dates = ssn_group[ssn_group[col].apply(is_valid_date)][col]
                        if not valid_dates.empty:
                            chosen_date = valid_dates.min() if col in ['Date of Hire', 'Match Eligibility Date'] else valid_dates.max()
                            processed[col] = chosen_date
                            logger.debug(f"Updated {col} for SSN {ssn} to {chosen_date}")
                    except Exception as e:
                        logger.error(f"Error processing date column {col} for SSN {ssn}: {str(e)}")
            
            # Fill blank fields
            for col in ssn_group.columns:
                if pd.isna(processed[col]) and not ssn_group[col].isna().all():
                    filled_value = ssn_group[col].fillna('').iloc[0]
                    processed[col] = filled_value
                    logger.debug(f"Filled {col} for SSN {ssn} with {filled_value}")

            result.append(processed)
            logger.debug(f"Record processed for SSN {ssn}")

        return pd.DataFrame(result)
    except Exception as e:
        logger.error(f"Error processing group for SSN: {group['SSN'].iloc[0] if not group.empty else 'unknown'} - {str(e)}")
        return pd.DataFrame()

def process_data(df):
    try:
        grouped = df.groupby('SSN')
        processed_groups = [process_group(group) for _, group in grouped]
        return pd.concat(processed_groups, ignore_index=True)
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return pd.DataFrame()

def main(folder_path):
    try:
        logger.info("Starting data processing")
        df = load_data_from_folder(folder_path)
        
        if 'SSN' not in df.columns:
            raise ValueError("SSN column is missing from the input data")
        
        df = process_data(df)
        logger.info(f"After processing: {len(df)} records")
        
        output_file = 'processed_fidelity_data.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Processed data saved to {output_file}")
        
    except FileNotFoundError:
        logger.error(f"The folder path {folder_path} does not exist.")
    except PermissionError:
        logger.error(f"Permission denied while accessing the files in {folder_path}.")
    except ValueError as ve:
        logger.error(f"Data validation error: {ve}")
    except Exception as e:
        logger.error(f"An unspecified error occurred: {str(e)}")

if __name__ == "__main__":
    folder_path = "path_to_your_csv_folder"  # Replace with actual folder path
    main(folder_path)
