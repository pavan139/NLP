import pandas as pd
import logging
from datetime import datetime
import os
import glob

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data_from_folder(folder_path):
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    df_list = []
    for filename in all_files:
        try:
            df = pd.read_csv(filename, keep_default_na=False)
            df_list.append(df)
            logger.info(f"Loaded file: {filename}")
        except Exception as e:
            logger.error(f"Error loading file {filename}: {str(e)}")
    
    if not df_list:
        raise ValueError("No CSV files found in the specified folder")
    
    combined_df = pd.concat(df_list, ignore_index=True)
    logger.info(f"Combined {len(df_list)} CSV files. Total records: {len(combined_df)}")
    return combined_df

def is_valid_date(date_str):
    if pd.isna(date_str) or not date_str:
        return False  # Consider null/NaN/empty string as invalid
    try:
        datetime.strptime(date_str, '%m/%d/%Y')
        return True
    except ValueError:
        return False

def process_group(group):
    if len(group) == 1:
        return group

    result = pd.DataFrame(columns=group.columns)

    for ssn in group['SSN'].unique():
        ssn_group = group[group['SSN'] == ssn]
        
        processed = ssn_group.iloc[0].copy()
        
        # Process date fields
        date_columns = ['Date of Hire', 'Date of Termination', 'Date of Rehire', 'Match Eligibility Date']
        for col in date_columns:
            if col in ssn_group.columns:
                valid_dates = ssn_group[ssn_group[col].apply(is_valid_date)][col]
                if not valid_dates.empty:
                    if col in ['Date of Hire', 'Match Eligibility Date']:
                        processed[col] = valid_dates.min()
                    else:
                        processed[col] = valid_dates.max()
        
        # Fill blank fields
        for col in ssn_group.columns:
            if pd.isna(processed[col]) and not ssn_group[col].isna().all():
                processed[col] = ssn_group[col].fillna('').iloc[0]

        result = result.append(processed, ignore_index=True)

    return result

def process_data(df):
    return df.groupby('SSN', group_keys=False).apply(process_group).reset_index(drop=True)

def filter_eligibility_dates(df):
    if 'Match Eligibility Date' in df.columns:
        cutoff_date = '08/16/2024'
        df['filter_flag'] = df['Match Eligibility Date'].apply(
            lambda x: pd.to_datetime(x, errors='coerce') <= pd.to_datetime(cutoff_date) if pd.notna(x) and is_valid_date(x) else True
        )
        logger.info(f"Flagged {(~df['filter_flag']).sum()} records with Match Eligibility Date after {cutoff_date}")
    return df

def main(folder_path):
    try:
        logger.info("Starting data processing")
        
        df = load_data_from_folder(folder_path)
        
        if 'SSN' not in df.columns:
            raise ValueError("SSN column is missing from the input data")
        
        df = process_data(df)
        logger.info(f"After processing: {len(df)} records")
        
        df = filter_eligibility_dates(df)
        logger.info(f"After flagging eligibility dates: {len(df)} records")
        
        output_file = 'processed_fidelity_data.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Processed data saved to {output_file}")
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}")

if __name__ == "__main__":
    folder_path = "path_to_your_csv_folder"  # Replace with actual folder path
    main(folder_path)
