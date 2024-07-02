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
        return True  # Consider null/NaN/empty string as valid to preserve original data
    try:
        datetime.strptime(date_str, '%m/%d/%Y')
        return True
    except ValueError:
        return False

def process_group(group):
    if len(group) == 1:
        return group

    date_columns = ['Hire Date', 'Termination Date', 'Date of Rehire', 'Match Eligibility Date']
    
    # Check if all date fields are equal
    if all(group[col].nunique() == 1 for col in date_columns if col in group.columns):
        logger.info(f"All dates are equal for SSN: {group['SSN'].iloc[0]}. Keeping one record.")
        return group.iloc[:1]

    processed_group = group.iloc[:1].copy()

    for col in date_columns:
        if col in group.columns:
            valid_dates = group[group[col].apply(is_valid_date)][col]
            if not valid_dates.empty:
                if col in ['Hire Date', 'Match Eligibility Date']:
                    processed_group[col] = valid_dates.min()
                else:
                    processed_group[col] = valid_dates.max()
            else:
                processed_group[col] = group[col].iloc[0]  # Keep original if no valid dates

    # Fill blank fields with non-blank values from other rows
    for column in group.columns:
        if pd.isna(processed_group[column].iloc[0]) and not group[column].isna().all():
            processed_group[column] = group[column].fillna(method='ffill').iloc[0]

    return processed_group

def process_data(df):
    return df.groupby('SSN', group_keys=False).apply(process_group)

def filter_eligibility_dates(df):
    if 'Match Eligibility Date' in df.columns:
        cutoff_date = '08/16/2024'
        mask = df['Match Eligibility Date'].apply(lambda x: pd.to_datetime(x, errors='coerce') <= pd.to_datetime(cutoff_date) if pd.notna(x) and is_valid_date(x) else True)
        filtered_df = df[mask]
        if len(filtered_df) < len(df):
            logger.info(f"Filtered out {len(df) - len(filtered_df)} records with Match Eligibility Date after {cutoff_date}")
        return filtered_df
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
        logger.info(f"After filtering eligibility dates: {len(df)} records")
        
        output_file = 'processed_fidelity_data.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Processed data saved to {output_file}")
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}")

if __name__ == "__main__":
    folder_path = "path_to_your_csv_folder"  # Replace with actual folder path
    main(folder_path)
