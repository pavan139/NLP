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
    if pd.isna(date_str):
        return True  # Consider null/NaN as valid to preserve original data
    if not date_str:
        return True  # Consider empty string as valid to preserve original data
    try:
        datetime.strptime(date_str, '%m/%d/%Y')
        return True
    except ValueError:
        return False

def is_valid_ssn(ssn):
    return bool(ssn)  # Consider any non-empty string as valid

def process_group(group):
    if len(group) == 1:
        # Check for null Hire Date and/or Termination Date
        if pd.isna(group['Hire Date'].iloc[0]) or pd.isna(group['Termination Date'].iloc[0]):
            logger.info(f"Removing record with null Hire Date or Termination Date: {group['SSN'].iloc[0]}")
            return pd.DataFrame()  # Return empty DataFrame to remove this record
        return group

    # Check if all date fields are equal
    date_columns = ['Hire Date', 'Termination Date', 'Date of Rehire', 'Match Eligibility Date']
    if group[date_columns].nunique().eq(1).all():
        logger.info(f"Removing duplicate account as all dates are equal: {group['SSN'].iloc[0]}")
        return group.iloc[:1]

    # Process date fields
    processed_group = group.iloc[:1].copy()  # Start with the first row

    # Hire Date: use the earliest
    processed_group['Hire Date'] = group['Hire Date'].min()

    # Termination Date: use the latest
    processed_group['Termination Date'] = group['Termination Date'].max()

    # Date of Rehire: use the latest
    if 'Date of Rehire' in group.columns:
        processed_group['Date of Rehire'] = group['Date of Rehire'].max()

    # Match Eligibility Date: use the earliest
    if 'Match Eligibility Date' in group.columns:
        processed_group['Match Eligibility Date'] = group['Match Eligibility Date'].min()

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
        mask = df['Match Eligibility Date'].apply(lambda x: pd.to_datetime(x, errors='coerce') <= pd.to_datetime(cutoff_date) if pd.notna(x) else True)
        filtered_df = df[mask]
        if len(filtered_df) < len(df):
            logger.info(f"Filtered out {len(df) - len(filtered_df)} records with Match Eligibility Date after {cutoff_date}")
        return filtered_df
    return df

def main(folder_path):
    try:
        logger.info("Starting data processing")
        
        # Load and combine data from all CSV files in the folder
        df = load_data_from_folder(folder_path)
        
        # Ensure SSN column exists
        if 'SSN' not in df.columns:
            raise ValueError("SSN column is missing from the input data")
        
        # Process data
        df = process_data(df)
        logger.info(f"After processing: {len(df)} records")
        
        # Filter eligibility dates
        df = filter_eligibility_dates(df)
        logger.info(f"After filtering eligibility dates: {len(df)} records")
        
        # Remove records with null Hire Date or Termination Date
        df = df.dropna(subset=['Hire Date', 'Termination Date'])
        logger.info(f"After removing null Hire Date or Termination Date: {len(df)} records")
        
        # Save processed data
        output_file = 'processed_fidelity_data.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Processed data saved to {output_file}")
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}")

if __name__ == "__main__":
    folder_path = "path_to_your_csv_folder"  # Replace with actual folder path
    main(folder_path)
