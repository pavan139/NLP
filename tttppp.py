import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data(file_path):
    try:
        return pd.read_csv(file_path, keep_default_na=False)
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def is_valid_date(date_str):
    if not date_str:
        return True  # Consider empty string as valid to preserve original data
    try:
        month, day, year = map(int, date_str.split('/'))
        if month < 1 or month > 12 or day < 1 or day > 31 or year < 1900 or year > 9999:
            return False
        datetime(year, month, day)
        return True
    except ValueError:
        return False

def is_valid_ssn(ssn):
    return bool(ssn)  # Consider any non-empty string as valid

def process_group(group):
    if not all(is_valid_ssn(ssn) for ssn in group['SSN']):
        logger.warning(f"Invalid SSN found in group: {group['SSN'].iloc[0]}")
        return group
    
    date_columns = ['Hire Date', 'Termination Date', 'Date of Rehire', 'Match Eligibility Date']
    if not all(is_valid_date(date) for col in date_columns for date in group[col] if col in group):
        logger.warning(f"Invalid date found in group with SSN: {group['SSN'].iloc[0]}")
        return group
    
    if len(group) == 1:
        return group
    
    # Process only if all dates are valid
    for col in date_columns:
        if col in group:
            if col in ['Hire Date', 'Match Eligibility Date']:
                group[col] = group[col].min()
            else:
                group[col] = group[col].max()
    
    return group.iloc[:1]  # Return only the first row after processing

def process_data(df):
    return df.groupby('SSN', group_keys=False).apply(process_group)

def filter_eligibility_dates(df):
    if 'Match Eligibility Date' in df.columns:
        cutoff_date = '08/16/2024'
        mask = df['Match Eligibility Date'].apply(lambda x: x <= cutoff_date if is_valid_date(x) else True)
        filtered_df = df[mask]
        if len(filtered_df) < len(df):
            logger.info(f"Filtered out {len(df) - len(filtered_df)} records with Match Eligibility Date after {cutoff_date}")
        return filtered_df
    return df

def main(file_path):
    try:
        logger.info("Starting data processing")
        
        # Load data
        df = load_data(file_path)
        logger.info(f"Loaded {len(df)} records")
        
        # Ensure SSN column exists
        if 'SSN' not in df.columns:
            raise ValueError("SSN column is missing from the input data")
        
        # Process data
        df = process_data(df)
        logger.info(f"After processing: {len(df)} records")
        
        # Filter eligibility dates
        df = filter_eligibility_dates(df)
        logger.info(f"After filtering eligibility dates: {len(df)} records")
        
        # Save processed data
        output_file = 'processed_fidelity_data.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Processed data saved to {output_file}")
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}")

if __name__ == "__main__":
    file_path = "path_to_your_input_file.csv"  # Replace with actual file path
    main(file_path)
