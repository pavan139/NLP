import pandas as pd
import logging
from datetime import datetime
import os
import glob
from multiprocessing import Pool

# Configure logging
log_filename = datetime.now().strftime('log_%Y_%m_%d_%H_%M_%S.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
logger = logging.getLogger(__name__)

def read_file(file):
    # Adjust data types during loading for efficiency
    df = pd.read_csv(file, keep_default_na=False, dtype={'SSN': str})
    return df

def load_data_from_folder(folder_path):
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not all_files:
        logger.error("No CSV files found in the specified folder")
        return pd.DataFrame()
    
    with Pool(processes=4) as pool:  # Number of processes depends on your machine's capabilities
        df_list = pool.map(read_file, all_files)

    combined_df = pd.concat(df_list, ignore_index=True)
    logger.info(f"Loaded and combined {len(all_files)} CSV files.")
    return combined_df

def preprocess_dates(df):
    date_columns = ['Date of Hire', 'Date of Termination', 'Date of Rehire', 'Match Eligibility Date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='%m/%d/%Y')
            df[f'{col}_valid'] = df[col].notna()
    return df

def process_group(group):
    try:
        # Assume date preprocessing and validity columns exist
        date_columns = [col for col in group.columns if "Date" in col and "valid" not in col]
        processed = group.iloc[0].copy()

        for col in date_columns:
            valid_col = f"{col}_valid"
            if group[valid_col].any():
                valid_dates = group.loc[group[valid_col], col]
                processed[col] = valid_dates.min() if 'Hire' in col or 'Eligibility' in col else valid_dates.max()

        return processed
    except Exception as e:
        logger.error(f"Error processing group with SSN: {group['SSN'].iloc[0] if not group.empty else 'unknown'} - {str(e)}")
        return pd.DataFrame()

def process_data(df):
    df = preprocess_dates(df)
    grouped = df.groupby('SSN')
    processed_df = grouped.apply(process_group).reset_index(drop=True)
    logger.info("Data processing complete.")
    return processed_df

def main(folder_path):
    df = load_data_from_folder(folder_path)
    if df.empty:
        logger.error("Failed to load any data.")
        return

    processed_df = process_data(df)
    output_file = 'processed_fidelity_data.csv'
    processed_df.to_csv(output_file, index=False)
    logger.info(f"Processed data saved to {output_file}")

if __name__ == "__main__":
    folder_path = "path_to_your_csv_folder"  # Ensure this is the correct path to your data
    main(folder_path)
