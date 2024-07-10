import pandas as pd
import logging
import os
from datetime import datetime

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
FOLDER_PATH = r"N:\INFORMATICA\SrcFiles\KROGER\R699_RehireSCF\zzzz_prk_load_test"
DATE_COLUMNS = ['Date of Hire', 'Date of Termination', 'Date of Rehire', 'Match Eligibility Date']
OUTPUT_DIR = r"output_directory"  # Define your output directory here

# Mock data
data = """
SSN,Status Code,Date of Hire,Date of Termination,Date of Rehire,Match Eligibility Date
123-45-6789,A,2020-01-01,2021-01-01,2021-06-01,2020-04-01
123-45-6789,A,2020-01-01,2021-02-01,2021-06-01,2020-03-01
234-56-7890,B,2020-02-01,2021-02-01,,2020-02-01
234-56-7890,B,2020-01-01,2021-01-01,,2020-01-01
345-67-8901,C,2020-03-01,,2021-07-01,2020-03-01
345-67-8901,C,2020-03-01,,2021-06-01,2020-04-01
456-78-9012,D,,,
456-78-9012,D,2020-05-01,2021-05-01,,2020-05-01
"""

# Load data
df = pd.read_csv(pd.compat.StringIO(data), parse_dates=['Date of Hire', 'Date of Termination', 'Date of Rehire', 'Match Eligibility Date'])

# Sort and forward fill the Status Code
df.sort_values(by=['SSN'] + DATE_COLUMNS, inplace=True)
df['Status Code'] = df.groupby('SSN')['Status Code'].ffill()

# Clean and transform date columns
for col in DATE_COLUMNS:
    df[col] = pd.to_datetime(df[col], errors='coerce', format='%m/%d/%Y')

# Define aggregation rules
aggregation_rules = {
    'Date of Hire': 'min',
    'Date of Termination': 'max',
    'Date of Rehire': 'max',
    'Match Eligibility Date': 'min',
    'Status Code': 'last'  # Using 'last' to take the last observed 'Status Code' after forward fill
}

# Group by SSN and aggregate
df_aggregated = df.groupby('SSN', as_index=False).agg(aggregation_rules)
df_aggregated.dropna(subset=['Date of Hire', 'Date of Termination'], how='all', inplace=True)

# Remove rows with incomplete 'Hire Date'
incomplete_date_df = df_aggregated[df_aggregated['Date of Hire'].str.contains('-00', na=False)]
df_aggregated = df_aggregated[~df_aggregated['Date of Hire'].str.contains('-00', na=False)]

# Validate unique SSN count
initial_unique_ssn_count = df['SSN'].nunique()
end_unique_ssn_count = df_aggregated['SSN'].nunique() + incomplete_date_df['SSN'].nunique()
if initial_unique_ssn_count != end_unique_ssn_count:
    logger.error("SSN count mismatch: data integrity issue detected.")

# Save DataFrames to files
df_aggregated.to_csv(os.path.join(OUTPUT_DIR, 'Aggregated_Data.csv'), index=False)
incomplete_date_df.to_csv(os.path.join(OUTPUT_DIR, 'Removed_Data.csv'), index=False)

# Log successful save
logger.info("Data files saved successfully.")
