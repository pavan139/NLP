# %%
import pandas as pd
import logging
from datetime import datetime
import os
import glob


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


folder_path = r"N:\INFORMATICA\SrcFiles\KROGER\R699_RehireSCF\zzzz_prk_load_test"
all_files = glob.glob(os.path.join(folder_path, "*.csv"))
df_list = []
for filename in all_files:
    try:
        df = pd.read_csv(filename)#, keep_default_na=False)
        df_list.append(df)
        logger.info(f"Loaded file: {filename}")
    except Exception as e:
        logger.error(f"Error loading file {filename}: {str(e)}")

if not df_list:
    raise ValueError("No CSV files found in the specified folder")

combined_df = pd.concat(df_list, ignore_index=True)
combined_df = combined_df.dropna(how='all')



data_types = combined_df.dtypes

# Get the number of missing values in each column
missing_values = combined_df.isnull().sum()

# Get the number of unique values in each column
unique_values = combined_df.nunique()

# Create a DataFrame to store the information

data_info = pd.DataFrame({
    'Data Type': data_types,
    'Missing Values': missing_values,
    'Unique Values': unique_values
})

rows_with_missing_ssn = combined_df[combined_df['SSN'].isnull()]


# %%
rows_with_missing_ssn = combined_df[combined_df['Date of Hire'].isnull()]
rows_with_missing_ssn

# %%
data_info

# %%
columns_to_check = ['SSN', 'Date of Hire', 'Date of Termination', 'Date of Rehire', 'Match Eligibility Date']
    
    # Remove duplicates based on the specified columns
df_cleaned = combined_df.drop_duplicates(subset=columns_to_check)

# %%


# %%
data_types = df_cleaned.dtypes

# Get the number of missing values in each column
missing_values = df_cleaned.isnull().sum()

# Get the number of unique values in each column
unique_values = df_cleaned.nunique()

# Create a DataFrame to store the information

data_info = pd.DataFrame({
    'Data Type': data_types,
    'Missing Values': missing_values,
    'Unique Values': unique_values
})
print(data_info)

# %%
data_types = combined_df.dtypes

# Get the number of missing values in each column
missing_values = combined_df.isnull().sum()

# Get the number of unique values in each column
unique_values = combined_df.nunique()

# Create a DataFrame to store the information

data_info = pd.DataFrame({
    'Data Type': data_types,
    'Missing Values': missing_values,
    'Unique Values': unique_values
})
print(data_info)

# %%
ssn_rows = df_cleaned[df_cleaned['SSN'] == "645-58-8248"]
ssn_rows

# %%
incomplete_date_df = df_cleaned[df_cleaned['Date of Hire'].str.contains('-00', na=False)]

# Remove rows with incomplete 'Hire Date' from the original DataFrame
df_cleaned = df_cleaned[~df_cleaned['Date of Hire'].str.contains('-00', na=False)]
incomplete_date_df

# %%
import pandas as pd

date_columns = ['Date of Hire', 'Date of Termination', 'Date of Rehire', 'Match Eligibility Date']
for col in date_columns:
    df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce', format='%m/%d/%Y')

# Sort by SSN and relevant dates
df_cleaned = df_cleaned.sort_values(by=['SSN', 'Date of Hire', 'Date of Termination', 'Date of Rehire', 'Match Eligibility Date'])

# Define aggregation rules
aggregation_rules = {
    'Date of Hire': 'min',
    'Date of Termination': 'max',
    'Date of Rehire': 'max',
    'Match Eligibility Date': 'min'
}

# Group by SSN and aggregate based on rules
df_aggregated = df_cleaned.groupby('SSN', as_index=False).agg(aggregation_rules)

# Remove records with no Hire Date and Termination Date if no duplicates exist
df_aggregated = df_aggregated.dropna(subset=['Hire Date', 'Termination Date'], how='all')
for col in date_columns:
    df_aggregated[col] = df_aggregated[col].dt.strftime('%m/%d/%Y')

df_aggregated


# %%
df_aggregated
ssn_rows = df_aggregated[df_aggregated['SSN'] == "645-58-8248"]
ssn_rows

# %%
data_types = df_aggregated.dtypes

# Get the number of missing values in each column
missing_values = df_aggregated.isnull().sum()

# Get the number of unique values in each column
unique_values = df_aggregated.nunique()

# Create a DataFrame to store the information

data_info = pd.DataFrame({
    'Data Type': data_types,
    'Missing Values': missing_values,
    'Unique Values': unique_values
})
print(data_info)

# %%
rows_with_missing_ssn = df_aggregated[df_aggregated['Date of Hire'].isnull()]
rows_with_missing_ssn

# %%
ssn_rows = combined_df[combined_df['SSN'] == "005-80-0322"]
ssn_rows

# %%



