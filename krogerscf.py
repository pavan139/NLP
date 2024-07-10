import pandas as pd

# Creating a sample DataFrame to mimic reading from a CSV
data = {
    'SSN': ['123-45-6789', '123-45-6789', '123-45-6789', '234-56-7890', '234-56-7890', '345-67-8901'],
    'Hire Date': ['01/01/2020', '01/15/2020', None, '02/20/2021', '02/25/2021', None],
    'Termination Date': [None, '12/31/2020', '11/30/2020', None, '03/30/2021', None],
    'Date of Rehire': ['02/01/2020', None, '03/15/2020', None, '03/01/2021', None],
    'Match Eligibility Date': ['01/10/2020', '01/20/2020', '02/28/2020', '02/25/2021', '03/05/2021', None]
}

df = pd.DataFrame(data)

# Convert date columns to datetime
date_columns = ['Hire Date', 'Termination Date', 'Date of Rehire', 'Match Eligibility Date']
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce', format='%m/%d/%Y')

# Sort by SSN and relevant dates
df = df.sort_values(by=['SSN', 'Hire Date', 'Termination Date', 'Date of Rehire', 'Match Eligibility Date'])

# Define aggregation rules
aggregation_rules = {
    'Hire Date': 'min',
    'Termination Date': 'max',
    'Date of Rehire': 'max',
    'Match Eligibility Date': 'min'
}

# Group by SSN and aggregate based on rules
df_aggregated = df.groupby('SSN', as_index=False).agg(aggregation_rules)

# Remove records with no Hire Date and Termination Date if no duplicates exist
df_aggregated = df_aggregated.dropna(subset=['Hire Date', 'Termination Date'], how='all')

df_aggregated
