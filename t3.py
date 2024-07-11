import pandas as pd

# Load data from an Excel file
df = pd.read_excel('path_to_your_excel_file.xlsx', sheet_name='Sheet1', parse_dates=['TERMINATION_DATE', 'DIVISION_CHANGE_DATE_1', 'ADOH'])

# Validate SSNs: Assuming SSN format is 'XXX-XX-XXXX' and no alpha characters should be present
df['SSN_VALID'] = df['SSN'].apply(lambda x: str(x).replace('-', '').isdigit())
valid_ssn_df = df[df['SSN_VALID']]

# Define active and terminated participants as the starting population
active_codes = ['A', 'E', 'I', 'L', 'S', 'U', 'X']  # Active type status codes
terminated_codes = ['D', 'M', 'R', 'T']  # Non-active type status codes

# Filter for active participants and terminated participants with recent termination dates
starting_population = valid_ssn_df[((valid_ssn_df['STATUS_CODE'].isin(active_codes)) |
                                   ((valid_ssn_df['STATUS_CODE'].isin(terminated_codes)) & (valid_ssn_df['TERMINATION_DATE'] >= '2023-04-01')))]

# Define populations from the starting population
def define_populations(dataframe):
    pop_1A = dataframe[(dataframe['STATUS_CODE'] == 'A') & (dataframe['DIVISION_CHANGE_DATE_1'] <= '2023-04-01') & ((dataframe['ADOH'] < '2023-04-01') | (dataframe['ADOH'].isna()))]
    pop_1B = dataframe[(dataframe['DIVISION_CHANGE_DATE_1'] > '2023-04-01') & ((dataframe['ADOH'] < '2023-04-01') | (dataframe['ADOH'].isna()))]
    pop_1C = dataframe[dataframe['ADOH'] > '2023-04-01']
    pop_2A = dataframe[(dataframe['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q'])) & (dataframe['HCE_INDICATOR'].isin([None, 'N'])) & (dataframe['DIVISION_CHANGE_DATE_1'] <= '2023-04-01')]
    pop_2B = dataframe[(dataframe['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q'])) & (dataframe['DIVISION_CHANGE_DATE_1'] > '2023-04-01')]
    pop_3A = dataframe[dataframe['DIVISION_NAME'].isin(['B', 'QP', 'N', 'FP', 'T', 'MAIN', 'NB', 'NULL']) & (dataframe['DIVISION_CHANGE_DATE_1'] <= '2023-04-01')]
    pop_3B = dataframe[dataframe['DIVISION_NAME'].isin(['B', 'QP', 'N', 'FP', 'T', 'MAIN', 'NB', 'NULL']) & (dataframe['DIVISION_CHANGE_DATE_1'] > '2023-04-01')]
    pop_4 = dataframe[((dataframe['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q']) & dataframe['HCE_INDICATOR'] == 'Y') | dataframe['DIVISION_NAME'].isin(['H', 'S', 'W']))]

    return pop_1A, pop_1B, pop_1C, pop_2A, pop_2B, pop_3A, pop_3B, pop_4

# Call the function and export to CSV
pop_1A, pop_1B, pop_1C, pop_2A, pop_2B, pop_3A, pop_3B, pop_4 = define_populations(starting_population)

# Print to verify
print(pop_1A)
print(pop_1B)
print(pop_1C)
print(pop_2A)
print(pop_2B)
print(pop_3A)
print(pop_3B)
print(pop_4)
