import pandas as pd
import re
from datetime import datetime 
# Load data from an Excel file
df = pd.read_excel(r"C:\Users\a760444\OneDrive - Fidelity Technology Group, LLC\Documents\VMCU\VUMC_data_pull.xlsx", sheet_name='Sheet1', parse_dates=['TERMINATION_DATE', 'DATE_OF_HIRE', 'DIVISION_CHANGE_DATE_1', 'ADJUSTED_DATE_OF_HIRE','FIRST_CONTRIBUTION_MANDATORY_AMOUNT'	,'LAST_CONTRIBUTION_MANDATORY_AMOUNT'	,	'FIRST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT',	'LAST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT', 'DIVISION_CHANGE_DATE_2', 'DIVISION_CHANGE_DATE_3', 'DIVISION_CHANGE_DATE_4', 'DIVISION_CHANGE_DATE_5'])


#Hours/Mandatory Contribution Population: 

#
# Validate SSNs: Assuming SSN format is 'XXX-XX-XXXX' and no alpha characters should be present
# df['SSN_VALID'] = df['SSN'].apply(lambda x: str(x).replace('-', '').isdigit())
# valid_ssn_df = df[df['SSN_VALID']]

def is_valid_ssn(ssn):
    pattern = r'^\d{3}-\d{2}-\d{4}$'
    return bool(re.match(pattern, ssn))


print(len(df))
# Divide the DataFrame based on valid and invalid SSN
valid_ssn_df = df[df['SSN'].apply(is_valid_ssn)]
print(len(valid_ssn_df))
invalid_ssn_df = df[~df['SSN'].apply(is_valid_ssn)]
print(len(invalid_ssn_df))



# Define active and terminated participants as the starting population
active_codes = ['A', 'E', 'I', 'L', 'S', 'U', 'X']  # Active type status codes
terminated_codes = ['D', 'M', 'R', 'T']  # Non-active type status codes

# Filter for active participants and terminated participants with recent termination dates
starting_population = valid_ssn_df[((valid_ssn_df['STATUS_CODE'].isin(active_codes)) |
                                   ((valid_ssn_df['STATUS_CODE'].isin(terminated_codes)) & (valid_ssn_df['TERMINATION_DATE'] >= '2023-04-01')))]

print(len(starting_population))
# Get data not in starting_population
not_in_starting_population = valid_ssn_df[~valid_ssn_df.index.isin(starting_population.index)]
print(len(not_in_starting_population))



print(starting_population)
# Define populations from the starting population



#Population 1

cutoff_date = datetime.strptime('04/01/2023', '%m/%d/%Y')

# Define the conditions
condition1 = (pd.to_datetime(starting_population['FIRST_CONTRIBUTION_MANDATORY_AMOUNT']) < cutoff_date) & (pd.isnull(starting_population['FIRST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT']) | (pd.to_datetime(starting_population['FIRST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT']) > cutoff_date))
condition2 = (pd.to_datetime(starting_population['FIRST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT']) < cutoff_date) & (pd.isnull(starting_population['FIRST_CONTRIBUTION_MANDATORY_AMOUNT']) | (pd.to_datetime(starting_population['FIRST_CONTRIBUTION_MANDATORY_AMOUNT']) > cutoff_date))

# Filter the rows based on the conditions
combined_condition = condition1 | condition2
Match_dates_to_review = starting_population[combined_condition]
print(Match_dates_to_review)

clean_starting_population = starting_population[~combined_condition]
#filtered_rows.to_csv(r'C:\Users\a760444\OneDrive - Fidelity Technology Group, LLC\Documents\VMCU\VUMC_Match_date_review.xlsx')


clean_starting_population = clean_starting_population.copy()

# Perform the operation on the copy
clean_starting_population['latest_DIVISION_CHANGE'] = clean_starting_population[['DIVISION_CHANGE_DATE_1', 'DIVISION_CHANGE_DATE_2', 'DIVISION_CHANGE_DATE_3', 'DIVISION_CHANGE_DATE_4', 'DIVISION_CHANGE_DATE_5']].max(axis=1)


print(len(clean_starting_population))
cutoff_date = datetime.strptime('04/01/2023', '%m/%d/%Y')
pop_1 = clean_starting_population[(pd.to_datetime(clean_starting_population['FIRST_CONTRIBUTION_MANDATORY_AMOUNT']) < cutoff_date)]
print(len(pop_1))



#filtered_pop_1 = pop_1[(pop_1['latest_DIVISION_CHANGE'] <= '2023-04-01') | (pop_1['latest_DIVISION_CHANGE'].isnull())]

pop_1A = pop_1[((pop_1['latest_DIVISION_CHANGE'] < '2023-04-01') | (pop_1['latest_DIVISION_CHANGE'].isnull())) & ((pop_1['ADJUSTED_DATE_OF_HIRE'] < '2023-04-01') | (pop_1['ADJUSTED_DATE_OF_HIRE'].isna()))]
pop_1B = pop_1[(pop_1['latest_DIVISION_CHANGE'] >= '2023-04-01') & ((pop_1['ADJUSTED_DATE_OF_HIRE'] < '2023-04-01') | (pop_1['ADJUSTED_DATE_OF_HIRE'].isna()))]
pop_1C = pop_1[pop_1['ADJUSTED_DATE_OF_HIRE'] >= '2023-04-01']





pop_2 = clean_starting_population[(clean_starting_population['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q'])) &((clean_starting_population['HCE_INDICATOR'].isin([None, 'N'])) | (pop_1['HCE_INDICATOR'].isnull() ))]


pop_2A = pop_2[((pop_2['latest_DIVISION_CHANGE'] < '2023-04-01') | (pop_2['latest_DIVISION_CHANGE'].isnull()))]
pop_2B = pop_2[(pop_2['latest_DIVISION_CHANGE'] >= '2023-04-01')]


excluded_divisions = ['F', 'SO', 'SD', 'Q', 'H', 'S', 'W']

pop_3 = clean_starting_population[~clean_starting_population['DIVISION_NAME'].isin(excluded_divisions)]

pop_3A = pop_3[((pop_3['latest_DIVISION_CHANGE'] < '2023-04-01') | (pop_3['latest_DIVISION_CHANGE'].isnull()))]
pop_3B = pop_3[(pop_3['latest_DIVISION_CHANGE'] >= '2023-04-01')]

pop_4 = clean_starting_population[((clean_starting_population['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q']) & clean_starting_population['HCE_INDICATOR'] == 'Y') | clean_starting_population['DIVISION_NAME'].isin(['H', 'S', 'W']))]



condition_pop1 = (starting_population['PRETAX_DEFERRAL_AMOUNT'].isna() | (starting_population['PRETAX_DEFERRAL_AMOUNT'] == 0)) & (starting_population['ROTH_DEFERRAL_AMOUNT'].isna() | (starting_population['ROTH_DEFERRAL_AMOUNT'] == 0))

# Population 2: In division F, SO, SD, Q and have an HCE indicator of N or NULL
condition_pop2 = starting_population['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q']) & (starting_population['HCE_INDICATOR'].isna() | (starting_population['HCE_INDICATOR'] == 'N'))

# Population 3: Division name of B, QP, N, FP, T, MAIN, NB, NULL (blank)
condition_pop3 = starting_population['DIVISION_NAME'].isin(['B', 'QP', 'N', 'FP', 'T', 'MAIN', 'NB']) | starting_population['DIVISION_NAME'].isna()

# Population 4: In division F, SO, SD, Q with an HCE indicator of Y and participants in division H, S, W
condition_pop4 = (starting_population['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q']) & (starting_population['HCE_INDICATOR'] == 'Y')) | starting_population['DIVISION_NAME'].isin(['H', 'S', 'W'])


population_1 = starting_population[condition_pop1]
population_2 = starting_population[condition_pop2]
population_3 = starting_population[condition_pop3]
population_4 = starting_population[condition_pop4]
