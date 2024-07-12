import pandas as pd
import re
from datetime import datetime 

def is_valid_ssn(ssn):
    pattern = r'^\d{3}-\d{2}-\d{4}$'
    return bool(re.match(pattern, str(ssn)))  # Convert to string to handle non-string inputs

# Function to save DataFrame to CSV
def save_to_csv(df, filename):
    df.to_csv(f"{filename}.csv", index=False)
    print(f"Saved {filename}.csv")

# Read the Excel file
df = pd.read_excel(r"C:\Users\a760444\OneDrive - Fidelity Technology Group, LLC\Documents\VMCU\VUMC_data_pull.xlsx", sheet_name='Sheet1', parse_dates=['TERMINATION_DATE', 'DATE_OF_HIRE', 'DIVISION_CHANGE_DATE_1', 'ADJUSTED_DATE_OF_HIRE','FIRST_CONTRIBUTION_MANDATORY_AMOUNT', 'LAST_CONTRIBUTION_MANDATORY_AMOUNT', 'FIRST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT', 'LAST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT', 'DIVISION_CHANGE_DATE_2', 'DIVISION_CHANGE_DATE_3', 'DIVISION_CHANGE_DATE_4', 'DIVISION_CHANGE_DATE_5'])

# Separate valid and invalid SSNs
valid_ssn_df = df[df['SSN'].apply(is_valid_ssn)]
invalid_ssn_df = df[~df['SSN'].apply(is_valid_ssn)]

save_to_csv(invalid_ssn_df, "HMC_BAD_SSN")

# Define active and terminated status codes
active_codes = ['A', 'E', 'I', 'L', 'S', 'U', 'X']
terminated_codes = ['D', 'M', 'R', 'T']

# Create starting population
starting_population = valid_ssn_df[((valid_ssn_df['STATUS_CODE'].isin(active_codes)) |
                                   ((valid_ssn_df['STATUS_CODE'].isin(terminated_codes)) & (valid_ssn_df['TERMINATION_DATE'] >= '2023-04-01')))]

save_to_csv(starting_population, "Starting_Population")

not_in_starting_population = valid_ssn_df[~valid_ssn_df.index.isin(starting_population.index)]
save_to_csv(not_in_starting_population, "Not_In_Starting_Population")

# Define cutoff date
cutoff_date = pd.to_datetime('2023-04-01')

# Identify participants with mismatched contribution dates
condition1 = (pd.to_datetime(starting_population['FIRST_CONTRIBUTION_MANDATORY_AMOUNT']) < cutoff_date) & (pd.isnull(starting_population['FIRST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT']) | (pd.to_datetime(starting_population['FIRST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT']) > cutoff_date))
condition2 = (pd.to_datetime(starting_population['FIRST_CONTRIBUTION_MANDATORY_MATCH_AMOUNT']) < cutoff_date) & (pd.isnull(starting_population['FIRST_CONTRIBUTION_MANDATORY_AMOUNT']) | (pd.to_datetime(starting_population['FIRST_CONTRIBUTION_MANDATORY_AMOUNT']) > cutoff_date))

combined_condition = condition1 | condition2
Match_dates_to_review = starting_population[combined_condition]
save_to_csv(Match_dates_to_review, "Match_Dates_To_Review")

# Create clean starting population
clean_starting_population = starting_population[~combined_condition].copy()
clean_starting_population['latest_DIVISION_CHANGE'] = clean_starting_population[['DIVISION_CHANGE_DATE_1', 'DIVISION_CHANGE_DATE_2', 'DIVISION_CHANGE_DATE_3', 'DIVISION_CHANGE_DATE_4', 'DIVISION_CHANGE_DATE_5']].max(axis=1)
save_to_csv(clean_starting_population, "Clean_Starting_Population")

# Population 1: Participants with contributions into Mandatory source 01 and Mandatory Match source 03 prior to 04/01/2023
pop_1 = clean_starting_population[pd.to_datetime(clean_starting_population['FIRST_CONTRIBUTION_MANDATORY_AMOUNT']) < cutoff_date]
save_to_csv(pop_1, "HMC_Pop_1")

# Population 1A: No division change since 04/01/2023 and ADOH < 04/01/2023 or blank ADOH
pop_1A = pop_1[((pop_1['latest_DIVISION_CHANGE'] < cutoff_date) | (pop_1['latest_DIVISION_CHANGE'].isnull())) & ((pop_1['ADJUSTED_DATE_OF_HIRE'] < cutoff_date) | (pop_1['ADJUSTED_DATE_OF_HIRE'].isna()))]
save_to_csv(pop_1A, "HMC_Pop_1A")

# Population 1B: Division changes after 04/01/2023 and ADOH <04/01/2023 or blank ADOH
pop_1B = pop_1[(pop_1['latest_DIVISION_CHANGE'] >= cutoff_date) & ((pop_1['ADJUSTED_DATE_OF_HIRE'] < cutoff_date) | (pop_1['ADJUSTED_DATE_OF_HIRE'].isna()))]
save_to_csv(pop_1B, "HMC_Pop_1B")

# Population 1C: Participants with an ADOH > 04/01/2023
pop_1C = pop_1[pop_1['ADJUSTED_DATE_OF_HIRE'] >= cutoff_date]
save_to_csv(pop_1C, "HMC_Pop_1C")

# Population 2: Participants in division F, SO, SD, and Q with HCE indicator of N or NULL
pop_2 = clean_starting_population[(clean_starting_population['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q'])) & ((clean_starting_population['HCE_INDICATOR'].isin([None, 'N'])) | (clean_starting_population['HCE_INDICATOR'].isnull()))]
save_to_csv(pop_2, "HMC_Pop_2")

# Population 2A: No division change since 04/01/2023
pop_2A = pop_2[((pop_2['latest_DIVISION_CHANGE'] < cutoff_date) | (pop_2['latest_DIVISION_CHANGE'].isnull()))]
save_to_csv(pop_2A, "HMC_Pop_2A")

# Population 2B: Division changes after 04/01/2023
pop_2B = pop_2[(pop_2['latest_DIVISION_CHANGE'] >= cutoff_date)]
save_to_csv(pop_2B, "HMC_Pop_2B")

# Population 3: Participants with division name not in excluded list
excluded_divisions = ['F', 'SO', 'SD', 'Q', 'H', 'S', 'W']
pop_3 = clean_starting_population[~clean_starting_population['DIVISION_NAME'].isin(excluded_divisions)]
save_to_csv(pop_3, "HMC_Pop_3")

# Population 3A: No division change since 04/01/2023
pop_3A = pop_3[((pop_3['latest_DIVISION_CHANGE'] < cutoff_date) | (pop_3['latest_DIVISION_CHANGE'].isnull()))]
save_to_csv(pop_3A, "HMC_Pop_3A")

# Population 3B: Division changes after 04/01/2023
pop_3B = pop_3[(pop_3['latest_DIVISION_CHANGE'] >= cutoff_date)]
save_to_csv(pop_3B, "HMC_Pop_3B")

# Population 4: Participants in division F, SO, SD, Q with HCE indicator Y, or in division H, S, W
pop_4 = clean_starting_population[((clean_starting_population['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q']) & (clean_starting_population['HCE_INDICATOR'] == 'Y')) | clean_starting_population['DIVISION_NAME'].isin(['H', 'S', 'W']))]
save_to_csv(pop_4, "HMC_Pop_4")

# Voluntary Contribution Populations

# Population 1: Participants with no Voluntary deferral history for Pre-tax or Roth deferrals
condition_pop1 = (starting_population['PRETAX_DEFERRAL_AMOUNT'].isna() | (starting_population['PRETAX_DEFERRAL_AMOUNT'] == 0)) & (starting_population['ROTH_DEFERRAL_AMOUNT'].isna() | (starting_population['ROTH_DEFERRAL_AMOUNT'] == 0))
population_1 = starting_population[condition_pop1]
save_to_csv(population_1, "VC_Pop_1")

# Population 2: Participants in division F, SO, SD, Q with HCE indicator of N or NULL
condition_pop2 = starting_population['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q']) & (starting_population['HCE_INDICATOR'].isna() | (starting_population['HCE_INDICATOR'] == 'N'))
population_2 = starting_population[condition_pop2]
save_to_csv(population_2, "VC_Pop_2")

# Population 3: Participants with division name B, QP, N, FP, T, MAIN, NB, or NULL
condition_pop3 = starting_population['DIVISION_NAME'].isin(['B', 'QP', 'N', 'FP', 'T', 'MAIN', 'NB']) | starting_population['DIVISION_NAME'].isna()
population_3 = starting_population[condition_pop3]
save_to_csv(population_3, "VC_Pop_3")

# Population 4: Participants in division F, SO, SD, Q with HCE indicator Y, or in division H, S, W
condition_pop4 = (starting_population['DIVISION_NAME'].isin(['F', 'SO', 'SD', 'Q']) & (starting_population['HCE_INDICATOR'] == 'Y')) | starting_population['DIVISION_NAME'].isin(['H', 'S', 'W'])
population_4 = starting_population[condition_pop4]
save_to_csv(population_4, "VC_Pop_4")

print("All processing complete. CSV files have been generated for all populations and intermediate steps.")
