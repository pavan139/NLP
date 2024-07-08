import pandas as pd

# Step 1: Load data from Excel
df = pd.read_excel('data.xlsx')

# Step 2: Create Helper Column
# This column is used to distinguish between multiple records for the same SSN
df['helper'] = df.groupby('SSN').cumcount() + 1

# Step 3: Pivot the Data for 'Prev' and 'Date'
# Pivot for 'Prev'
prev_pivot = df.pivot(index='SSN', columns='helper', values='Prev').add_prefix('Prev_')

# Pivot for 'Date'
date_pivot = df.pivot(index='SSN', columns='helper', values='Date').add_prefix('Date_')

# Concatenate horizontally
result = pd.concat([prev_pivot, date_pivot], axis=1)

# Optional: Sort and flatten the columns for a cleaner format
result = result.sort_index(axis=1, level=1)  # Sort by helper column number if mixed
result.columns = [f'{val[0]}_{val[1]}' for val in result.columns]

# Step 4: Add the constant 'Plan' Column
# Assuming 'Plan' is constant across the dataset, take the first value
plan_value = df['Plan'].iloc[0]
result['Plan'] = plan_value

# Step 5: Reset index to make SSN a column again
result = result.reset_index()

# Step 6: Save the data to CSV
result.to_csv('pivoted_output.csv', index=False)

# Optional: Print the output to check
print(result.head())
