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


import pandas as pd

# Attempt to load data from Excel
try:
    df = pd.read_excel('data.xlsx', engine='openpyxl')
except FileNotFoundError:
    print("Error: The file 'data.xlsx' was not found in the specified path.")
    exit()
except Exception as e:
    print(f"An error occurred: {e}")
    exit()

# Create Helper Column
df['helper'] = df.groupby('SSN').cumcount() + 1

# Pivot the Data for 'Prev' and 'Date'
prev_pivot = df.pivot(index='SSN', columns='helper', values='Prev').add_prefix('Prev_')
date_pivot = df.pivot(index='SSN', columns='helper', values='Date').add_prefix('Date_')

# Concatenate horizontally
result = pd.concat([prev_pivot, date_pivot], axis=1)

# Interleave columns P1, D1, P2, D2, ...
# Generate new column order
new_order = [f'Prev_{i}' if 'Prev_{i}' in result else f'Date_{i}' for i in range(1, max(len(prev_pivot.columns), len(date_pivot.columns))+1) for col in ['Prev_', 'Date_'] if f'{col}{i}' in result.columns]
result = result[new_order]

# Add the constant 'Plan' Column
plan_value = df['Plan'].iloc[0]
result['Plan'] = plan_value

# Reset index to make SSN a column again
result = result.reset_index()

# Save the data to CSV
result.to_csv('pivoted_output.csv', index=False)

# Print the output to check
print(result.head())
