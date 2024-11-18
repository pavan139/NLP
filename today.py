import pandas as pd

# Assume df1 and df2 are your DataFrames with date columns in datetime format

# Step 1: Add a unique identifier for each row in df1
df1 = df1.reset_index().rename(columns={'index': 'df1_index'})

# Step 2: Merge df1 and df2 on 'SSN_N' to get all combinations
merged_df = pd.merge(df1, df2, on='SSN_N', how='left')

# Step 3: Calculate 'Date_Diff' between 'TXN_TRD_D' and 'Payment Date'
merged_df['Date_Diff'] = (merged_df['TXN_TRD_D'] - merged_df['Payment Date']).dt.days

# Step 4: Apply conditions
# Mask for '403b Amount' == 0
amount_zero_mask = merged_df['403b Amount'] == 0

# Set 'TXN_TRD_D' and 'SUM(TXN_CASH_A)' to NaT/NaN where '403b Amount' == 0
merged_df.loc[amount_zero_mask, ['TXN_TRD_D', 'SUM(TXN_CASH_A)']] = [pd.NaT, pd.NA]

# Mask for valid 'Date_Diff' within 28 days
valid_mask = (~amount_zero_mask) & (merged_df['Date_Diff'] >= 0) & (merged_df['Date_Diff'] <= 28)

# Filter valid rows
valid_df = merged_df[valid_mask]

# Step 5: For each df1 row, select the latest 'TXN_TRD_D' within 28 days
idx = valid_df.groupby('df1_index')['TXN_TRD_D'].idxmax()
latest_df = valid_df.loc[idx]

# Step 6: Merge back with df1 to retain all original columns
result_df = df1.merge(latest_df[['df1_index', 'TXN_TRD_D', 'SUM(TXN_CASH_A)']], on='df1_index', how='left')

# Drop 'df1_index' as it's no longer needed
result_df = result_df.drop(columns=['df1_index'])

# Display the final DataFrame
print(result_df)
