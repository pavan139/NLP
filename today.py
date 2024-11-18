# Step 1: Add 'df1_index'
df1 = df1.reset_index().rename(columns={'index': 'df1_index'})

# Step 2: Merge df1 and df2 on 'SSN_N'
merged_df = pd.merge(df1, df2, on='SSN_N', how='left')

# Step 3: Calculate 'Date_Diff'
merged_df['Date_Diff'] = (merged_df['TXN_TRD_D'] - merged_df['Payment Date']).dt.days

# Step 4: Apply conditions
amount_zero_mask = merged_df['403b Amount'] == 0
merged_df.loc[amount_zero_mask, ['TXN_TRD_D', 'SUM(TXN_CASH_A)', 'Date_Diff']] = [pd.NaT, pd.NA, pd.NA]
valid_mask = (~amount_zero_mask) & (merged_df['Date_Diff'] >= 0) & (merged_df['Date_Diff'] <= 28)
valid_df = merged_df[valid_mask]

# Step 5: Select 'TXN_TRD_D' with least 'Date_Diff'
idx = valid_df.groupby('df1_index')['Date_Diff'].idxmin()
closest_df = valid_df.loc[idx]

# Step 6: Merge back with df1
result_df = df1.merge(closest_df[['df1_index', 'TXN_TRD_D', 'SUM(TXN_CASH_A)']], on='df1_index', how='left')

# Drop 'df1_index'
result_df = result_df.drop(columns=['df1_index'])

# Display the result
print(result_df)
