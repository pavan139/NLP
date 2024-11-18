# Merge and calculate 'Date_Diff'
merged_df = pd.merge(df1, df2, on='SSN_N', how='left')
merged_df['Date_Diff'] = (merged_df['TXN_TRD_D'] - merged_df['Payment Date']).dt.days

# Masks
amount_zero_mask = merged_df['403b Amount'] == 0
valid_mask = (merged_df['Date_Diff'] >= 28) & (~amount_zero_mask)

# Set NaT/NaN where '403b Amount' == 0
merged_df.loc[amount_zero_mask, ['TXN_TRD_D', 'SUM(TXN_CASH_A)', 'Date_Diff']] = [pd.NaT, pd.NA, pd.NA]

# Get latest 'TXN_TRD_D'
valid_df = merged_df[valid_mask]
idx = valid_df.groupby(['SSN_N', 'Payment Date'])['TXN_TRD_D'].idxmax()
latest_df = valid_df.loc[idx]

# Final merge
result_df = pd.merge(df1, latest_df[['SSN_N', 'Payment Date', 'TXN_TRD_D', 'SUM(TXN_CASH_A)']],
                     on=['SSN_N', 'Payment Date'], how='left')

# Display result
print(result_df)
