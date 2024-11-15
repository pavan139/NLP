import pandas as pd

# Assuming df1 and df2 are your DataFrames
# Replace these with your actual data
df1 = pd.DataFrame({
    'SSN_N': [1, 2, 1],
    'Payment_Date': ['2023-11-10', '2023-11-12', '2023-11-20']
})
df1['Payment_Date'] = pd.to_datetime(df1['Payment_Date'])

df2 = pd.DataFrame({
    'SSN_N': [1, 1, 2, 1],
    'AMT_VAL_Q': [100, 200, 150, 300],
    'FDBCK_SENT_D': ['2023-10-10', '2023-10-05', '2023-10-01', '2023-10-15']
})
df2['FDBCK_SENT_D'] = pd.to_datetime(df2['FDBCK_SENT_D'])

# Merge and filter rows based on criteria
def filter_and_merge(df1, df2):
    # Perform a self-join on SSN_N
    merged_df = pd.merge(df1, df2, on='SSN_N', how='left')
    
    # Add a column for the difference in days between Payment_Date and FDBCK_SENT_D
    merged_df['days_diff'] = (merged_df['Payment_Date'] - merged_df['FDBCK_SENT_D']).dt.days
    
    # Filter rows where days_diff >= 28
    filtered_df = merged_df[merged_df['days_diff'] >= 28]
    
    # Find the newest FDBCK_SENT_D for each SSN_N and Payment_Date
    filtered_df = filtered_df.sort_values(by=['SSN_N', 'Payment_Date', 'FDBCK_SENT_D'], ascending=[True, True, False])
    result_df = filtered_df.groupby(['SSN_N', 'Payment_Date']).first().reset_index()
    
    # Drop helper columns
    result_df = result_df.drop(columns=['days_diff'])
    
    return result_df

# Apply the function
result = filter_and_merge(df1, df2)

# Display the final result
print(result)
