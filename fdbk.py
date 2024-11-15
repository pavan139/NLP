import pandas as pd

# Function to perform the left join logic, preserving additional columns and handling nulls
def join_feedback_with_payments_with_date(df1, df2):
    # Add a new column for the calculated 28-day cutoff
    df1['Cutoff Date'] = df1['Payment Date'] - pd.Timedelta(days=28)
    
    # Perform a left join-like operation
    result_amt_val_q = []
    result_fdbck_sent_d = []
    for _, row in df1.iterrows():
        ssn = row['SSN_N']
        cutoff_date = row['Cutoff Date']
        
        if pd.isna(cutoff_date) or pd.isna(ssn):
            # Handle cases where data is missing
            result_amt_val_q.append(None)
            result_fdbck_sent_d.append(None)
            continue
        
        # Filter df2 for the current SSN_N and dates before cutoff_date
        relevant_feedback = df2[(df2['SSN_N'] == ssn) & (df2['FDBCK_SENT_D'] < cutoff_date)]
        
        if not relevant_feedback.empty:
            # Get the row with the greatest FDBCK_SENT_D
            best_feedback = relevant_feedback.loc[relevant_feedback['FDBCK_SENT_D'].idxmax()]
            amt_val_q = best_feedback['AMT_VAL_Q']
            fdbck_sent_d = best_feedback['FDBCK_SENT_D']
        else:
            amt_val_q = None  # No valid feedback found
            fdbck_sent_d = None  # No valid feedback date found
        
        result_amt_val_q.append(amt_val_q)
        result_fdbck_sent_d.append(fdbck_sent_d)
    
    # Add the results to df1
    df1['AMT_VAL_Q'] = result_amt_val_q
    df1['FDBCK_SENT_D'] = result_fdbck_sent_d
    df1.drop(columns=['Cutoff Date'], inplace=True)  # Remove the helper column
    
    return df1

# Example usage with diverse test cases
df1_test = pd.DataFrame({
    'SSN_N': [1, 2, 1, 3, 4],
    'Payment Date': ['2023-11-10', '2023-11-12', '2023-11-20', '2023-11-05', None],
    'Extra Column 1': ['Extra1', 'Extra2', 'Extra3', 'Extra4', 'Extra5'],
    'Extra Column 2': [10, 20, 30, 40, 50]
})
df1_test['Payment Date'] = pd.to_datetime(df1_test['Payment Date'])

df2_test = pd.DataFrame({
    'SSN_N': [1, 2, 2, 3, None],
    'AMT_VAL_Q': [100, 200, 150, 300, None],
    'FDBCK_SENT_D': ['2023-10-10', '2023-09-01', '2023-10-01', '2023-10-15', '2023-09-10']
})
df2_test['FDBCK_SENT_D'] = pd.to_datetime(df2_test['FDBCK_SENT_D'])

# Execute the function
result_df_test = join_feedback_with_payments_with_date(df1_test, df2_test)

# Display the result
import ace_tools as tools; tools.display_dataframe_to_user(name="Cleaned Test Results", dataframe=result_df_test)
