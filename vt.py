import pandas as pd
import numpy as np

# Sample data (replace this with your actual data)
data_df1 = {
    'SSN_N': ['123-45-6789'] * 30,
    'Payment Date': [
        '04/14/2023', '04/28/2023', '05/12/2023', '05/26/2023', '06/09/2023',
        '06/23/2023', '07/07/2023', '07/21/2023', '08/04/2023', '08/18/2023',
        '09/01/2023', '09/15/2023', '09/29/2023', '10/13/2023', '10/27/2023',
        '11/09/2023', '11/22/2023', '12/08/2023', '12/21/2023', '01/05/2024',
        '01/19/2024', '02/02/2024', '02/16/2024', '03/01/2024', '03/15/2024',
        '03/29/2024', '04/12/2024', '04/26/2024', '05/10/2024', '05/24/2024'
    ]
}

data_df2 = {
    'SSN_N': ['123-45-6789'] * 25,
    'TXN_TRD_D': [
        '04/24/2023', '04/28/2023', '05/12/2023', '05/26/2023', '06/09/2023',
        '06/23/2023', '07/07/2023', '07/21/2023', '08/04/2023', '08/18/2023',
        '09/01/2023', '09/15/2023', '09/29/2023', '10/13/2023', '10/27/2023',
        '11/09/2023', '11/22/2023', '12/08/2023', '12/21/2023', '03/01/2024',
        '03/15/2024', '04/01/2024', '04/12/2024', '04/26/2024', '05/10/2024'
    ],
    'SUM(TXN_CASH_A)': [
        120.92, 118.65, 116.39, 120.92, 120.92,
        120.92, 126.97, 126.59, 99.38, 108.20,
        108.83, 119.83, 107.88, 108.83, 108.83,
        119.11, 93.90, 99.38, 108.83, 13.33,
        76.13, 116.42, 61.55, 109.37, 88.50
    ]
}

# Create DataFrames
df1 = pd.DataFrame(data_df1)
df2 = pd.DataFrame(data_df2)

# Convert 'date' columns to datetime
df1['Payment Date'] = pd.to_datetime(df1['Payment Date'], format='%m/%d/%Y')
df2['TXN_TRD_D'] = pd.to_datetime(df2['TXN_TRD_D'], format='%m/%d/%Y')

# Sort df1 by date to process dates in chronological order
df1 = df1.sort_values('Payment Date').reset_index(drop=True)

# Optionally, sort df2 by date (for consistency and easier debugging)
df2 = df2.sort_values('TXN_TRD_D').reset_index(drop=True)

# Add an 'available' column to df2 to track used rows
df2['available'] = True

# Initialize a list to store the result rows
results = []

# Iterate over each row in df1
for idx1, row1 in df1.iterrows():
    ssn = row1['SSN_N']
    date = row1['Payment Date']
    
    # Step 1: Attempt to find an exact match
    exact_match = df2[
        (df2['SSN_N'] == ssn) &
        (df2['TXN_TRD_D'] == date) &
        (df2['available'])
    ]
    
    if not exact_match.empty:
        # Use the first exact match found
        matched_row = exact_match.iloc[0]
        df2_index = matched_row.name
        df2.at[df2_index, 'available'] = False  # Mark as used
        
        # Append the matched data
        result_row = row1.to_dict()
        result_row['SUM(TXN_CASH_A)'] = matched_row['SUM(TXN_CASH_A)']
        results.append(result_row)
    else:
        # Step 2: Find the closest date within 28 days
        date_range = pd.Timedelta(days=28)
        potential_matches = df2[
            (df2['SSN_N'] == ssn) &
            (df2['available']) &
            (df2['TXN_TRD_D'] >= date - date_range) &
            (df2['TXN_TRD_D'] <= date + date_range)
        ]
        
        if not potential_matches.empty:
            # Calculate the absolute difference in days
            potential_matches = potential_matches.copy()
            potential_matches['date_diff'] = (potential_matches['TXN_TRD_D'] - date).abs()
            # Find the row with the smallest date difference
            closest_match = potential_matches.loc[potential_matches['date_diff'].idxmin()]
            df2_index = closest_match.name
            df2.at[df2_index, 'available'] = False  # Mark as used
            
            # Append the matched data
            result_row = row1.to_dict()
            result_row['SUM(TXN_CASH_A)'] = closest_match['SUM(TXN_CASH_A)']
            results.append(result_row)
        else:
            # No match found; append NaN for 'SUM(TXN_CASH_A)'
            result_row = row1.to_dict()
            result_row['SUM(TXN_CASH_A)'] = np.nan
            results.append(result_row)

# Create the final DataFrame from results
result_df = pd.DataFrame(results)

# Optional: Reorder columns if needed
result_df = result_df[['SSN_N', 'Payment Date', 'SUM(TXN_CASH_A)']]

# Display the result
print(result_df)
