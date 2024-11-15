import pandas as pd
import numpy as np

# Assume df1 and df2 are your dataframes
# Convert 'date' columns to datetime
df1['Payment Date'] = pd.to_datetime(df1['Payment Date'])
df2['TXN_TRD_D'] = pd.to_datetime(df2['TXN_TRD_D'])

# Separate rows where amount != 0 and amount == 0
df1_nonzero = df1[df1['amount'] != 0].copy()
df1_zero = df1[df1['amount'] == 0].copy()

# **Step 1: Attempt Exact Matches**

# Merge on SSN_N and Payment Date
exact_matches = pd.merge(
    df1_nonzero,
    df2,
    left_on=['SSN_N', 'Payment Date'],
    right_on=['SSN_N', 'TXN_TRD_D'],
    how='left',
    suffixes=('_df1', '_df2'),
    indicator=True
)

# Rows where matches were found
matched_exact = exact_matches[exact_matches['_merge'] == 'both'].copy()
unmatched_exact = exact_matches[exact_matches['_merge'] == 'left_only'].copy()

# Drop the merge indicator
matched_exact.drop(columns=['_merge'], inplace=True)
unmatched_exact.drop(columns=['_merge', 'SUM(TXN_CASH_A)', 'Other_df2_Column', 'TXN_TRD_D'], errors='ignore', inplace=True)

# **Step 2: Find Closest Date Matches Within 28 Days for Unmatched Rows**

if not unmatched_exact.empty:
    # Prepare unmatched df1 rows
    unmatched_exact['key'] = unmatched_exact['SSN_N']
    unmatched_exact['Payment_Date_minus_28'] = unmatched_exact['Payment Date'] - pd.Timedelta(days=28)
    unmatched_exact['Payment_Date_plus_28'] = unmatched_exact['Payment Date'] + pd.Timedelta(days=28)

    # Prepare df2 for matching
    df2['key'] = df2['SSN_N']

    # Merge to find potential matches
    potential_matches = pd.merge(
        unmatched_exact,
        df2,
        on='key',
        suffixes=('_df1', '_df2')
    )

    # Filter potential matches within the date range
    potential_matches = potential_matches[
        (potential_matches['TXN_TRD_D'] >= potential_matches['Payment_Date_minus_28']) &
        (potential_matches['TXN_TRD_D'] <= potential_matches['Payment_Date_plus_28'])
    ]

    # Calculate the absolute difference in days
    potential_matches['date_diff'] = (potential_matches['TXN_TRD_D'] - potential_matches['Payment Date']).abs()

    # **Assign Unique df2 Rows to df1 Rows**

    # Sort to prioritize closest matches
    potential_matches.sort_values(['date_diff'], inplace=True)

    # Remove duplicates based on df2 index to ensure each df2 row is used only once
    potential_matches = potential_matches.drop_duplicates(subset=['index_df2'])

    # Also ensure that each df1 row gets the best available df2 row
    potential_matches = potential_matches.drop_duplicates(subset=['index_df1'])

    # Get indices of matched df1 and df2 rows
    matched_indices_df1 = potential_matches['index_df1'].tolist()
    matched_indices_df2 = potential_matches['index_df2'].tolist()

    # Update availability in df2
    df2.loc[matched_indices_df2, 'available'] = False

    # Prepare matched and unmatched dataframes
    matched_closest = potential_matches.copy()
    unmatched_final = unmatched_exact[~unmatched_exact.index.isin(matched_indices_df1)].copy()
else:
    matched_closest = pd.DataFrame()
    unmatched_final = unmatched_exact.copy()

# **Step 3: Combine All Matched Rows**

# Combine exact and closest matches
matched = pd.concat([matched_exact, matched_closest], ignore_index=True)

# For unmatched_final, append NaNs for df2 columns
unmatched_final[df2.columns.difference(unmatched_final.columns)] = np.nan

# For amount == 0 rows, append NaNs for df2 columns
df1_zero[df2.columns.difference(df1_zero.columns)] = np.nan

# Combine all results
final_result = pd.concat([matched, unmatched_final, df1_zero], ignore_index=True)

# Optional: Drop helper columns
final_result.drop(columns=['Payment_Date_minus_28', 'Payment_Date_plus_28', 'date_diff', 'key', 'index_df1', 'index_df2'], errors='ignore', inplace=True)

# Display or save the result
print(final_result)
