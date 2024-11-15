import pandas as pd
import numpy as np
from scipy.optimize import linear_sum_assignment

# Ensure 'SSN_N' columns are of the same type
df1['SSN_N'] = df1['SSN_N'].astype(str)
df2['SSN_N'] = df2['SSN_N'].astype(str)

# Convert 'Payment Date' and 'TXN_TRD_D' columns to datetime
df1['Payment Date'] = pd.to_datetime(df1['Payment Date'])
df2['TXN_TRD_D'] = pd.to_datetime(df2['TXN_TRD_D'])

# Step 0: Sort df1 by 'Payment Date' to process dates in chronological order
df1 = df1.sort_values('Payment Date').reset_index(drop=True)

# **Add 'df2_index' to df2 before any merges**
df2 = df2.reset_index().rename(columns={'index': 'df2_index'})
df2['used'] = False

# Step 1: Perform an exact merge on 'SSN_N' and 'Payment Date'
exact_matches = pd.merge(
    df1,
    df2[['SSN_N', 'TXN_TRD_D', 'SUM(TXN_CASH_A)', 'df2_index']],
    left_on=['SSN_N', 'Payment Date'],
    right_on=['SSN_N', 'TXN_TRD_D'],
    how='left',
    indicator=True
)

# Mark df2 rows used in exact matches
exact_matches_matched = exact_matches[exact_matches['_merge'] == 'both']
matched_df2_indices = exact_matches_matched['df2_index'].dropna().astype(int)
df2.loc[matched_df2_indices, 'used'] = True

# Separate matched and unmatched df1 rows
exact_matches_df1 = exact_matches_matched.drop(columns=['_merge', 'TXN_TRD_D', 'df2_index'])
unmatched_df1 = exact_matches[exact_matches['_merge'] == 'left_only'].drop(columns=['SUM(TXN_CASH_A)', '_merge', 'TXN_TRD_D', 'df2_index'])

# Step 2: Find potential approximate matches for unmatched df1 rows
# Available df2 rows (not used in exact matches)
available_df2 = df2[~df2['used']].copy()

# **Reset index and keep track of original indices**
available_df2 = available_df2.reset_index(drop=True)
available_df2['available_df2_idx'] = available_df2.index

# **Reset index for unmatched_df1**
unmatched_df1 = unmatched_df1.reset_index(drop=True)
unmatched_df1['unmatched_df1_idx'] = unmatched_df1.index

# Merge unmatched df1 with available df2 on 'SSN_N'
potential_matches = unmatched_df1.merge(
    available_df2[['SSN_N', 'TXN_TRD_D', 'SUM(TXN_CASH_A)', 'df2_index', 'available_df2_idx']],
    on='SSN_N'
)

# Calculate absolute date differences
potential_matches['date_diff'] = (potential_matches['Payment Date'] - potential_matches['TXN_TRD_D']).abs()

# Filter matches where date difference ≤28 days
potential_matches = potential_matches[potential_matches['date_diff'] <= pd.Timedelta(days=28)]

# **Check if potential_matches is empty**
if not potential_matches.empty:
    # Create a cost matrix for the assignment problem
    num_unmatched_df1 = len(unmatched_df1)
    num_available_df2 = len(available_df2)
    cost_matrix = np.full((num_unmatched_df1, num_available_df2), np.inf)

    # Fill the cost matrix
    for _, row in potential_matches.iterrows():
        i = row['unmatched_df1_idx']
        j = row['available_df2_idx']
        cost = row['date_diff'].days  # Use days as cost
        cost_matrix[i, j] = cost

    # **Check if the cost matrix has any finite values**
    if np.isfinite(cost_matrix).any():
        # Solve the assignment problem
        row_ind, col_ind = linear_sum_assignment(cost_matrix)

        # Extract valid assignments where cost is not infinity
        valid_assignments = cost_matrix[row_ind, col_ind] != np.inf
        assigned_df1_indices = row_ind[valid_assignments]
        assigned_df2_indices = col_ind[valid_assignments]

        # Retrieve matched rows
        matched_df1 = unmatched_df1.loc[assigned_df1_indices].reset_index(drop=True)
        matched_df2 = available_df2.loc[assigned_df2_indices].reset_index(drop=True)

        # Combine matched data
        approximate_matches = pd.concat(
            [matched_df1.reset_index(drop=True), matched_df2[['SUM(TXN_CASH_A)']].reset_index(drop=True)],
            axis=1
        )

        # Mark df2 rows as used
        matched_df2_indices = matched_df2['df2_index'].astype(int)
        df2.loc[matched_df2_indices, 'used'] = True
    else:
        print("Cost matrix contains only infinite values. No feasible assignment.")
        approximate_matches = pd.DataFrame(columns=unmatched_df1.columns.tolist() + ['SUM(TXN_CASH_A)'])
        assigned_df1_indices = []
else:
    print("No potential matches found within 28 days.")
    approximate_matches = pd.DataFrame(columns=unmatched_df1.columns.tolist() + ['SUM(TXN_CASH_A)'])
    assigned_df1_indices = []

# Step 3: Identify unmatched df1 rows after approximate matching
remaining_unmatched_df1 = unmatched_df1[~unmatched_df1['unmatched_df1_idx'].isin(assigned_df1_indices)].copy()
remaining_unmatched_df1['SUM(TXN_CASH_A)'] = np.nan

# Step 4: Combine all results
result_df = pd.concat(
    [
        exact_matches_df1.reset_index(drop=True),
        approximate_matches.reset_index(drop=True),
        remaining_unmatched_df1.reset_index(drop=True)
    ],
    ignore_index=True
)

# Sort the result if needed
result_df = result_df.sort_values(by=['SSN_N', 'Payment Date']).reset_index(drop=True)

# Display the result
print(result_df)
