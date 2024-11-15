import pandas as pd
import numpy as np
from scipy.optimize import linear_sum_assignment

# Assuming df1 and df2 are your actual DataFrames
# Ensure 'ssn' columns are of the same type
df1['ssn'] = df1['ssn'].astype(str)
df2['ssn'] = df2['ssn'].astype(str)

# Convert 'date' columns to datetime
df1['date'] = pd.to_datetime(df1['date'])
df2['date'] = pd.to_datetime(df2['date'])

# Step 1: Perform an exact merge on 'ssn' and 'date'
exact_matches = pd.merge(df1, df2[['ssn', 'date', 'cash']], on=['ssn', 'date'], how='left', indicator=True)

# Mark df2 rows used in exact matches
df2['used'] = False
df2_exact_matched = df2.merge(df1, on=['ssn', 'date'], how='inner')
df2.loc[df2_exact_matched.index, 'used'] = True

# Separate matched and unmatched df1 rows
exact_matches_df1 = exact_matches[exact_matches['_merge'] == 'both'].drop(columns=['_merge'])
unmatched_df1 = exact_matches[exact_matches['_merge'] == 'left_only'].drop(columns=['cash', '_merge'])

# Step 2: Find potential approximate matches for unmatched df1 rows
# Available df2 rows (not used in exact matches)
available_df2 = df2[~df2['used']].copy()

# Merge unmatched df1 with available df2 on 'ssn'
potential_matches = unmatched_df1.merge(available_df2[['ssn', 'date', 'cash']], on='ssn', suffixes=('_df1', '_df2'))

# Calculate absolute date differences
potential_matches['date_diff'] = (potential_matches['date_df1'] - potential_matches['date_df2']).abs()

# Filter matches where date difference ≤28 days
potential_matches = potential_matches[potential_matches['date_diff'] <= pd.Timedelta(days=28)]

if not potential_matches.empty:
    # Assign unique indices to unmatched_df1 and available_df2
    unmatched_df1 = unmatched_df1.reset_index(drop=True)
    available_df2 = available_df2.reset_index(drop=True)

    # Map indices for unmatched_df1 and available_df2
    unmatched_df1['df1_index'] = unmatched_df1.index
    available_df2['df2_index'] = available_df2.index

    # Merge indices into potential_matches
    potential_matches = potential_matches.merge(
        unmatched_df1[['ssn', 'date', 'df1_index']].rename(columns={'date': 'date_df1'}),
        on=['ssn', 'date_df1']
    )
    potential_matches = potential_matches.merge(
        available_df2[['ssn', 'date', 'df2_index']].rename(columns={'date': 'date_df2'}),
        on=['ssn', 'date_df2']
    )

    # Create a cost matrix for the assignment problem
    num_df1 = len(unmatched_df1)
    num_df2 = len(available_df2)
    cost_matrix = np.full((num_df1, num_df2), np.inf)

    for _, row in potential_matches.iterrows():
        i = row['df1_index']
        j = row['df2_index']
        cost = row['date_diff'].days  # Use days as cost
        cost_matrix[i, j] = cost

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
        [matched_df1[['ssn', 'date']], matched_df2[['cash']]],
        axis=1
    )

    # Mark df2 rows as used
    df2.loc[available_df2.loc[assigned_df2_indices].index, 'used'] = True
else:
    approximate_matches = pd.DataFrame(columns=['ssn', 'date', 'cash'])

# Step 3: Identify unmatched df1 rows after approximate matching
remaining_unmatched_df1 = unmatched_df1[~unmatched_df1.index.isin(assigned_df1_indices)].copy()
remaining_unmatched_df1['cash'] = np.nan

# Step 4: Combine all results
result_df = pd.concat(
    [exact_matches_df1[['ssn', 'date', 'cash']], approximate_matches, remaining_unmatched_df1[['ssn', 'date', 'cash']]],
    ignore_index=True
)

# Sort the result if needed
result_df = result_df.sort_values(by=['ssn', 'date']).reset_index(drop=True)

# Display the result
print(result_df)
