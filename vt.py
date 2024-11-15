import pandas as pd
import numpy as np
import networkx as nx

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

# Reset index for consistent indexing
df1 = df1.reset_index(drop=True)
df2 = df2.reset_index(drop=True)

# Add an 'available' column to df2 to track used rows
df2['available'] = True

# Function to perform matching per SSN_N
def match_ssn(ssn, df1_ssn, df2_ssn):
    # Reset index to get sequential indices
    df1_ssn = df1_ssn.reset_index(drop=True)
    df2_ssn = df2_ssn.reset_index(drop=True)
    df1_ssn['df1_idx'] = df1_ssn.index
    df2_ssn['df2_idx'] = df2_ssn.index

    # First, find exact matches
    exact_matches = df1_ssn.merge(
        df2_ssn,
        left_on=['Payment Date'],
        right_on=['TXN_TRD_D'],
        suffixes=('_df1', '_df2')
    )
    # Mark matched df2 rows
    matched_df2_indices = exact_matches['df2_idx']
    df2_ssn_matched = df2_ssn.loc[matched_df2_indices]
    # Collect exact matches
    exact_results = exact_matches[['SSN_N_df1', 'Payment Date', 'SUM(TXN_CASH_A)']]
    exact_results = exact_results.rename(columns={'SSN_N_df1': 'SSN_N'})

    # Get unmatched df1 and df2
    df1_unmatched = df1_ssn[~df1_ssn['df1_idx'].isin(exact_matches['df1_idx'])]
    df2_unmatched = df2_ssn[~df2_ssn['df2_idx'].isin(exact_matches['df2_idx'])]

    if df1_unmatched.empty:
        return exact_results

    # Generate possible matches within 28 days
    date_range = pd.Timedelta(days=28)
    # Merge to get all combinations and compute date differences
    possible_matches = df1_unmatched.assign(key=1).merge(
        df2_unmatched.assign(key=1),
        on='key',
        suffixes=('_df1', '_df2')
    ).drop('key', axis=1)
    possible_matches['date_diff'] = (possible_matches['Payment Date'] - possible_matches['TXN_TRD_D']).abs()
    # Filter for date differences within 28 days
    possible_matches = possible_matches[possible_matches['date_diff'] <= date_range]

    if possible_matches.empty:
        # Return exact results and unmatched df1 with NaN
        df1_unmatched['SUM(TXN_CASH_A)'] = np.nan
        result = pd.concat(
            [exact_results, df1_unmatched[['SSN_N', 'Payment Date', 'SUM(TXN_CASH_A)']]],
            ignore_index=True
        )
        return result

    # Build bipartite graph
    G = nx.Graph()
    for idx in df1_unmatched['df1_idx']:
        G.add_node(f'df1_{idx}', bipartite=0)
    for idx in df2_unmatched['df2_idx']:
        G.add_node(f'df2_{idx}', bipartite=1)

    for _, row in possible_matches.iterrows():
        u = f'df1_{row["df1_idx"]}'
        v = f'df2_{row["df2_idx"]}'
        weight = row['date_diff'].days
        G.add_edge(u, v, weight=weight)

    # Compute minimum weight matching
    matching = nx.algorithms.matching.min_weight_matching(G, maxcardinality=True, weight='weight')

    # Collect matched pairs
    matched_pairs = []
    for u, v in matching:
        if u.startswith('df1_'):
            df1_idx = int(u[4:])
            df2_idx = int(v[4:])
        else:
            df1_idx = int(v[4:])
            df2_idx = int(u[4:])
        matched_pairs.append((df1_idx, df2_idx))

    # Build DataFrame of matched pairs
    matched_df = pd.DataFrame(matched_pairs, columns=['df1_idx', 'df2_idx'])
    # Merge with df1_unmatched and df2_unmatched
    matched_df = matched_df.merge(df1_unmatched, on='df1_idx')
    matched_df = matched_df.merge(df2_unmatched, on='df2_idx', suffixes=('_df1', '_df2'))
    # Collect the results
    matched_results = matched_df[['SSN_N', 'Payment Date', 'SUM(TXN_CASH_A)']]

    # For unmatched df1 rows, append NaN
    matched_df1_indices = matched_df['df1_idx']
    df1_unmatched_unmatched = df1_unmatched[~df1_unmatched['df1_idx'].isin(matched_df1_indices)]
    df1_unmatched_unmatched['SUM(TXN_CASH_A)'] = np.nan

    # Combine all results
    result = pd.concat(
        [exact_results, matched_results, df1_unmatched_unmatched[['SSN_N', 'Payment Date', 'SUM(TXN_CASH_A)']]],
        ignore_index=True
    )

    return result

# Apply the matching function per SSN_N
result_list = []
for ssn in df1['SSN_N'].unique():
    df1_ssn = df1[df1['SSN_N'] == ssn]
    df2_ssn = df2[(df2['SSN_N'] == ssn) & (df2['available'])]
    result_ssn = match_ssn(ssn, df1_ssn, df2_ssn)
    result_list.append(result_ssn)

# Combine all results
result_df = pd.concat(result_list, ignore_index=True)

# Display the result
print(result_df)
