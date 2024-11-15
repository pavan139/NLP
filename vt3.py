import pandas as pd
import numpy as np
import time

# Assume df1 and df2 are your large dataframes

# Convert 'date' columns to datetime
df1['Payment Date'] = pd.to_datetime(df1['Payment Date'])
df2['TXN_TRD_D'] = pd.to_datetime(df2['TXN_TRD_D'])

# Separate rows where amount != 0 and amount == 0
df1_nonzero = df1[df1['amount'] != 0].copy()
df1_zero = df1[df1['amount'] == 0].copy()

# Initialize variables for chunk processing
chunk_size = 100000  # Adjust based on your system's memory capacity
total_rows = len(df1_nonzero)
total_chunks = (total_rows // chunk_size) + 1

# Initialize a list to store results
results = []

# Start processing time
start_time = time.time()

for i, start_row in enumerate(range(0, total_rows, chunk_size)):
    end_row = min(start_row + chunk_size, total_rows)
    df1_chunk = df1_nonzero.iloc[start_row:end_row].copy()

    # Begin processing the chunk
    chunk_start_time = time.time()

    # **Exact Matches**

    # Merge on SSN_N and Payment Date
    exact_matches = pd.merge(
        df1_chunk,
        df2,
        left_on=['SSN_N', 'Payment Date'],
        right_on=['SSN_N', 'TXN_TRD_D'],
        how='left',
        suffixes=('_df1', '_df2'),
        indicator=True
    )

    # Separate matched and unmatched rows
    matched = exact_matches[exact_matches['_merge'] == 'both'].copy()
    unmatched = exact_matches[exact_matches['_merge'] == 'left_only'].copy()

    # Drop the merge indicator
    matched.drop(columns=['_merge'], inplace=True)
    unmatched.drop(columns=['_merge', 'SUM(TXN_CASH_A)', 'Other_df2_Column', 'TXN_TRD_D'], errors='ignore', inplace=True)

    # **Closest Date Matches Within 28 Days**

    if not unmatched.empty:
        # Sort df2 for merge_asof
        df2_sorted = df2.sort_values('TXN_TRD_D')

        # Sort unmatched_chunk
        unmatched_sorted = unmatched.sort_values('Payment Date')

        # Perform merge_asof to find closest dates
        closest_matches = pd.merge_asof(
            unmatched_sorted,
            df2_sorted,
            left_on='Payment Date',
            right_on='TXN_TRD_D',
            by='SSN_N',
            tolerance=pd.Timedelta(days=28),
            direction='nearest',
            suffixes=('_df1', '_df2')
        )

        # Separate rows where a match was found
        matched_closest = closest_matches[closest_matches['TXN_TRD_D'].notna()]
        unmatched_final = closest_matches[closest_matches['TXN_TRD_D'].isna()]

        # Combine exact and closest matches
        matched = pd.concat([matched, matched_closest], ignore_index=True)
    else:
        unmatched_final = unmatched.copy()

    # Append NaNs for df2 columns in unmatched_final
    unmatched_final[df2.columns.difference(unmatched_final.columns)] = np.nan

    # Combine matched and unmatched_final
    chunk_result = pd.concat([matched, unmatched_final], ignore_index=True)

    # Append chunk_result to results
    results.append(chunk_result)

    # Progress update
    elapsed_time = time.time() - start_time
    progress = (end_row) / total_rows * 100
    estimated_total_time = elapsed_time / (end_row) * total_rows
    remaining_time = estimated_total_time - elapsed_time
    print(f"Processed chunk {i + 1}/{total_chunks} ({progress:.1f}%). Estimated time remaining: {remaining_time:.1f} seconds.")

# After processing all chunks, concatenate results
final_result = pd.concat(results + [df1_zero], ignore_index=True)

# Optional: Drop helper columns if any
final_result.drop(columns=['TXN_TRD_D'], errors='ignore', inplace=True)

# Display or save the result
print("\nProcessing complete.")
print(final_result)
