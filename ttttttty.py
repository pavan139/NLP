import os
import pandas as pd

def sort_csv_files(directory):
    # Create an output folder if it doesn't exist
    output_dir = os.path.join(directory, "sorted_files")
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over all CSV files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            print(f"Processing file: {filename}")

            try:
                # Read the CSV file
                data = pd.read_csv(file_path)

                # Ensure 'Payment Date' is in datetime format
                data['Payment Date'] = pd.to_datetime(data['Payment Date'])

                # Sort by 'SSN' and 'Payment Date'
                sorted_data = data.sort_values(by=['SSN', 'Payment Date'])

                # Save the sorted DataFrame as a CSV file in the output folder
                output_file = os.path.join(output_dir, f"sorted_{filename}")
                sorted_data.to_csv(output_file, index=False)

                print(f"Sorted file saved as: {output_file}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

# Input: Path to the directory containing CSV files
directory = input("Enter the path to the directory containing the CSV files: ")
sort_csv_files(directory)