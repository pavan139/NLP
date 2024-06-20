import pandas as pd
import logging

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def validate_required_columns(df, expected_columns):
    """ Check if all expected columns exist in the DataFrame. """
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        error_message = f"Missing columns: {missing_columns}"
        logging.error(error_message)
        raise ValueError(error_message)

def wide_to_long(input_dict):
    """ Transform wide format dictionary into a list of dictionaries representing long format. """
    try:
        # Convert the input dictionary to a DataFrame
        df_wide = pd.DataFrame(input_dict)
        logging.info("DataFrame created from input dictionary.")

        # Define required columns based on expected patterns
        required_columns = ['FirstName', 'LastName', 'Email', 'ExternalDataReference', 'ACCESS_CODE', 'DOB'] + \
                           [col for col in df_wide.columns if col.startswith('Grant_') or col.startswith('yr_Grant_') or col.startswith('ID_')]
        validate_required_columns(df_wide, required_columns)

        long_format_data = []
        num_grants = sum(1 for col in df_wide.columns if col.startswith('Grant_'))

        for _, row in df_wide.iterrows():
            base_info = {
                'CLIENT_ID': row['ExternalDataReference'],
                'FIRST_NM': row['FirstName'],
                'LAST_NM': row['LastName'],
                'EMAIL_AD_X': row['Email'],
                'ACCESS_CODE': row['ACCESS_CODE'],
                'BIRTH_D': row['DOB']
            }

            for i in range(1, num_grants + 1):
                grant_data = {
                    'GRANT_D': row.get(f'yr_Grant_{i}', ''),
                    'FMV_PRC_AT_GRANT_A': row.get(f'Grant_{i}', ''),
                    'CLIENT_GRANT_ID': row.get(f'ID_{i}', ''),
                    'GRANT_SEQ_N': i
                }
                full_record = {**base_info, **grant_data}
                long_format_data.append(full_record)
                logging.debug(f"Processed grant {i} for client {base_info['CLIENT_ID']}.")

        logging.info("Successfully transformed wide format to long format.")
        return long_format_data
    except Exception as e:
        logging.error(f"An error occurred during transformation: {str(e)}")
        return []

# Example usage
if __name__ == "__main__":
    input_dict = {
        'ExternalDataReference': ['001', '002', '003'],
        'FirstName': ['John', 'Jane', 'Alice'],
        'LastName': ['Doe', 'Smith', 'Johnson'],
        'Email': ['john.doe@example.com', 'jane.smith@example.com', 'alice.johnson@example.com'],
        'ACCESS_CODE': ['AC101, AC102', 'AC201, AC202', 'AC301, AC302'],
        'DOB': ['1990-01-01', '1985-05-15', '1979-07-23'],
        'Grant_1': [1000, 1200, 1100],
        'yr_Grant_1': ['2022-01-01', '2022-03-01', '2022-02-01'],
        'ID_1': [101, 201, 301],
        'Grant_2': [1500, 1300, 1600],
        'yr_Grant_2': ['2022-06-01', '2022-09-01', '2022-08-01'],
        'ID_2': [102, 202, 302]
    }
    result = wide_to_long(input_dict)
    for record in result:
        print(record)
