import pandas as pd
import logging

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_columns(df, required_columns):
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing columns: {missing_columns}")
        raise ValueError(f"Missing columns: {missing_columns}")

def create_wide_format(df, grant_instances):
    """
    Transforms detailed grant data into a wide format dataframe where each row represents one client with all their grants.
    """
    try:
        validate_columns(df, ['CLIENT_ID', 'FIRST_NM', 'LAST_NM', 'EMAIL_AD_X', 'ACCESS_CODE', 'BIRTH_D', 'GRANT_D', 'FMV_PRC_AT_GRANT_A', 'CLIENT_GRANT_ID', 'GRANT_SEQ_N'])
        logging.info("All required columns are present.")
    except ValueError as e:
        logging.error(f"Data validation error: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error
    
    base_columns = ['FirstName', 'LastName', 'Email', 'ExternalDataReference', 'ACCESS_CODE', 'DOB']
    grant_columns = [f'{field}_{i}' for i in range(1, grant_instances + 1) for field in ['Grant', 'yr_Grant', 'cb_Grant', 'ID']]
    columns = base_columns + grant_columns
    
    data_list = []
    for client_id, group in df.groupby('CLIENT_ID'):
        client_info = {
            'FirstName': group['FIRST_NM'].iloc[0],
            'LastName': group['LAST_NM'].iloc[0],
            'Email': group['EMAIL_AD_X'].iloc[0],
            'ExternalDataReference': client_id,
            'ACCESS_CODE': ', '.join(group['ACCESS_CODE'].unique()),
            'DOB': group['BIRTH_D'].iloc[0]
        }
        for i in range(1, grant_instances + 1):
            grant_info = group[group['GRANT_SEQ_N'] == i]
            if not grant_info.empty:
                client_info.update({
                    f'Grant_{i}': grant_info['FMV_PRC_AT_GRANT_A'].sum(),
                    f'yr_Grant_{i}': grant_info['GRANT_D'].max(),
                    f'cb_Grant_{i}': '',  # Placeholder
                    f'ID_{i}': grant_info['CLIENT_GRANT_ID'].sum()
                })
            else:
                logging.debug(f"No grant data for client {client_id} for grant sequence {i}.")
        
        data_list.append(client_info)
        logging.info(f"Processed data for client ID {client_id}.")
    
    return pd.DataFrame(data_list, columns=columns)

# Example use case
data = {
    'CLIENT_ID': ['001', '001', '002', '002', '003', '003'],
    'FIRST_NM': ['John', 'John', 'Jane', 'Jane', 'Alice', 'Alice'],
    'LAST_NM': ['Doe', 'Doe', 'Smith', 'Smith', 'Johnson', 'Johnson'],
    'EMAIL_AD_X': ['john.doe@example.com', 'john.doe@example.com', 'jane.smith@example.com', 'jane.smith@example.com', 'alice.johnson@example.com', 'alice.johnson@example.com'],
    'ACCESS_CODE': ['AC101', 'AC102', 'AC201', 'AC202', 'AC301', 'AC302'],
    'BIRTH_D': ['1990-01-01', '1990-01-01', '1985-05-15', '1985-05-15', '1979-07-23', '1979-07-23'],
    'GRANT_D': ['2022-01-01', '2022-06-01', '2022-03-01', '2022-09-01', '2022-02-01', '2022-08-01'],
    'FMV_PRC_AT_GRANT_A': [1000, 1500, 1200, 1300, 1100, 1600],
    'CLIENT_GRANT_ID': [101, 102, 201, 202, 301, 302],
    'GRANT_SEQ_N': [1, 2, 1, 2, 1, 2]
}
df = pd.DataFrame(data)
GRANT_INSTANCES = df['GRANT_SEQ_N'].max()

try:
    output_df = create_wide_format(df, GRANT_INSTANCES)
    logging.info("Successfully created wide format DataFrame.")
    output_df
except Exception as e:
    logging.error(f"Failed to create wide format DataFrame: {str(e)}")

