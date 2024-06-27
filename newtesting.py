import pandas as pd
import logging
import os
from argparse import ArgumentParser

# Constants
GRANT_INSTANCES = 50

def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file,
        filemode='w'
    )
    return logging.getLogger(__name__)

def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument("-src", dest="SrcFile", required=True, help="Full path to the input CSV file.")
    parser.add_argument("-tgt", dest="TgtDir", required=True, help="Target directory path to place the output file.")
    parser.add_argument("-sl", dest="SessionLog", required=True, help="Session Log directory for project.")
    return parser.parse_args()

def validate_columns(df, required_columns):
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing columns: {missing_columns}")
        raise ValueError(f"Missing columns: {missing_columns}")

def create_wide_format(df, grant_instances):
    """
    Transforms detailed grant data into a wide format dataframe where each row represents one client with all their grants.
    """
    required_columns = ['CLIENT_ID', 'CLIENT_EMP_ID', 'FIRST_NM', 'LAST_NM', 'EMAIL_AD_X', 'ACCESS_CODE', 'CLIENT_EMP_ID', 'BIRTH_D', 'PARTICIPANT_NM',
                        'CERT_I', 'ACP_DCLN_C', 'GRANT_D', 'QTY_OUTSTANDING_N', 'FMV_PRC_AT_GRANT_A', 'CLIENT_GRANT_ID', 'GRANT_TRACKING_SEQ_N']
    
    try:
        validate_columns(df, required_columns)
        logging.info("All required columns are present.")
    except ValueError as e:
        logging.error(f"Data validation error: {str(e)}")
        return pd.DataFrame()

    base_columns = ['FirstName', 'LastName', 'Email', 'ExternalDataReference', 'ACCESS_CODE', 'DOB', 'Display_Name']
    grant_columns = [f'{field}_{i}' for i in range(1, grant_instances + 1) for field in ['Grant', 'yr_Grant', 'cb_Grant', 'ID']]
    columns = base_columns + grant_columns

    data_list = []
    for client_emp_id, group in df.groupby('CLIENT_EMP_ID'):
        logging.info(f"Processing {client_emp_id}.")
        client_info = {
            'FirstName': group['FIRST_NM'].iloc[0],
            'LastName': group['LAST_NM'].iloc[0],
            'Email': group['EMAIL_AD_X'].iloc[0],
            'ExternalDataReference': client_emp_id,
            'ACCESS_CODE': group['ACCESS_CODE'].iloc[0],
            'DOB': group['BIRTH_D'].iloc[0],
            'Display_Name': group['PARTICIPANT_NM'].iloc[0],
            'CERT_I': group['CERT_I'].iloc[0],
            'ACP_DCLN_C_ALL': "N" if not group['ACP_DCLN_C'].eq("A").all() else "Y"
        }

        for i in range(1, grant_instances + 1):
            grant_info = group[group['GRANT_TRACKING_SEQ_N'] == i]
            if not grant_info.empty:
                client_info.update({
                    f'Grant_{i}': grant_info['QTY_OUTSTANDING_N'].iloc[0],
                    f'yr_Grant_{i}': grant_info['GRANT_D'].iloc[0],
                    f'cb_Grant_{i}': grant_info['FMV_PRC_AT_GRANT_A'].iloc[0],
                    f'ID_{i}': grant_info['CLIENT_GRANT_ID'].iloc[0]
                })
            else:
                client_info.update({f'Grant_{i}': '', f'yr_Grant_{i}': '', f'cb_Grant_{i}': '', f'ID_{i}': ''})
                logging.debug(f"No grant data for client {client_emp_id} for grant sequence {i}.")

        client_info.update({
            'Eligible': 'Y',
            'Price': 'PLACEHOLDER_PARAM',
            'Shares_avail': group['QTY_OUTSTANDING_N'].sum(),
            'Date-Time': '',
            'Date-Time_Q': '',
            'DateTaken': '',
            'Ext_val': '',
            'Finished': '',
            'Inactive_close': '',
            'Requested': '',
            'TimeTaken': ''
        })

        data_list.append(client_info)
        logging.info(f"Processed data for client ID {client_emp_id}.")

    return pd.DataFrame(data_list, columns=columns)

def main():
    args = parse_arguments()
    
    # Setup logging
    log_file = os.path.join(args.SessionLog, f"{os.path.basename(__file__).split('.')[0]}.log")
    logger = setup_logging(log_file)
    logger.info("Process Start")
    logger.info(f"Target Directory: {args.TgtDir}")

    try:
        # For testing, you can uncomment these lines and comment out the args usage
        # args.SrcFile = r"Z:\CLIENT\UKG\Data Consulting\Project 1 - Tender Offer Site & Grant Load Automation\Development\Python\Formating POC\ukg_qualtrics_script\egt_r803_elig_available_lots.csv"
        # args.TgtDir = r"Z:\CLIENT\UKG\Data Consulting\Project 1 - Tender Offer Site & Grant Load Automation\Development\Python\Formating POC\ukg_qualtrics_script"
        
        df = pd.read_csv(args.SrcFile)
        logger.info(f"Successfully read input file: {args.SrcFile}")

        output_df = create_wide_format(df, GRANT_INSTANCES)
        if not output_df.empty:
            output_file = os.path.join(args.TgtDir, "UKG_qualtrics_testQA2.csv")
            output_df.to_csv(output_file, index=False)
            logger.info(f"Successfully created wide format DataFrame and saved to {output_file}.")
        else:
            logger.error("Failed to create wide format DataFrame.")
    except Exception as e:
        logger.error(f"Failed to create wide format DataFrame: {str(e)}", exc_info=True)
    
    logger.info("Process End")

if __name__ == "__main__":
    main()
