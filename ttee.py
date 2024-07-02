import pandas as pd
import logging
import os
import json
from argparse import ArgumentParser

# ... (previous code remains the same)

def df_to_json(df):
    """
    Converts DataFrame to JSON format as per the specified requirements.
    """
    result = {'elements': []}
    for _, row in df.iterrows():
        element = {
            'contactId': f"CID_{row['ExternalDataReference']}",
            'firstName': row['FirstName'],
            'lastName': row['LastName'],
            'email': row['Email'],
            'phone': None,
            'extRef': row['ExternalDataReference'],
            'language': None,
            'unsubscribed': False,
            'embeddedData': {}
        }
        
        for i in range(1, GRANT_INSTANCES + 1):
            grant = row.get(f'Grant_{i}')
            yr_grant = row.get(f'yr_Grant_{i}')
            cb_grant = row.get(f'cb_Grant_{i}')
            id_grant = row.get(f'ID_{i}')
            
            if any(v is not None and v != '' for v in [grant, yr_grant, cb_grant, id_grant]):
                element['embeddedData'].update({
                    f'{i}-ID': id_grant if id_grant else None,
                    f'{i}-cb_Grant': cb_grant if cb_grant else None,
                    f'{i}-yr_Grant': yr_grant if yr_grant else None,
                    f'{i}Grant': grant if grant else None
                })
        
        result['elements'].append(element)
    
    return result

def main():
    global LOGGER
    args = parse_arguments()
    
    # Setup logging
    log_file = os.path.join(args.SessionLog, f"{os.path.basename(__file__).split('.')[0]}.log")
    LOGGER = setup_logging(log_file)
    LOGGER.info("Process Start")
    LOGGER.info(f"Target Directory: {args.TgtDir}")

    try:
        df = pd.read_csv(args.SrcFile)
        LOGGER.info(f"Successfully read input file: {args.SrcFile}")

        output_df = create_wide_format(df, GRANT_INSTANCES)
        if not output_df.empty:
            # Save CSV
            csv_output_file = os.path.join(args.TgtDir, "UKG_qualtrics_testQA2.csv")
            output_df.to_csv(csv_output_file, index=False)
            LOGGER.info(f"Successfully created wide format DataFrame and saved to {csv_output_file}.")
            
            # Convert to JSON and save
            json_data = df_to_json(output_df)
            json_output_file = os.path.join(args.TgtDir, "UKG_qualtrics_testQA2.json")
            with open(json_output_file, 'w') as f:
                json.dump(json_data, f, indent=2)
            LOGGER.info(f"Successfully converted DataFrame to JSON and saved to {json_output_file}.")
        else:
            LOGGER.error("Failed to create wide format DataFrame.")
    except Exception as e:
        LOGGER.error(f"Failed to process data: {str(e)}", exc_info=True)
    
    LOGGER.info("Process End")

if __name__ == "__main__":
    main()
