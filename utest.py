def unpivot_data_with_strict_key_filter(data):
    elements = data['result']['elements']
    transformed_records = []
    for element in elements:
        base_info = {key: value for key, value in element.items() if key != 'embeddedData'}
        embedded_data = element.get('embeddedData', {})
        # Extract unique prefixes (e.g., 1, 2, 100) correctly for keys like '1-ID', '1-cb_Grant'
        grant_numbers = set()
        for key in embedded_data:
            if "ID" in key or "Grant" in key or "yr_Grant" in key or "cb_Grant" in key:
                number = ''.join([char for char in key if char.isdigit()])
                if number:  # Ensuring it's not empty
                    grant_numbers.add(number)
        
        # Reorganize grants by their number prefix using the original sequence numbers
        for num in sorted(grant_numbers, key=int):
            grant_info = {
                "contactId": base_info['contactId'],
                "firstName": base_info['firstName'],
                "lastName": base_info['lastName'],
                "email": base_info['email'],
                "phone": base_info['phone'],
                "extRef": base_info['extRef'],
                "language": base_info['language'],
                "unsubscribed": base_info['unsubscribed'],
                "ID": embedded_data.get(f"{num}-ID"),
                "cb_Grant": embedded_data.get(f"{num}-cb_Grant"),
                "yr_Grant": embedded_data.get(f"{num}-yr_Grant"),
                "Grant": embedded_data.get(f"{num}Grant"),
                "Grant_seq": int(num)  # Use the original number as the sequence
            }
            # Filter out None values and ensure the record is created if ID exists
            grant_info = {k: v for k, v in grant_info.items() if v is not None}
            if "ID" in grant_info:
                transformed_records.append(grant_info)

    return {"result": transformed_records}

# Re-run the transformation with strict key filtering on the test data
transformed_test_json_corrected = unpivot_data_with_strict_key_filter(json_test_data)
transformed_test_json_corrected

import pandas as pd

# Converting JSON data to DataFrame for saving as CSV
def json_to_csv(json_data, filename):
    # Flatten JSON to DataFrame
    df = pd.json_normalize(json_data['result'])
    # Save DataFrame to CSV
    csv_path = f"/mnt/data/{filename}.csv"
    df.to_csv(csv_path, index=False)
    return csv_path

# Convert and save the transformed JSON with original sequence numbers
csv_path_original = json_to_csv(transformed_test_json_corrected, "transformed_grants_original_sequence")

# Convert and save the transformed JSON with strict key filtering for optional fields
csv_path_corrected = json_to_csv(transformed_test_json_corrected, "transformed_grants_strict_key_filter")

csv_path_original, csv_path_corrected


def flatten_input_json_for_csv(json_data, filename):
    # List to store flattened data
    flattened_data = []
    
    # Iterate over each element in the JSON data
    elements = json_data['result']['elements']
    for element in elements:
        # Base information without embeddedData
        base_info = {key: value for key, value in element.items() if key != 'embeddedData'}
        # Extract and flatten embeddedData into the base info
        embedded_data = element.get('embeddedData', {})
        base_info.update(embedded_data)
        
        # Append the flattened data to the list
        flattened_data.append(base_info)

    # Convert flattened data to DataFrame
    df = pd.DataFrame(flattened_data)
    # Save DataFrame to CSV
    csv_path = f"/mnt/data/{filename}.csv"
    df.to_csv(csv_path, index=False)
    return csv_path

# Flatten and save the input JSON data
csv_path_input = flatten_input_json_for_csv(json_test_data, "input_json_flattened")
csv_path_input

