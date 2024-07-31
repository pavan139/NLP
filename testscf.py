import pandas as pd
import json

# Sample JSON configuration mimicking the structure described
config = {
    "63": {
        "PLAN_N": 5,
        "BATCH_GROUP_ID": 4,
        "FILLER_1": 3,
        "SSN_N": 11,
        "RESERVED_1": 1,
        "RESERVED_2": 5,
        "REC_I": 2,
        "SVC_UNIT_AMT": 4,
        "HOURS_EFF_D": 8,
        "REP_IND": 1,
        "FILLER_2": 1,
        "RESERVED_3": 24,
        "FILLER_3": 11
    }
}

# Define the class with necessary methods
class RecordFormatter:
    def __init__(self, dataframe, config):
        self.dataframe = dataframe
        self.config = config["63"]  # Load configuration for record type "63"

    def get_formatted_records(self):
        """Process the DataFrame and return formatted records."""
        try:
            return self.dataframe.apply(self._create_record, axis=1).tolist()
        except Exception as e:
            print(f"An error occurred during record formatting: {e}")
            return []

    def _create_record(self, row):
        """Create a formatted record string from a DataFrame row."""
        record = []
        for field, length in self.config.items():
            value = str(row.get(field, ''))
            if field == "SVC_UNIT_AMT":
                value = self._format_signed_service_unit(int(value), length)
            elif field == "SSN_N":
                value = self._format_ssn(value).ljust(length)
            else:
                value = value.ljust(length)
            record.append(value[:length])
        return ''.join(record)

    def _format_signed_service_unit(self, amount, length):
        positive_map = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 0: '0'}
        negative_map = {1: 'J', 2: 'K', 3: 'L', 4: 'M', 5: 'N', 6: 'O', 7: 'P', 8: 'Q', 9: 'R', 0: '0'}
        abs_amount = abs(amount)
        sign_map = positive_map if amount >= 0 else negative_map
        sign_digit = sign_map[abs_amount % 10]
        number_without_last_digit = abs_amount // 10
        formatted_number = f"{number_without_last_digit}{sign_digit}".zfill(length)
        return formatted_number

    def _format_ssn(self, value):
        ssn = ''.join(filter(str.isdigit, str(value)))
        return f"{ssn[:3]}-{ssn[3:5]}-{ssn[5:9]}"

# Sample DataFrame as per the configuration
df = pd.DataFrame({
    'PLAN_N': [99871]*8,
    'BATCH_GROUP_ID': ['0196', '1165', '2245', '2855', '2919', '4033', '4072', '0196'],
    'SSN_N': ['116527782', '285528180', '224555813', '291908389', '403312200', '407211747', '19608951', '116527782'],
    'SVC_UNIT_AMT': [1261, 800, 640, -419, 668, -800, 800, 1261],
    'HOURS_EFF_D': ['20240724', '20240721', '20240721', '20240721', '20240721', '20240721', '20240721', '20240724'],
    'REP_IND': ['R']*8
})

# Instantiate the formatter and generate records
formatter = RecordFormatter(df, config)
records = formatter.get_formatted_records()
records
