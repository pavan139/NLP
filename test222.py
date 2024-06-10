        for col in full_df.columns:
            if '_valid' in col:
                full_df.drop(col, axis=1, inplace=True)

        essential_columns = ["SSN", "HIRE_DT", "REHIRE_DT"]
        columns = {col: self.get_report_column(col) for col in essential_columns}
        for col, name in columns.items():
            if name is None:
                logging.error(f"Essential column {col} name could not be retrieved. Exiting.")
                return None
