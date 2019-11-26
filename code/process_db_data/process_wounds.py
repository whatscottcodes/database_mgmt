#!/usr/bin/env python3

import pandas as pd
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import raw_data, processed_data


def process_wounds():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    NA member_id rows are dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """

    wounds = pd.read_csv(
        f"{raw_data}\\wounds.csv", parse_dates=["Date Time Occurred", "Date Healed"]
    )
    cols_to_drop = ["Participant"]
    wounds.drop(cols_to_drop, axis=1, inplace=True)
    wounds.columns = clean_table_columns(wounds.columns)
    wounds.dropna(subset=["member_id"], inplace=True)
    wounds["member_id"] = wounds["member_id"].astype(int)
    wounds.to_csv(f"{processed_data}\\wounds.csv", index=False)
    return wounds


if __name__ == "__main__":
    process_wounds()
