#!/usr/bin/env python3

import pandas as pd
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import raw_data, processed_data


def process_center_days():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    As of column is added to indicate day change was recorded
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """

    center_days = pd.read_csv(f"{raw_data}\\center_days.csv")

    cols_to_drop = ["ParticipantName", "Center", "TimeAttending"]
    center_days.drop(cols_to_drop, axis=1, inplace=True)

    center_days.columns = clean_table_columns(center_days.columns)

    # create an as of column, so we can keep track of historic changes
    center_days["as_of"] = pd.to_datetime("today").date()

    assert len(set(center_days.columns)) == len(center_days.columns)
    center_days = center_days[center_days.member_id != 1003]
    center_days.to_csv(f"{processed_data}\\center_days.csv", index=False)

    return center_days


if __name__ == "__main__":
    process_center_days()
