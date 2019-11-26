#!/usr/bin/env python3

import pandas as pd
from process_db_data.process_incidents import process_incidents
from file_paths import raw_data


def process_burns():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    NA rows are dropped
    Yes/No Columns are changed to 1/0
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    burns = pd.read_csv(f"{raw_data}\\burns.csv")
    cols_to_drop = [
        "First Name",
        "Last Name",
        "Submitted By",
        "Center",
        "Control Number",
    ]
    return process_incidents(burns, cols_to_drop, "burns", break_location=False)


if __name__ == "__main__":
    process_burns()
