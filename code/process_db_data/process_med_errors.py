#!/usr/bin/env python3

import pandas as pd
from process_db_data.process_incidents import process_incidents
from file_paths import raw_data


def process_med_errors():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    NA rows are dropped
    Yes/No Columns are changed to 1/0
    Location column is broken up into a location
        and a location details column
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    med_errors = pd.read_csv(f"{raw_data}\\med_errors.csv")
    cols_to_drop = [
        "First Name",
        "Last Name",
        "Submitted By",
        "Center",
        "Control Number",
    ]
    return process_incidents(
        med_errors, cols_to_drop, "med_errors", break_location=False
    )


if __name__ == "__main__":
    process_med_errors()
