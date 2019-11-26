#!/usr/bin/env python3

import pandas as pd
from process_db_data.process_incidents import process_incidents
from file_paths import raw_data


def process_infections():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Dose status is made binary
        Administered: 1, Not Administered: 0
        Contra is added to column as 99
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    infections = pd.read_csv(f"{raw_data}\\infections.csv")
    cols_to_drop = ["First Name", "Last Name", "Submitted By", "Center"]
    return process_incidents(
        infections, cols_to_drop, "infections", break_location=False
    )


if __name__ == "__main__":
    process_infections()
