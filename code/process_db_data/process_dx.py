#!/usr/bin/env python3

import pandas as pd
from file_paths import raw_data, processed_data


def process_dx():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Active Dx column is added to each dataframe
    Dataframes are merged
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    not_current = pd.read_csv(f"{raw_data}\\dx_not_current.csv")
    current = pd.read_csv(f"{raw_data}\\dx_current.csv")

    cols_to_drop = ["txtName"]

    not_current.drop(cols_to_drop, axis=1, inplace=True)
    current.drop(cols_to_drop, axis=1, inplace=True)

    dx_col_map = {
        "txtPatientID": "member_id",
        "txtProblemPmhxName": "dx_desc",
        "textBox7": "icd10",
        "txtDocumentsFor": "documents",
        "txtAddedPMhx": "date_added",
        "txtPACEHcc": "pace_hcc",
        "txtPLRapsStatus": "raps_status",
    }

    not_current.rename(columns=dx_col_map, inplace=True)
    current.rename(columns=dx_col_map, inplace=True)

    not_current["icd10"].dropna(inplace=True)
    current["icd10"].dropna(inplace=True)

    not_current["active_dx"] = 0
    current["active_dx"] = 1

    dx = current.append(not_current, sort=False)

    assert len(set(dx.columns)) == len(dx.columns)
    dx = dx[dx.member_id != 1003]
    dx.to_csv(f"{processed_data}\\dx.csv", index=False)

    return dx


if __name__ == "__main__":
    process_dx()
