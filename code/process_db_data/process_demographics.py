#!/usr/bin/env python3

import pandas as pd
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import raw_data, processed_data


def process_demographics():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Unknown race is replaced with Hispanic or Latino
        when that is the corresponding value in the ethnicity
        column
    Gender column is changed from Female/Male to 1/0
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    demographics = pd.read_csv(f"{raw_data}\\demographics.csv")

    # fill race with Hispanic/Latino from ethnicity if unknown
    latino_mask = (demographics["ethnicity"] == "Hispanic or Latino") & (
        demographics["race"] == "Unknown"
    )

    demographics.at[latino_mask, "race"] = "Hispanic or Latino"

    # Shorten Other Race
    demographics["race"].replace({"Other Race": "Other"}, inplace=True)

    # drop ethnicity col
    demographics.drop("ethnicity", axis=1, inplace=True)

    # code Female/Male as 1/0
    demographics["gender"] = demographics["gender"].str.strip()
    demographics["gender"].replace({"Female": 1, "Male": 0}, inplace=True)

    # create datetime col of date of birth column
    demographics["dob"] = pd.to_datetime(demographics["dob"])

    # clean column names
    demographics.columns = clean_table_columns(demographics.columns)

    # insure no duplicate column names
    assert len(set(demographics.columns)) == len(demographics.columns)

    demographics = demographics[demographics.member_id != 1003]
    demographics.to_csv(f"{processed_data}\\demographics.csv", index=False)

    return demographics


if __name__ == "__main__":

    process_demographics()
