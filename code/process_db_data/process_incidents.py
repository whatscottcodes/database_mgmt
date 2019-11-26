#!/usr/bin/env python3

import pandas as pd
from process_db_data.data_cleaning_utils import (
    clean_table_columns,
    code_y_n,
    create_id_col,
)
from file_paths import processed_data


def process_incidents(df, cols_to_drop, incident_name, break_location=False):

    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    NA rows are dropped
    Yes/No Columns are changed to 1/0

    If there is a location column:
        Location column is broken up into a location
        and a location details column. Replacements are made
        to standardize data.
    Training member is dropped

    Args:
        df(DataFrame): pandas dataframe to clean
        cols_to_drop(list): list of columns to drop
        incident_name(str): name of incident to save cleaned file as
        break_location(bool): indicates if the dataframe has a location column
            that can be broken up in the format "location-location detail".

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """

    df.drop(cols_to_drop, axis=1, inplace=True)

    df.columns = clean_table_columns(df.columns)

    df = code_y_n(df)

    df.dropna(axis=1, how="all", inplace=True)

    df["date_time_occurred"] = pd.to_datetime(df["date_time_occurred"])

    try:
        df["date_discovered"] = pd.to_datetime(df["date_discovered"])
    except KeyError:
        pass

    if break_location:
        df["location_details"] = df["location"].str.split(" - ", expand=True)[1]
        df["location"] = df["location"].str.split(" - ", expand=True)[0]

        location_replacements = {
            "Participant": "",
            "PACE Center": "PACE",
            "Nursing Facility": "NF",
            "Assisted Living Facility": "ALF",
        }
        df["location"].replace(location_replacements, inplace=True)

    df.reset_index(inplace=True, drop=True)

    df["incident_id"] = create_id_col(
        df, ["member_id", "date_time_occurred"], "incident_id"
    )

    assert len(set(df.columns)) == len(df.columns)
    df = df[df.member_id != 1003]
    df.to_csv(f"{processed_data}\\{incident_name}.csv", index=False)

    return df
