#!/usr/bin/env python3

import pandas as pd
from file_paths import processed_data


def process_vaccinations(df, contra, vacc_name):
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Dose status is made binary
        Administered: 1, Not Administered: 0
        Contra is added to column as 99
    Training member is dropped

    Args:
        df(DataFrame): pandas dataframe to clean
        contra(DataFrame): df with contra ppts for vaccination
        vacc_name(str): vaccination name for saving cleaned file

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    column_names = {
        "Patient: Patient ID": "member_id",
        "Immunization: Vaccine Series": "vacc_series",
        "Immunization: Date Administered": "date_administered",
        "Immunization: Dose Status": "dose_status",
    }

    df.rename(columns=column_names, inplace=True)
    contra.rename(columns=column_names, inplace=True)

    dose_status = {"Administered": 1, "Not Administered": 0}

    df["dose_status"].replace(dose_status, inplace=True)

    df.loc[df["member_id"].isin(contra["member_id"].tolist()), "dose_status"] = 99

    df["date_administered"] = pd.to_datetime(df["date_administered"]).dt.date

    df = df[df.member_id != 1003].copy()
    df = df[["member_id", "vacc_series", "date_administered", "dose_status"]].copy()

    df.drop_duplicates(inplace=True)

    assert len(set(df.columns)) == len(df.columns)

    df.to_csv(f"{processed_data}\\{vacc_name}.csv", index=False)
    return df
