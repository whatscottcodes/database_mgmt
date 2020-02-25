#!/usr/bin/env python3

import pandas as pd
import numpy as np
from process_db_data.data_cleaning_utils import clean_table_columns, code_y_n
from file_paths import processed_data, raw_data, database_path
from paceutils import Helpers


def process_medications():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Meds are grouped by member and description and the minimum start date is found
        and the maximum start date
    The discontinue date is filled in with either the estimated discontinue date
        or the date of the more recent script
    The resulting dataframe has a column indicating if a med is active
        a column for the create date of the script
        one for the most recent script
        and one to indicate the discontinue date

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    meds = pd.read_csv(
        f"{raw_data}\\meds.csv",
        parse_dates=[
            "start_date",
            "discontinue_date",
            "estimated_end_date",
            "create_date",
        ],
    )

    helpers = Helpers(database_path)
    enroll = helpers.dataframe_query(
        q="SELECT member_id, disenrollment_date FROM enrollment", params=None
    )

    meds.columns = clean_table_columns(meds.columns)

    df = (
        meds.groupby(["member_id", "desc"])
        .min()
        .reset_index()[["member_id", "desc", "name", "start_date", "create_date"]]
    )

    df[["discontinue_date", "estimated_end_date", "most_recent_script"]] = (
        meds.groupby(["member_id", "desc"])
        .max()
        .reset_index()[["discontinue_date", "estimated_end_date", "start_date"]]
    )

    status = meds[
        ["member_id", "start_date", "status", "class", "discontinue_type"]
    ].copy()
    status.rename(columns={"start_date": "date"}, inplace=True)

    dff = df.merge(
        status,
        left_on=["member_id", "most_recent_script"],
        right_on=["member_id", "date"],
        how="left",
    )

    dff.drop("date", axis=1, inplace=True)

    dff.drop_duplicates(inplace=True)

    dff.reset_index(drop=True, inplace=True)

    dff = dff.merge(enroll, on="member_id")

    dff["discontinue_date"] = np.where(
        ((dff.discontinue_date < dff.most_recent_script) & (dff.status != "Active")),
        dff["estimated_end_date"],
        dff["discontinue_date"],
    )

    dff["discontinue_date"] = np.where(
        ((dff.discontinue_date < dff.most_recent_script) & (dff.status != "Active")),
        dff["most_recent_script"],
        dff["discontinue_date"],
    )

    dff["discontinue_date"] = np.where(
        ((dff.discontinue_date.notnull()) & (dff.status == "Active")),
        pd.NaT,
        dff["discontinue_date"],
    )

    dff["discontinue_date"] = np.where(
        ((dff.discontinue_date.isnull()) & (dff.status != "Active")),
        dff["most_recent_script"],
        dff["discontinue_date"],
    )

    dff["discontinue_date"] = np.where(
        ((dff.discontinue_date.isnull()) & (dff.status != "Active")),
        dff["disenrollment_date"],
        dff["discontinue_date"],
    )

    dff["discontinue_date"] = np.where(
        (
            (dff.discontinue_date.isnull())
            & (dff.status != "Active")
            & dff.start_date.isnull()
        ),
        dff["create_date"],
        dff["discontinue_date"],
    )

    dff["start_date"] = np.where(
        dff.start_date.isnull(), dff["create_date"], dff["start_date"]
    )

    dff.drop(["disenrollment_date", "create_date"], axis=1, inplace=True)

    dff = code_y_n(dff)

    df.dropna(axis=1, how="all", inplace=True)

    dff["start_date"] = pd.to_datetime(dff["start_date"])
    dff["discontinue_date"] = pd.to_datetime(dff["discontinue_date"])

    dff.reset_index(inplace=True, drop=True)

    final_df = dff[dff.member_id != 1003].copy()

    assert (
        final_df[
            (final_df["status"] == "Active") & (final_df["discontinue_date"].notnull())
        ].shape[0]
        == 0
    )
    assert (
        final_df[
            (final_df["status"] != "Active") & (final_df["discontinue_date"].isnull())
        ].shape[0]
        == 0
    )
    assert final_df[(final_df["start_date"].isnull())].shape[0] == 0
    final_df = final_df.sort_values("discontinue_date")
    final_df.drop_duplicates(subset=["member_id", "desc"], inplace=True)

    final_df.to_csv(f"{processed_data}\\meds.csv", index=False)

    return df
