#!/usr/bin/env python3

import numpy as np
import pandas as pd
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import raw_data, processed_data


def process_enrollment():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Name column is split into last and first name columns
    Medicare and Medicaid numbers are replaced with 1s
        empty IDs are replaced with 0
    Disenrolled reason is split into type and reason and cleaned
    Training member is dropped

    Ppt data (first, last) are merged with team data and saved apart from
    enrollment file

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data files
    """
    enrollment = pd.read_csv(
        f"{raw_data}\\enrollment.csv",
        parse_dates=["EnrollmentDate", "DisenrollmentDate"],
    )
    cols_to_drop = ["ParticipantName", "Gender", "SSN", "DeathDate"]

    # create first and last name cols
    enrollment[["last", "first"]] = enrollment["ParticipantName"].str.split(
        ",", expand=True
    )

    # drop information that is in other tables/cols
    # or not needed (SSN)

    enrollment.drop(cols_to_drop, axis=1, inplace=True)
    enrollment.columns = clean_table_columns(enrollment.columns)

    enrollment.rename(columns={"other": "disenroll_reason"}, inplace=True)

    # code medicare/medicaid as 1 for has 0 for does not
    enrollment["medicare"] = np.where(enrollment["medicare"].notnull(), 1, 0)
    enrollment["medicaid"] = np.where(enrollment["medicaid"].notnull(), 1, 0)

    # disenroll_reasons begins with the type (voluntary/non)
    # Split that info out in to a new column

    enrollment["disenroll_type"] = (
        enrollment["disenroll_reason"]
        .astype(str)
        .str.split(" ", expand=True)[0]
        .replace("", "")
    )

    enrollment["disenroll_reason"] = enrollment["disenroll_reason"].apply(
        lambda x: " ".join(str(x).split(" ")[1:])
    )

    # dissatisfied with is implied in all of these reasons
    enrollment["disenroll_reason"] = (
        enrollment["disenroll_reason"].astype(str).str.replace("Dissatisfied with ", "")
    )

    enrollment["disenroll_type"] = enrollment["disenroll_type"].astype(str).str.title()
    enrollment["disenroll_reason"] = (
        enrollment["disenroll_reason"].astype(str).str.title()
    )

    assert len(set(enrollment.columns)) == len(enrollment.columns)
    enrollment = enrollment[enrollment.member_id != 1003]

    ppts = enrollment[["member_id", "last", "first"]].copy()

    centers = enrollment[
        ["member_id", "center", "enrollment_date", "disenrollment_date"]
    ].copy()
    centers.rename(
        columns={"enrollment_date": "start_date", "disenrollment_date": "end_date"},
        inplace=True,
    )
    try:
        transfers = pd.read_csv(
            f"{raw_data}\\transfers.csv", parse_dates=["TransferDate"]
        )
        transfers.columns = clean_table_columns(transfers.columns)
        transfers.rename(columns={"transfer_date": "start_date"}, inplace=True)

        transfers["end_date"] = np.nan

        transfers["old_center"] = transfers["comment"].str.split().str[4]
        transfers.drop(
            ["text_box5", "pariticipant_name", "comment"], axis=1, inplace=True
        )

        centers = centers.append(transfers, sort=True)
    except ValueError:
        centers["old_center"] = None

    centers = centers[["member_id", "center", "start_date", "end_date", "old_center"]]

    ppts.drop_duplicates(subset=["member_id"], inplace=True)
    enrollment.to_csv(f"{processed_data}\\enrollment_for_census.csv", index=False)
    enrollment.drop(["last", "first"], axis=1, inplace=True)

    centers.to_csv(f"{processed_data}\\centers.csv", index=False)
    enrollment.to_csv(f"{processed_data}\\enrollment.csv", index=False)

    ppts.to_csv(f"{processed_data}\\ppts.csv", index=False)

    return enrollment


if __name__ == "__main__":
    process_enrollment()
