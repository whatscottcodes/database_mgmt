#!/usr/bin/env python3

import numpy as np
import pandas as pd
import sqlite3
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import raw_data, processed_data, database_path


def process_enrollment(update=True):
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

    if update:
        from paceutils import Helpers

        centers = enrollment[["member_id", "center"]].copy()

        helpers = Helpers(database_path)
        conn = sqlite3.connect(database_path)

        centers.to_sql("centers_temp", conn, index=False, if_exists="replace")

        keep_new_only_q = """
            SELECT * from centers_temp
            WHERE member_id NOT IN (
            SELECT centers_temp.member_id
            FROM centers_temp
            JOIN centers
            ON centers_temp.member_id=centers.member_id
            AND centers_temp.center=centers.center
            )
            """

        centers = helpers.dataframe_query(keep_new_only_q)

        centers.to_sql("centers_temp", conn, index=False, if_exists="replace")

        as_of_date = str(pd.to_datetime("today").date())

        update_q = """
            UPDATE centers
            SET end_date = ?
            WHERE member_id IN (
            SELECT member_id
            FROM centers_temp)
            """
        conn.execute(update_q, (as_of_date,))
        conn.commit()

        conn.execute(f"DROP TABLE IF EXISTS centers_temp")

        conn.commit()
        conn.close()

        centers["start_date"] = as_of_date
        centers["end_date"] = pd.np.nan
    else:
        centers = enrollment[["member_id", "center", "enrollment_date"]].copy()
        centers.rename(columns={"enrollment_date", "start_date"}, inplace=True)
        centers["end_date"] = pd.np.nan

    ppts.drop_duplicates(subset=["member_id"], inplace=True)
    enrollment.to_csv(f"{processed_data}\\enrollment_for_census.csv", index=False)
    enrollment.drop(["last", "first", "center"], axis=1, inplace=True)

    centers.to_csv(f"{processed_data}\\centers.csv", index=False)
    enrollment.to_csv(f"{processed_data}\\enrollment.csv", index=False)

    ppts.to_csv(f"{processed_data}\\ppts.csv", index=False)

    return enrollment


if __name__ == "__main__":
    process_enrollment()
