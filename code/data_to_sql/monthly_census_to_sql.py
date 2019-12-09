#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, create_sql_dates
from file_paths import database_path, processed_data, update_logs_folder


def monthly_census_to_sql(update=True):
    """
    Creates or updates a monthly census as of the first table in the database
    This table is an aggregate table used in 100 member month queries.

    If updating - finds the most recent first of the month value from today
        and checks to see if there is a value for the census as of that date
        If none exists it is created and appended
    
    Args:
        update(bool): is the database being updated or not

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    conn = sqlite3.connect(database_path)
    primary_key = ["month"]

    if update is True:
        today = pd.to_datetime("today")
        add_leading_0 = lambda x: "0" if len(str(x)) == 1 else ""
        last_first_of_month = (
            f"{today.year}-{add_leading_0(today.month)}{today.month}-01"
        )
        c = conn.cursor()

        month_exists = c.execute(
            "SELECT * FROM monthly_census WHERE month = ?", [last_first_of_month]
        ).fetchone()

        if month_exists is None:

            enrollment = pd.read_csv(f"{processed_data}\\enrollment_for_census.csv")
            enrollment = create_sql_dates(enrollment)

            most_recent_month = pd.read_sql(
                "SELECT * FROM monthly_census ORDER BY month DESC LIMIT 1", conn
            )
            disenrollments_mask = (
                enrollment["disenrollment_date"] >= most_recent_month["month"][0]
            ) & (enrollment["disenrollment_date"] < last_first_of_month)

            enrollments_mask = enrollment["enrollment_date"] == last_first_of_month

            center_census = {}
            for center in enrollment.center.unique():
                center_census[center.lower()] = {}

            for center in enrollment.center.unique():
                center_mask = enrollment["center"] == center

                center_census[center.lower()][last_first_of_month] = most_recent_month[
                    center.lower()
                ][0] + (
                    enrollment[enrollments_mask & center_mask].shape[0]
                    - enrollment[disenrollments_mask & center_mask].shape[0]
                )

            monthly_census = pd.DataFrame.from_dict(center_census)
            monthly_census["total"] = monthly_census.sum(axis=1)
            monthly_census.reset_index(inplace=True)
            monthly_census.rename(columns={"index": "month"}, inplace=True)
            monthly_census = create_sql_dates(monthly_census, ["month"])
            monthly_census.to_sql(
                "monthly_census", conn, if_exists="append", index=False
            )

            print("monthly_census updated...")

    else:

        enrollment = pd.read_sql(
            "SELECT * FROM enrollment JOIN centers on enrollment.member_id=centers.member_id",
            conn,
        )
        enrollment["enrollment_date"] = pd.to_datetime(enrollment["enrollment_date"])
        enrollment["disenrollment_date"] = pd.to_datetime(
            enrollment["disenrollment_date"]
        )

        center_census = {}
        for center in enrollment.center.unique():
            center_census[center.lower()] = {}

        for month_start in pd.date_range(
            enrollment.enrollment_date.min(), pd.to_datetime("today"), freq="MS"
        ):
            enrollment_mask = enrollment["enrollment_date"] <= month_start
            disenrollment_mask = (enrollment["disenrollment_date"] > month_start) | (
                enrollment["disenrollment_date"].isnull()
            )
            for center in enrollment.center.unique():
                center_mask = enrollment["center"] == center
                center_census[center.lower()][month_start] = enrollment[
                    center_mask & enrollment_mask & disenrollment_mask
                ].shape[0]

        monthly_census = pd.DataFrame.from_dict(center_census)
        monthly_census["total"] = monthly_census.sum(axis=1)
        monthly_census.reset_index(inplace=True)
        monthly_census.rename(columns={"index": "month"}, inplace=True)
        monthly_census = create_sql_dates(monthly_census, ["month"])

        create_table(monthly_census, "monthly_census", conn, primary_key)

        print("monthly_census created...")

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\monthly_census_{str(pd.to_datetime('today').date())}.txt",
        "a",
    ).close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    monthly_census_to_sql(**vars(arguments))
