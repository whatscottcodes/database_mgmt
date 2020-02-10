#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, update_sql_table, create_sql_dates
from file_paths import database_path, processed_data, update_logs_folder


def referrals_to_sql(update=True):
    """
    Reads a cleaned dataset
    Parse the dates to match SQL format of YYYY-MM-DD
    Creates or Updates the database table using the
    indicated primary keys

    Args:
        update(bool): indicates if database table is being updated or not

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    referrals = pd.read_csv(f"{processed_data}\\referrals.csv")
    referrals = create_sql_dates(
        referrals,
        [
            "intake_visit",
            "first_visit_day",
            "enrollment_signed",
            "enrollment_effective",
        ],
    )

    primary_key = ["member_id", "referral_date", "referral_source"]

    conn = sqlite3.connect(database_path)

    if update is True:
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS referrals;")

        create_table(referrals, "referrals", conn, primary_key)

        print("referrals updated...")

    else:
        create_table(referrals, "referrals", conn, primary_key)

        print("referrals created...")

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\referrals_{str(pd.to_datetime('today').date())}.txt",
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

    referrals_to_sql(**vars(arguments))
