#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, create_sql_dates, update_sql_table
from file_paths import database_path, processed_data, update_logs_folder


def teams_to_sql(update=True):
    """
    Reads a cleaned dataset
    Parse the dates to match SQL format of YYYY-MM-DD
    Creates or Updates the database table using the
    indicated primary keys and foreign keys

    Args:
        update(bool): indicates if database table is being updated or not

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    teams = pd.read_csv(f"{processed_data}\\teams.csv")
    teams = create_sql_dates(teams)

    primary_key = ["member_id", "team", "start_date"]
    foreign_key = ["member_id"]
    ref_table = ["ppts"]
    ref_col = ["member_id"]

    conn = sqlite3.connect(database_path)

    if update:

        update_sql_table(teams, "teams", conn, primary_key)

        print("teams updated...")

    else:
        create_table(teams, "teams", conn, primary_key, foreign_key, ref_table, ref_col)

        print("teams created...")

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\teams_{str(pd.to_datetime('today').date())}.txt", "a"
    ).close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    teams_to_sql(**vars(arguments))
