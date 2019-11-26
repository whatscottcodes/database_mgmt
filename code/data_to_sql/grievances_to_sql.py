#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, update_sql_table
from file_paths import (
    database_path,
    processed_data,
    update_logs_folder,
)


def grievances_to_sql(update=True):

    date_cols = [
        "date_grievance_received",
        "date_of_resolution",
        "date_of_oral_notification",
        "date_of_written_notification",
    ]

    grievances = pd.read_csv(f"{processed_data}\\grievances.csv", parse_dates=date_cols)

    for col in date_cols:
        grievances[col] = grievances[col].dt.date

    primary_key = ["griev_id"]
    foreign_key = ["member_id"]
    ref_table = ["ppts"]
    ref_col = ["member_id"]

    conn = sqlite3.connect(database_path)

    if update is True:
        update_sql_table(grievances, "grievances", conn, primary_key)

        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS temp;")

        print("grievances updated...")

    else:
        create_table(
            grievances, "grievances", conn, primary_key, foreign_key, ref_table, ref_col
        )

        print("grievances created...")

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\grievances_{str(pd.to_datetime('today').date())}.txt",
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

    grievances_to_sql(**vars(arguments))
