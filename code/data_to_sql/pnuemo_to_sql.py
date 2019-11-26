#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, update_sql_table, create_sql_dates
from file_paths import database_path, processed_data, update_logs_folder


def pnuemo_to_sql(update=True):
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
    pneumo = pd.read_csv(f"{processed_data}\\pneumo.csv")
    pneumo = create_sql_dates(pneumo)

    primary_key = ["member_id", "date_administered"]
    foreign_key = ["member_id"]
    ref_table = ["ppts"]
    ref_col = ["member_id"]

    conn = sqlite3.connect(database_path)

    if update is True:
        update_sql_table(pneumo, "pneumo", conn, primary_key)

        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS temp;")

        print("pneumo updated...")

    else:
        create_table(
            pneumo, "pneumo", conn, primary_key, foreign_key, ref_table, ref_col
        )

        print("pneumo created...")

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\pnuemo_{str(pd.to_datetime('today').date())}.txt", "a"
    ).close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    pnuemo_to_sql(**vars(arguments))
