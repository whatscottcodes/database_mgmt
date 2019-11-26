#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, create_sql_dates
from file_paths import database_path, processed_data, update_logs_folder


def claims_detail_to_sql(update=True):
    """
    Reads a cleaned dataset
    Parse the dates to match SQL format of YYYY-MM-DD
    Creates or Updates the database table using the
    indicated primary keys and foreign keys

    If table is being updated - it is dropped and replaced entirely
    
    Args:
        update(bool): indicates if database table is being updated or not

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    claims_detail = pd.read_csv(
        f"{processed_data}\\claims_detail.csv", low_memory=False
    )

    claims_detail = create_sql_dates(claims_detail, ["first_dos", "last_dos"])

    primary_key = ["claim_line_id"]
    foreign_key = ["member_id"]
    ref_table = ["ppts"]
    ref_col = ["member_id"]

    conn = sqlite3.connect(database_path)

    if update is True:
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS claims_detail")

        create_table(
            claims_detail,
            "claims_detail",
            conn,
            primary_key,
            foreign_key,
            ref_table,
            ref_col,
        )

        print("claims_detail updated...")

    else:
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS claims_detail")
        create_table(
            claims_detail,
            "claims_detail",
            conn,
            primary_key,
            foreign_key,
            ref_table,
            ref_col,
        )

        print("claims_detail created...")

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\claims_detail_{str(pd.to_datetime('today').date())}.txt",
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

    claims_detail_to_sql(**vars(arguments))
