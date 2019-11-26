#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, update_sql_table, create_sql_dates
from file_paths import database_path, processed_data, update_logs_folder
from paceutils import Helpers


def payments_to_sql(update=True):
    """
    Reads a cleaned dataset
    Parse the dates to match SQL format of YYYY-MM-DD
    Creates or Updates the database table using the
    indicated primary keys and foreign keys

    If being updated only payments with a date greater than or equal to
        the most recent date in the database table are added
        
    Args:
        update(bool): indicates if database table is being updated or not

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    payments = pd.read_csv(f"{processed_data}\\payments.csv", low_memory=False)
    if update:
        h = Helpers(database_path)
        first_id_val = h.single_value_query("SELECT MAX(id_col) FROM payments") + 1
        max_date = h.single_value_query("SELECT MAX(date_paid) FROM payments")

        payments = payments[payments["date_paid"] >= max_date].copy()
        payments.reset_index(inplace=True, drop=True)

        payments["id_col"] = list(range(first_id_val, first_id_val + payments.shape[0]))

    else:
        payments.reset_index(inplace=True)
        payments.rename(columns={"index": "id_col"}, inplace=True)

    payments = create_sql_dates(payments)

    primary_key = ["id_col"]
    foreign_key = ["member_id"]
    ref_table = ["ppts"]
    ref_col = ["member_id"]

    conn = sqlite3.connect(database_path)

    if update is True:

        update_sql_table(payments, "payments", conn, primary_key)

        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS temp;")

        print("payments updated...")

    else:
        create_table(
            payments, "payments", conn, primary_key, foreign_key, ref_table, ref_col
        )

        print("payments created...")

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\payments_{str(pd.to_datetime('today').date())}.txt", "a"
    ).close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    payments_to_sql(**vars(arguments))
