import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, update_sql_table, create_sql_dates
from file_paths import database_path, processed_data, update_logs_folder


def alfs_to_sql(update=True):
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
    alfs = pd.read_csv(f"{processed_data}\\alfs.csv")
    alfs = create_sql_dates(alfs)

    primary_key = ["member_id", "admission_date", "facility_name"]
    foreign_key = ["member_id"]
    ref_table = ["ppts"]
    ref_col = ["member_id"]

    conn = sqlite3.connect(database_path)

    if update is True:
        update_sql_table(alfs, "alfs", conn, primary_key)

        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS temp;")

        print("alfs updated...")

    else:
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS alfs")
        create_table(alfs, "alfs", conn, primary_key, foreign_key, ref_table, ref_col)

        print("alfs created...")

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\alfs_{str(pd.to_datetime('today').date())}.txt", "a"
    ).close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    alfs_to_sql(**vars(arguments))
