import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, create_sql_dates
from file_paths import database_path, processed_data, update_logs_folder


def auths_to_sql(update=True):
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
    auths = pd.read_csv(f"{processed_data}\\auths.csv")
    auths = create_sql_dates(auths)

    primary_key = ["member_id", "authorization_number"]
    foreign_key = ["member_id"]
    ref_table = ["ppts"]
    ref_col = ["member_id"]

    conn = sqlite3.connect(database_path)

    if update is True:
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS authorizations")

        create_table(
            auths, "authorizations", conn, primary_key, foreign_key, ref_table, ref_col
        )
        print("authorizations updated...")

    else:
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS authorizations")
        create_table(
            auths, "authorizations", conn, primary_key, foreign_key, ref_table, ref_col
        )

        print("authorizations created...")

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\authorizations_{str(pd.to_datetime('today').date())}.txt",
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

    auths_to_sql(**vars(arguments))
