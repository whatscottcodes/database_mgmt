#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
from data_to_sql.sql_table_utils import create_table, update_sql_table, create_sql_dates
from file_paths import database_path, processed_data, update_logs_folder


def inpatient_to_sql(update=True):
    """
    Reads a cleaned dataset
    Parse the dates to match SQL format of YYYY-MM-DD
    Creates or Updates the database table using the
    indicated primary keys and foreign keys

    When creating the database, 6 views are created;
        acute, psych, nursing_home, custodial, respite, and skilled

    If table is being updated - any visit with an admission date greater
    than 3 months from today is dropped
    
    Args:
        update(bool): indicates if database table is being updated or not

    Output:
        creates empty text fill in log folder so the Lugi pipeline
            can be told the process is complete.
    """
    acute = pd.read_csv(f"{processed_data}\\inpatient.csv")
    acute = create_sql_dates(acute)

    primary_key = ["visit_id"]
    foreign_key = ["member_id"]
    ref_table = ["ppts"]
    ref_col = ["member_id"]

    conn = sqlite3.connect(database_path)

    if update is True:
        c = conn.cursor()
        c.execute(
            f"DELETE FROM inpatient WHERE (discharge_date >= ? OR discharge_date IS NULL)",
            [str((pd.to_datetime("today") - pd.DateOffset(months=3)).date())],
        )

        update_sql_table(acute, "inpatient", conn, primary_key)

        print("inpatient updated...")

    else:
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS inpatient")
        create_table(
            acute, "inpatient", conn, primary_key, foreign_key, ref_table, ref_col
        )

        print("inpatient created...")

        c.execute(
            """CREATE VIEW acute AS
            SELECT * FROM inpatient
            WHERE admission_type = 'Acute Hospital';
            """
        )

        c.execute(
            """CREATE VIEW psych AS
            SELECT * FROM inpatient
            WHERE admission_type = 'Psych Unit / Facility';
            """
        )

        c.execute(
            """CREATE VIEW nursing_home AS
            SELECT * FROM inpatient
            WHERE (admission_type = 'Nursing Home'
            OR admission_type = 'Rehab Unit / Facility'
            OR admission_type = 'End of Life')
            """
        )

        c.execute(
            """CREATE VIEW skilled AS
            SELECT * FROM inpatient
            WHERE (admission_type = 'Nursing Home'
            OR admission_type = 'Rehab Unit / Facility')
            AND admit_reason = 'skilled';
            """
        )

        c.execute(
            """CREATE VIEW respite AS
            SELECT * FROM inpatient
            WHERE (admission_type = 'Nursing Home'
            OR admission_type = 'Rehab Unit / Facility')
            AND admit_reason = 'respite';
            """
        )

        c.execute(
            """CREATE VIEW custodial AS
            SELECT * FROM inpatient
            WHERE (admission_type = 'Nursing Home'
            OR admission_type = 'Rehab Unit / Facility'
            OR admission_type = 'End of Life')
            AND admit_reason = 'custodial';
            """
        )

    conn.commit()
    conn.close()

    open(
        f"{update_logs_folder}\\inpatient_{str(pd.to_datetime('today').date())}.txt",
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

    inpatient_to_sql(**vars(arguments))
