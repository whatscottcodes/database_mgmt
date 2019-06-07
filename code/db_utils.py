#!/usr/bin/env python3

import os
import pandas as pd
import numpy as np
import warnings
import sqlite3
from functools import reduce
import datetime
from db_rename_cols import *
from geomap import geolocate_addresses
import shutil
from titlecase import titlecase
import distutils.dir_util
from data_cleaning_utils import *
from db_file_mgmt import get_csv_files, archive_files

warnings.simplefilter(action="ignore", category=FutureWarning)


def create_table(
    df, table_name, primary_key, conn, foreign_key=None, ref_table=None, ref_col=None
):
    # create dictionary that will map pandas types to sqlite types
    pd2sql = {
        "flo": "FLOAT",
        "int": "INTEGER",
        "dat": "DATETIME",
        "tim": "DATETIME",
        "cat": "TEXT",
        "obj": "TEXT",
    }
    conn.execute("PRAGMA foreign_keys = 1")

    # build sql query to create tables
    sql_query = """"""
    sql_query += f"CREATE TABLE IF NOT EXISTS {table_name} ("

    # dtype_dict = {}

    if (foreign_key is None) and (len(primary_key) == 1):
        for col, dtype in df.dtypes.iteritems():
            sql_type = pd2sql[str(dtype)[:3]]
            if col == df.columns[-1]:
                end = ""
            else:
                end = ","
            if col in primary_key:
                sql_query += f"{col} {sql_type} PRIMARY KEY{end}"
            else:
                sql_query += f"{col} {sql_type}{end}"

    elif (foreign_key is not None) and (len(primary_key) == 1):
        for col, dtype in df.dtypes.iteritems():
            sql_type = pd2sql[str(dtype)[:3]]
            if col in primary_key:
                sql_query += f"{col} {sql_type} PRIMARY KEY,"
            else:
                sql_query += f"{col} {sql_type},"

        # append foreign key creation sql if needed
        for fk, rtb, rcol in zip(foreign_key, ref_table, ref_col):
            sql_query += f"FOREIGN KEY ({fk}) REFERENCES {rtb} ({rcol}) "
    else:
        for col, dtype in df.dtypes.iteritems():
            sql_type = pd2sql[str(dtype)[:3]]
            sql_query += f"{col} {sql_type},"
        # create primary key SQL

        if foreign_key is not None:
            for fk, rtb, rcol in zip(foreign_key, ref_table, ref_col):
                sql_query += f"FOREIGN KEY ({fk}) REFERENCES {rtb} ({rcol}) "
        pk = f"PRIMARY KEY ({primary_key[0]}"

        try:
            for i, k in enumerate(primary_key[1:]):
                if (i + 1) == len(primary_key) - 1:
                    pk += f", {k})"
                else:
                    pk += f", {k}"
        except IndexError:
            primary_key = f"PRIMARY KEY ({primary_key[0]}))"

        sql_query += pk

    sql_query += ");"
    c = conn.cursor()

    c.execute(sql_query)
    conn.commit()
    # take pandas dataframe and append all rows to our sql table
    df.drop_duplicates(subset=primary_key, inplace=True)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    conn.commit()


def update_sql_table(df, table_name, conn, primary_key):
    c = conn.cursor()

    # create temp table with possibly new data from cognify
    df.to_sql("temp", conn, index=False, if_exists="replace")

    # filters sql table for non new rows
    # and updates the cols
    filter_sql = f"""WHERE {primary_key[0]} = {table_name}.{primary_key[0]}"""

    try:
        for col in primary_key[1:]:
            filter_sql += f"""
                AND {col} = {table_name}.{col}
                """
    except IndexError:
        pass

    set_cols = [df_col for df_col in df.columns if df_col not in primary_key]

    set_sql = ", ".join(
        [f"""{col} = (SELECT {col} FROM temp {filter_sql})""" for col in set_cols]
    )

    exists_sql = f"""(SELECT {', '.join(set_cols)} FROM temp {filter_sql})"""

    c.execute(
        f"""
        UPDATE {table_name}
        SET {set_sql}
        WHERE EXISTS {exists_sql};
        """
    )
    conn.commit()
    # inserts new data if there is a primary key in the pandas df
    # that is not in the sql table
    insert_cols = ", ".join(col for col in df.columns)

    compare_pk_sql = " AND ".join([f"""f.{col} = t.{col}""" for col in primary_key])

    c.execute(
        f"""
        INSERT OR REPLACE INTO {table_name} ({insert_cols})
        SELECT {insert_cols} FROM temp t
        WHERE NOT EXISTS 
            (SELECT {insert_cols} from {table_name} f
            WHERE {compare_pk_sql});
        """
    )
    conn.commit()

    if table_name == "addresses":
        c.execute(
            """
        UPDATE addresses
        SET active = 0
        WHERE addresses.member_id IN (SELECT member_id FROM temp);        
        """
        )


# start main
def create_or_update_table(db_name, update_table=True):
    # load and clean any csv files

    tables, incident_dict, utl_dict, vacc_dict = get_csv_files()
    try:
        tables["addresses"] = clean_addresses(tables["addresses"])
    except KeyError:
        pass

    try:
        tables["enrollment"] = clean_enrollment(
            tables["enrollment"], enrollment_rename_dict, enrollment_drop
        )
    except KeyError:
        pass

    try:
        tables["demographics"] = clean_demos(tables["demographics"])
    except KeyError:
        pass

    try:
        tables["center_days"] = clean_center_days(
            tables["center_days"], center_days_rename_dict, center_days_drop
        )
    except KeyError:
        pass

    try:
        tables["dx"] = clean_dx(tables["dx"], dx_cols)
    except KeyError:
        pass

    try:
        tables["grievances"] = clean_grievances(
            tables["grievances"], tables["grievances_2018"]
        )
    except KeyError:
        pass

    if len(utl_dict) != 0:
        tables["inpatient"], tables["er_only"], tables[
            "inpatient_snf"
        ] = clean_utilization(
            utl_dict,
            utl_drop,
            utl_cols,
            tables["enrollment"],
            update=update_table,
            conn=sqlite3.connect(database_path),
        )

    incident_dict = clean_incidents(incident_dict, incident_drop_cols)
    vacc_dict = clean_vacc(vacc_dict, vacc_rename_dict)

    # append sub dictionary dfs to tables dictionary
    for df in incident_dict.keys():
        tables[df] = incident_dict[df]

    for df in [key for key in vacc_dict.keys() if "contra" not in key]:
        tables[df] = vacc_dict[df]

    try:
        tables["ppts"] = tables["enrollment"][["member_id", "last", "first"]].copy()
        tables["ppts"].drop_duplicates(subset=["member_id"], inplace=True)

        tables["enrollment"].drop(["last", "first"], axis=1, inplace=True)
    except KeyError:
        pass
    if update_table:
        del tables["grievances_2018"]

    for table_name in tables.keys():
        df = tables[table_name].copy()
        tables[table_name] = df[df.member_id != 1003].copy()

    # build dictionary of table info for the sql tables includes
    # table name:
    # primary key
    # foriegn key
    sql_table_info = {
        "ppts": {
            "primary_key": ["member_id"],
            "foreign_key": None,
            "ref_table": None,
            "ref_col": None,
        },
        "center_days": {
            "primary_key": ["member_id", "days"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "dx": {
            "primary_key": ["member_id", "icd10"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "enrollment": {
            "primary_key": ["member_id", "enrollment_date"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "addresses": {
            "primary_key": ["member_id", "address"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "demographics": {
            "primary_key": ["member_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "burns": {
            "primary_key": ["incident_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "falls": {
            "primary_key": ["incident_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "infections": {
            "primary_key": ["incident_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "med_errors": {
            "primary_key": ["incident_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "influ": {
            "primary_key": ["member_id", "date_administered"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "pneumo": {
            "primary_key": ["member_id", "date_administered"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "inpatient": {
            "primary_key": ["visit_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "er_only": {
            "primary_key": ["visit_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "inpatient_snf": {
            "primary_key": ["visit_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "grievances": {
            "primary_key": ["griev_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
        "wounds": {
            "primary_key": ["incident_id"],
            "foreign_key": ["member_id"],
            "ref_table": ["ppts"],
            "ref_col": ["member_id"],
        },
    }

    conn = sqlite3.connect(database_path)
    # if we only need to update the tables
    if update_table:
        shutil.copy(
            database_path,
            f"{databases_folder}\\PaceDashboard_{pd.to_datetime('today').date()}.db",
        )
        c = conn.cursor()
        table_keys = sql_table_info["ppts"]
        update_sql_table(tables["ppts"], "ppts", conn, table_keys["primary_key"])
        for table_name in [table for table in tables.keys() if table != "ppts"]:
            if table_name in [
                "burns",
                "falls",
                "infections",
                "med_errors",
                "wounds",
                "er_only",
                "inpatient",
                "inpatient_snf",
            ]:
                c.execute(f"DROP TABLE {table_name}")

                table_keys = sql_table_info[table_name]
                create_table(
                    tables[table_name],
                    table_name,
                    table_keys["primary_key"],
                    conn,
                    table_keys["foreign_key"],
                    table_keys["ref_table"],
                    table_keys["ref_col"],
                )
            else:
                print(table_name)
                table_keys = sql_table_info[table_name]
                update_sql_table(
                    tables[table_name], table_name, conn, table_keys["primary_key"]
                )

                c.execute("DROP TABLE temp;")

            print(f"{table_name} updated...")

        text_file = open(f"{databases_folder}\\update_log.txt", "w")
        text_file.write(str(pd.to_datetime("today")))
        text_file.close()
    else:
        # for first time creation only
        table_keys = sql_table_info["ppts"]
        create_table(
            tables["ppts"],
            "ppts",
            table_keys["primary_key"],
            conn,
            table_keys["foreign_key"],
            table_keys["ref_table"],
            table_keys["ref_col"],
        )
        for table_name in [table for table in tables.keys() if table != "ppts"]:
            table_keys = sql_table_info[table_name]
            create_table(
                tables[table_name],
                table_name,
                table_keys["primary_key"],
                conn,
                table_keys["foreign_key"],
                table_keys["ref_table"],
                table_keys["ref_col"],
            )

    archive_files()

    conn.commit()
    conn.close()

    shutil.copy2(database_path, report_db)

    print("All Set!")


def create_or_replace_db():
    exists = os.path.isfile(database_path)

    if exists:
        shutil.copy(
            database_path,
            f"{databases_folder}\\PaceDashboard_{pd.to_datetime('today').date()}.db",
        )
    return create_or_update_table(database_path)


def create_db():
    return create_or_update_table(database_path, False)


def update_db():
    return create_or_update_table(database_path)
