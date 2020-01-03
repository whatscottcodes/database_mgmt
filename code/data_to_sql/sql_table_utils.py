#!/usr/bin/env python3

import warnings
import pandas as pd
from file_paths import database_path
from paceutils import Helpers

warnings.simplefilter(action="ignore", category=FutureWarning)


def create_sql_dates(df, additional_date_cols=None):
    """
    Looks for any columns with date in the name or is in the
    additional_cols list and parses the date to SQL format
    YYYY-MM-DD

    Args:
        df(DataFrame): pandas dataframe to have dates parsed
        additional_date_cols(list): list of columns that have date
            data in them, but do not have date in the name
    
    Returns:
        DataFrame: cleaned dataframe
    """
    date_cols = [col for col in df.columns if "date" in col]
    if additional_date_cols is not None:
        date_cols = date_cols + additional_date_cols
    for col in date_cols:
        df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")
        df[col].replace({"NaT": pd.np.nan}, inplace=True)
    return df


def create_table(
    df,
    table_name,
    conn,
    primary_key,
    foreign_key=None,
    ref_table=None,
    ref_col=None,
    agg_table=False,
):
    """
    Takes a pandas dataframe, sqlite3 connection, primary key columns,
    foreign key column, reference table and column to create a table
    in the connected database.

    Foreign_keys are is set to 1 for the database
    Any ppts not found in the ppts table are removed from df
    SQL Query is built;
        Create TABLE IF NOT EXISTS table_name
        Then columns, dtypes are looped through and if/then statements
        decide if Primary Key or Foreign Key language needs to be added

    Args:
        df(DataFrame): pandas dataframe to be turned into sql table
        table_name(str): name of the table to be create in the database
        conn(Sqlite3 Connection): connection to the database
        primary_key(list): list of columns to use as a primary key
        foreign_key(list): list of column to use as a foreign key
        ref_table(list): list of tables the foreign key columns correspond to
        ref_col(list): list of columns in the references tables the foreign key columns correspond to
        agg_table(bool): Indicates if this is a table in the aggregate database

    Output:
        New table in the connected database

    """
    # create dictionary that will map pandas types to SQLite types
    pd2sql = {
        "flo": "FLOAT",
        "int": "INTEGER",
        "dat": "DATETIME",
        "tim": "DATETIME",
        "cat": "TEXT",
        "obj": "TEXT",
    }

    if not agg_table:
        if ref_table[0] == "ppts":
            df[ref_col] = df[ref_col].astype(int)
            helper = Helpers(database_path)
            current_mem_ids = [
                tup[0]
                for tup in helper.fetchall_query("SELECT member_id FROM ppts", [])
            ]

            df = df[df.member_id.isin(current_mem_ids)].copy()

    conn.execute("PRAGMA foreign_keys = 1")
    conn.execute("PRAGMA journal_mode = OFF")
    # build sql query to create tables
    sql_query = """"""
    sql_query += f"CREATE TABLE IF NOT EXISTS {table_name} ("

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


def update_sql_table(df, table_name, conn, primary_key, agg_table=False):
    """
    Takes a pandas dataframe, sqlite3 connection and primary key columns,
    to create a table in the connected database.

    Foreign_keys are is set to 1 for the database
    Any ppts not found in the ppts table are removed from df
    New dataframe is added to the database as a temp table

    SQL to filter the existing table for updating is created;
        This is where the primary key columns match
        For the updating query we want to only update rows with matching primary keys
            this means we do not want to SET on the primary key columns
        SET SQL is of the form table.col = (SELECT col FROM temp WHERE pks match)
        The update query ends up being;
            UPDATE table_nam
            SET using the set SQL above created with a for loop
            WHERE EXISTS (set cols in temp where pks match)

        Next new rows are added to the table using INSERT OR REPLACE
        WHERE NOT EXISTS rows in temp where the pks match.

    If the updating table is addresses, any member_id in the temp table that
    already exists in addresses with an as_of date less than the as_of in
    the temp has active set to 0.

    Lastly the temp table is dropped.

    Args:
        df(DataFrame): pandas dataframe to be turned into sql table
        table_name(str): name of the table to be create in the database
        conn(Sqlite3 Connection): connection to the database
        primary_key(list): list of columns to use as a primary key
        agg_table(bool): Indicates if this is a table in the aggregate database

    Output:
        Updated table in the connected database

    """
    if df.shape[0] == 0:
        return None
    if (table_name != "ppts") & (not agg_table):
        helper = Helpers(database_path)
        current_mem_ids = [
            tup[0] for tup in helper.fetchall_query("SELECT member_id FROM ppts", [])
        ]
        df = df[df.member_id.isin(current_mem_ids)].copy()

    conn.execute("PRAGMA foreign_keys = 1")
    conn.execute("PRAGMA journal_mode = OFF")
    c = conn.cursor()

    # create temp table with possibly new data from Cognify
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

    if table_name == "medications":
        set_cols = [
            df_col
            for df_col in df.columns
            if (df_col not in primary_key) & (df_col != "start_date")
        ]
    elif table_name == "teams":
        set_cols = [df_col for df_col in df.columns if df_col != "end_date"]
    else:
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

    if table_name == "addresses":
        as_of_date = df["as_of"].unique()[0]
        c.execute(
            f"""
        UPDATE addresses
        SET active = 0
        WHERE addresses.member_id IN (SELECT member_id FROM temp)
        AND addresses.as_of < {as_of_date};       
        """
        )

    if table_name == "teams":
        as_of_date = df["start_date"].unique()[0]
        c.execute(
            f"""
        UPDATE teams
        SET end_date = {as_of_date}
        WHERE teams.member_id IN (SELECT member_id FROM temp)
        AND teams.start_date < {as_of_date}
        AND teams.end_date IS NULL;
        """
        )
        c.execute(
            f"""
        UPDATE teams
        SET end_date = (SELECT disenrollment_date FROM enrollment
                        WHERE member_id=teams.member_id)
        WHERE teams.member_id IN (SELECT member_id FROM enrollment
        WHERE disenrollment_date NOT NULL)
        AND teams.end_date IS NULL;
        """
        )

    if table_name == "centers":
        as_of_date = df["start_date"].unique()[0]
        c.execute(
            f"""
        UPDATE centers
        SET end_date = {as_of_date}
        WHERE centers.member_id IN (SELECT member_id FROM temp)
        AND centers.start_date < {as_of_date};  
        """
        )
        c.execute(
            f"""
        UPDATE centers
        SET end_date = (SELECT disenrollment_date FROM enrollment
                        WHERE member_id=centers.member_id)
        WHERE centers.member_id IN (SELECT member_id FROM enrollment
        WHERE disenrollment_date NOT NULL)
        AND centers.end_date IS NULL;
        """
        )

    c.execute(f"DROP TABLE IF EXISTS temp")
    conn.commit()
