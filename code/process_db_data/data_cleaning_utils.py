#!/usr/bin/env python3

from functools import reduce
import warnings
import re
import string
import numpy as np

warnings.simplefilter(action="ignore", category=FutureWarning)


def clean_table_columns(table_cols):
    """
    Takes a list of column names and changes MEMBERID to member_id if needed,
    makes them lowercase, and
    replaces indicated characters and spaces with _.
    
    ####note: drop any not needed cols prior to using this function

    Args:
        table_cols(list): list of column names

    Returns:
        list: cleaned list of column names
    """

    table_cols = ["member_id" if "MemberID" in col else col for col in table_cols]
    table_cols = ["los" if col == "LOS" else col for col in table_cols]

    table_cols = [
        "_".join(re.findall("[a-zA-Z][^A-Z]*", col)).lower() if " " not in col else col
        for col in table_cols
    ]
    # replace spaces, slashes, and dashes with underscores
    repls = (
        (" - ", "_"),
        ("/", "_"),
        ("-", "_"),
        (" ", "_"),
        ("\n", ""),
        ("\r", ""),
        (" _", "_"),
        ("_ ", "_"),
        (" _ ", "_"),
        ("__", "_"),
    )
    table_cols = [
        reduce(lambda a, kv: a.replace(*kv), repls, col.lower()) for col in table_cols
    ]
    # delete all punctuation except the underscores
    punc = string.punctuation
    punc = punc.replace("_", "")
    table_cols = [col.translate(str.maketrans("", "", punc)) for col in table_cols]

    return table_cols


def get_id(name_string):
    """
    Finds the member_id in a string

    Args:
        string with member_id in it
    
    Returns:
        str: member_id or nan value
    """
    mem_id = re.findall(r"\(\d*\)", name_string)
    try:
        mem_id = mem_id[0].replace("(", "").replace(")", "")
    except IndexError:
        mem_id = np.nan
    return mem_id


def create_indicator_col(df, indicator_cat, indicator_cols):
    """
    For Grievances WIP - might be dropped
    """
    df[indicator_cat] = np.nan

    for indicator in indicator_cols:
        df[indicator] = df[indicator].replace("0.5", "1")
        df[indicator_cat] = np.where(df[indicator] == "1", indicator, df[indicator_cat])

    # below we clean up some common data entry issuses we saw
    df[indicator_cat] = df[indicator_cat].str.replace("(", "")
    df[indicator_cat] = df[indicator_cat].str.replace(")", "")
    df[indicator_cat] = df[indicator_cat].str.replace("transportation", "transport")

    df[indicator_cat] = df[indicator_cat].str.replace(
        "snfs_hospitals_alfs", "facilities"
    )
    df[indicator_cat] = df[indicator_cat].str.replace(".", "_")

    df[indicator_cat] = df[indicator_cat].str.replace("(products)", "")
    df[indicator_cat] = df[indicator_cat].str.replace(
        "equip_supplies()", "equip_supplies"
    )
    df[indicator_cat] = df[indicator_cat].str.replace("commun-ication", "communication")
    df[indicator_cat] = df[indicator_cat].str.replace("person-nel", "personnel")

    return df


def create_id_col(df, pk, id_col, create_col=True):
    """
    Takes a dataframe and adds a unique id_col.
    This column is the merger of the member_id and date column as ints,
    then any duplicated columns have a 1 added to them until there
    are no duplicates

    Args:
        df(DataFrame): pandas dataframe to add id_col to
        pk(list or tuple): member id column name and a date column name
            to be used to crate an ID column
        id_col: name of the id column
        create_col(bool): indicates if the column should be added to the df

    Returns:
        pandas Series: the newly created id column
    """
    member_ints = df[pk[0]].astype(int)
    date_ints = df[pk[1]].dt.strftime("%Y%m%d").astype(int)

    if create_col:
        df[id_col] = member_ints + date_ints

    if df[id_col].duplicated().sum() != 0:
        df[id_col] += df[id_col].duplicated()
        create_id_col(df, pk, id_col, create_col=False)

    df.drop_duplicates(
        subset=[col for col in df.columns if col != id_col], inplace=True, keep="last"
    )

    return df[id_col]


def code_y_n(df):
    """
    Takes a pandas dataframe and codes any columns containing Yes/No as 1/0

    Args:
        df(DataFrame): pandas dataframe containing yes/no columns

    Returns:
        DataFrame: dataframe with replacements made
    """
    for col in df.columns:
        if "No" in df[col].unique():
            df[col] = df[col].str.title()
            df[col].replace({"Yes": 1, "No": 0}, inplace=True)
    return df

