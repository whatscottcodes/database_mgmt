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

warnings.simplefilter(action="ignore", category=FutureWarning)


def get_csv_files():

    files = os.listdir(".\\data")

    folders = [
        folder for folder in files if "csv" not in folder and folder != "archive"
    ]
    files = [file for file in files if "csv" in file and "statewide" not in file]
    tables = {}

    for file in files:
        tables[file[:-4]] = pd.read_csv(f".\\data\\{file}", low_memory=False)

    for table in tables.keys():
        date_cols = [
            col for col in tables[table].columns if "date" in col.lower()]
        for col in date_cols:
            try:
                tables[table][col] = pd.to_datetime(tables[table][col])
            except ValueError:
                pass

    incident_dict = {}
    utl_dict = {}
    vacc_dict = {}

    folder_dicts = [incident_dict, utl_dict, vacc_dict]

    for folder, folder_dict in zip(folders, folder_dicts):
        files = os.listdir(f".\\data\\{folder}")
        for file in files:
            folder_dict[file[:-4]] = pd.read_csv(
                f".\\data\\{folder}\\{file}", low_memory=False
            )

    for folder in folder_dicts:
        for df in folder.keys():
            date_cols = [
                col for col in folder[df].columns if "date" in col.lower()]
            for col in date_cols:
                try:
                    folder[df][col] = pd.to_datetime(folder[df][col])
                except ValueError:
                    pass

    return tables, incident_dict, utl_dict, vacc_dict


def archive_files():
    pathName = os.getcwd()

    shutil.make_archive(
        f"C:\\Users\\snelson\\repos\\db_mgmt\\data_archive\\{pd.datetime.today().date()}_update",
        "zip",
        f".\\data",
    )

    files = os.listdir(".\\data")

    folders = [
        folder for folder in files if "csv" not in folder and folder != "archive"
    ]

    for file in [filename for filename in files if "csv" in filename]:
        os.remove(f".\\data\\{file}")

    for folder in folders:
        files = os.listdir(f".\\data\\{folder}")

        files = [
            file
            for file in files
            if "csv" in file
            and file
            not in [
                "ut_grid_inp_2018.csv",
                "ut_grid_er_2018.csv",
                "pco_inpatient.csv",
                "pco_er.csv",
            ]
        ]
        for file in files:
            os.remove(f".\\data\\{folder}\\{file}")
