import os
import pandas as pd
import shutil
from create_new_addresses import create_addresses_to_add
from db_rename_cols import ehr_file_location
import argparse


def get_cognify_csvs():
    download_csvs = {
        "ParticipantEnrollmentDisenrollmentDetail": "enrollment",
        "ServiceUtilizationInpatient": "utilization\\inpatient",
        "ServiceUtilizationEmergency": "utilization\\er_non",
        "ServiceUtilizationEmergency_IP": "utilization\\er_adm",
        "incident_Falls": "incidents\\falls",
        "incident_Med Errors": "incidents\\med_errors",
        "incident_Burns": "incidents\\burns",
        "incident_Infection": "incidents\\infections",
        "ParticipantCenterDays": "center_days",
    }
    # copy and rename files then delete the file
    for file in download_csvs.keys():
        try:
            shutil.copy2(
                f"{ehr_file_location}{file}.csv",
                f"C:\\Users\\snelson\\repos\\db_mgmt\\data\\{download_csvs[file]}.csv",
            )
            os.remove(f"{ehr_file_location}{file}.csv")
        except FileNotFoundError:
            print(f"Missing {file}")
            pass

    print("Cognify Complete")


def get_utlization_grid():
    utl_grid = "V:\\Utilization Review\\2019 Utilization Grid.xlsx"
    utl_grid_copy = f"{ehr_file_location}2019 Utilization Grid.xlsx"
    utl_sheets = ["inpt", "ER"]
    utl_csv_name = {
        "inpt": "C:\\Users\\snelson\\repos\\db_mgmt\\data\\utilization\\ut_grid_inp.csv",
        "ER": "C:\\Users\\snelson\\repos\\db_mgmt\\data\\utilization\\ut_grid_er.csv",
    }

    # copy grid from V: Drive to db_mgmt folder
    shutil.copy2(utl_grid, utl_grid_copy)

    # parse utlization sheets to csv files
    for sheet in utl_sheets:
        data_xls = pd.read_excel(utl_grid_copy, sheet, index_col=None)
        data_xls.to_csv(utl_csv_name[sheet], encoding="utf-8", index=False)

    # delete excel file from data folder
    os.remove(utl_grid_copy)
    print("Utilization Complete!")


def get_wound_grid():
    wounds_grid = "V:\\Woundwork\\Wounds_Master.xlsx"
    wounds_grid_copy = f"{ehr_file_location}Wounds_Master.xlsx"

    # copy grid from V: Drive to db_mgmt folder
    shutil.copy2(wounds_grid, wounds_grid_copy)

    # parse utlization sheets to csv files
    data_xls = pd.read_excel(wounds_grid_copy, index_col=None)
    data_xls.to_csv(
        "C:\\Users\\snelson\\repos\\db_mgmt\\data\\incidents\\wounds.csv",
        encoding="utf-8",
        index=False,
    )

    # delete excel file from data folder
    os.remove(wounds_grid_copy)
    print("Wounds Complete!")


def get_PS_data():
    download_excel_vacc = ["pneumo_contra", "influ", "influ_contra", "pneumo"]
    download_excel_other = ["demographics", "dx"]
    for file in download_excel_vacc:
        try:
            data_xls = pd.read_excel(
                f"{ehr_file_location}{file}.xls", "Sheet1", index_col=None
            )
            data_xls.to_csv(
                f"C:\\Users\\snelson\\repos\\db_mgmt\\data\\vaccination\\{file}.csv",
                encoding="utf-8",
                index=False,
            )
            os.remove(f"{ehr_file_location}{file}.xls")
        except FileNotFoundError:
            print(f"Missing {file}")

    print("Vaccination Complete!")
    for file in download_excel_other:
        try:
            data_xls = pd.read_excel(
                f"{ehr_file_location}{file}.xls", "Sheet1", index_col=None
            )
            data_xls.to_csv(
                f"C:\\Users\\snelson\\repos\\db_mgmt\\data\\{file}.csv",
                encoding="utf-8",
                index=False,
            )
            os.remove(f"{ehr_file_location}{file}.xls")
        except FileNotFoundError:
            print(f"Missing {file}")
            pass
    print("PS Complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    get_cognify_csvs()
    get_utlization_grid()
    get_PS_data()
    get_wound_grid()
    create_addresses_to_add(**vars(arguments))

    print("Complete!")
