import os
import pandas as pd
import shutil
from create_new_addresses import create_addresses_to_add
import argparse


def get_cognify_csvs():
    download_csvs = {
        "ParticipantEnrollmentDisenrollmentDetail": "enrollment",
        "ServiceUtilizationInpatient": "utilization\\inpatient",
        "ServiceUtilizationEmergency": "utilization\\er_only",
        "ServiceUtilizationEmergency_IP": "utilization\\er_adm",
        "incident_Falls": "incidents\\falls",
        "incident_Med Errors": "incidents\\med_errors",
        "incident_Burns": "incidents\\burns",
        "incident_Infection": "incidents\\infections",
        "ParticipantQuickList": "addresses\\address_enrolled",
        "ParticipantQuickList_DIS": "addresses\\address_disenrolled",
        "ParticipantCenterDays": "center_days",
    }
    # copy and rename files then delete the file
    for file in download_csvs.keys():
        shutil.copy2(
            f"C:\\Users\\snelson\\Downloads\\{file}",
            f"C:\\Users\\snelson\\work\\db_mgmt\\data\\{download_csvs[file]}",
        )
        os.remove(f"C:\\Users\\snelson\\Downloads\\{file}.xls")

    print("Cognify Complete")


def get_utlization_grid():
    utl_grid = "V:\\Utilization Review\\2019 Utilization Grid.xlsx"
    utl_grid_copy = "C:\\Users\\snelson\\Downloads\\2019 Utilization Grid.xlsx"
    utl_sheets = ["inpt", "ER"]
    utl_csv_name = {
        "inpt": "C:\\Users\\snelson\\work\\db_mgmt\\data\\utilization\\ut_grid_inp.csv",
        "ER": "C:\\Users\\snelson\\work\\db_mgmt\\data\\utilization\\ut_grid_er.csv",
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


def get_PS_data():
    download_excel = ["pneumo_contra", "influ", "influ_contra", "pneumo"]
    for file in download_excel:
        data_xls = pd.read_excel(
            f"C:\\Users\\snelson\\Downloads\\{file}.xls", "Sheet1", index_col=None
        )
        data_xls.to_csv(
            f"C:\\Users\\snelson\\work\\db_mgmt\\data\\vaccination\\{file}.csv",
            encoding="utf-8",
            index=False,
        )
        os.remove(f"C:\\Users\\snelson\\Downloads\\{file}.xls")
    print("Vaccination Complete!")

    data_xls = pd.read_excel(
        f"C:\\Users\\snelson\\Downloads\\addresses.xls", "Sheet1", index_col=None
    )
    data_xls.to_csv(
        f"C:\\Users\\snelson\\work\\db_mgmt\\data\\addresses.csv",
        encoding="utf-8",
        index=False,
    )

    os.remove(f"C:\\Users\\snelson\\Downloads\\addresses.xls")

    print("Addresses Complete!")


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
    create_addresses_to_add(**vars(arguments))

    print("Complete!")
