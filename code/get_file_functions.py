import os
import argparse
import shutil
import pandas as pd
from file_paths import raw_data, ehr_file_location


def get_csv_file(csv_filename):
    """
    Files file with the csv_filename in the EHR for DB folder
    and copies it to the raw_data folder with the name
    indicated in the download_csvs dictionary

    Args:
        csv_filename(str): filename of file to be copied

    Output:
        csv: file with new filename in raw_data folder
            to be used in the database pipeline
            related functions
    """
    download_csvs = {
        "ParticipantEnrollmentDisenrollmentDetail": "enrollment",
        "ServiceUtilizationInpatient": "inpatient",
        "ServiceUtilizationEmergency": "er_only",
        "ServiceUtilizationEmergency_IP": "er_adm",
        "incident_Falls": "falls",
        "incident_Med Errors": "med_errors",
        "incident_Burns": "burns",
        "incident_Infection": "infections",
        "ParticipantCenterDays": "center_days",
        "PCMClaimAdmissionDischarge": "admit_claims",
        "PCMPaymentRegister": "payments",
        "ParticipantQuickList": "ppt_quick_list",
        "Admission Changes": "alfs",
        "EmrDroppedHcc_nc": "dx_not_current",
        "EmrDroppedHcc": "dx_current",
        "wound_grid": "wounds",
        "ReferralDetail": "referrals",
    }

    shutil.copy2(
        f"{ehr_file_location}\\{csv_filename}.csv",
        f"{raw_data}\\{download_csvs[csv_filename]}.csv",
    )
    print("success")
    return "success"


def get_xls_file(xls_filename):
    """
    Files file with the xls_filename in the EHR for DB folder,
    reads in as a pandas DataFrame and
    and copies it to the raw_data folder as a csv

    Args:
        xls_filename(str): filename of file to be copied

    Output:
        csv: file in raw_data folder
            to be used in the database pipeline
            related functions
    """
    data_xls = pd.read_excel(f"{ehr_file_location}\\{xls_filename}.xls", index_col=None)
    data_xls.to_csv(f"{raw_data}\\{xls_filename}.csv", encoding="utf-8", index=False)
    print("success")
    return "success"


def get_authorizations():
    """
    Gets authorizations file which contains PRI_auth from the EHR for DB
    folder and copies it to the raw_data folder with the name auths


    Output:
        csv: file in raw_data folder
            to be used in the database pipeline
            related functions
    """
    try:
        ehr_file = [x for x in os.listdir(ehr_file_location) if "PRI_auth" in x][0]
    except IndexError:
        print("Auths file missing")
        return "failed"

    shutil.copy2(f"{ehr_file_location}\\{ehr_file}", f"{raw_data}\\auths.csv")
    print("success")
    return "success"


def get_claim_details():
    """
    Gets claims detail file which contains ClaimDetail_PRI from the EHR for DB
    folder, reads it into a pandas dataframe and saves it to the 
    raw_data folder as csv with the name claims_detail

    Output:
        csv: file in raw_data folder
            to be used in the database pipeline
            related functions
    """
    try:
        ehr_file = [x for x in os.listdir(ehr_file_location) if "ClaimDetail_PRI" in x][
            0
        ]
    except IndexError:
        print("Claim file missing")
        return "failed"

    data_xls = pd.read_excel(
        f"{ehr_file_location}\\{ehr_file}", header=4, index_col=None
    )
    data_xls.to_csv(f"{raw_data}\\claims_detail.csv", encoding="utf-8", index=False)
    print("success")
    return "success"


file_type_to_func = {
    "csv": get_csv_file,
    "xls": get_xls_file,
    "authorizations": get_authorizations,
    "claim details": get_claim_details,
}


def choose_file_to_get(file_type, filename=""):
    """
    Wrapped function to decided which "get" file
    function to use based on filetype.
    Makes it so this file can run with two easy arguments

    Output:
        file_type(str): csv, xlsx, authorizations, claim details
            indicates which type of file function we need
        filename(str): name of the file to be retrieved
            from the ehr_for_db folder
    """
    get_func = file_type_to_func[file_type]
    if (file_type == "csv") or (file_type == "xls"):
        return get_func(filename)
    return get_func()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--file_type",
        default="csv",
        help="Getting a csv, xls, the authorizations, or the claim details file?",
    )

    parser.add_argument(
        "--filename",
        default="",
        help="Name of file in ehr_for_db folder (do not include file extension)",
    )

    arguments = parser.parse_args()

    choose_file_to_get(**vars(arguments))
