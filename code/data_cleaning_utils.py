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
from data_cleaning_helpers import *

warnings.simplefilter(action="ignore", category=FutureWarning)


def clean_addresses(df, one_file=False):

    # create dataframe of all Cognify addrsses
    # they come out of Cognify by center
    if one_file:
        geocode_cols = ["coordinates", "full_address",
                        "geocode_address", "local"]
        df.drop(geocode_cols, axis=1, inplace=True)
        df.dropna(subset=["address"], inplace=True)
        return df

    df["as_of"] = pd.to_datetime("today").date()

    df["active"] = 1

    return df


def clean_demos(df):

    df.drop(demographics_drop, axis=1, inplace=True)

    latino_mask = (df["ethnicity"] == "Hispanic or Latino") & (
        df["race"] == "Unknown")

    df.at[latino_mask, "race"] = "Hispanic or Latino"

    # short Other Race
    df["race"].replace({"Other Race": "Other"}, inplace=True)

    # drop ethnicity col
    df.drop("ethnicity", axis=1, inplace=True)

    # code Female/Male as 1/0
    df["gender"] = df["gender"].str.strip()
    df["gender"].replace({"Female": 1, "Male": 0}, inplace=True)

    # Create datetime col of date of birth column
    df["dob"] = pd.to_datetime(df["dob"])

    return df


def clean_incidents(incident_dict, drop_cols):

    # loop through each dataset and
    # drop cols/replace characters
    # code yes/no cols as 1/0
    # create datetime cols
    # create location and location details cols
    for key in incident_dict.keys():
        incident_dict[key].drop(drop_cols[key], axis=1, inplace=True)

        repls = (" - ", "_"), ("/", "_"), ("(", ""), (")",
                                                      ""), (" ", "_"), ("'", "")

        incident_dict[key].columns = [
            reduce(lambda a, kv: a.replace(*kv), repls, col.lower())
            for col in incident_dict[key].columns
        ]
        incident_dict[key].columns = [
            col.replace("-", "_") for col in incident_dict[key].columns
        ]

        code_y_n(incident_dict[key])

        incident_dict[key].dropna(axis=1, how="all", inplace=True)

        incident_dict[key]["date_time_occurred"] = pd.to_datetime(
            incident_dict[key]["date_time_occurred"]
        )

        # date_discovered is not in each report
        # this will keep script from breaking in that case
        try:
            incident_dict[key]["date_discovered"] = pd.to_datetime(
                incident_dict[key]["date_discovered"]
            )
        except KeyError:
            pass
        # location is not in each report
        # this will keep script from breaking in that case
        try:
            # creates a location details sections and allows us to have a col for just PACE/NF/ALF/Home
            # location_details contains Home-Living Room, NF-Living Room and so forth
            incident_dict[key]["location_details"] = incident_dict[key][
                "location"
            ].str.replace("Participant", "")
            incident_dict[key]["location_details"] = incident_dict[key][
                "location_details"
            ].str.replace("PACE Center", "PACE")
            incident_dict[key]["location_details"] = incident_dict[key][
                "location_details"
            ].str.replace("Nursing Facility", "NF")
            incident_dict[key]["location_details"] = incident_dict[key][
                "location_details"
            ].str.replace("Assisted Living Facility", "ALF")
            incident_dict[key]["location"] = incident_dict[key][
                "location_details"
            ].str.split(" - ", expand=True)[0]
        except KeyError:
            pass
        incident_dict[key].reset_index(inplace=True, drop=True)
        incident_dict[key] = create_id_col(
            incident_dict[key], ['member_id', 'date_time_occurred'], 'incident_id')

    return incident_dict


def clean_vacc(vacc_dict, rename_dict):

    for df in [key for key in vacc_dict.keys() if "contra" not in key]:
        if df == "pneumo":
            pneumo_flag = True
        else:
            pneumo_flag = False

        vacc_dict[df].rename(columns=rename_dict, inplace=True)
        vacc_dict[f"{df}_contra"].rename(columns=rename_dict, inplace=True)
        vacc_dict[df] = build_immunization_status(
            vacc_dict[df], vacc_dict[f"{df}_contra"], pneumo=(df == "pneumo")
        )
        vacc_dict[df] = vacc_dict[df][
            ["member_id", "immunization_status", "date_administered"]
        ]
        vacc_dict[df].drop_duplicates(inplace=True)
        vacc_dict[df].dropna(subset=["immunization_status"], inplace=True)

    return vacc_dict


def clean_enrollment(df, rename_dict, drop_cols):

    # create first and last name cols
    df[["last", "first"]] = df["ParticipantName"].str.split(",", expand=True)

    # drop information that is in other tables/cols
    # or not needed (SSN)

    df.drop(drop_cols, axis=1, inplace=True)

    df.rename(columns=rename_dict, inplace=True)

    # code medicare/medicaid as 1 for has 0 for does not
    df["medicare"] = np.where(df["medicare"].notnull(), 1, 0)
    df["medicaid"] = np.where(df["medicaid"].notnull(), 1, 0)

    # disenroll_reasons begins with the type (volunatry/non)
    # Split that info out in to a new column
    df["disenroll_type"] = (
        df["disenroll_reason"].str.split(" ", expand=True)[
            0].replace("", np.nan)
    )

    df["disenroll_reason"] = df["disenroll_reason"].apply(
        lambda x: " ".join(str(x).split(" ")[1:])
    )
    # replace blank reasons with null values

    df["disenroll_reason"].replace("", np.nan, inplace=True)
    # dissatified with is implied in all of these reasons
    df["disenroll_reason"] = df["disenroll_reason"].str.replace(
        "Dissatisfied with ", ""
    )

    # create datetime cols
    df["enrollment_date"] = pd.to_datetime(df["enrollment_date"])
    df["disenrollment_date"] = pd.to_datetime(df["disenrollment_date"])

    return df


def clean_center_days(df, rename_dict, drop_cols):
    # drop cols that are not needed
    # or show up in the enrollment table

    df.drop(drop_cols, axis=1, inplace=True)

    df.rename(columns=rename_dict, inplace=True)

    # create an as of column, so we can keep track of historic changes
    df["as_of"] = pd.to_datetime("today").date()

    return df


def clean_dx(df, dx_cols):
    df.dropna(thresh=2, inplace=True)

    df.drop(dx_drop_cols, axis=1, inplace=True)

    df.columns = dx_cols

    df["icd_simple"] = df["icd10"].str.split(".", expand=True)[0]
    # PrimeSuite has duplicates sometimes

    df["as_of"] = pd.to_datetime("today").date()

    df.drop_duplicates(subset=["member_id", "as_of", "icd10"], inplace=True)

    df.dropna(inplace=True)

    return df


def clean_grievances(df):

    # works with Pauline's grid
    # looks for where she indicates the providers/types start
    # this is in the current first row
    provider_start = df.columns.tolist().index("Provider")
    provider_end_type_start = df.columns.tolist().index("TYPE")
    # type_end = df.columns.tolist().index("EOT")
    type_end = df.iloc[0].values.tolist().index(
        "full resolution for internal tracking")
    # actual col names are in the second row
    df.columns = df.iloc[1].values.tolist()

    df.drop([0, 1], inplace=True)

    df.reset_index(drop=True, inplace=True)

    # fix one column that needs the long title for others that use the grid
    df.rename(
        columns={
            "grievance # (if highlighted, indicates letter was sent)": "grievance_num"
        },
        inplace=True,
    )

    df["grievance_num"] = df["grievance_num"].str.split("-", expand=True)[0]

    # fix some odd formating in the col names
    df.columns = [
        str(col).lower().replace(" ", "_").replace("\n", "").replace("\r", "")
        for col in df.columns
    ]
    df.columns = [col.replace("/", "_").replace("-", "_")
                  for col in df.columns]
    df.rename(columns={"participant_id": "member_id"}, inplace=True)
    # get cols that indicate if a grievances is attributed to
    # a specific provider
    providers = df.columns[provider_start:provider_end_type_start].tolist()

    # or a specific type
    types = df.columns[provider_end_type_start:type_end].tolist()

    # create column that indicates the has the name of the provider
    # the grievance is attributed to
    df["providers"] = np.nan

    for provider in providers:
        df[provider] = df[provider].replace("0.5", "1")
        df["providers"] = np.where(
            df[provider] == "1", provider, df["providers"])

    # create column that indicates the has the type of each grievance
    df["types"] = np.nan

    for type_griev in types:
        df[provider] = df[provider].replace("0.5", "1")
        df["types"] = np.where(df[type_griev] == "1", type_griev, df["types"])

    # below we clean up some common data entry issuses we saw
    df["providers"] = df["providers"].str.replace("(", "")
    df["providers"] = df["providers"].str.replace(")", "")
    df["providers"] = df["providers"].str.replace(
        "transportation", "transport")

    df["providers"] = df["providers"].str.replace(
        "snfs_hospitals_alfs", "facilities")
    df["providers"] = df["providers"].str.replace(".", "_")

    df["types"] = df["types"].str.replace("(products)", "")
    df["types"] = df["types"].str.replace("equip_supplies()", "equip_supplies")
    df["types"] = df["types"].str.replace("(", "")
    df["types"] = df["types"].str.replace(")", "")
    df["types"] = df["types"].str.replace("commun-ication", "communication")
    df["types"] = df["types"].str.replace("person-nel", "personnel")

    df["category_of_the_grievance"] = np.where(
        df["category_of_the_grievance"].str.contains("Contracted"),
        "Contracted Facility",
        df["category_of_the_grievance"],
    )

    # drop cols that we do not need, includes all the provider
    # and types cols that we have essentially "un" one hot encoded
    grievances_drop = (
        [
            "participant_first_name",
            "participant_last_name",
            "year_and_qtr_received",
            "quarter_reported",
            "nan",
        ]
        + providers
        + types
    )

    df.drop(grievances_drop, axis=1, inplace=True)

    df["description_of_the_grievance"] = np.where(
        df["description_of_the_grievance"].str.contains("Other"),
        "Other",
        df["description_of_the_grievance"],
    )

    # turn quality analysis col to binary 1/0 for Y/N
    df["quality_analysis"].replace(["Y", "N"], [1, 0], inplace=True)

    # create datetime cols
    df["date_grievance_received"] = pd.to_datetime(
        df["date_grievance_received"])
    df["date_of_resolution"] = pd.to_datetime(df["date_of_resolution"])
    df["date_of_oral_notification"] = pd.to_datetime(
        df["date_of_oral_notification"])
    df["date_of_written_notification"] = pd.to_datetime(
        df["date_of_written_notification"]
    )

    df.dropna(subset=["member_id", "date_grievance_received"], inplace=True)

    df = create_id_col(
        df, ['member_id', 'date_grievance_received'], 'griev_id')

    return df


def clean_utilization(
    utl_dict, utl_drop, utl_cols, enrollment_df, update=False, conn=None
):
    enrollment_new = enrollment_df[["member_id", "enrollment_date"]]
    enrollment = enrollment_new.copy()

    if update:
        enrollment_db = pd.read_sql(
            "SELECT member_id, enrollment_date FROM enrollment",
            conn,
            parse_dates=["enrollment_date"],
        )
        enrollment = enrollment_db.append(enrollment_new)

    conn.close()

    utl_dict["ut_grid_inp"] = utl_dict["ut_grid_inp_2018"].append(
        utl_dict["ut_grid_inp"]
    )
    utl_dict["ut_grid_er"] = utl_dict["ut_grid_er_2018"].append(
        utl_dict["ut_grid_er"])
    utl_dict["inpatient"] = utl_dict["pco_inpatient"].append(
        utl_dict["inpatient"])
    utl_dict["er_non"] = utl_dict["pco_er"].append(utl_dict["er_non"])

    utl_dict["inpatient"], utl_dict["ut_grid_inp"] = clean_add_merge_col(
        "inpatient", utl_dict, utl_drop, utl_cols
    )
    utl_dict["er_non"], utl_dict["ut_grid_er"] = clean_add_merge_col(
        "er_non", utl_dict, utl_drop, utl_cols
    )

    inpatient = check_intergrity_and_merge(
        "inpatient", utl_dict, utl_drop, utl_cols, enrollment, update
    )
    er_only = check_intergrity_and_merge(
        "er_non", utl_dict, utl_drop, utl_cols, enrollment, update
    )
    inpatient_snf = check_intergrity_and_merge(
        "inpatient_snf", utl_dict, utl_drop, utl_cols, enrollment, update
    )

    return inpatient, er_only, inpatient_snf
