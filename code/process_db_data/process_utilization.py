#!/usr/bin/env python3

import sqlite3
import pandas as pd
import numpy as np
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import (
    database_path,
    raw_data,
    processed_data,
    output_folder,
    icd_10_file,
)


def cognify_facility_changes(df, facility_col):
    """
    Facility names in indicated facility column are
        replaced with decided common names

    Args:
        df(DataFrame): pandas dataframe to have facilities replaced
        facility_col(str): name of col to have facilities replaced in
    
    Returns:
        df(DataFrame): df with cleaned facility column
    """
    df[facility_col].replace(
        {
            "Roger Williams Hospital": "Roger Williams Medical Center",
            "Kent Hospital": "Kent County Memorial Hospital",
            "Fatima Hospital": "Our Lady Of Fatima Hospital",
            "FirstHealth Moore Reginal Hospital": "Firsthealth of the Carolinas",
            "Steere House Nursing & Rehab Center": "Steere House Nursing & Rehabilitation",
            "Crestwood Nursing Home": "Crestwood Nursing & Rehabilitation Center",
            "St. Antoine Residence": "Saint Antoine Residence",
            "The Green House Homes at Saint Elizabeth Home": "Saint Elizabeth Home East Greenwich",
        },
        inplace=True,
    )

    df[facility_col] = df[facility_col].str.rstrip()
    return df


def load_utlization(path):
    """
    Loads indicated csv into pandas DataFrame
    Cleans column names
    Facility names in vendor column are
        replaced with decided common names
    Removed training member
    
    Args:
        path(str): name of raw data csv in raw_data folder
    
    Returns:
        df(DataFrame): pandas dataframe
    """
    df = pd.read_csv(f"{raw_data}\\{path}", parse_dates=["AdmissionDate"])

    df.rename(
        columns={"MemberID": "member_id", "LOSDays": "los", "FacilityName": "facility"},
        inplace=True,
    )

    df.columns = clean_table_columns(df.columns)

    facility_col = [col for col in df.columns if "facility" in col][0]

    df = cognify_facility_changes(df, facility_col)

    df = df[df.member_id != 1003]
    return df


def admission_within_6_mo(df, update=True):
    """
    Adds column indicating if the admission is within 6 months of enrollment.
    1 indicates yes
    
    Args:
        df(DataFrame): pandas dataframe to have admission with 6 months enrolled column added.
        update(bool): Indicates if the database is being updated or created

    Returns:
        df(DataFrame): pandas dataframe
    """
    enrollment = pd.read_csv(
        f"{raw_data}\\enrollment.csv",
        usecols=["MemberID", "EnrollmentDate"],
        parse_dates=["EnrollmentDate"],
    )

    enrollment.rename(
        columns={"MemberID": "member_id", "EnrollmentDate": "enrollment_date"},
        inplace=True,
    )
    if update:
        conn = sqlite3.connect(database_path)
        enrollment_db = pd.read_sql(
            "SELECT member_id, enrollment_date FROM enrollment",
            conn,
            parse_dates=["enrollment_date"],
        )
        enrollment = enrollment_db.append(enrollment)

        conn.close()

    df = df.merge(enrollment, how="left", on="member_id")

    df["enrollment_date"] = pd.to_datetime(df["enrollment_date"])
    df["admission_date"] = pd.to_datetime(df["admission_date"])

    df["w_six_months"] = np.where(
        (
            df["admission_date"].dt.to_period("M")
            - df["enrollment_date"].dt.to_period("M")
        )
        .apply(lambda x: x.freqstr[:-1])
        .replace("", np.nan)
        .astype(float)
        <= 6,
        1,
        0,
    )

    return df


def admission_dow(df, claims=False):
    """
    Adds column indicating the name of the day of the admission
    
    Args:
        df(DataFrame): pandas dataframe to have day of the week column added.
            Should be inpatient, er_only, or admission_claims df
        claims(bool): Indicates if the dataframe is a claim file or not

    Returns:
        df(DataFrame): pandas dataframe
    """
    if claims:
        df["dow"] = df["first_service_date"].dt.weekday
    else:
        df["dow"] = df["admission_date"].dt.weekday

    df["dow"].replace(
        {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday",
        },
        inplace=True,
    )

    return df


def time_of_visit_bins(df):
    """
    Adds column binning the time of admission to Open Hours, Evening, and Overnight

    Args:
        df(DataFrame): pandas dataframe to have hour_category column added
            should be the admission_claims df

    Returns:
        df(DataFrame): pandas dataframe
    """
    mask_8_5 = (df["admit_hour_code"] >= 8) & (df["admit_hour_code"] < 17)
    mask_5_11 = (df["admit_hour_code"] >= 17) & (df["admit_hour_code"] < 23)
    mask_11_8 = (df["admit_hour_code"] >= 23) | (df["admit_hour_code"] < 8)

    df["hour_category"] = np.where(mask_8_5, "Open Hours", "Unknown")
    df["hour_category"] = np.where(mask_5_11, "Evening", df["hour_category"])
    df["hour_category"] = np.where(mask_11_8, "Overnight", df["hour_category"])

    return df


def admit_from_er(df):
    """
    Adds column indicating with a 1 if the inpatient admission started in the ER.
    Looks for the inpatient admission to have appeared in the er_admit file

    Args:
        df(DataFrame): pandas dataframe to have from_er column added
            should be the inpatient df

    Returns:
        df(DataFrame): pandas dataframe
    """
    from_er = pd.read_csv(
        f"{raw_data}\\er_adm.csv",
        usecols=["MemberID", "AdmissionDate", "Facility"],
        parse_dates=["AdmissionDate"],
    )

    from_er = cognify_facility_changes(from_er, "Facility")

    from_er["merge"] = (
        from_er["MemberID"].astype(str)
        + from_er["AdmissionDate"].astype(str)
        + from_er["Facility"]
    )

    df["merge"] = (
        df["member_id"].astype(str) + df["admission_date"].astype(str) + df["facility"]
    )
    df["er"] = np.where(df["merge"].isin(from_er["merge"].tolist()), 1, 0)

    df.drop(["merge"], axis=1, inplace=True)
    return df


def fill_missing_admission_type(df):
    """
    Adds column indicating with a 1 if the inpatient admission started in the ER.
    Looks for the inpatient admission to have appeared in the er_admit file

    Args:
        df(DataFrame): pandas dataframe to have from_er column added

    Returns:
        df(DataFrame): pandas dataframe
    """
    for admit_type in df["admission_type"].unique():
        type_facilities = df[df["admission_type"] == admit_type]["facility"].unique()

        df["admission_type"] = np.where(
            (df["admission_type"].isnull() & df["facility"].isin(type_facilities)),
            admit_type,
            df["admission_type"],
        )

    return df


def discharge_admit_diff(df, table_name="", update=True, admit_diff=False):
    """
    Adds column of the days since previous admission for each admission

    Args:
        df(DataFrame): pandas dataframe to have column added
        table_name(str): table in database the data needs to be compared to
            if the database is being updated
        update(bool): Indicates if the database is being updated or created
        admit_diff: indicates if the difference is in admission dates
            or in the discharge date and next admission date
            ie; ER visits don't have a discharge date

    Returns:
        df(DataFrame): pandas dataframe
    """
    dff = df.copy()
    if update is True:
        conn = sqlite3.connect(database_path)

        q = f"""SELECT {','.join([col for col in df.columns if col != 'merge'])} FROM {table_name}
        WHERE member_id IN {tuple(df.member_id.unique())};"""

        current_table = pd.read_sql(
            q, conn, parse_dates=[col for col in df.columns if "date" in col]
        )

        df = current_table.append(df, sort=False)
        df.reset_index(inplace=True, drop=True)

        conn.close()
    if admit_diff:
        diff_date = "admission_date"
        sorted_df = (
            df.sort_values(["member_id", "admission_date"], ascending=False)
            .reset_index(drop=True)
            .copy()
        )
    else:
        diff_date = "discharge_date"
        df["discharge_date"] = pd.to_datetime(df["discharge_date"])
        sorted_df = (
            df.sort_values(
                ["member_id", "admission_date", "discharge_date"], ascending=False
            )
            .reset_index(drop=True)
            .copy()
        )
    # sort dataframe by member_id and then admission date

    sorted_df["days_since_last_admission"] = np.nan

    # iterate through unique member_ids
    for mem_id in sorted_df.member_id.unique():
        # if the member_id appears more than once in the df
        if sorted_df[sorted_df.member_id == mem_id].shape[0] > 1:
            # iterate through each occurrence, first occurrence
            # will be the most recent admission_date
            for i in sorted_df[sorted_df.member_id == mem_id].index[:-1]:
                # find difference between current admission_date and
                # most recent discharge_date
                sorted_df.at[i, "days_since_last_admission"] = (
                    sorted_df.at[i, "admission_date"] - sorted_df.at[(i + 1), diff_date]
                ) / np.timedelta64(1, "D")

    sorted_df.reset_index(drop=True, inplace=True)

    sorted_df = sorted_df[
        (
            (sorted_df["days_since_last_admission"] >= 0)
            | (sorted_df["days_since_last_admission"].isnull())
        )
    ]

    if update is True:
        dff = dff.merge(
            sorted_df[["member_id", "admission_date", "days_since_last_admission"]],
            on=["member_id", "admission_date"],
            how="left",
        )
        dff.sort_values("days_since_last_admission", ascending=False, inplace=True)
        # added 6-18 // see note above
        dff.drop_duplicates(
            subset=[col for col in dff.columns if col != "days_since_last_admission"],
            inplace=True,
            keep="last",
        )
        return dff
    # added 6-18 // see note above
    sorted_df.drop_duplicates(
        subset=[col for col in sorted_df.columns if col != "days_since_last_admission"],
        inplace=True,
        keep="last",
    )

    return sorted_df


def split_inpatient(df):
    """
    Splits inpatient dataframe on admission types

    Args:
        df(DataFrame): pandas dataframe to be split, should be inpatient df
       
    Returns:
        acute(DataFrame): pandas dataframe of acute hospital admissions
        psych(DataFrame): pandas dataframe of psych admissions
        nf(DataFrame): pandas dataframe of nursing facility admissions
    """
    skilled_mask = (
        df["admit_reason"]
        .str.lower()
        .str.contains("skilled|rehab|pt|ot|skil|restorative")
    ) & (
        (df["admission_type"] == "Nursing Home")
        | (df["admission_type"] == "Rehab Unit / Facility")
    )

    respite_mask = (
        df["admit_reason"].str.lower().str.contains("respite|resp|behavior")
    ) & (
        (df["admission_type"] == "Nursing Home")
        | (df["admission_type"] == "Rehab Unit / Facility")
    )

    custodial_mask = (
        df["admit_reason"]
        .str.lower()
        .str.contains(
            "custodial|cust|long term|eol|end of life|hosp|permanent|functional decline|cutodial|ltc|hospic"
        )
    ) & (
        (df["admission_type"] == "Nursing Home")
        | (df["admission_type"] == "End of Life")
        | (df["admission_type"] == "Rehab Unit / Facility")
    )

    df["admit_reason"] = np.where(skilled_mask, "skilled", df["admit_reason"])
    df["admit_reason"] = np.where(respite_mask, "respite", df["admit_reason"])
    df["admit_reason"] = np.where(custodial_mask, "custodial", df["admit_reason"])

    # break up by admit type
    acute_mask = df["admission_type"] == "Acute Hospital"

    psych_mask = df["admission_type"] == "Psych Unit / Facility"

    nf_mask = df["admission_type"].isin(
        ["Nursing Home", "Rehab Unit / Facility", "End of Life"]
    )

    df[
        (-df["admit_reason"].isin(["skilled", "respite", "custodial"])) & (nf_mask)
    ].to_csv(f"{output_folder}\\nf_missing_reason.csv", index=False)

    acute = df[acute_mask].copy()
    psych = df[psych_mask].copy()
    nf = df[nf_mask].copy()

    assert df.shape[0] == (acute.shape[0] + psych.shape[0] + nf.shape[0])
    return acute, psych, nf

