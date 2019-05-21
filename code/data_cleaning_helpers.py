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
import string

warnings.simplefilter(action="ignore", category=FutureWarning)


def create_id_col(df, pk, id_col, create_col=True):
    member_ints = df[pk[0]].astype(int)
    date_ints = df[pk[1]].dt.strftime("%Y%m%d").astype(int)

    if create_col:
        df[id_col] = member_ints + date_ints

    if df[id_col].duplicated().sum() != 0:
        df[id_col] += df[id_col].duplicated()
        create_id_col(df, pk, id_col, create_col=False)

    df.drop_duplicates(
        subset=[col for col in df.columns if col != id_col], inplace=True, keep='last')

    return df


def code_y_n(df):
    """
    Takes a pandas dataframe and codes any columns containing Yes/No as 1/0
    """
    for col in df.columns:
        if "No" in df[col].unique():
            df[col] = df[col].str.title()
            df[col].replace({"Yes": 1, "No": 0}, inplace=True)


def build_immunization_status(df, contra, pneumo=True):
    # code administered as 1/not administered as 0
    df["immunization_status"] = np.where(
        df["Immunization: Dose Status"] == "Administered", 1, 0
    )

    # change status of ppts alergic to the vaccine
    contra_df = df[df.member_id.isin(contra["member_id"].unique())]
    admin_contra = contra_df[contra_df.immunization_status ==
                             1]["member_id"].unique()
    contra_ppts = [
        member_id
        for member_id in contra["member_id"].unique()
        if member_id not in admin_contra
    ]

    contra_index = df[df["member_id"].isin(contra_ppts)].index

    for i in contra_index:
        if df.at[i, "immunization_status"] == 0:
            df.at[i, "immunization_status"] = 99

    # list of ppts who have had the vaccine
    admin_members = df[df["immunization_status"] == 1].member_id.unique()

    # list of ppts who have had a not administered interaction
    not_admin_members = df[df["immunization_status"] == 0].member_id

    # check for any ppts who have had both a not administered & administered interaction
    not_admin_now_admin = not_admin_members[
        not_admin_members.isin(admin_members)
    ].values

    if pneumo:
        mask = (df.member_id.isin(not_admin_now_admin)) & (
            df["immunization_status"] == 0
        )

        # remove any the not administered record for any ppts who have
        # had both records indicated

        if len(not_admin_now_admin) != 0:
            df.drop(df[mask].index, inplace=True)

    df.sort_values("date_administered", inplace=True)

    return df


def discharge_admit_diff(
    df, sql_table="", update=False, admit_diff=False, admit_type=""
):
    dff = df.copy()
    if update:
        if admit_type != "":
            admission_sql = f"AND admission_type == '{admit_type}'"
        else:
            admission_sql = ""
        conn = sqlite3.connect(database_path)

        q = f"""SELECT {','.join([col for col in df.columns if col != 'merge'])} FROM {sql_table}
        WHERE member_id IN {tuple(df.member_id.unique())}
        {admission_sql};"""

        current_table = pd.read_sql(
            q, conn, parse_dates=[col for col in df.columns if "date" in col]
        )

        df = current_table.append(df, sort=False)
        df.reset_index(inplace=True, drop=True)

    if admit_diff:
        diff_date = "admission_date"
        sorted_df = (
            df.sort_values(["member_id", "admission_date"], ascending=False)
            .reset_index(drop=True)
            .copy()
        )
    else:
        diff_date = "discharge_date"
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
                    sorted_df.at[i, "admission_date"] -
                    sorted_df.at[(i + 1), diff_date]
                ) / np.timedelta64(1, "D")

    sorted_df.reset_index(drop=True, inplace=True)
    if update:
        dff = dff.merge(
            sorted_df[["member_id", "admission_date",
                       "days_since_last_admission"]],
            on=["member_id", "admission_date"],
            how="left",
        )
        return dff

    return sorted_df


def cognify_check(utl_type, utl_df, utl_grid, er_inp_check):

    # check that all UR Grid visits are in Cognify correctly
    grid_vs_cognify = utl_grid[
        (
            -utl_grid["merge"].isin(utl_df["merge"].tolist())
            & utl_grid["Hospital"].notnull()
        )
    ][
        [
            "Member ID",
            "Ppt Name",
            "Date of visit",
            "Date of Discharge",
            "Hospital",
            "merge",
        ]
    ]

    if utl_type == "er_non":
        # this is any UR Grid ER Visits that is not in either Cognify report
        missing_df = grid_vs_cognify[
            -(grid_vs_cognify["merge"].isin(er_inp_check))
        ].copy()

    else:
        missing_df = grid_vs_cognify.copy()

    if not missing_df.empty:
        missing_df.to_csv(
            f".\\output\\ut_grid_cognify_diffs_{utl_type}.csv", index=False
        )
        mask = (utl_df["member_id"].isin(missing_df["Member ID"])
                ) & (-utl_df["merge"].isin(utl_grid["merge"]))
        utl_df[mask].to_csv(
            f".\\output\\{utl_type}_id_in_diffs.csv", index=False)
        raise AssertionError(
            f"All UR Grid {utl_type} Visits are not in Cognify correctly."
        )


def clean_acute_psych(utl_df, update):
    inpatient_hosp = (
        utl_df[utl_df.admission_type.isin(["Acute Hospital", np.nan])]
        .copy()
        .reset_index(drop=True)
    )

    inpatient_pysch = (
        utl_df[utl_df.admission_type.isin(["Psych Unit / Facility"])]
        .copy()
        .reset_index(drop=True)
    )

    inpatient_hosp.drop_duplicates(inplace=True)
    inpatient_pysch.drop_duplicates(inplace=True)

    # Calculate Day Since Last Admit
    inpatient_hosp = discharge_admit_diff(
        inpatient_hosp,
        sql_table="inpatient",
        admit_type="Acute Hospital",
        update=update,
    )
    inpatient_pysch = discharge_admit_diff(
        inpatient_pysch,
        sql_table="inpatient",
        admit_type="Psych Unit / Facility",
        update=update,
    )

    # put them back together
    inpatient = inpatient_hosp.append(inpatient_pysch)
    return inpatient


def clean_add_merge_col(utl_type, utl_dict, utl_drop, utl_cols):

    cognify_faciliity_fix = {
        "Our Lady of Fatima Hospital": "Fatima Hospital",
        "The Miriam Hospital": "Miriam Hospital",
        "Westerly Hospital ": "Westerly Hospital",
        "Roger Williams Hospital": "Roger Williams Medical Center",
        "Roger Williams Cancer Center": "Roger Williams Medical Center",
    }

    utl_hospital = {
        "Kent": "Kent Hospital",
        "kent": "Kent Hospital",
        "RIH": "Rhode Island Hospital",
        "Landmark": "Landmark Medical Center",
        "Miriam": "Miriam Hospital",
        "RWMC": "Roger Williams Medical Center",
        "Butler": "Butler Hospital",
        "SCH": "South County Hospital",
        "Fatima": "Fatima Hospital",
        "FirstHealth*": "FirstHealth Moore Reginal Hospital",
        "East Side urgent care": "East Side Urgent Care",
        "Westerly": "Westerly Hospital",
        "SCC *": "Carolinas Hosptial System",
        "W&I": "Women & Infants Hospital",
    }

    # inpatient table
    utl_df = utl_dict[utl_type].copy()

    utl_df.drop(utl_drop[utl_type], axis=1, inplace=True)
    utl_df.rename(columns=utl_cols[utl_type], inplace=True)

    utl_df["admission_date"] = pd.to_datetime(utl_df["admission_date"])

    if utl_type == "inpatient":
        utl_df["discharge_date"] = pd.to_datetime(utl_df["discharge_date"])

        utl_dict["er_adm"].drop(utl_drop["er_non"], axis=1, inplace=True)
        utl_dict["er_adm"].rename(columns=utl_cols["er_non"], inplace=True)
        utl_dict["er_adm"]["admission_date"] = pd.to_datetime(
            utl_dict["er_adm"]["admission_date"]
        )
        utl_dict["er_adm"]["facility"].replace(
            cognify_faciliity_fix, inplace=True)
        utl_dict["er_adm"]["merge"] = (
            utl_dict["er_adm"]["member_id"].astype(str)
            + utl_dict["er_adm"]["admission_date"].astype(str)
            + utl_dict["er_adm"]["facility"]
        )

        utl_grid = utl_dict["ut_grid_inp"].copy()

    else:
        utl_grid = utl_dict["ut_grid_er"].copy()

    utl_df["facility"].replace(cognify_faciliity_fix, inplace=True)

    utl_grid["Date of visit"] = pd.to_datetime(utl_grid["Date of visit"])
    utl_grid["Date of visit"] = pd.to_datetime(utl_grid["Date of visit"])

    utl_grid["Hospital"].replace(utl_hospital, inplace=True)

    utl_grid.dropna(subset=["Member ID"], inplace=True)

    utl_grid["Member ID"] = utl_grid["Member ID"].astype(int)

    utl_df["merge"] = (
        utl_df["member_id"].astype(str)
        + utl_df["admission_date"].astype(str)
        + utl_df["facility"]
    )

    utl_grid["merge"] = (
        utl_grid["Member ID"].astype(str)
        + utl_grid["Date of visit"].astype(str)
        + utl_grid["Hospital"]
    )

    return utl_df, utl_grid


def check_intergrity_and_merge(utl_type, utl_dict, utl_drop, utl_cols, enrollment_df, update):

    if utl_type == "inpatient_snf":
        utl_df = utl_dict["inpatient"][
            utl_dict["inpatient"]["admission_type"].isin(
                ["Nursing Home", "Rehab Unit / Facility", "End of Life"]
            )
        ].copy()

        # replace long admit reasons with the core reason custodial/respite/skilled/other
        snf_reasons = utl_df[utl_df["admit_reason"].notnull()
                             ]["admit_reason"].copy()
        skilled = snf_reasons[snf_reasons.str.contains(
            "skilled", case=False)].unique()
        custodial = snf_reasons[
            snf_reasons.str.contains(
                "custodial|EOL|end of life|long term", case=False)
        ].unique()
        respite = snf_reasons[snf_reasons.str.contains(
            "respite", case=False)].unique()
        other = [
            reason
            for reason in snf_reasons.unique()
            if reason not in skilled.tolist() + custodial.tolist() + respite.tolist()
        ]

        skill = {x: "Skilled" for x in skilled}
        cust = {x: "Custodial" for x in custodial}
        res = {x: "Respite" for x in respite}
        oth = {x: "Other" for x in other}

        reason_dict = {**cust, **res, **oth, **skill}

        utl_df["admit_reason"].replace(reason_dict, inplace=True)

        # calculate LOS
        utl_df["los"] = np.where(
            utl_df.discharge_date.isnull(), np.nan, utl_df.los)

        merged_df = utl_df.copy()

    else:
        utl_df = utl_dict[utl_type].copy()

        if utl_type == "inpatient":
            utl_grid = utl_dict["ut_grid_inp"].copy()

        else:
            utl_grid = utl_dict["ut_grid_er"].copy()

        # Cognify inpatetient does not indicate ER or not
        # check if inpatient stay is in the ER admited report
        er_inp_check = utl_dict["inpatient"][
            (
                utl_dict["inpatient"]["merge"].isin(
                    utl_dict["ut_grid_er"]["merge"].tolist()
                )
            )
        ]["merge"]

        if utl_type == "inpatient":
            utl_df["er"] = np.where(
                utl_df["merge"].isin(
                    utl_dict["er_adm"]["merge"].tolist()), 1, 0
            )
            utl_grid.drop(
                utl_grid[utl_grid["Visit type"] == "Scheduled"].index.tolist(),
                inplace=True,
            )

        # create OBS column
            utl_dict["ut_grid_er"]["Visit type"] = np.where(
                utl_dict["ut_grid_er"]["Visit type"] == "OBS", 1, 0
            )

            inp_obs = utl_dict["ut_grid_er"][
                (utl_dict["ut_grid_er"]["merge"].isin(er_inp_check))
                & (utl_dict["ut_grid_er"]["Visit type"] == 1)
            ]["merge"].tolist()

            utl_df["observation"] = np.where(
                utl_df["merge"].isin(inp_obs), 1, 0)

            hosp_list = utl_df[utl_df["admission_type"]
                               == 'Acute Hospital']['facility'].unique()

            nursing_facility_list = utl_df[utl_df["admission_type"]
                                           == 'Nursing Home']['facility'].unique()

            utl_df["admission_type"] = np.where(
                (
                    utl_df["admission_type"].isnull()
                    & utl_df["facility"].isin(hosp_list)
                ),
                "Acute Hospital",
                utl_df["admission_type"],
            )

            utl_df["admission_type"] = np.where(
                (
                    utl_df["admission_type"].isnull()
                    & utl_df["facility"].isin(nursing_facility_list)
                ),
                "Nursing Home",
                utl_df["admission_type"],
            )

            utl_grid.append(
                utl_dict["ut_grid_er"][
                    utl_dict["ut_grid_er"]["merge"].isin(er_inp_check)
                ]
            )
        else:
            utl_grid = utl_grid[-utl_grid["merge"].isin(er_inp_check)]

        cognify_check(utl_type, utl_df, utl_grid, er_inp_check)

        # drop cols
        utl_grid.drop(utl_drop["grid"], axis=1, inplace=True)

        utl_grid.rename(columns=utl_cols["grid"], inplace=True)

        utl_grid["days_MD"] = np.where(
            utl_grid["days_MD"].astype(
                float) >= 1000, np.nan, utl_grid["days_MD"]
        )
        utl_grid["days_RN"] = np.where(
            utl_grid["days_RN"].astype(
                float) >= 1000, np.nan, utl_grid["days_RN"]
        )


        if utl_type == "inpatient":
            utl_df = clean_acute_psych(utl_df, update)

        else:
            utl_df = discharge_admit_diff(
                utl_df, sql_table="er_only", admit_diff=True, update=update
            )

        utl_df.drop_duplicates(subset = [col for col in utl_df if col != 'days_since_last_admission'], inplace=True, keep='last')

        # these are all weird copies that are checking the Admission Date against the same stays Discharge Date
        utl_df["days_since_last_admission"] = np.where(
            utl_df["days_since_last_admission"] <= 0,
            np.nan,
            utl_df["days_since_last_admission"],
        )

        # merge inpatient Cognify report with the UR Grid Inpatient
        merged_df = utl_df.merge(utl_grid, on="merge",
                                 how="left").drop_duplicates()

    # merge with enrollment to create a column indicating
    # if the visit was within 6 months of enrollment

    merged_df = merged_df.merge(
        enrollment_df[["member_id", "enrollment_date"]])

    merged_df["enrollment_date"] = pd.to_datetime(merged_df["enrollment_date"])
    merged_df["admission_date"] = pd.to_datetime(merged_df["admission_date"])

    merged_df["w_six_months"] = np.where(
        (
            merged_df["admission_date"].dt.to_period("M")
            - merged_df["enrollment_date"].dt.to_period("M")
        )
        .apply(lambda x: x.freqstr[:-1])
        .replace("", np.nan)
        .astype(float)
        <= 6,
        1,
        0,
    )

    # Create day of the week column
    merged_df["dow"] = merged_df["admission_date"].dt.weekday
    merged_df["dow"].replace(
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

    # Code any Yes/No cols as 1/0
    code_y_n(merged_df)

    # Drop left over enrollment data and the merge column - we are done with them
    merged_df.drop(["enrollment_date", "merge"], axis=1, inplace=True)

    # create datetime cols
    merged_df["admission_date"] = pd.to_datetime(merged_df["admission_date"])

    if 'inpatient' in utl_type:
        merged_df["discharge_date"] = pd.to_datetime(
            merged_df["discharge_date"])

    for col in merged_df.columns:
        if merged_df[col].dtype == "O":
            merged_df[col] = merged_df[col].apply(
                lambda x: titlecase(str(x))
                if x is not None
                or x.lower() not in ["uti", "chf", "copd", "snf", "alf" "nf"]
                else None
            )

    merged_df = create_id_col(
        merged_df, ['member_id', 'admission_date', 'facility'], 'visit_id')
    return merged_df
