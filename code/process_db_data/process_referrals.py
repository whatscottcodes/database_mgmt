#!/usr/bin/env python3

import pandas as pd
import numpy as np
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import raw_data, processed_data


def process_referrals():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Close reason column is split into 3 columns;
        close_date
        close_type
        close_details
    
    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """

    referrals = pd.read_csv(
        f"{raw_data}\\referrals.csv",
        parse_dates=[
            "ReferralDate",
            "IntakeVisit",
            "FirstVisitDay",
            "EnrollmentEffective",
        ],
    )

    referrals.columns = clean_table_columns(referrals.columns)

    referrals[["close_date", "close_type"]] = referrals["close_reason"].str.split(
        ":", expand=True
    )

    referrals[["close_type", "close_details"]] = referrals["close_type"].str.split(
        "- ", expand=True
    )

    referrals.drop("close_reason", axis=1, inplace=True)
    referrals["close_type"] = referrals["close_type"].str.strip()
    referrals["close_details"] = referrals["close_details"].str.strip()

    referrals["close_date"] = pd.to_datetime(referrals["close_date"])

    referrals["referral_source"].replace({"NOT SPECIFIED": np.nan}, inplace=True)

    assert len(set(referrals.columns)) == len(referrals.columns)

    referrals = referrals[referrals.member_id != 1003]
    referrals.to_csv(f"{processed_data}\\referrals.csv", index=False)

    return referrals


if __name__ == "__main__":
    process_referrals()
