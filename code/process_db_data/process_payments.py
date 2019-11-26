#!/usr/bin/env python3

from locale import setlocale, LC_NUMERIC, atof
import pandas as pd
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import raw_data, processed_data

setlocale(LC_NUMERIC, "")


def process_payments():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Facility names in vendor column are
        replaced with decided common names
    Total paid column is made to floats from US currency

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    payments = pd.read_csv(
        f"{raw_data}\\payments.csv",
        parse_dates=["DatePaid", "DateClaim", "ServiceDate", "ServiceDateTo"],
        low_memory=False,
    )

    payments.rename(
        columns={
            "ClaimID": "claim_id",
            "UB_Invoice": "ub_invoice",
            "AuthID": "auth_id",
            "DMEItem": "dme_item",
            "Check": "check_num",
        },
        inplace=True,
    )

    payments.columns = clean_table_columns(payments.columns)

    payments["total_paid"] = payments["total_paid"].apply(atof)

    hosp_replace = {
        "Roger Williams Med Center": "Roger Williams Medical Center",
        "Psych Our Lady of Fatima": "Our Lady of Fatima Hospital",
        "Our Lady of Fatima Hosp": "Our Lady of Fatima Hospital",
        "Our Lady of Fatima": "Our Lady of Fatima Hospital",
        "The Miriam Hospital Lab": "The Miriam Hospital",
        "Hosp The Miriam Hospital": "The Miriam Hospital",
        "Bayberry Commons": "Bayberry Commons Nursing & Rehabilitation Center",
        "Cedar Crest Nursing Centre": "Cedar Crest Nursing Center",
        "Berkshire Place, Ltd.": "Berkshire Place Nursing and Rehab",
        "Scandinavian Home Inc": "Scandinavian Home",
    }

    payments["vendor"].replace(hosp_replace, inplace=True)
    cols_to_drop = ["program", "center", "participant"]

    payments.drop(cols_to_drop, axis=1, inplace=True)

    payments.to_csv(f"{processed_data}\\payments.csv", index=False)

    return payments


if __name__ == "__main__":
    process_payments()
