#!/usr/bin/env python3

import pandas as pd
from file_paths import raw_data, processed_data
from process_db_data.process_utilization import admission_dow, time_of_visit_bins


def process_admission_claims():
    """
    Cleans/Processes dataset
    
    All columns are made lowercase
    Facility names in provider column are
        replaced with decided common names
    Day of week column is added
    Time of visit is binned in a new column
    Indicated columns are dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file

    """
    admit_claims = pd.read_csv(
        f"{raw_data}\\admit_claims.csv",
        parse_dates=["First_Service_Date", "Last_Service_Date"],
    )

    admit_claims.columns = [col.lower() for col in admit_claims.columns]

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

    admit_claims["provider"].replace(hosp_replace, inplace=True)

    # admit_claims = create_dx_desc_cols(admit_claims)

    admit_claims = admission_dow(admit_claims, claims=True)
    admit_claims = time_of_visit_bins(admit_claims)

    cols_to_drop = ["first_name", "last_name", "textbox24", "textbox25"]

    admit_claims.drop(cols_to_drop, axis=1, inplace=True)
    admit_claims.to_csv(f"{processed_data}\\admit_claims.csv", index=False)

    return admit_claims


if __name__ == "__main__":
    process_admission_claims()
