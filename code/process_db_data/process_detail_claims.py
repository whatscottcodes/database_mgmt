#!/usr/bin/env python3

import pandas as pd
from process_db_data.data_cleaning_utils import clean_table_columns
from file_paths import raw_data, processed_data


def process_detail_claims():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Facility names in vendor column are
        replaced with decided common names

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    claims_detail = pd.read_csv(
        f"{raw_data}\\claims_detail.csv",
        parse_dates=[
            "First DOS",
            "Last DOS",
            "Received Date",
            "InAccountingDate",
            "CheckDate",
            "ClaimLineCreatedDate",
        ],
        low_memory=False,
    )

    claims_detail.columns = clean_table_columns(claims_detail.columns)

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

    claims_detail["vendor"].replace(hosp_replace, inplace=True)

    # claims_detail = create_dx_desc_cols(claims_detail, detail=True)
    cols_to_drop = ["participant_name"]

    claims_detail.drop(cols_to_drop, axis=1, inplace=True)
    claims_detail.to_csv(f"{processed_data}\\claims_detail.csv", index=False)

    return claims_detail


if __name__ == "__main__":
    process_detail_claims()
