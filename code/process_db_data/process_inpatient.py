#!/usr/bin/env python3

import argparse
from process_db_data.data_cleaning_utils import create_id_col
from file_paths import processed_data
import process_db_data.process_utilization as utl


def process_inpatient(update=True):
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Training member is dropped
    Column added to indicate if admission is
        within 6 months of enrollment
    Day of week column is added
    Column is added to indicate the admission began in the ER
    Admission Type are filled based on facility
    Inpatient is split to calculate days since last admission
    Column to count days since last admission is created

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data files
    """
    inpatient = utl.load_utlization("inpatient.csv")

    inpatient = utl.admission_within_6_mo(inpatient, update=update)
    inpatient = utl.admission_dow(inpatient)

    inpatient = utl.admit_from_er(inpatient)
    inpatient = utl.fill_missing_admission_type(inpatient)
    acute, psych, nf = utl.split_inpatient(inpatient)

    acute = utl.discharge_admit_diff(acute, table_name="acute", update=False)
    psych = utl.discharge_admit_diff(psych, table_name="psych", update=False)
    nf = utl.discharge_admit_diff(nf, table_name="nursing_home", update=False)

    inpatient = acute.append(psych, sort=False).append(nf, sort=False)

    inpatient["visit_id"] = create_id_col(
        inpatient, ["member_id", "admission_date", "facility"], "visit_id"
    )

    cols_to_drop = [
        "enrollment_date",
        "text_box5",
        "center",
        "participant_name",
        "p_c_p",
        "i_c_u_days",
    ]

    inpatient.drop(cols_to_drop, axis=1, inplace=True)

    inpatient.to_csv(f"{processed_data}\\inpatient.csv", index=False)

    # utl_grid = load_clean_utl_grid(utl_type="inp")

    # update_utl_grid(utl_grid, acute, utl_type="inp")

    # merge_utl(utl_grid, acute, utl_filename="acute")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    process_inpatient(**vars(arguments))
