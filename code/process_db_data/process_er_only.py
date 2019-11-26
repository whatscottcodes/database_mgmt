#!/usr/bin/env python3

import argparse
from process_db_data.data_cleaning_utils import create_id_col
from file_paths import processed_data
import process_db_data.process_utilization as utl


def process_er_only(update=True):
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Training member is dropped
    Column added to indicate if admission is
        within 6 months of enrollment
    Day of week column is added
    Column to count days since last visit is created

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data files
    """
    er_only = utl.load_utlization("er_only.csv")

    er_only = utl.admission_within_6_mo(er_only, update=update)

    er_only = utl.admission_dow(er_only)

    er_only = utl.discharge_admit_diff(
        er_only, table_name="er_only", update=False, admit_diff=True
    )

    er_only["visit_id"] = create_id_col(
        er_only, ["member_id", "admission_date", "facility"], "visit_id"
    )

    cols_to_drop = [
        "enrollment_date",
        "participant_name",
        "text_box5",
        "text_box2",
        "p_c_p",
        "center",
    ]

    er_only.drop(cols_to_drop, axis=1, inplace=True)
    er_only.to_csv(f"{processed_data}\\er_only.csv", index=False)

    # utl_grid = load_clean_utl_grid(utl_type="er")

    # update_utl_grid(utl_grid, er_only_merged, utl_type="er")

    # merge_utl(utl_grid, er_only_merged, utl_filename="er_only")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    process_er_only(**vars(arguments))
