#!/usr/bin/env python3

from process_db_data.data_cleaning_utils import (
    clean_table_columns,
    create_indicator_col,
    create_id_col,
)
from file_paths import raw_data, processed_data
import numpy as np
import pandas as pd
import argparse


def process_grievances(update=True):
    ### NOT USED CURRENTLY
    grievances = pd.read_csv(f"{raw_data}\\grievances.csv")

    if not update:
        prev_griev = pd.read_csv(f"{raw_data}\\grievances_prev.csv")
        grievances.append(prev_griev, sort=False)

    # works with Pauline's grid
    # looks for where she indicates the providers/types start
    # this is in the current first row
    provider_start = grievances.columns.tolist().index("Provider")
    provider_end_type_start = grievances.columns.tolist().index("TYPE")
    # type_end = df.columns.tolist().index("EOT")
    type_end = (
        grievances.iloc[0]
        .values.tolist()
        .index("full resolution for internal tracking")
    )
    # actual col names are in the second row
    grievances.columns = [
        col for col in grievances.iloc[1].values.tolist() if type(col) != float
    ]

    grievances.drop([0, 1], inplace=True)

    grievances.reset_index(drop=True, inplace=True)

    # fix one column that needs the long title for others that use the grid

    grievances.rename(
        columns={
            "grievance # (if highlighted, indicates letter was sent)": "grievance_num"
        },
        inplace=True,
    )
    # fix some odd formatting in the col names - should be able to remove soon

    # get cols that indicate if a grievances is attributed to
    # a specific provider
    providers = grievances.columns[provider_start:provider_end_type_start].tolist()

    # or a specific type
    types = grievances.columns[provider_end_type_start:type_end].tolist()
    grievances.columns = [
        col.lower() if col in providers or col in types else col
        for col in grievances.columns
    ]
    grievances.dropna(axis=1, how="all", inplace=True)

    grievances.columns = clean_table_columns(grievances.columns)
    grievances.rename(columns={"participant_id": "member_id"}, inplace=True)

    grievances["grievance_num"] = grievances["grievance_num"].str.split(
        "-", expand=True
    )[0]
    providers = grievances.columns[provider_start:provider_end_type_start].tolist()
    types = grievances.columns[provider_end_type_start:type_end].tolist()

    # create column that indicates the name of the provider
    # the grievance is attributed to
    grievances = create_indicator_col(grievances, "providers", providers)

    # create column that indicates the has the type of each grievance
    grievances = create_indicator_col(grievances, "types", types)

    grievances["category_of_the_grievance"] = np.where(
        grievances["category_of_the_grievance"].str.contains("Contracted"),
        "Contracted Facility",
        grievances["category_of_the_grievance"],
    )

    grievances["description_of_the_grievance"] = np.where(
        grievances["description_of_the_grievance"].str.contains("Other"),
        "Other",
        grievances["description_of_the_grievance"],
    )

    # turn quality analysis col to binary 1/0 for Y/N
    grievances["quality_analysis"].replace(["Y", "N"], [1, 0], inplace=True)

    # create datetime cols
    grievances["date_grievance_received"] = pd.to_datetime(
        grievances["date_grievance_received"]
    )
    grievances["date_of_resolution"] = pd.to_datetime(grievances["date_of_resolution"])
    grievances["date_of_oral_notification"] = pd.to_datetime(
        grievances["date_of_oral_notification"]
    )
    grievances["date_of_written_notification"] = pd.to_datetime(
        grievances["date_of_written_notification"]
    )

    # drop cols that are not needed, includes all the provider
    # and types cols that have been essentially "un" one hot encoded
    col_to_drop = (
        [
            "participant_first_name",
            "participant_last_name",
            "year_and_qtr_received",
            "quarter_reported",
            "notes",
        ]
        + providers
        + types
    )

    grievances.drop(col_to_drop, axis=1, inplace=True)

    grievances.dropna(subset=["member_id", "date_grievance_received"], inplace=True)

    grievances["griev_id"] = create_id_col(
        grievances, ["member_id", "date_grievance_received"], "griev_id"
    )

    grievances.to_csv(f"{processed_data}\\grievances.csv", index=False)
    return grievances


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update",
        default=True,
        help="Are we updating the database or creating it? True for update",
    )

    arguments = parser.parse_args()

    process_grievances(**vars(arguments))
