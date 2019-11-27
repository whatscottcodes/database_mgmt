import pandas as pd
from process_db_data.data_cleaning_utils import clean_table_columns, get_id
from file_paths import processed_data, raw_data


def process_alfs():
    """
    Cleans/Processes dataset
    
    Filtered for Assisted Living admissions
    Column names are cleaned
    Indicated columns are dropped
    Member ID column is created
    Discharge type is split into type and facility columns
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """

    alfs = pd.read_csv(
        f"{raw_data}\\alfs.csv", parse_dates=["AdmissionDate", "DischargeDate"]
    )
    alfs = alfs[
        (alfs["FacilityType"] == "Assisted Living - Permanent")
        | (alfs["FacilityType"] == "Assisted Living - Respite")
    ].copy()

    alfs.columns = clean_table_columns(alfs.columns)

    drop_cols = ["observation_only", "participant_name"]

    alfs["member_id"] = alfs["participant_name"].apply(get_id)

    if (alfs.shape[0] != 0) & (
        alfs["discharge_type"].isnull().sum() != alfs["discharge_type"].shape[0]
    ):
        try:
            alfs[
                ["discharge_type", "discharge_facility", "discharge_facility2"]
            ] = alfs["discharge_type"].str.split(" - ", expand=True)
        except ValueError:
            alfs[["discharge_type", "discharge_facility"]] = alfs[
                "discharge_type"
            ].str.split(" - ", expand=True)

            alfs["discharge_facility2"] = pd.np.nan

        alfs["discharge_type"] = pd.np.where(
            alfs["discharge_facility"].isin(["Respite", "Permanent", "Skilled"]),
            alfs["discharge_type"] + " " + alfs["discharge_facility"],
            alfs["discharge_type"],
        )

        alfs["discharge_facility"] = pd.np.where(
            alfs["discharge_facility"].isin(["Respite", "Permanent", "Skilled"]),
            alfs["discharge_facility2"],
            alfs["discharge_facility"],
        )

        alfs.drop(["discharge_facility2"], axis=1, inplace=True)

    alfs.drop(drop_cols, axis=1, inplace=True)
    alfs = alfs[alfs.member_id != 1003]
    alfs.to_csv(f"{processed_data}\\alfs.csv", index=False)


if __name__ == "__main__":
    process_alfs()
