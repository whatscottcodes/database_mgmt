import pandas as pd
from file_paths import processed_data, raw_data


def process_appointments():
    """
    Cleans/Processes dataset
    
    Periods are stripped from the chief complaint column
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    appts = pd.read_csv(
        f"{raw_data}\\appts.csv", parse_dates=["appt_date", "create_date"]
    )
    appts.drop(["Unnamed: 8"], axis=1, inplace=True)
    appts["chief_complaint"] = appts["chief_complaint"].astype(str).str.strip(".")
    appts = appts[appts.member_id != 1003]
    appts.drop_duplicates(subset=["member_id", "type", "appt_date"], inplace=True)
    appts.to_csv(f"{processed_data}\\appts.csv", index=False)


if __name__ == "__main__":
    process_appointments()
