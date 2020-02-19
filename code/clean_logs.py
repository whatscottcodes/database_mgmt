import os
import pandas as pd
import argparse

from file_paths import (
    update_log,
    processed_data,
    raw_data,
    ehr_file_location,
    update_logs_folder,
    luigi_log,
)


def clean_update_logs():
    """
    Deletes all empty log files created during SQL
    creation/updating files    
    """
    files = os.listdir(f"{update_logs_folder}")

    for filename in files:
        print(filename)
        update_line = filename.split(".")[0]
        update_line = ",".join(update_line.split("_"))
        with open(f"{update_log}", "a") as myfile:
            myfile.write(f"{update_line}\n")
        os.remove(f"{update_logs_folder}\\{filename}")


def remove_ehr_files():
    """
    Deletes all files in the ehr for db folder
    """
    files = os.listdir(f"{ehr_file_location}")
    for filename in files:
        os.remove(f"{ehr_file_location}\\{filename}")


def clean_raw():
    """
    Deletes all files in the raw data folder
    """
    files = os.listdir(raw_data)

    for filename in files:
        os.remove(f"{raw_data}\\{filename}")


def clean_processed():
    """
    Deletes all files in the processed data folder
    """
    files = os.listdir(processed_data)

    for filename in files:
        os.remove(f"{processed_data}\\{filename}")


def clean_files(override=False):
    """
    Deletes all files in the logs, raw data, and processed data folder
    if the Luigi log indicates that the pipeline ran
    correctly on today's date.
    """
    override = override is not False

    date_string = f"Date: {str(pd.to_datetime('today').date())}True"

    file_handle = open(luigi_log, "r")
    line_list = [line.strip("\n") for line in file_handle.readlines()]
    file_handle.close()

    if (date_string == line_list[0]) | override:
        clean_update_logs()
        remove_ehr_files()
        clean_raw()
        clean_processed()

        print("Success")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--override",
        default=False,
        help="True to override database completion for clean up",
    )

    arguments = parser.parse_args()
    clean_files(**vars(arguments))
