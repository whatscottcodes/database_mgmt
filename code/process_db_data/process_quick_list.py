#!/usr/bin/env python3

import re
import numpy as np
import pandas as pd
import sqlite3
from file_paths import raw_data, processed_data, database_path


def process_quick_list(update=True):
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Ppts ID is pulled from name column
    Team name is pulled from center day column
    
    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """

    def get_id(name_string):
        mem_id = re.findall(r"\(\d*\)", name_string)
        try:
            mem_id = mem_id[0].replace("(", "").replace(")", "")
        except IndexError:
            mem_id = np.nan
        return mem_id

    quick_list = pd.read_csv(f"{raw_data}\\ppt_quick_list.csv")
    quick_list["member_id"] = quick_list["ParticipantName"].apply(get_id)
    quick_list["team"] = (
        quick_list["CenterDays"]
        .str.split(":", expand=True)[1]
        .str.split(expand=True)[0]
    )
    quick_list["team"].replace(
        {"North(Providence)": "North", "North(Woonsocket)": "North"}, inplace=True
    )
    quick_list.dropna(subset=["member_id"], inplace=True)
    team_df = quick_list[["member_id", "team"]].copy()
    team_df["member_id"] = team_df["member_id"].astype(int)

    if update:
        conn = sqlite3.connect(database_path)
        team_df.to_sql("team_temp", conn, index=False, if_exists="replace")

        c = conn.cursor()

        drop_member_ids = [
            tup[0]
            for tup in c.execute(
                """SELECT team_temp.member_id FROM team_temp
        INNER JOIN teams ON team_temp.member_id=teams.member_id
        WHERE team_temp.team = teams.team
        AND end_date IS NULL"""
            ).fetchall()
        ]
        team_df = team_df[-team_df["member_id"].isin(pd.Series(drop_member_ids))].copy()

        c.execute(f"DROP TABLE IF EXISTS team_temp")

        conn.close()
        team_df["start_date"] = pd.to_datetime("today").date()
        team_df["end_date"] = pd.np.nan

    else:
        enrollment = pd.read_csv(f"{processed_data}/enrollment.csv")
        team_df = team_df.merge(enrollment, on="member_id")
        team_df.rename(columns={"enrollment_date": "start_date"}, inplace=True)
        team_df = team_df[["member_id", "team", "start_date"]].copy()
        team_df["end_date"] = pd.np.nan

    team_df.to_csv(f"{processed_data}\\teams.csv", index=False)

    return team_df


if __name__ == "__main__":
    process_quick_list()
