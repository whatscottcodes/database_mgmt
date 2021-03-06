#!/usr/bin/env python3

import pandas as pd
from process_db_data.process_vaccinations import process_vaccinations
from file_paths import raw_data


def process_pneumococcal():
    """
    Cleans/Processes dataset
      
    Indicated columns are dropped
    Column names are cleaned
    Dose status is made binary
        Administered: 1, Not Administered: 0
        Contra is added to column as 99
    Training member is dropped

    Returns:
        DataFrame: cleaned dataframe
    
    Outputs:
        csv: processed data file
    """
    pneumo = pd.read_csv(f"{raw_data}\\pneumo.csv")
    contra = pd.read_csv(f"{raw_data}\\pneumo_contra.csv")
    return process_vaccinations(pneumo, contra, "pneumo")


if __name__ == "__main__":
    process_pneumococcal()
