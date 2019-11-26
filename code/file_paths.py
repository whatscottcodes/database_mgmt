###This file contains variables that are used to pull in the correct file path and names
###Indicated here, so only one file needs to be changed to get all functions running correctly

# Database Info
databases_folder = "V:\\Databases"
database_path = f"{databases_folder}\\PaceDashboard.db"
report_db = f"{databases_folder}\\reporting.db"
agg_db_path = f"{databases_folder}\\agg.db"
update_log = f"{databases_folder}\\update_log.txt"

# data locations
ehr_file_location = "C:\\Users\\snelson\\data\\ehr_for_db"
daily_census_data = "C:\\Users\\snelson\\repos\\day_center_attendance\\daily_census_data\\all_census.xlsx"
icd_10_file = "C:\\Users\\snelson\\data\\icd10.csv"
# path for this folder
db_mgmt_path = "C:\\Users\\snelson\\repos\\db_mgmt"

# db_mgmt output for data and logs
raw_data = f"{db_mgmt_path}\\data_raw"
processed_data = f"{db_mgmt_path}\\data_processed"
archive_data = f"{db_mgmt_path}\\data_archive"
output_folder = f"{db_mgmt_path}\\output"
update_logs_folder = f"{db_mgmt_path}\\logs"
luigi_log = f"{output_folder}\\luigi_log.txt"

# address Info - could be moved into these folders
statewide_geocoding = "C:\\Users\\snelson\\data\\statewide.csv"
non_geopy_addresses = "C:\\Users\\snelson\\data\\addresses_to_parse\\tough_adds.csv"
