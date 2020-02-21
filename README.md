# PACE Database

Scripts for creating a SQLite database that combines financial and clinical data from the PaceLogic EHR and other sources.

## Requirements

All required packages are in the requirements.txt file. There is also an included environment.yml file for setting up a conda environment. Requires paceutils package to be installed in environment - use pip install e <local_path/to/pace_utils>.

### PaceUtils

Requires that the paceutils package to be installed. Can be found at http://github.com/whatscottcodes/paceutils.

## Use

Read the Database Guide file in the docs folder. Update file_path.py, run create_database.py with files in proper places.

## Future Consideration

As the amount of data captured continues to grow, this database will need to be moved out of SQLite. Change should not be too difficult to make, thinking about MariaDB or MySQL.
