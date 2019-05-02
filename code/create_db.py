import argparse
from db_utils import create_db

if __name__ == "__main__":
    create_db()
    # parser = argparse.ArgumentParser()

    # parser.set_defaults(method=create_or_update_table)

    # parser.add_argument("db_name", type=str, help="name of database (include .db)")

    # arguments = parser.parse_args()

    # create_db(**vars(arguments))

