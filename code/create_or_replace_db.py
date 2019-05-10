from db_utils import create_or_replace_db

if __name__ == "__main__":
    create_or_replace_db()
    # parser = argparse.ArgumentParser()

    # parser.add_argument("db_name", type=str, help="name of database (include .db)")

    # arguments = parser.parse_args()

    # update_db(**vars(arguments))
