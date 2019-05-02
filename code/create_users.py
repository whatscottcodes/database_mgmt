import sqlite3
import pandas as pd
import argparse


def main(db_name, username_csv_path):
    usernames = pd.read_csv(username_csv_path)
    # production: f"data\\{db_name}"
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    q = """CREATE TABLE user (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password NOT NULL
            );
            """

    c.execute(q)
    conn.commit()

    q = """CREATE TABLE usernames (
            username TEXT NOT NULL UNIQUE PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            has_password INT NOT NULL
            );
        """
    c.execute(q)

    usernames.to_sql("usernames", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("db_name", type=str, help="name of database (include .db)")
    parser.add_argument(
        "username_csv_path",
        type=str,
        help="path to username csv file (include filename and .csv)",
    )

    arguments = parser.parse_args()
    main(**vars(arguments))
