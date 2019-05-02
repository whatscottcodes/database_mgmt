import sqlite3
import pandas as pd
import argparse


def main():
    usernames = pd.read_csv("V:\\\Dashboard\\users\\usernames.csv")

    conn = sqlite3.connect("V:\\Databases\\users.db")
    c = conn.cursor()

    usernames.to_sql("temp", conn, if_exists="append", index=False)
    c.execute(
        f"""
            INSERT INTO usernames (username, first_name, last_name, email, has_password)
            SELECT username, first_name, last_name, email, has_password FROM temp t
            WHERE NOT EXISTS 
                (SELECT * from usernames f
                WHERE f.username = t.username);
            """
    )
    conn.commit()
    pd.read_sql("SELECT * FROM usernames", conn).to_csv(
        "V:\\\Dashboard\\users\\usernames.csv", index=False
    )
    conn.close()
    return "Users Updated"


if __name__ == "__main__":
    main()
