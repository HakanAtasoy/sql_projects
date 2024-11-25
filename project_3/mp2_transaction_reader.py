import psycopg2
from config import read_config


def read_plans():

    db_conn_params = read_config(filename="database.cfg", section="postgresql")
    conn = psycopg2.connect(**db_conn_params)
    cur = conn.cursor()

    try:
        isolation_levels = [
            psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
            psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
        ]

        for level in isolation_levels:
            conn.set_isolation_level(level)
            print(f"\nIsolation Level: {level}")

            print("Reading plans before writer commits...")
            cur.execute("SELECT * FROM plans")
            rows = cur.fetchall()
            for row in rows:
                print(row)

            input("Hit enter to read after commit  ")

            print("Reading plans after writer commits...")
            cur.execute("SELECT * FROM plans")
            rows = cur.fetchall()
            for row in rows:
                print(row)

            input("Hit enter to continue to next isolation level")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    read_plans()
