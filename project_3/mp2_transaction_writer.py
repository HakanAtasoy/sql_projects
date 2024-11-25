import psycopg2
from config import read_config


def write_plan():

    db_conn_params = read_config(filename="database.cfg", section="postgresql")
    conn = psycopg2.connect(**db_conn_params)
    cur = conn.cursor()

    try:
        isolation_levels = [
            psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
            psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
        ]

        plan_id = 4  # Generate a unique integer for plan_id
        max_parallel_sessions = 8

        for level in isolation_levels:
            conn.set_isolation_level(level)
            print(f"\nIsolation Level: {level}")

            name = f"New Plan {plan_id}"

            print("Inserting a new plan...")
            cur.execute("INSERT INTO plans (plan_id, name, max_parallel_sessions) VALUES (%s, %s, %s)",
                        (plan_id, name, max_parallel_sessions))

            input("Hit enter to commit changes")

            conn.commit()
            print("Commit successful.")

            input("Hit enter to continue to next isolation level")

            plan_id += 1
            max_parallel_sessions += 2

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    write_plan()
