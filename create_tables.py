import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    # Connect to the default database
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="postgres",       # Or any existing database name
        user="postgres",
        password="******"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Drop and create the sparkifydb database (not postgres)
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        print("Existing database 'sparkifydb' dropped (if it existed).")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
        print("Database 'sparkifydb' created successfully.")
    except Exception as e:
        print("Error during database creation:")
        print(e)

    # Close connection to the default database
    conn.close()

    # Connect to the new sparkifydb database
    conn = psycopg2.connect(
        host="127.0.0.1",
        dbname="sparkifydb",  # Use the new database name
        user="postgres",
        password="******"
    )
    cur = conn.cursor()

    return cur, conn

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables dropped (if they existed).")

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables created successfully.")

def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
