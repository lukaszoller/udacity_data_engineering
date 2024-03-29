import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries


def drop_tables(cur, conn):
    """
    Runs the drop table queries on redshift db.
    Params:
        cur: db cursor
        conn: db connection
    """
    print("drop tables started")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("drop tables ended")


def create_tables(cur, conn):
    """
    Runs the create table queries on redshift db.
    Params:
        cur: db cursor
        conn: db connection
    """
    print("create tables started")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("create tables ended")


def load_staging_tables(cur, conn):
    """
    Runs the queries which copy from s3 to redshift.
    Params:
        cur: db cursor
        conn: db connection
    """
    print("load staging tables started")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("load staging tables ended")


def insert_tables(cur, conn):
    """
    Runs the queries, which translate from staging to final tables on redshift.
    Params:
        cur: db cursor
        conn: db connection
    """
    print("insert into tables started")
    for query in insert_table_queries:
        print(f'Query: {query}')
        cur.execute(query)
        conn.commit()
    print("insert into tables ended")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # drop_tables(cur, conn)
    # create_tables(cur, conn)
    # load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
