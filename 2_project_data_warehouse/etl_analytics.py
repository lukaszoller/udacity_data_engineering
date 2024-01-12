import configparser
import psycopg2
from sql_queries import analytic_queries
from tabulate import tabulate


def load_analytics_tables(cur, conn):
    """
    Execute a series of analytical queries, fetch the results, and print the formatted results.

    Parameters:
    - cur : db cursor
    - conn : db connection
    """
    for query in analytic_queries:
        cur.execute(query)
        rows = cur.fetchall()
        print(f'{tabulate(rows, headers=[desc[0] for desc in cur.description])}\n')

        # for row in rows:
        #     print(row)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_analytics_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
