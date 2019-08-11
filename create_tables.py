"""Create tables script."""

import configparser
import psycopg2
from cluster_setup import get_endpoint
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all the tables if they exist.

    Execute the drop queries defined in sql_queries.py.

    Parameters
    ----------
    cur: class
        This executes the sql commands within a database session.
    conn: class
        This takes care of the connection to a database instance.

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create the tables if they don't exist.

    Execute the create table queries in sql_queries.py.

    Parameters
    ----------
    cur: class
        This executes the sql commands within a database session.
    conn: class
        This takes care of the connection to a database instance.

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Perform the following roles.

        1.) Call the create cluster script.
        2.) Establish a db connection.
        3.) Execute the drop and create table methods.
    """
    get_endpoint()
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    print('Tables have been dropped successfully!')
    create_tables(cur, conn)
    print('Success! New tables have been created')

    conn.close()


if __name__ == "__main__":
    main()
