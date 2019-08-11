"""
Script that loads s3 data to the staging tables.

It also extracts and loads the relevant data from staging /
to the fact and dimension tables.
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load the data from the s3 bucket to the staging tables.

    Parameters
    ----------
    cur: class
        This executes the sql commands within a database session.
    conn: class
        This takes care of the connection to a database instance.

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from the staging tables to the analytics tables on redshift.

    Parameters
    ----------
    cur: class
        This executes the sql commands within a database session.
    conn: class
        This takes care of the connection to a database instance.

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Perform the following roles.

    1.) Read the config file and retrieve the configs
    necessary to create a db connection.
    2.) Create a connection to the database.
    3.) Execute the load methods to insert data to the
    staging, fact and dimension tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    print('Data has been loaded from s3 successfully!')
    insert_tables(cur, conn)
    print('Success! Data has been inserted into the tables')

    conn.close()


if __name__ == "__main__":
    main()
