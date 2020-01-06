import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''This function takes a db-cursor and db-connection as inputs and executes a query to drop tables
    as defined in sql_queries.py'''
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def create_tables(cur, conn):
    '''This function takes a db-cursor and db-connection as inputs and executes a query to create tables
    as defined in sql_queries.py'''
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)
    


def main():
    '''This function is the main function to create tables, and calls helper functions to drop and create tables.
    It reads configurationf file to get all parameters to connect to db, and create db-connection and db-cursor.'''
    try:
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
        print('Successful Completion of table creation jobs.')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()