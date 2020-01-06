import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time


def load_staging_tables(cur, conn):
    '''This function takes a db-cursor and db-connection as inputs and executes a query to copy data
    from S3-bucket into Redshift staging tables. Copy statement is defined in sql_queries.py'''
    try:
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()
        print('Staging tables load- succesfully completed!')
    except Exception as e:
        print(e)


def insert_tables(cur, conn):
    '''This function takes a db-cursor and db-connection as inputs and executes queries to load data into 
    AWS-Redshift Datawarehouse tables. SQL statements for INSERT operation are defined in sql_queries.py'''
    try:
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()
        print('Datwarehouse Fact and Dimension tables load- succesfully completed!')
    except Exception as e:
        print(e)


def main():
    '''This function is the main function for ETL, and calls helper functions to load staging and Datawarehouse tables.
    It reads configurationf file to get all parameters to connect to db, and create db-connection and db-cursor.'''
    try:
        start_time=time.time()
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        conn.close()
        print('Execution Time is:',(time.time()-start_time),' seconds')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()