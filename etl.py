import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """_summary_
    Load The Data From File In S3 To Staging Tables 
    Args:
        cur (object): Object To Execute Query
        conn (object): Connect To Database Object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """_summary_
        Insert And Tranform Data From Staging Tables To the Dimensional Tables
    Args:
        cur (object): Object To Execute Query
        conn (object): Connect To Database Object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """_summary_
        Read Configration File 
        Setup DataBase
        Connect To DataBase
        Drop Table If Exists
        Create Table
    """
    print("Read Configration File....")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connect To DataBase....")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Load Data From S3 To Staging Tables....")
    load_staging_tables(cur, conn)
    print("Load Done ):")

    print("Insert Data To Dimensional Tables...")
    insert_tables(cur, conn)
    print("Insert Done ):")


    conn.close()
    print("ETL Finished And Close Connection ):")


if __name__ == "__main__":
    main()