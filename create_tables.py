import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """_summary_
        Drop Table From drop_table_queries List
        to Create Them From Scratch
    Args:
        cur (object): execute sql query
        conn (object): Connection To DataBase
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """_summary_
        Create Staging And Dimensional Tables From create_table_queries List
    Args:
        cur (object): execute sql query
        conn (object): Connection To DataBase
    """
    for query in create_table_queries:
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


    print("Drop Table If Exists....")
    drop_tables(cur, conn)
    
    print("Create  Create Staging And Dimensional Tables.... ")
    create_tables(cur, conn)

    conn.close()
    print("Done And Close  Connection ): ")


if __name__ == "__main__":
    main()