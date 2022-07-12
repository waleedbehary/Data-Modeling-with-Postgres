import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Description:
        This functions is accountable to do the following: 
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    
    Arguments:
        conn: connection to the databse
        cur: the cursor object
        
    Returns:
        None.
        Database will be created with name "sparkifydb"
        
    
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    """
    This part accountable to create the Database. It ensures to check if any database exist with the same name of the new Database. If exist, it will delete the old database to support creation of new one.
    Them it creates the database with UTF8 encoding
    
    Arguments:
        cur: the cursor object
    """
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    
    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Description: Creates each table using the queries in `create_table_queries` list.
        Arguments:
            cur: the cursor object
            conn: connection to the database
            
            Returns:
                NONE
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description:
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    
    Arguments:
        cur: The cursor object
        conn: the connection to the database
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
