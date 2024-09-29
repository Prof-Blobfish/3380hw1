#
 # @Author: Lei Si
 # @Date: 2024-09-02 09:41:07
 # @LastEditTime: 2024-09-26 10:36:10
 # @LastEditors: Lei Si
 # @Description: 
 # @FilePath: \undefinedc\python_connect_postgres.py
 # @YooooHooooo
 #
 
import psycopg2
from psycopg2 import OperationalError
import sys
import os


# Define our connection parameters
db_params = {
    "host": "127.0.0.1",
    "dbname": "cosc3380",
    "user": "dbsxx",
    "port": "5432",
    "password": "xxxx"
}

def check_file_exists(file_path):
    # Method 1: Using os.path.exists()
    if os.path.exists(file_path):
        print(f"File exists : {file_path}")
    else:
        print(f"File does not exist : {file_path}")
        
def get_filename_without_extension(filepath):
    # Get the base name (filename with extension, without path)
    base_name = os.path.basename(filepath)
    # Split the base name and return just the filename without extension
    return os.path.splitext(base_name)[0]

def connect_to_db():
    try:
        # Attempt to establish a connection
        print("Connecting to database...")
        conn = psycopg2.connect(**db_params)

        # Create a cursor
        cursor = conn.cursor()

        print("Connected successfully!")

        return conn, cursor

    except OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return None, None

def run_sql(cursor, sql_query=""):
    print(f"Query : {sql_query}")
    cursor.execute(sql_query)
    records = cursor.fetchall()
    for record in records :
        print ( record )

def set_search_path(cursor, sql_query=""):
    print(f"Query : {sql_query}")
    cursor.execute(sql_query)
    # Verify the search_path
    cursor.execute("SHOW search_path;")
    records = cursor.fetchall()
    for record in records :
        print ( record )
        
def main():
    # Check if there are any command-line arguments
    if len(sys.argv) < 2:
        print("Usage: python3 script_name.py database=filename.txt")
        sys.exit(1)

    # Process command-line arguments
    for arg in sys.argv[1:]:
        if arg.startswith("database="):
            database_file = arg.split("=")[1]
            # print(f"Database file: {database_file}")
            check_file_exists(database_file)
            
    database_name = get_filename_without_extension(database_file)
    
    # read file
    # tables = read_file(database_file)
    
    tables = ["T1()", 
              "T2()",
              "T3()"]
            
    # Usage
    connection, cursor = connect_to_db()

    if connection:
        # Your database operations here
        user_name = db_params["user"]
        
        # 1. set search path
        query = f"SET search_path TO HW1, examples, public, {user_name}"
        set_search_path(cursor, query)
        
        # save query to .sql file
        # save_query(query)
        
        # 2. other path
        for t in tables:
            table_name = t.split("(")[0]
            query = f"Select * from HW1.{database_name}_{table_name}"
            run_sql(cursor, sql_query=query)
        
        # Don't forget to close the connection when you're done
        cursor.close()
        connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()