import psycopg2
from psycopg2 import OperationalError
import sys
import os
import re

#def connect_to_db():
#    db_params = {
#        "host": "127.0.0.1",
#        "dbname": "cosc3380",
#        "user": "dbs22",
#        "port": "5432",
#        "password": "3380tranmart" 
#   }

def connect_to_db():
    db_params = {
        "host": "localhost",
        "dbname": "hw1",
        "password": "3380tranmart",
        "user": "postgres",
        "port": 5432
    }

    try:
        # Establish a connection
        print("Connecting to database...")
        conn = psycopg2.connect(**db_params)
        
        cursor = conn.cursor()
        
        print("Connected successfully!")
        return conn, cursor
    
    except OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return None, None

def check_file_exists(file_path):
    if os.path.exists(file_path):
        print(f"File exists: {file_path}")
        return True
    else:
        print(f"File does not exist: {file_path}")
        return False
    
def get_filename_without_extension(filepath):
    base_name = os.path.basename(filepath)
    return os.path.splitext(base_name)[0]

#Use inputSQL to create tables
def execute_sql_file(cursor, conn, file_path):
    try:
        # Set search path to the desired schema
        print("Search path set to hw1")
        
        # Read and execute SQL script
        with open(file_path, 'r') as file:
            sql = file.read()
        
        cursor.execute(sql)
        print("SQL script executed successfully!")
    
    except Exception as e:
        print(f"Error executing SQL file: {e}")



#Use inputTXT to assign keys to tables
# def assign_keys():
#     stuff here

# #Check for referential integrity
# def RI_check():
#     stuff here

# #Check for BCNF
# def BCNF_check():
#     stuff here


def main():
    # Check for arguments
    if len(sys.argv) < 2:
        print("Usage: python3 script_name.py database=filename.txt")
        sys.exit(1)

    for arg in sys.argv[1:]:
        if arg.startswith("database="):
            input_file = arg.split("=")[1]
            print(f"Database file: {input_file}")
            
            # Check if the file exists
            if not check_file_exists(input_file):
                print(f"Error: The file '{input_file}' does not exist.")
                sys.exit(1)

            conn, cursor = connect_to_db()
            if conn is None:
                print("Exiting due to database connection failure.")
                sys.exit(1)

            # Set the search path to hw1
            cursor.execute("SET search_path TO public;")

            # New stuff
            base_name = get_filename_without_extension(input_file) 
            execute_sql_file(cursor, conn, base_name + ".sql")
            conn.commit()  # Commit the transaction

            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()