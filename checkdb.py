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
        # Read and execute SQL script
        with open(file_path, 'r') as file:
            sql = file.read()
        
        cursor.execute(sql)
        print("SQL script executed successfully!")
    
    except Exception as e:
        print(f"Error executing SQL file: {e}")



#Use inputTXT to assign keys to tables
def parse_input_file(file_path):
    tables = []
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip().rstrip(';')  # Remove trailing semicolon
            if line:
                # Partition the line at the first occurrence of '('
                table_name, _, schema = line.partition('(')
                schema = schema[:-1]  # Remove the closing parenthesis

                # Initialize the dictionary for the table
                table_dict = {
                    'table_name': table_name.strip(),
                    'primary_key': None,
                    'foreign_keys': [],
                    'columns': []
                }

                # Split the schema into columns
                columns = [col.strip() for col in schema.split(',')]
                for column in columns:
                    column_lower = column.lower()
                    if '(pk)' in column_lower:
                        table_dict['primary_key'] = column_lower.replace('(pk)', '').strip()
                    elif '(fk:' in column_lower:
                        # Extract the foreign key
                        foreign_key = column.split('(fk:')[1].strip().rstrip(')')
                        table_dict['foreign_keys'].append((column.split('(')[0].strip(), foreign_key))
                    else:
                        table_dict['columns'].append(column.strip())

                tables.append(table_dict)

    return tables

def generate_keys_sql(tables, base_name):
    add_primary_key_queries = []
    add_foreign_key_queries = []

    for table in tables:
        table_name = table['table_name']  # Extract table name
        primary_key = table['primary_key']  # Extract primary key
        foreign_keys = table['foreign_keys']  # Extract foreign keys
        columns = table['columns']  # Extract other columns

        # Add primary key constraint if specified
        if primary_key:
            pk_query = f"ALTER TABLE {base_name}_{table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY ({primary_key});"
            add_primary_key_queries.append(pk_query)

        # Generate foreign key constraints separately
        for fk_column, referenced in foreign_keys:
            referenced_table, referenced_column = referenced.split('.')
            fk_query = f"ALTER TABLE {base_name}_{table_name} ADD CONSTRAINT fk_{fk_column}_{referenced_table} " \
                       f"FOREIGN KEY ({fk_column}) REFERENCES {base_name}_{referenced_table}({referenced_column});"
            add_foreign_key_queries.append(fk_query)

    # Combine the queries: first primary keys, then foreign keys
    return add_primary_key_queries, add_foreign_key_queries

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

            tables_data = parse_input_file(input_file)
            add_primary_key_queries, add_foreign_key_queries = generate_keys_sql(tables_data, base_name)

            # Execute primary key constraints first
            for query in add_primary_key_queries:
                print(f"Executing primary key constraint query: \n{query}")
                try:
                    cursor.execute(query)
                except Exception as e:
                    print(f"Error executing primary key constraint query: {query}")
                    print(f"Error: {e}")

            # Then execute foreign key constraints
            for query in add_foreign_key_queries:
                print(f"Executing foreign key constraint query: \n{query}")
                try:
                    cursor.execute(query)
                except Exception as e:
                    print(f"Error executing foreign key constraint query: {query}")
                    print(f"Error: {e}")

            conn.commit()  # Commit the transaction

            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()