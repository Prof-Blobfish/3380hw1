import psycopg2
from psycopg2 import OperationalError
import sys
import os
import re

def connect_to_db():
    db_params = {
        "host": "127.0.0.1",
        "dbname": "cosc3380",
        "user": "dbs22",
        "port": "5432",
        "password": "3380tranmart" 
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
        print(f"File exists : {file_path}")
    else:
        print(f"File does not exist : {file_path}")
        
def get_filename_without_extension(filepath):
    base_name = os.path.basename(filepath)
    return os.path.splitext(base_name)[0]

def drop_all_tables(cursor):
    drop_queries = []
    try:
        # Find all tables in the 'public' schema
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            drop_query = (f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            drop_queries.append(drop_query)
            cursor.execute(drop_query)
            print(f"Dropped table {table_name}")

        return drop_queries

    except Exception as e:
        print(f"Error dropping tables: {e}")

def parse_input_file(file_path):
    tables = []
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip().rstrip(';')
            if line:
                table_name, _, schema = line.partition('(')
                schema = schema[:-1] 

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

def generate_create_table_sql(tables):
    create_table_queries = []
    add_foreign_key_queries = []

    for table in tables:
        table_name = table['table_name'] 
        primary_key = table['primary_key']
        foreign_keys = table['foreign_keys']
        columns = table['columns']

        # Add primary and foreign key columns to the columns list
        if primary_key not in columns:
            columns.insert(0, primary_key)
        for fk_column, _ in foreign_keys:
            if fk_column not in columns:
                columns.insert(0, fk_column)

        sql_query = f"CREATE TABLE {table_name} (\n"
        
        for column in columns:
            sql_query += f"    {column} int,\n"
        
        sql_query += f"    PRIMARY KEY ({primary_key})\n"

        sql_query += ");"
        
        # Add the generated SQL query for table creation
        create_table_queries.append(sql_query)

        for fk_column, referenced in foreign_keys:
            referenced_table, referenced_column = referenced.split('.')
            fk_query = f"ALTER TABLE {table_name} ADD CONSTRAINT fk_{fk_column}_{referenced_table} " \
                       f"FOREIGN KEY ({fk_column}) REFERENCES {referenced_table}({referenced_column});"
            add_foreign_key_queries.append(fk_query)

    return create_table_queries, add_foreign_key_queries

def check_3nf_bcnf(tables):
    normalization_results = {}

    for table in tables:
        primary_key = table['primary_key']
        foreign_keys = table['foreign_keys']
        columns = table['columns']

        bcnf_violated = False
        for fk_column, referenced in foreign_keys:
            if fk_column != primary_key and fk_column in columns:
                bcnf_violated = True
                break

        if not bcnf_violated:
            normalization_results[table['table_name']] = 'Y' 
        else:
            normalization_results[table['table_name']] = 'N'

    return normalization_results


def main():
    # Check for arguments
    if len(sys.argv) < 2:
        print("Usage: python3 script_name.py database=filename.txt")
        sys.exit(1)

    for arg in sys.argv[1:]:
        if arg.startswith("database="):
            input_file = arg.split("=")[1]
            print(f"Database file: {input_file}")
            check_file_exists(input_file)

    results_file = "output_" + get_filename_without_extension(input_file) + ".txt"
    sql_file = get_filename_without_extension(input_file) + ".sql"
    print("Results: " + results_file)
    print("SQL Queries: " + sql_file)
    
    connection, cursor = connect_to_db()

    if connection:
        try:

            drop_queries = drop_all_tables(cursor)
            connection.commit()
            print("All tables dropped successfully.")

            tables_data = parse_input_file(input_file)
            print(f"Processing file: {input_file}")

            create_table_queries, add_foreign_key_queries = generate_create_table_sql(tables_data)

            with open(sql_file, 'w') as file:
                for dq in drop_queries:
                    file.write(dq + "\n")
                file.write('\n')
                for q in create_table_queries:
                    file.write(q + "\n\n")

            print(f"SQL queries successfully written to {sql_file}")

            integrity_results = {}
            normalization_results = {}

            for table, query in zip(tables_data, create_table_queries):
                table_name = table['table_name']
                print(f"Executing table creation query for {table_name}: \n{query}")
                try:
                    cursor.execute(query)
                    integrity_results[table_name] = 'Y'
                    normalization_results[table_name] = 'Y'
                except Exception as e:
                    print(f"Error executing query for {table_name}: {e}")
                    integrity_results[table_name] = 'N'
                    normalization_results[table_name] = 'Y'

            # Commit the table creation changes
            connection.commit()

            for query in add_foreign_key_queries:
                print(f"Executing foreign key constraint query: \n{query}")
                try:
                    cursor.execute(query)
                except Exception as e:
                    print(f"Error executing foreign key constraint query: {query}")
                    print(f"Error: {e}")

            connection.commit()
            print(f"Tables and foreign keys from {input_file} created successfully!\n")

            normalization_results = check_3nf_bcnf(tables_data)

            with open(results_file, 'w') as file:
                file.write("-----------------------------------------\n")
                file.write(f"{'Table':<10} {'Referential Integrity':<25} {'Normalized'}\n")
                for table_name in integrity_results.keys():
                    file.write(f"{table_name:<20} {integrity_results[table_name]:<20} {normalization_results[table_name]}\n")
                file.write("-----------------------------------------\n")
                
                # Check integrity and normalization
                db_integrity = 'Y' if all(result == 'Y' for result in integrity_results.values()) else 'N'
                db_normalized = 'Y' if all(result == 'Y' for result in normalization_results.values()) else 'N'
                file.write(f"DB Referential Integrity: {db_integrity}\n")
                file.write(f"DB Normalized: {db_normalized}\n")
                file.write("-----------------------------------------\n")

            print(f"Results successfully written to {results_file}")

        except Exception as e:
            print(f"An error occurred: {e}")
            connection.rollback()

        finally:
            # Close connection
            cursor.close()
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()