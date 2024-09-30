import psycopg2
import re
from psycopg2 import OperationalError

def connect_to_db():
    # Define our connection parameters
    # db_params = {
    #     "host": "bds01.cs.uh.edu",
    #     "dbname": "cosc3380",
    #     "user": "dbs22",
    #     "port": "5432",
    #     "password": "3380tranmart"  # It's better to use environment variables for passwords
    # }

    db_params = {
        "host": "localhost",
        "dbname": "hw1",
        "user": "postgres",
        "port": 5432
    }

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
    
def parse_input_file(file_path):
    tables = []
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
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
                    if '(pk)' in column:
                        table_dict['primary_key'] = column.replace('(pk)', '').strip()
                    elif '(fk:' in column:
                        # Extract the foreign key
                        foreign_key = column.split('(fk:')[1].strip().rstrip(')')
                        table_dict['foreign_keys'].append((column.split('(')[0].strip(), foreign_key))
                    else:
                        table_dict['columns'].append(column.strip())

                tables.append(table_dict)

    return tables

# Make a function to check for referential integrity (checks referred table if fk is that table's pk)


def generate_create_table_sql(tables):
    create_table_queries = []
    add_foreign_key_queries = []

    for table in tables:
        table_name = table['table_name']  # Extract table name
        primary_key = table['primary_key']  # Extract primary key
        foreign_keys = table['foreign_keys']  # Extract foreign keys
        columns = table['columns']  # Extract other columns

        # Add primary and foreign key columns to the columns list if not already present
        if primary_key not in columns:
            columns.insert(0, primary_key)
        for fk_column, _ in foreign_keys:
            if fk_column not in columns:
                columns.insert(0, fk_column)

        # Start creating the SQL statement without foreign keys
        sql_query = f"CREATE TABLE {table_name} (\n"
        
        # Add columns
        for column in columns:
            sql_query += f"    {column} VARCHAR(255),\n"
        
        # Add the primary key
        sql_query += f"    PRIMARY KEY ({primary_key})\n"

        # Remove the last comma and add closing parenthesis
        sql_query += ");"
        
        # Add the generated SQL query for table creation
        create_table_queries.append(sql_query)

        # Generate foreign key constraints separately
        for fk_column, referenced in foreign_keys:
            referenced_table, referenced_column = referenced.split('.')
            fk_query = f"ALTER TABLE {table_name} ADD CONSTRAINT fk_{fk_column}_{referenced_table} " \
                       f"FOREIGN KEY ({fk_column}) REFERENCES {referenced_table}({referenced_column});"
            add_foreign_key_queries.append(fk_query)

    return create_table_queries, add_foreign_key_queries



def main():
    # Usage
    connection, cursor = connect_to_db()

    if connection:
        try:
            # List of files to process
            files = ['tc1.txt']
            
            for file in files:
                # Parse the file to get table schema
                tables_data = parse_input_file(file)
                print(f"Processing file: {file}")

                # Generate SQL queries for creating tables and foreign keys
                create_table_queries, add_foreign_key_queries = generate_create_table_sql(tables_data)

                # Execute table creation queries first
                for query in create_table_queries:
                    print(f"Executing table creation query: \n{query}")
                    try:
                        cursor.execute(query)
                    except Exception as e:
                        print(f"Error executing query: {query}")
                        print(f"Error: {e}")

                # Commit the table creation changes
                connection.commit()

                # Now execute the foreign key constraints
                for query in add_foreign_key_queries:
                    print(f"Executing foreign key constraint query: \n{query}")
                    try:
                        cursor.execute(query)
                    except Exception as e:
                        print(f"Error executing query: {query}")
                        print(f"Error: {e}")

                # Commit the foreign key constraints
                connection.commit()
                print(f"Tables and foreign keys from {file} created successfully!\n")

        except Exception as e:
            print(f"An error occurred: {e}")
            connection.rollback()  # Rollback if something goes wrong

        finally:
            # Close the cursor and connection
            cursor.close()
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()