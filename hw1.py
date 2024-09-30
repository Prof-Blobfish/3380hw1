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
    sql_queries = []

    for table in tables:
        table_name = table['table_name']  # Extract table name
        primary_key = table['primary_key']  # Extract primary key
        foreign_keys = table['foreign_keys']  # Extract foreign keys
        columns = table['columns']  # Extract columns

        # Start creating the SQL statement
        sql_query = f"CREATE TABLE {table_name} (\n"
        
        # Add columns
        for column in columns:
            # Assuming all columns are VARCHAR for simplicity. Adjust data types as needed.
            sql_query += f"    {column} VARCHAR(255),\n"
        
        # Add the primary key
        sql_query += f"    PRIMARY KEY ({primary_key}),\n"
        
        # Add foreign keys
        for fk_column, referenced in foreign_keys:
            referenced_table, referenced_column = referenced.split('.')
            sql_query += f"    FOREIGN KEY ({fk_column}) REFERENCES {referenced_table}({referenced_column}),\n"
        
        # Remove the last comma and add closing parenthesis
        sql_query = sql_query.rstrip(",\n") + "\n);"
        
        # Add the generated SQL query to the list
        sql_queries.append(sql_query)

    return sql_queries

def main():
    # Usage
    connection, cursor = connect_to_db()

    if connection:
        # Your database operations here
        
        # Usage
        files = ['tc1.txt', 'tc2.txt', 'tc3.txt', 'tc4.txt', 'tc5.txt', 'tc6.txt', 'tc7.txt', 'tc8.txt', 'tc9.txt']
        for file in files:
            tables_data = parse_input_file(file)
            print(file)
            for table in tables_data:
                print(table)
            # Generate SQL queries
            sql_queries = generate_create_table_sql(tables_data)
            
            # Output the SQL queries
            for query in sql_queries:
                print(query)
            print("\n\n")

        
        # Don't forget to close the connection when you're done
        cursor.close()
        connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()