import psycopg2
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

def main():
    # Usage
    connection, cursor = connect_to_db()

    if connection:
        # Your database operations here
        
        # Usage
        tables_data = parse_input_file('tc1.txt')
        for table in tables_data:
            print(table)

        
        # Don't forget to close the connection when you're done
        cursor.close()
        connection.close()
        print("Database connection closed.")
 
if __name__ == "__main__":
    main()