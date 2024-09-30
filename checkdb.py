import psycopg2
import sys
from psycopg2 import OperationalError

def connect_to_db():
    db_params = {
        "host": "localhost",
        "dbname": "hw1",
        "password": "3380tranmart",
        "user": "postgres",
        "port": 5432
    }

    try:
        print("Connecting to database...")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print("Connected successfully!")
        return conn, cursor
    except OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return None, None

def drop_all_tables(cursor):
    try:
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            print(f"Dropped table {table_name}")
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

                table_dict = {
                    'table_name': table_name.strip(),
                    'primary_key': None,
                    'foreign_keys': [],
                    'columns': []
                }

                columns = [col.strip() for col in schema.split(',')]
                for column in columns:
                    column_lower = column.lower()
                    if '(pk)' in column_lower:
                        table_dict['primary_key'] = column_lower.replace('(pk)', '').strip()
                    elif '(fk:' in column_lower:
                        foreign_key = column.split('(fk:')[1].strip().rstrip(')')
                        table_dict['foreign_keys'].append((column.split('(')[0].strip(), foreign_key))
                    else:
                        table_dict['columns'].append(column.strip())
                tables.append(table_dict)
    return tables

def check_bcnf(cursor, table):
    """Check if a table is in BCNF."""
    primary_key = table['primary_key']
    table_name = table['table_name']

    # Get all columns except the primary key
    columns = [col for col in table['columns'] if col != primary_key]

    # Check if there are any functional dependencies where the determinant is not a superkey
    for column in columns:
        # Check if any column functionally determines the primary key or another column
        query = f"""
            SELECT DISTINCT {column}, COUNT(DISTINCT {primary_key})
            FROM {table_name}
            GROUP BY {column}
            HAVING COUNT(DISTINCT {primary_key}) = 1;
        """
        cursor.execute(query)
        result = cursor.fetchall()

        if result:
            # If there are results, it means this column can determine another column, violating BCNF
            print(f"Table {table_name} violates BCNF. Column {column} functionally determines the primary key.")
            return False

    return True

def generate_create_table_sql(tables):
    create_table_queries = []
    add_foreign_key_queries = []

    for table in tables:
        table_name = table['table_name']
        primary_key = table['primary_key']
        foreign_keys = table['foreign_keys']
        columns = table['columns']

        if primary_key not in columns:
            columns.insert(0, primary_key)
        for fk_column, _ in foreign_keys:
            if fk_column not in columns:
                columns.insert(0, fk_column)

        sql_query = f"CREATE TABLE {table_name} (\n"
        for column in columns:
            sql_query += f"    {column} VARCHAR(255),\n"
        sql_query += f"    PRIMARY KEY ({primary_key})\n"
        sql_query += ");"
        create_table_queries.append(sql_query)

        for fk_column, referenced in foreign_keys:
            referenced_table, referenced_column = referenced.split('.')
            fk_query = f"ALTER TABLE {table_name} ADD CONSTRAINT fk_{fk_column}_{referenced_table} " \
                       f"FOREIGN KEY ({fk_column}) REFERENCES {referenced_table}({referenced_column});"
            add_foreign_key_queries.append(fk_query)

    return create_table_queries, add_foreign_key_queries

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 checkdb.py database=your_file.txt")
        return

    arg = sys.argv[1]
    if arg.startswith("database="):
        input_file = arg.split("=", 1)[1].strip()
        print(f"Input file: '{input_file}'")
    else:
        print("Invalid argument format. Expected format: database=your_file.txt")
        return

    connection, cursor = connect_to_db()

    if connection:
        try:
            drop_all_tables(cursor)
            connection.commit()
            print("All tables dropped successfully.")

            tables_data = parse_input_file(input_file)
            print(f"Processing file: {input_file}")

            create_table_queries, add_foreign_key_queries = generate_create_table_sql(tables_data)

            integrity_results = {}
            normalization_results = {}

            for table, query in zip(tables_data, create_table_queries):
                table_name = table['table_name']
                print(f"Executing table creation query for {table_name}: \n{query}")
                try:
                    cursor.execute(query)
                    integrity_results[table_name] = 'Y'
                    # Check BCNF after table creation
                    if check_bcnf(cursor, table):
                        normalization_results[table_name] = 'Y'
                    else:
                        normalization_results[table_name] = 'N'
                except Exception as e:
                    print(f"Error executing query for {table_name}: {e}")
                    integrity_results[table_name] = 'N'
                    normalization_results[table_name] = 'N'

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

            print("-----------------------------------------")
            print(f"{'Table':<20} {'Referential Integrity':<20} {'Normalized'}")
            for table_name in integrity_results.keys():
                print(f"{table_name:<20} {integrity_results[table_name]:<20} {normalization_results[table_name]}")
            print("-----------------------------------------")

            db_integrity = 'Y' if all(result == 'Y' for result in integrity_results.values()) else 'N'
            db_normalized = 'Y' if all(result == 'Y' for result in normalization_results.values()) else 'N'
            print(f"DB Referential Integrity: {db_integrity}")
            print(f"DB Normalized: {db_normalized}")
            print("-----------------------------------------")

        except Exception as e:
            print(f"An error occurred: {e}")
            connection.rollback()

        finally:
            cursor.close()
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
