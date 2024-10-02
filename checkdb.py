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
        with open(file_path, 'r') as file:
            sql = file.read()

        cursor.execute(sql)
        conn.commit()  # commit after executing SQL script
        print(f"SQL script executed successfully for {file_path}!")
    except Exception as e:
        print(f"Error executing SQL file {file_path}: {e}")

def drop_all_tables(cursor):
    try:
        # Query to get all table names across all schemas
        cursor.execute("""
            DO $$ DECLARE
            r RECORD;
            BEGIN
                FOR r IN (SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')) LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        """)
        print("All tables dropped across all schemas.")
    except Exception as e:
        print(f"Error dropping tables: {e}")


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
    
def check_ref_int(cursor):
    integrity_results = {}
    tables_with_violations = []  # Store tables with referential integrity violations
    
    # Get the list of tables
    cursor.execute(""" 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'; 
    """)
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        is_referential_integrity = 'Y'

        # Get foreign keys for the table
        cursor.execute(f"""
            SELECT conname, confkey, conrelid::regclass::text AS table_name
            FROM pg_constraint 
            WHERE contype = 'f' AND conrelid = '{table_name}'::regclass;
        """)
        foreign_keys = cursor.fetchall()

        for fk in foreign_keys:
            fk_name, confkey, ref_table = fk
            # Check for the existence of the referenced keys
            ref_key_ids = [str(col) for col in confkey]
            for ref_key_id in ref_key_ids:
                # Get the referenced column's primary key
                cursor.execute(f"""
                    SELECT {ref_key_id} 
                    FROM {ref_table} 
                    WHERE {ref_key_id} IS NULL;
                """)
                if cursor.rowcount > 0:
                    is_referential_integrity = 'N'
                    print(f"Referential integrity violation in '{table_name}': "
                          f"Foreign key '{fk_name}' references missing values in '{ref_table}'.")
                    tables_with_violations.append(table_name)  # Store the table with violations
                    break  # Exit inner loop as we found a violation

        integrity_results[table_name] = is_referential_integrity

    return integrity_results, tables_with_violations


def generate_keys_sql(tables, base_name, cursor):
    add_primary_key_queries = []
    add_foreign_key_queries = []
    
    # Check referential integrity first
    integrity_results, tables_with_violations = check_ref_int(cursor)

    for table in tables:
        table_name = table['table_name']
        primary_key = table['primary_key']
        foreign_keys = table['foreign_keys']
        
        # If referential integrity is violated, skip key generation for this table
        for table_name, integrity in integrity_results.items():
            if integrity == 'N':
                print(f"Skipping key generation for '{table_name}' due to integrity violations.")
                continue  # Skip this table if it has violations

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

    return add_primary_key_queries, add_foreign_key_queries

def check_normalization(cursor):
    normalization_results = {}

    # Get the list of tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        is_normalized = 'Y'

        # Get the columns of the table
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """)
        columns = [col[0] for col in cursor.fetchall()]

        # Get primary keys
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_name = '{table_name}' AND constraint_name LIKE '%pkey%';
        """)
        primary_keys = [pk[0] for pk in cursor.fetchall()]

        if primary_keys:
            # Assuming there is only one primary key for simplicity
            primary_key = primary_keys[0]

            # Check for partial dependencies (to ensure 2NF)
            for non_key in columns:
                if non_key == primary_key:
                    continue

                # Count distinct values of the non-key column based on the primary key
                cursor.execute(f"""
                    SELECT {primary_key}, {non_key}
                    FROM {table_name}
                    GROUP BY {primary_key}, {non_key}
                    HAVING COUNT(*) > 1;
                """)

                if cursor.rowcount > 0:
                    # If there are multiple values for a non-key based on the primary key,
                    # it implies a partial dependency; thus it is not in 2NF.
                    is_normalized = 'N'
                    print(f"Table '{table_name}' is not in 2NF: partial dependency on '{non_key}'.")

            # Check for transitive dependencies (to ensure 3NF/BCNF)
            for non_key in columns:
                if non_key == primary_key:
                    continue

                # Check if there are dependencies on non-key attributes
                cursor.execute(f"""
                    SELECT {non_key}, COUNT(DISTINCT {primary_key})
                    FROM {table_name}
                    GROUP BY {non_key}
                    HAVING COUNT(DISTINCT {primary_key}) > 1;
                """)

                if cursor.rowcount > 0:
                    # If a non-key attribute can determine another non-key attribute,
                    # it indicates a transitive dependency.
                    is_normalized = 'N'
                    print(f"Table '{table_name}' is not in BCNF: '{non_key}' determines other attributes.")

        normalization_results[table_name] = is_normalized

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

            drop_all_tables(cursor)

            # New stuff
            base_name = get_filename_without_extension(input_file)
            execute_sql_file(cursor, conn, base_name + ".sql")

            tables_data = parse_input_file(input_file)
            add_primary_key_queries, add_foreign_key_queries = generate_keys_sql(tables_data, base_name, cursor)

            foreign_key_violations = {}  # Store foreign key violations here

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
                    # Store violation in the dictionary
                    table_name = query.split(" ")[-1]  # Extract table name from query (adjust as necessary)
                    foreign_key_violations[table_name] = foreign_key_violations.get(table_name, [])
                    foreign_key_violations[table_name].append(query)  # Store the violation details
                    print(f"Error executing foreign key constraint query: {query}")
                    print(f"Error: {e}")

            conn.commit()  # Commit the transaction

            # Check referential integrity
            integrity_results, tables_with_violations = check_ref_int(cursor)

            # Now integrity_results is a dictionary, and you can use it as before
            for table_name in integrity_results:
                if integrity_results[table_name] == 'N':
                    print(f"Table '{table_name}' has referential integrity violations.")
                    # Handle additional logic here if needed

            normalization_results = check_normalization(cursor)

            # Define the result file path
            results_file = "refintnorm-" + base_name + ".txt"

            # Write the results to the file
            with open(results_file, 'w') as file:
                file.write("-----------------------------------------\n")
                file.write(f"{'Table':<10} {'Referential Integrity':<25} {'Normalized'}\n")
                for table_name in integrity_results.keys():
                    file.write(f"{table_name:<20} {integrity_results[table_name]:<20} {normalization_results[table_name]}\n")
                file.write("-----------------------------------------\n")

                # Check overall integrity and normalization
                db_integrity = 'Y' if all(result == 'Y' for result in integrity_results.values()) else 'N'
                db_normalized = 'Y' if all(result == 'Y' for result in normalization_results.values()) else 'N'
                file.write(f"DB Referential Integrity: {db_integrity}\n")
                file.write(f"DB Normalized: {db_normalized}\n")
                file.write("-----------------------------------------\n")

                # Debug prints
                print("Integrity Results:", integrity_results)
                print("Normalization Results:", normalization_results)



            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()
