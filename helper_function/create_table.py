import sqlite3

def execute_schema_from_file(db_file, schema_file, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Drop the existing table
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Table {table_name} dropped successfully.")
    except sqlite3.Error as e:
        print(f"Error occurred while dropping the table: {e}")

    # Read schema from the file
    with open(schema_file, 'r') as file:
        schema_sql = file.read()

    # Execute the schema commands
    try:
        cursor.executescript(schema_sql)
        print("New schema executed successfully.")
    except sqlite3.Error as e:
        print(f"Error occurred while executing the new schema: {e}")

    # Commit changes and close the connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    # Pass the table name along with the database and schema files
    execute_schema_from_file("data/hospital_pricing.db", "data/schema/rate.sql", "rate")
