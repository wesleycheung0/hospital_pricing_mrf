import pandas as pd
from sqlalchemy import create_engine

def write_dataframe_to_sqlite(df, table_name, db_file):
    """
    Write a DataFrame to a SQLite table, appending to it if it already exists.

    Parameters:
    df (pandas.DataFrame): The DataFrame to write to the SQLite table.
    table_name (str): The name of the SQLite table.
    db_file (str): The path to the SQLite database file.
    """
    # Create a SQLAlchemy engine
    engine = create_engine(f'sqlite:///{db_file}')

    # Write the DataFrame to the SQLite table, appending if it exists
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

    print(f"DataFrame is written to SQLite table '{table_name}' successfully.")
