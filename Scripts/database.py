import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, DateTime
from dotenv import load_dotenv

load_dotenv()
connection_string = os.environ.get('ConnectionString')


def create_tables(csv_directory, engine):
    metadata = MetaData()  # Create a metadata object

    for csv_file in os.listdir(csv_directory):
        if csv_file.endswith('.csv'):
            table_name = os.path.splitext(csv_file)[0]

            # Read the entire CSV to leverage SQLAlchemy introspection
            df = pd.read_csv(os.path.join(csv_directory, csv_file))

            # Create an empty table using SQLAlchemy and associate it with the engine
            table = Table(table_name, metadata, extend_existing=True)

            # Use inferred data types from the dataframe
            for column in df.columns:
                data_type = infer_column_type(df[column])
                table.append_column(Column(column, data_type))

            # Create the table
            metadata.create_all(bind=engine)  # Associate the metadata with the engine
            print(f"Table '{table_name}' created successfully.")

            # Insert data into the table
            df.to_sql(table_name, con=engine, index=False, if_exists='replace')
            print(f"Data inserted into table '{table_name}'.")

def infer_column_type(column):
    # Infer data types based on the column values
    if pd.api.types.is_integer_dtype(column):
        return Integer()
    elif pd.api.types.is_float_dtype(column):
        return Float()
    elif pd.api.types.is_datetime64_any_dtype(column):
        return DateTime()
    else:
        # For any other types, use a string with a length of 255 (you can adjust this based on your needs)
        return String(255)

# Example usage:
csv_directory = './data'  # Update with the correct path to your directory
engine = create_engine(connection_string)

create_tables(csv_directory, engine)
