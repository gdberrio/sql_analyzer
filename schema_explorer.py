from typing import Dict, List
from sqlalchemy import Engine, MetaData, ResultProxy, create_engine, inspect
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateTable
from dotenv import load_dotenv
from os import getenv
from dataclasses import dataclass
from enum import Enum
import logging
import csv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

server = getenv("SERVER_DB")
database = getenv("DATABASE")
port = getenv("PORT_DB")
username = getenv("USER_DB")
password = getenv("PASSWORD_DB")

class Database(str, Enum):
    PostgreSQL = 'postgresql'

@dataclass
class DBConfig:
    db_type: Database
    server: str
    database: str
    port: str
    username: str
    password: str 

def get_engine(DB: DBConfig) -> Engine:
    """
    Create and return a SQLAlchemy engine for a PostgreSQL database.

    Returns:
        Engine: SQLAlchemy engine instance connected to the PostgreSQL database.
    """

    # TODO: generalize this to allow for selection of DB, and correct selection of Driver and connection string
    if DB is None:
        DB = DBConfig(db_type=Database.PostgreSQL, server=server, database=database, port=port, username=username, password=password) # type: ignore

    connection_url = f"{DB.db_type.value}://{DB.username}:{DB.password}@{DB.server}:{DB.port}/{DB.database}"
    engine = create_engine(connection_url)
    logging.info('Engine created: %s', repr(engine.url))
    return engine


def get_metadata(engine: Engine, schema: str) -> MetaData:
    """
    Reflect a database schema using a given engine and return a MetaData instance.

    Args:
        engine (Engine): SQLAlchemy engine instance connected to the PostgreSQL database.
        schema (str): The name of the schema to reflect.

    Returns:
        MetaData: MetaData instance containing information about the schema.
    """

    metadata = MetaData()

    metadata.reflect(bind=engine, schema=schema)
    logging.info('Metadata object created: %s', metadata)
    return metadata


def get_samples(engine: Engine, metadata: MetaData) -> Dict[str, List[ResultProxy]]:
    """
    Retrieve a sample of data (10 rows) from each table in the database.

    Args:
        engine (Engine): SQLAlchemy engine instance connected to the PostgreSQL database.
        metadata (MetaData): MetaData instance containing information about the schema.

    Returns:
        Dict[str, List[ResultProxy]]: Dictionary mapping table names to lists of result proxies
        containing the sampled data.
    """

    samples = dict()
    for table in metadata.tables.values():
        session = Session(engine)
        results = session.query(table).limit(10).all()
        samples[table.name] = results
        session.close()
    return samples


def store_schema_details(engine: Engine, metadata: MetaData):  
    """
    Retrieve metadata about each table in the database, including column names and types.

    Args:
        engine (Engine): SQLAlchemy engine instance connected to the PostgreSQL database.
        metadata (MetaData): MetaData instance containing information about the schema.

    Returns:
        Dict[str, Dict[str, str]]: Dictionary mapping table names to dictionaries of column names
        and types.
    """

    with open('data/table_definitions.sql', 'w') as sql_file:
        for table in metadata.tables.values():
            sql_file.write(str(CreateTable(table).compile(engine)))
            sql_file.write(';\n')

        logging.info('Saved metadata into file %s', sql_file)

    samples = get_samples(engine=engine, metadata=metadata)
    for table_name, results in samples.items():
        if results:  # check if there are results for the table
            # Open the CSV file in write mode
            with open(f'data/{table_name}.csv', 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)

                # Write the header
                inspector = inspect(engine)
                columns = inspector.get_columns(table_name)
                writer.writerow([column['name'] for column in columns])

                # Iterate over the query results and write each row to the CSV
                for row in results:
                    writer.writerow([getattr(row, column['name']) for column in columns])
            logging.info('Saved %s table sample to %s', table_name, csv_file)

if __name__ == "__main__":
    DB = DBConfig(db_type=Database.PostgreSQL, server=server, database=database, port=port, username=username, password=password) # type: ignore
    engine = get_engine(DB=DB)
    metadata = get_metadata(engine=engine, schema='public')
    store_schema_details(engine=engine, metadata=metadata)
