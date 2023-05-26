from typing import Dict, List
from sqlalchemy import Engine, MetaData, ResultProxy, Table, create_engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from os import getenv
from dataclasses import dataclass
from enum import Enum

load_dotenv()

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

psql_db_config = DBConfig(db_type=Database.PostgreSQL, server=server, database=database, port=port, username=username, password=password)

def get_engine(DB: DBConfig = psql_db_config) -> Engine:
    """
    Create and return a SQLAlchemy engine for a PostgreSQL database.

    Returns:
        Engine: SQLAlchemy engine instance connected to the PostgreSQL database.
    """

    # TODO: generalize this to allow for selection of DB, and correct selection of Driver and connection string
    connection_url = f"{DB.db_type.value}://{DB.username}:{DB.password}@{DB.server}:{DB.port}/{DB.database}"

    engine = create_engine(connection_url)
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
        print(f"getting sample for table {table.name}")
        results = session.query(table).limit(10).all()
        samples[table.name] = results
        session.close()
    return samples


def get_table_metadata(engine: Engine, metadata: MetaData) -> Dict[str, Dict[str, str]]:
    """
    Retrieve metadata about each table in the database, including column names and types.

    Args:
        engine (Engine): SQLAlchemy engine instance connected to the PostgreSQL database.
        metadata (MetaData): MetaData instance containing information about the schema.

    Returns:
        Dict[str, Dict[str, str]]: Dictionary mapping table names to dictionaries of column names
        and types.
    """

    schema_dict = dict()
    for table in metadata.tables.values():
        full_table_name = f"{table.schema}.{table.name}"
        table_obj = Table(full_table_name, metadata, autoload_with=engine)
        table_dict = dict()
        table_dict = {column.name: str(column.type) for column in table_obj.columns}
        primary_key = table_obj.primary_key.columns.keys()
        foreign_keys = [
            key.name for column in table_obj.columns for key in column.foreign_keys
        ]
        table_dict["Primary_key"] = primary_key  # type: ignore
        table_dict["Foreign_keys"] = foreign_keys  # type: ignore
        schema_dict[table.name] = table_dict
    return schema_dict
