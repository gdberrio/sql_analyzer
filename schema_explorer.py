from typing import Dict, List
from sqlalchemy import Engine, MetaData, ResultProxy, Table, create_engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from os import getenv
import urllib

load_dotenv()

server = getenv("SERVER_DB")
database = getenv("DATABASE")
username = getenv("USER_DB")
password = getenv("PASSWORD_DB")


def get_engine() -> Engine:
    """
    Create and return a SQLAlchemy engine for a SQL Server database.

    Returns:
        Engine: SQLAlchemy engine instance connected to the SQL Server database.
    """

    params = urllib.parse.quote_plus(
        r"Driver={ODBC Driver 18 for SQL Server};"
        r"Server=tcp:contoso-tests.database.windows.net,1433;"
        r"Database=Contoso-DW;"
        r"Uid=admin-gdb;"
        rf"Pwd={password};"
        r"Encrypt=yes;"
        r"TrustServerCertificate=no;"
        r"Connection Timeout=30;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine


def get_metadata(engine: Engine, schema: str) -> MetaData:
    """
    Reflect a database schema using a given engine and return a MetaData instance.

    Args:
        engine (Engine): SQLAlchemy engine instance connected to the SQL Server database.
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
        engine (Engine): SQLAlchemy engine instance connected to the SQL Server database.
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
        engine (Engine): SQLAlchemy engine instance connected to the SQL Server database.
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
        foreign_keys = [key.name for column in table_obj.columns for key in column.foreign_keys]
        table_dict["Primary_key"] = primary_key #type: ignore
        table_dict["Foreign_keys"] = foreign_keys #type: ignore
        schema_dict[table.name] = table_dict
    return schema_dict