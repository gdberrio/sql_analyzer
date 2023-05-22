from typing import Dict, List
from sqlalchemy import Engine, MetaData, ResultProxy, create_engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from os import getenv
import urllib

load_dotenv()

server = getenv('SERVER_DB')
database = getenv('DATABASE')
username = getenv('USER_DB')
password = getenv('PASSWORD_DB')

def get_engine() -> Engine:
    params = urllib.parse.quote_plus(r'Driver={ODBC Driver 18 for SQL Server};'
                                     r'Server=tcp:contoso-tests.database.windows.net,1433;'
                                     r'Database=Contoso-DW;'
                                     r'Uid=admin-gdb;'
                                     rf'Pwd={password};'
                                     r'Encrypt=yes;'
                                     r'TrustServerCertificate=no;'
                                     r'Connection Timeout=30;')

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}") 
    return engine

def get_metadata(engine: Engine, schema: str) -> MetaData:

    metadata = MetaData()

    metadata.reflect(bind=engine, schema=schema)
    return metadata

def get_samples(engine: Engine, metadata: MetaData) -> Dict[str, List[ResultProxy]]:
    samples = dict()
    for table in metadata.tables.values():
        session = Session(engine)
        print(f"getting sample for table {table.name}")
        results = session.query(table).limit(10).all()
        samples[table.name] = results
        session.close()
    return samples