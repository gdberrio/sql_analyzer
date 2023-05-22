from sqlalchemy import MetaData, create_engine, text
from dotenv import load_dotenv
from os import getenv
import urllib

load_dotenv()

server = getenv('SERVER_DB')
database = getenv('DATABASE')
username = getenv('USER_DB')
password = getenv('PASSWORD_DB')

print(server, database, username, password)
params = urllib.parse.quote_plus(r'Driver={ODBC Driver 18 for SQL Server};'
                                 r'Server=tcp:contoso-tests.database.windows.net,1433;'
                                 r'Database=Contoso-DW;'
                                 r'Uid=admin-gdb;'
                                 rf'Pwd={password};'
                                 r'Encrypt=yes;'
                                 r'TrustServerCertificate=no;'
                                 r'Connection Timeout=30;')

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}") 

metadata = MetaData()

metadata.reflect(bind=engine, schema='cso')

for schema in metadata._schemas:
    print(f"Schema: {schema}")
    for table_name in metadata.tables:
        print(f"Table: {table_name}")
        

print(metadata.tables['cso.DimAccount'])
