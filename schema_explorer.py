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
                                 r'Pwd=keu4mjap12@;'
                                 r'Encrypt=yes;'
                                 r'TrustServerCertificate=no;'
                                 r'Connection Timeout=30;')
#params = urllib.parse.quote_plus("'DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password")

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}") 

#with engine.connect() as connection:
#    sql = text("select * from dbo.DimAccount")
#    result = connection.execute(sql)
#    
#for row in result:
#    print(row)

metadata = MetaData()

metadata.reflect(bind=engine, schema='dbo')

for schema in metadata._schemas:
    print(f"Schema: {schema}")
    for table_name in metadata.tables:
        print(f"Table: {table_name}")
