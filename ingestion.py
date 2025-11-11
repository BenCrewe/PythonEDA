import pyodbc
from sqlalchemy import create_engine

def Engine():
    server = 'localhost'
    database = 'library'
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = f'mssql+pyodbc://@{server}/{database}?trusted_connection=yes&Driver=ODBC+Driver+17+for+SQL+Server'

    engine = create_engine(connection_string)

    return engine

