import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect


class DatabaseConnector:
    '''
    This class is used to connect to an RDS database and upload to a local SQL database.
    '''
    def __init__(self, creds):
        self.creds = creds
    
    def read_db_creds(self):
        '''
        This function is used to read and return credentials of a database.

        Returns:
            data (DataFrame): The data from the database.
        '''
        with open(self.creds, 'r') as file:
            data = yaml.safe_load(file)
        return data

    def init_db_engine(self):
        '''
        This function initiates an SQLalchemy database engine.
            
        Returns:
            engine: An SQLalchemy engine.
        '''
        data = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data['HOST']
        USER = data['USER']
        PASSWORD = data['PASSWORD']
        DATABASE = data['DATABASE']
        PORT = data['PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def list_db_tables(self):
        '''
        This function lists the tables in the database.
        
        Returns:
            List of table names.
        '''
        inspector = inspect(self.init_db_engine('db_creds.yaml'))
        table_list = inspector.get_table_names()
        return table_list

    def upload_to_db(self, dataframe: pd.DataFrame, table: str):
        '''
        This function uploads data to a table in the SQL database.
        
        Args:
            dataframe (DataFrame): The data to upload.
            table (SQL table): The name of the table to store the data.
        '''
        dataframe.to_sql(table, self.init_db_engine(), if_exists='replace')

print(DatabaseConnector('config.yaml').read_db_creds()['STORES_ENDPOINT'])