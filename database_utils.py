import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            data = yaml.safe_load(file)

        return data

    def init_db_engine(self):
        data = self.read_db_creds()

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data['RDS_HOST']
        USER = data['RDS_USER']
        PASSWORD = data['RDS_PASSWORD']
        DATABASE = data['RDS_DATABASE']
        PORT = data['RDS_PORT']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine

    def list_db_tables(self):
        inspector = inspect(self.init_db_engine())
        table_list = inspector.get_table_names()
        
        return table_list

    def upload_to_db(self, dataframe, table):
        dataframe.to_sql(table, self.init_db_engine(), if_exists='replace')



