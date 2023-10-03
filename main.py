import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
from sqlalchemy import text
import re
import numpy as np
from datetime import datetime as dt

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

class DataExtractor:

    def read_db_data(self, table):
        with DatabaseConnector.init_db_engine().connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table}"))
            for row in result:
                print(row)

    def read_rds_table(self, conn, table):
        table = pd.read_sql_table(table, conn.init_db_engine(), index_col='index')
        
        return table




extr = DataExtractor()
conn = DatabaseConnector()
df = extr.read_rds_table(conn, conn.list_db_tables()[1])

df = df.replace({'NULL': np.nan})
df.dropna(how='all', subset=['email_address', 'address', 'phone_number'], inplace=True)
df = df[~df['email_address'].str.contains('@') == False]
df['country_code'] = df['country_code'].replace({'GGB':'GB'})
df.reset_index(inplace=True)

for i in range(len(df)):
    try:
        df.loc[i, 'date_of_birth'] = dt.strptime(df.loc[i, 'date_of_birth'], '%B %Y %d').date()
    except ValueError:
        try:
            df.loc[i, 'date_of_birth'] = dt.strptime(df.loc[i, 'date_of_birth'], '%Y/%m/%d').date()
        except ValueError:
            try:
                df.loc[i, 'date_of_birth'] = dt.strptime(df.loc[i, 'date_of_birth'], '%Y %B %d').date()
            except ValueError:
                pass




for i in range(len(df)):
    if df['country_code'][i] == 'DE':
        df['phone_number'] = df['phone_number'].str.replace(' ', '', regex=False)
        df['phone_number'] = df['phone_number'].str.replace('(', '', regex=False)
        df['phone_number'] = df['phone_number'].str.replace(')', '', regex=False)
        if df.loc[i, 'phone_number'][:4] != '+490':
            df.loc[i, 'phone_number'] = '+49(0)' + df.loc[i, 'phone_number'][1:]
        if df.loc[i, 'phone_number'][:4] == '+490':
            df.loc[i, 'phone_number'] = '+49(0)' + df.loc[i, 'phone_number'][4:]


'''
lengths = set()


for i in range(len(df)):
    if df['country_code'][i] == 'DE':
        lengths.add(len(df['phone_number'][i]))
            
print(lengths)'''


print(df[df['country_code'] == 'DE']['phone_number'].head(30))