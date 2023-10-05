import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
from sqlalchemy import text
import re
import numpy as np
from datetime import datetime as dt
from data_extraction import DataExtractor as de
from database_utils import DatabaseConnector as dc

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


class DataCleaning:

    def clean_user_data(self):
        extr = de()
        conn = dc()
        df = extr.read_rds_table(conn, conn.list_db_tables()[1])
        df = df.replace({'NULL': np.nan})
        df.dropna(how='all', subset=['email_address', 'address', 'phone_number'], inplace=True) # Drop people that we cannot contact
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

            if df['country_code'][i] == 'GB':
                df.loc[i, 'phone_number'] = df.loc[i, 'phone_number'].replace(' ', '')
                df.loc[i, 'phone_number'] = df.loc[i, 'phone_number'].replace('(', '')
                df.loc[i, 'phone_number'] = df.loc[i, 'phone_number'].replace(')', '')
                if df.loc[i, 'phone_number'][:4] == '+440':
                    df.loc[i, 'phone_number'] = '+44(0)' + df.loc[i, 'phone_number'][4:]
                elif df.loc[i, 'phone_number'][:3] == '+44':
                    df.loc[i, 'phone_number'] = '+44(0)' + df.loc[i, 'phone_number'][3:]
                else:
                    df.loc[i, 'phone_number'] = '+44(0)' + df.loc[i, 'phone_number'][1:]
            if df['country_code'][i] == 'DE':
                df.loc[i, 'phone_number'] = df.loc[i, 'phone_number'].replace(' ', '')
                df.loc[i, 'phone_number'] = df.loc[i, 'phone_number'].replace('(', '')
                df.loc[i, 'phone_number'] = df.loc[i, 'phone_number'].replace(')', '')
                if df.loc[i, 'phone_number'][:4] != '+490':
                    df.loc[i, 'phone_number'] = '+49(0)' + df.loc[i, 'phone_number'][1:]
                if df.loc[i, 'phone_number'][:4] == '+490':
                    df.loc[i, 'phone_number'] = '+49(0)' + df.loc[i, 'phone_number'][4:]
            if df['country_code'][i] == 'US':
                result = re.sub('001', '+1', df['phone_number'][i])
                result = re.sub('\.', '-', result)
                result = re.sub('\)', '-', result)
                result = re.sub('/', '-', result)
                result = re.sub('\(', '', result)
                if '-' not in result:
                    result = result[:3] + '-' + result[3:6] + '-' + result[6:]
                if result[0:2] != '+1':
                    result = '+1-' + result
        conn.upload_to_db(df, 'dim_users')

cleaner = DataCleaning()
cleaner.clean_user_data()



