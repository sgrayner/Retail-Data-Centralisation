import re
import pandas as pd
import numpy as np
from data_extraction import DataExtractor as de
from database_utils import DatabaseConnector as dc
from datetime import datetime as dt

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
                df['date_of_birth'][i] = dt.strptime(df['date_of_birth'][i], '%B %Y %d').date()
            except ValueError:
                try:
                    df['date_of_birth'][i] = dt.strptime(df['date_of_birth'][i], '%Y/%m/%d').date()
                except ValueError:
                    try:
                        df['date_of_birth'][i] = dt.strptime(df['date_of_birth'][i], '%Y %B %d').date()
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

        conn.upload_to_db(df, 'dim_users', 'sql_creds.yaml')
















#dg = df['date_of_birth'].str.match(r'\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])')
#dg = df['email_address'].str.match('/([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$/')
#dg = df['email_address'].str.contains('@')