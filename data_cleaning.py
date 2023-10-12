import re
import boto3
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
        df.reset_index(drop=True, inplace=True)
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


    def clean_card_data(self):
        extr = de()
        conn = dc()
        df = extr.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')[0]
        df.dropna(how='all', inplace=True)
        df.reset_index(drop=True, inplace=True)
        for i in range(len(df)):
            try:
                df.loc[i, 'date_payment_confirmed'] = dt.strptime(str(df.loc[i, 'date_payment_confirmed']), '%B %Y %d').date()
            except ValueError:
                try:
                    df.loc[i, 'date_payment_confirmed'] = dt.strptime(str(df.loc[i, 'date_payment_confirmed']), '%Y/%m/%d').date()
                except ValueError:
                    try:
                        df.loc[i, 'date_payment_confirmed'] = dt.strptime(str(df.loc[i, 'date_payment_confirmed']), '%Y %B %d').date()
                    except ValueError:
                        pass
        df = df[df['date_payment_confirmed'].str.match(r'\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])') == True]
        df = df[df['card_number'].str.match(r'(\d{9}|\d{11}|\d{12}|\d{13}|\d{14}|\d{15}|\d{16}|\d{19})') == True]
        df.reset_index(drop=True, inplace=True)
        
        conn.upload_to_db(df, 'dim_card_details', 'sql_creds.yaml')

    def clean_store_data(self):
        extr = de()
        conn = dc()
        df = extr.retrieve_store_data()
        df = df[df['continent'] != 'NULL']
        df['continent'] = df['continent'].replace({'eeEurope':'Europe'})
        df['continent'] = df['continent'].replace({'eeAmerica':'America'})
        df = df[df['continent'].isin(['Europe', 'America'])]
        df.drop(['index', 'lat'], axis=1, inplace=True)
        df.reset_index(drop=True, inplace=True)
        for i in range(len(df)):
                    try:
                        df.loc[i, 'opening_date'] = dt.strptime(str(df.loc[i, 'opening_date']), '%B %Y %d').date()
                    except ValueError:
                        try:
                            df.loc[i, 'opening_date'] = dt.strptime(str(df.loc[i, 'opening_date']), '%Y/%m/%d').date()
                        except ValueError:
                            try:
                                df.loc[i, 'opening_date'] = dt.strptime(str(df.loc[i, 'opening_date']), '%Y %B %d').date()
                            except ValueError:
                                pass
        conn.upload_to_db(df, 'dim_store_details', 'sql_creds.yaml')

    def convert_product_weights(self):
        extr = de()
        df = extr.extract_from_s3()
        df['weight_check'] = df['weight'].str.contains('x')
        dg = df['weight'][df['weight_check'] == True].str.replace('[xg]', '', regex=True).str.split(expand=True)
        dg['weight'] = dg[0].astype(int) * dg[1].astype(int)
        dg['weight'] = dg['weight'].astype(str) + 'g'
        dg = dg.drop([0, 1], axis=1)
        df.update(dg)
        df = df.drop(['weight_check'], axis=1)
        grams = df[df.loc[:, 'weight'].str.contains('([^k][g])|[ml]') == True]
        grams.loc[:, 'weight'] = grams.loc[:, 'weight'].str.removesuffix('g')
        grams.loc[:, 'weight'] = grams.loc[:, 'weight'].str.removesuffix('g .')
        grams.loc[:, 'weight'] = grams.loc[:, 'weight'].str.removesuffix('ml')
        grams.loc[:, 'weight'] = grams.loc[:, 'weight'].astype(float) / 1000
        df.update(grams)
        ounces = df[df.loc[:, 'weight'].str.contains('oz') == True]
        ounces.loc[:, 'weight'] = ounces.loc[:, 'weight'].str.removesuffix('oz')
        ounces.loc[:, 'weight'] = round(ounces.loc[:, 'weight'].astype(float) / 35.274, 2)
        df.update(ounces)
        kg = df[df.loc[:, 'weight'].str.contains('[k]') == True]
        kg.loc[:, 'weight'] = kg.loc[:, 'weight'].str.removesuffix('kg')
        df.update(kg)
        df.rename(columns={'weight': 'weight (kg)'}, inplace=True)
        
        return df

    def clean_product_data(self):
        conn = dc()
        df = self.convert_product_weights()
        df = df.drop('Unnamed: 0', axis=1)
        df.dropna(how='any', inplace=True)
        df = df[~(df['weight (kg)'].str.contains('[a-zA-Z]') == True)]
        df.loc[:, 'product_price'] = df.loc[:, 'product_price'].str.replace('£', '')
        df.rename(columns={'product_price': 'product_price (£)'}, inplace=True)
        df.loc[307, 'date_added'] = '2018-10-22'
        df.loc[1217, 'date_added'] = '2017-09-06'
        df['product_price (£)'] = df['product_price (£)'].astype(float)
        df['weight (kg)'] = df['weight (kg)'].astype(float)

        conn.upload_to_db(df, 'dim_products', 'sql_creds.yaml')


    def clean_orders_data(self):
        extr = de()
        conn = dc()
        df = extr.read_rds_table(conn, conn.list_db_tables()[2])

cleaner = DataCleaning()

df = cleaner.clean_card_data()























#dg = df['date_of_birth'].str.match(r'\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])')
#dg = df['email_address'].str.match('/([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$/')
#dg = df['email_address'].str.contains('@')