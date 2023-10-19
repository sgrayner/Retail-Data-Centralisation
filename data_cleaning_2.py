import numpy as np
import pandas as pd
from datetime import datetime as dt

from data_extraction import DataExtractor as de
from database_utils import DatabaseConnector as dc


class DataCleaning:

    def clean_user_data(self):
        '''
        This function extracts, cleans and uploads user data from the RDS database
        to an SQL database.
        '''
        extr = de()
        conn = dc()
        df = extr.read_rds_table(conn, conn.list_db_tables()[1])
        df = df.replace({'NULL': np.nan})
        df.dropna(how='all', subset=['email_address', 'address', 'phone_number'], inplace=True) # Drop people that we cannot contact
        df = df[~df['email_address'].str.contains('@') == False]
        df['country_code'] = df['country_code'].replace({'GGB':'GB'})
        df.reset_index(drop=True, inplace=True)
        df.loc[:, 'phone_number'].replace(to_replace=['\(', '\)', '\s', '/', '\.', '\-'], value=['', '', '', '', '', ''], regex=True, inplace=True)
        GB_numbers = df[df['country_code'] == 'GB'].loc[:, 'phone_number']
        GB_numbers = GB_numbers.str.removeprefix('+440')
        GB_numbers = GB_numbers.str.removeprefix('+44')
        GB_numbers = GB_numbers.str.removeprefix('0')
        GB_numbers = '+44(0)' + GB_numbers.astype(str)
        df.update(GB_numbers)
        DE_numbers = df[df['country_code'] == 'DE'].loc[:, 'phone_number']
        DE_numbers = DE_numbers.str.removeprefix('+490')
        DE_numbers = DE_numbers.str.removeprefix('0')
        DE_numbers = '+49(0)' + DE_numbers.astype(str)
        df.update(DE_numbers)
        US_numbers = df[df['country_code'] == 'US'].loc[:, 'phone_number']
        US_numbers = US_numbers.str.removeprefix('001')
        US_numbers = US_numbers.str.removeprefix('+1')
        US_numbers = US_numbers.str[:3] + '-' + US_numbers.str[3:6] + '-' + US_numbers.str[6:]
        US_numbers = '+1-' + US_numbers.astype(str)
        df.update(US_numbers)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth']).dt.date
        df['join_date'] = pd.to_datetime(df['join_date']).dt.date
    
        conn.upload_to_db(df, 'dim_users', 'sql_creds.yaml')


    def clean_card_data(self):
        '''
        This function extracts, cleans and uploads payment card data from the RDS database
        to an SQL database.
        '''
        extr = de()
        conn = dc()
        df = extr.retrieve_card_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')[0]
        df = df[df['expiry_date'].str.match(r'(0[1-9]|1[012])/\d{2}') == True]
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed']).dt.date
        df = df[df['card_number'].str.contains(r'\?') == False]
        df.reset_index(drop=True, inplace=True)
        
        conn.upload_to_db(df, 'dim_card_details', 'sql_creds.yaml')

    def clean_store_data(self):
        '''
        This function extracts, cleans and uploads store data from the RDS database
        to an SQL database.
        '''
        extr = de()
        conn = dc()
        df = extr.retrieve_store_data()
        df['continent'] = df['continent'].replace({'eeEurope':'Europe'})
        df['continent'] = df['continent'].replace({'eeAmerica':'America'})
        df = df[df['continent'].isin(['Europe', 'America'])]
        df['staff_numbers'] = df['staff_numbers'].str.replace('\D', '', regex=True)
        df.drop(['index', 'lat'], axis=1, inplace=True)                            
        df.reset_index(drop=True, inplace=True)
        df['opening_date'] = pd.to_datetime(df['opening_date']).dt.date
        conn.upload_to_db(df, 'dim_store_details', 'sql_creds.yaml')

    def convert_product_weights():
        '''
        This function extracts and cleans the 'weight' column of product data.

        Returns:
            DataFrame: The product data with the cleaned 'weight' column.
        '''
        extr = de()
        df = extr.retrieve_product_data()
        df['weight_check'] = df['weight'].str.contains('x')
        dg = df['weight'][df['weight_check'] == True].str.replace('[xg]', '', regex=True).str.split(expand=True)
        dg['weight'] = dg[0].astype(int) * dg[1].astype(int)
        dg['weight'] = dg['weight'].astype(str) + 'g'
        df.update(dg)
        df = df.drop(['weight_check'], axis=1)
        grams = df[df.loc[:, 'weight'].str.contains('([^k][g])|[ml]') == True]
        grams.loc[:, 'weight'] = grams.loc[:, 'weight'].str.removesuffix('g')
        grams.loc[:, 'weight'] = grams.loc[:, 'weight'].str.removesuffix('g .')
        grams.loc[:, 'weight'] = grams.loc[:, 'weight'].str.removesuffix('ml')
        grams.loc[:, 'weight'] = pd.to_numeric(grams.loc[:, 'weight']) / 1000
        df.update(grams)
        ounces = df[df.loc[:, 'weight'].str.contains('oz') == True]
        ounces.loc[:, 'weight'] = ounces.loc[:, 'weight'].str.removesuffix('oz')
        ounces.loc[:, 'weight'] = round(ounces.loc[:, 'weight'].astype(float) / 35.274, 2)
        df.update(ounces)
        kg = df[df.loc[:, 'weight'].str.contains('kg') == True]
        kg.loc[:, 'weight'] = kg.loc[:, 'weight'].str.removesuffix('kg')
        df.update(kg)
        df.rename(columns={'weight': 'weight_kg'}, inplace=True)
        
        return df

    def clean_product_data(self):
        '''
        This function cleans and uploads product data from the RDS database
        to an SQL database.
        '''
        conn = dc()
        df = DataCleaning.convert_product_weights()
        df = df.drop('Unnamed: 0', axis=1)
        df = df[df['removed'].isin(['Still_avaliable', 'Removed']) == True]
        df.loc[:, 'product_price'] = df.loc[:, 'product_price'].str.replace('£', '')
        df.rename(columns={'product_price': 'product_price_£'}, inplace=True)
        df['date_added'] = pd.to_datetime(df['date_added']).dt.date
        df['product_price_£'] = df['product_price_£'].astype(float)
        df['weight_kg'] = df['weight_kg'].astype(float)
        df.reset_index(drop=True, inplace=True)
        print(df['weight_kg'])
        conn.upload_to_db(df, 'dim_products', 'sql_creds.yaml')


    def clean_orders_data(self):
        '''
        This function extracts, cleans and uploads orders data from the RDS database
        to an SQL database.
        '''
        extr = de()
        conn = dc()
        df = extr.read_rds_table(conn, conn.list_db_tables()[2])
        df = df.drop(['level_0', 'first_name', 'last_name', '1'], axis=1)

        conn.upload_to_db(df, 'orders_table', 'sql_creds.yaml')


    def clean_events_data(self):
        '''
        This function extracts, cleans and uploads events data from the RDS database
        to an SQL database.
        '''
        extr = de()
        conn = dc()
        df = extr.retrieve_events_data()
        df = df[df['year'].str.match(r'\d{4}') == True]
        df.reset_index(drop=True, inplace=True)

        conn.upload_to_db(df, 'dim_date_times', 'sql_creds.yaml')
cleaner = DataCleaning()

#cleaner.clean_user_data()
#cleaner.clean_card_data()
#cleaner.clean_store_data()
#cleaner.clean_product_data()
#cleaner.clean_orders_data()
#cleaner.clean_events_data()























#dg = df['date_of_birth'].str.match(r'\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])')
#dg = df['email_address'].str.match('/([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$/')
#dg = df['email_address'].str.contains('@')