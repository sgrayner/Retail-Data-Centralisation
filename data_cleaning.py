import numpy as np
import pandas as pd

import data_transforms as dt
from data_extraction import DataExtractor as de
from database_utils import DatabaseConnector as dc


class DataCleaning:
    '''
    This class is used to extract, clean and then upload data to an SQL database.
    '''

    @staticmethod
    def clean_user_data():
        '''
        This function extracts, cleans and uploads user data from the RDS database
        to an SQL database.
        '''
        df = de.read_rds_table(dc.list_db_tables()[1])
        df = df.replace({'NULL': np.nan})
        df.dropna(how='all', subset=['email_address', 'address', 'phone_number'], inplace=True) # Drop people that we cannot contact
        df = df[~df['email_address'].str.contains('@') == False]
        df['country_code'] = df['country_code'].replace({'GGB':'GB'})
        df.reset_index(drop=True, inplace=True)
        df = dt.clean_phone_numbers(df)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth']).dt.date
        df['join_date'] = pd.to_datetime(df['join_date']).dt.date
        dc.upload_to_db(df, 'dim_users', 'sql_creds.yaml')

    @staticmethod
    def clean_card_data():
        '''
        This function extracts, cleans and uploads payment card data from the RDS database
        to an SQL database.
        '''
        data = dc('config.yaml').read_db_creds()
        df = de.retrieve_card_data(data['CARD_DATA'])
        # This regex expression matches every expiry date of the format mm/yy
        df = df[df['expiry_date'].str.match(r'(0[1-9]|1[012])/\d{2}') == True]
        df['card_number'] = df['card_number'].astype(str).str.replace('?', '', regex=False)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed']).dt.date
        df['card_number'] = df['card_number'].astype(np.int64)
        df.reset_index(drop=True, inplace=True)
        dc.upload_to_db(df, 'dim_card_details', 'sql_creds.yaml')

    @staticmethod
    def clean_store_data():
        '''
        This function extracts, cleans and uploads store data from the RDS database
        to an SQL database.
        '''
        df = de.retrieve_store_data()
        df['continent'] = df['continent'].replace({'eeEurope': 'Europe'})
        df['continent'] = df['continent'].replace({'eeAmerica': 'America'})
        df = df[df['continent'].isin(['Europe', 'America'])]
        df['staff_numbers'] = df['staff_numbers'].str.replace('\D', '', regex=True)
        df.drop(['index', 'lat'], axis=1, inplace=True)                            
        df.reset_index(drop=True, inplace=True)
        df['opening_date'] = pd.to_datetime(df['opening_date']).dt.date
        dc.upload_to_db(df, 'dim_store_details', 'sql_creds.yaml')
    
    @staticmethod
    def clean_product_data():
        '''
        This function cleans and uploads product data from the RDS database
        to an SQL database.
        '''
        df = de.retrieve_product_data()
        df = dt.convert_product_weights(df, 'weight')
        df = df.drop('Unnamed: 0', axis=1)
        df = df[df['removed'].isin(['Still_avaliable', 'Removed']) == True]
        df.loc[:, 'product_price'] = df.loc[:, 'product_price'].str.replace('£', '')
        df.rename(columns={'product_price': 'product_price_£'}, inplace=True)
        df['date_added'] = pd.to_datetime(df['date_added']).dt.date
        df['product_price_£'] = df['product_price_£'].astype(float)
        df['weight_kg'] = df['weight_kg'].astype(float)
        df.reset_index(drop=True, inplace=True)
        dc.upload_to_db(df, 'dim_products', 'sql_creds.yaml')

    @staticmethod
    def clean_orders_data():
        '''
        This function extracts, cleans and uploads orders data from the RDS database
        to an SQL database.
        '''
        df = de.read_rds_table(dc.list_db_tables()[2])
        df = df.drop(['level_0', 'first_name', 'last_name', '1'], axis=1)
        dc.upload_to_db(df, 'orders_table', 'sql_creds.yaml')

    @staticmethod
    def clean_events_data():
        '''
        This function extracts, cleans and uploads events data from the RDS database
        to an SQL database.
        '''
        df = de.retrieve_events_data()
        df = df[df['year'].str.match(r'\d{4}') == True]
        df.reset_index(drop=True, inplace=True)
        dc.upload_to_db(df, 'dim_date_times', 'sql_creds.yaml')


if __name__ == '__main__':
    DataCleaning.clean_user_data()
    DataCleaning.clean_card_data()
    DataCleaning.clean_store_data()
    DataCleaning.clean_product_data()
    DataCleaning.clean_orders_data()
    DataCleaning.clean_events_data()
