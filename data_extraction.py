import tabula
import requests
import pandas as pd
from sqlalchemy import text
from database_utils import DatabaseConnector as dc


class DataExtractor:
    '''
    This class is used to extract data from various sources.
    '''
    def read_rds_table(table):
        engine = dc('db_creds.yaml')
        '''
        This function reads data from an RDS database.
        
        Args:
            table: Name of table to extract data from.
            
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        table = pd.read_sql_table(table, engine.init_db_engine(), index_col='index')
        return table
    
    def retrieve_card_data(link):
        '''
        This function reads data from a PDF file.
        
        Args:
            link: The URL to the PDF document.
            
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        df = tabula.read_pdf(link, stream=False, pages='all')
        df = pd.concat(df)
        return df

    def list_number_of_stores():
        '''
        This function retrieves information from an API on the number of stores.
            
        Returns:
            dict: the number of stores.
        '''
        data = dc('config.yaml').read_db_creds()
        endpoint = data['NUMBER_STORES_ENDPOINT']
        header = {data['HEADER_KEY']: data['HEADER_VALUE']}
        response = requests.get(endpoint, headers=header)
        return response.json()['number_stores']

    def retrieve_store_data():
        '''
        This function retrieves data from an RDS database.
        
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        data = dc('config.yaml').read_db_creds()
        header = {data['HEADER_KEY']: data['HEADER_VALUE']}
        data_list = []
        for i in range(DataExtractor.list_number_of_stores()):
            endpoint = data['STORES_ENDPOINT'] + f'{i}'
            response = requests.get(endpoint, headers=header)
            response = response.json()
            data_list.append(response)
        df = pd.DataFrame(data_list)
        return df

    def retrieve_product_data():
        '''
        This function retrieves product data from an S3 bucket.
        
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        data = dc('config.yaml').read_db_creds()
        path = data['PRODUCTS_PATH']
        df = pd.read_csv(path)
        return df
    
    def retrieve_events_data():
        '''
        This function retrieves event data from an S3 bucket.
        
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        data = dc('config.yaml').read_db_creds()
        path = data['EVENTS_PATH']
        df = pd.read_json(path)
        return df

print(DataExtractor.retrieve_events_data())