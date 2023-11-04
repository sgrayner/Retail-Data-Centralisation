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
        '''
        This function reads data from an RDS database.
        
        Args:
            table: Name of table to extract data from.
            
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        table = pd.read_sql_table(table, dc.init_db_engine('db_creds.yaml'), index_col='index')
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

    def __list_number_of_stores():
        '''
        This function retrieves information from an API on the number of stores.
            
        Returns:
            dict: the number of stores.
        '''
        endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        response = requests.get(endpoint, headers=header)
        return response.text

    def retrieve_store_data():
        '''
        This function retrieves data from an RDS database.
        
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        data_list = []
        for i in range(451):
            endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}'
            response = requests.get(endpoint, headers=header)
            data = response.json()
            data_list.append(data)
        df = pd.DataFrame(data_list)
        return df

    def retrieve_product_data():
        '''
        This function retrieves product data from an S3 bucket.
        
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        path = 's3://data-handling-public/products.csv'
        df = pd.read_csv(path)
        return df
    
    def retrieve_events_data():
        '''
        This function retrieves event data from an S3 bucket.
        
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        df = pd.read_json(path)
        return df
