import tabula
import requests
import json
import boto3
import pandas as pd
from sqlalchemy import text
from database_utils import DatabaseConnector as dc

class DataExtractor:
    '''
    This class is used to extract data from various sources.
    '''
    def read_rds_table(self, conn, table):
        '''
        This function reads data from an RDS database.
        
        Args:
            conn: Instance of a connection to an RDS database.
            table: Name of table to extract data from.
            
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        table = pd.read_sql_table(table, conn.init_db_engine('db_creds.yaml'), index_col='index')
        
        return table

    def retrieve_card_data(self, link):
        '''
        This function reads data from a PDF file.
        
        Args:
            link: The URL to the PDF document.
            
        Returns:
            DataFrame: the extracted data as a DataFrame.
        '''
        df = tabula.read_pdf(link, output_format='dataframe', pages='all')

        return df

    def list_number_of_stores(self):
        '''
        This function retrieves information from an API on the number of stores.
            
        Returns:
            dict: the number of stores.
        '''
        endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        response = requests.get(endpoint, headers=header)
        return response.text

    def retrieve_store_data(self):
        '''
        This function reads data from an RDS database.
        
        Args:
            conn: Instance of a connection to an RDS database.
            table: Name of table to extract data from.
            
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

    def retrieve_product_data(self):
        client = boto3.client('s3')
        path = 's3://data-handling-public/products.csv'
        df = pd.read_csv(path)
        return df
    
    def retrieve_events_data():
        client = boto3.client('s3')
        path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        df = pd.read_json(path)
        return df


extr = DataExtractor()

print(extr.list_number_of_stores())

