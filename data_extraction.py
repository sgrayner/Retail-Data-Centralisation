import tabula
import requests
import json
import boto3
import pandas as pd
from sqlalchemy import text
from database_utils import DatabaseConnector as dc

class DataExtractor:

    def read_db_data(self, table):
        with dc.init_db_engine().connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table}"))
            for row in result:
                print(row)

    def read_rds_table(self, conn, table):
        table = pd.read_sql_table(table, conn.init_db_engine('db_creds.yaml'), index_col='index')
        
        return table

    def retrieve_pdf_data(self, link):
        df = tabula.read_pdf(link, output_format='dataframe', pages='all')

        return df

    def list_number_of_stores(self):
        endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        response = requests.get(endpoint, headers=header)
        return response.text

    def retrieve_store_data(self):
        header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        data_list = []
        for i in range(451):
            endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}'
            response = requests.get(endpoint, headers=header)
            data = response.json()
            data_list.append(data)
        df = pd.DataFrame(data_list)
        return df

    def extract_from_s3(self):
        client = boto3.client('s3')
        path = 's3://data-handling-public/products.csv'
        df = pd.read_csv(path)
        return df
    
    def retrieve_events_data():
        client = boto3.client('s3')
        path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        df = pd.read_json(path)
        return df




