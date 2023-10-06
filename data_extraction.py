import tabula
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
        df = tabula.read_pdf(link)

        return df

extr = DataExtractor()





