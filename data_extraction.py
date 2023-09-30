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
        table = pd.read_sql_table(table, conn.init_db_engine(), index_col='index')
        
        return table

extr = DataExtractor()
conn = dc()

df = extr.read_rds_table(conn, conn.list_db_tables()[1])





