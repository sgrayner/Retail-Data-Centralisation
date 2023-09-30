import re
import pandas as pd
from data_extraction import DataExtractor as de
from database_utils import DatabaseConnector as dc
from datetime import datetime as dt

class DataCleaning:

    def clean_user_data(self):
        extr = de()
        conn = dc()
        df = extr.read_rds_table(conn, conn.list_db_tables()[1])
        df = df.drop(df[df['first_name'] == 'NULL'].index)
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


#cleaner = DataCleaning()
#cleaner.clean_user_data()

extr = de()
conn = dc()
df = extr.read_rds_table(conn, conn.list_db_tables()[1])
df = df.drop(df[df['first_name'] == 'NULL'].index)
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


#print(df[df['date_of_birth'] == 'KBTI7FI7Y3']['email_address'])
#print(df[df['index'] == 752][['company', 'email_address']])



#dg = df['date_of_birth'].str.match(r'\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])')
dg = df['email_address'].str.match('/^([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$/')


'''
funny_emails = []
for i in range(len(dg)):
    if dg[i] == True:
        funny_emails.append(df['email_address'][i])

print(funny_emails)
print(len(funny_emails))
'''

