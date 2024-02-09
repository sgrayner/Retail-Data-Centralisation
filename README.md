# Multinational Retail Data Centralisation Project

## Project description
This project extracts retail data from an AWS RDS database and an S3 bucket, cleans the data in Python and then queries the data in PostgreSQL to extract business insights that would be useful to a retail company.

## Installation instructions

Clone the repository by running the following command inside a terminal:
```
git clone https://github.com/sgrayner/Retail-Data-Centralisation.git
```

## Python libraries used

- numpy
- pandas
- sqlalchemy
- tabula
- requests
- yaml

## Github repository structure

```
├── data_cleaning.py
│── data_extraction.py 
├── data_transforms.py
├── database_queries.sql
├── database_setup.sql
├── database_utils.py
```

## File descriptions

- **database_utils.py** - Contains functions that create connections to the various data sources, as well as upload data to the SQL database.
- **data_extraction.py** - Contains functions that extract data from the data sources and return them as pandas dataframes.
- **data_cleaning.py** - Contains functions that clean the data and upload it to the SQL database.
- **data_transforms.py** - Contains functions called in the data_cleaning.py file to perform longer cleaning transformations on certain data columns.
- **database_setup.sql** - Contains SQL queries that set column types and create primary and foreign keys in the database.
- **database_queries.sql** - Contains SQL queries that extract business intelligence from the database.

## Data sources

**From Amazon RDS**
- **legacy_users**. Contains columns: 'first_name', 'last_name', 'date_of_birth', 'company', 'email_address', 'address', 'country', 'country_code', 'phone_number', 'join_date', 'user_uuid'.
- **orders_table**. Contains columns: 'level_0', 'date_uuid', 'first_name', 'last_name', 'user_uuid', 'card_number', 'store_code', 'product_code', '1', 'product_quantity'.

**From Amazon S3**
- **card_details.pdf**. Contains columns: 'card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed'.
- **products.csv**. Contains columns: 'Unnamed: 0', 'product_name', 'product_price', 'weight', 'category', 'EAN', 'date_added', 'uuid', 'removed', 'product_code'.
- **date_details.json**. Contains columns: 'timestamp', 'month', 'year', 'day', 'time_period', 'date_uuid'.

**From an API**
- **store_details**. Contains columns: 'address', 'logitude', 'lat', 'locality', 'store_code', 'staff_numbers', 'opening_date', 'store_type', 'latitude', 'country_code', 'continent'.

## Data extraction

These are the python functions used in extracting the data from the data sources.
 
- To extract data from Amazon RDS (for the **legacy_users** data and **orders_table** data:
```
pd.read_sql_table(table, conn.init_db_engine(), index_col='index')
```
where ```conn.init_db_engine()``` initialises a SQLalchemy engine that connects to the SQL database.

- To extract data from **card_details.pdf**:
```
df = tabula.read_pdf(link, stream=False, pages='all')
df = pd.concat(df)
```

- To extract data from **products.csv** and **date_details.json**:
```
pd.read_csv(path)
pd.read_json(path)
```

- To extract data from **store_details**:
```
for i in range(DataExtractor.list_number_of_stores()):
    endpoint = data['STORES_ENDPOINT'] + f'{i}'
    response = requests.get(endpoint, headers=header)
    response = response.json()
    data_list.append(response)
df = pd.DataFrame(data_list)
```

## Data cleaning steps

**clean_user_data()**:
- drop records with null values in 'email_address', 'address' and 'phone_number'
- drop records with 'email_address' not containing '@'
- replace 'GGB' with 'GB' in 'country_code' values
- string methods .replace, .removeprefix and concatenation to make phone_number values into a consistent format.
- convert the 'date_of_birth' and 'join_date' columns to date type.

**clean_card_data()**:
- drop records with 'expiry_date' not of the format mm/yy
- remove '?' characters from 'card_number' values
- convert 'date_payment_confirmed' to date type.
- convert 'card_number' to int64 type.

**clean_store_data**:
- removed 'ee' characters from 'continent' values.
- filtered out erroneous values of 'continent'.
- removed records with non-numerical values for 'staff_numbers'.
- dropped the'index' and 'lat' columns.
- converted 'opening_date' to date type.

**clean_product_data**:

## SQL database

## SQL queries
