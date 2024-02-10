# Multinational Retail Data Centralisation Project
***

## Contents

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#project-description">Project description</a>
    </li>
    <li>
      <a href="#installation-instructions">Installation instructions</a>
    </li>
    <li>
      <a href="#python-libraries">Python libraries</a>
    </li>
    <li>
      <a href="#github-repository-structure">Github repository structure</a>
    </li>
    <li>
      <a href="#file-descriptions">File descriptions</a>
    </li>
    <li>
      <a href="#data-sources">Data sources</a>
    </li>
    <li>
      <a href="#data-extraction">Data extraction</a>
    </li>
    <li>
      <a href="#data-cleaning-steps">Data cleaning steps</a>
    </li>
    <li><a href="#sql-database">SQL database</a>
        <ul>
        <li><a href="#upload-to-sql-database">Upload to SQL database</a></li>
        <li><a href="#sql-database-structure">SQL database structure</a></li>
        </ul>
    </li>
    <li>
      <a href="#sql-queries">SQL queries</a>
    </li>
  </ol>
</details>

<!-- PROJECT DESCRIPTION -->
## Project description
This project extracts retail data from an AWS RDS database and an S3 bucket, cleans the data in Python and then queries the data in PostgreSQL to extract business insights that would be useful to a retail company.

<!-- INSTALLATION INSTRUCTIONS -->
## Installation instructions

Clone the repository by running the following command inside a terminal:
```
git clone https://github.com/sgrayner/Retail-Data-Centralisation.git
```

<!-- PYTHON LIBRARIES -->
## Python libraries

- numpy
- pandas
- sqlalchemy
- tabula
- requests
- yaml

<!-- GITHUB REPOSITORY STRUCTURE -->
## Github repository structure

```
├── images
│   ├── database.png
├── README.md
├── data_cleaning.py
│── data_extraction.py 
├── data_transforms.py
├── database_queries.sql
├── database_setup.sql
├── database_utils.py
```

<!-- FILE DESCRIPTIONS -->
## File descriptions

- **database_utils.py** - Contains functions that create connections to the various data sources, as well as upload data to the SQL database.
- **data_extraction.py** - Contains functions that extract data from the data sources and return them as pandas dataframes.
- **data_cleaning.py** - Contains functions that clean the data and upload it to the SQL database.
- **data_transforms.py** - Contains functions called in the data_cleaning.py file to perform longer cleaning transformations on certain data columns.
- **database_setup.sql** - Contains SQL queries that set column types and create primary and foreign keys in the database.
- **database_queries.sql** - Contains SQL queries that extract business intelligence from the database.

<!-- DATA SOURCES -->
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

<!-- DATA EXTRACTION -->
## Data extraction

These are the python functions used in extracting the data from the data sources.
 
- To extract data from Amazon RDS (for the **legacy_users** data and **orders_table** data):
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

<!-- DATA CLEANING STEPS -->
## Data cleaning steps

**clean_user_data()**:
- drop records with null values in 'email_address', 'address' and 'phone_number'
- drop records with 'email_address' not containing '@'
- replace 'GGB' with 'GB' in 'country_code' values
- string methods .replace, .removeprefix and concatenation to make phone_number values into a consistent format.
- convert the 'date_of_birth' and 'join_date' columns to date type.

**clean_card_data()**:
- removed records with erroneous values in the 'expiry_date' column.
- remove '?' characters from 'card_number' values
- convert 'date_payment_confirmed' to date type.
- convert 'card_number' to int64 type.

**clean_store_data()**:
- removed 'ee' characters from 'continent' values.
- filtered out erroneous values of 'continent'.
- removed records with non-numerical values for 'staff_numbers'.
- dropped the'index' and 'lat' columns.
- converted 'opening_date' to date type.

**clean_product_data()**:
- removed the characters 'x' and 'g' from 'weight' values of the form 3 x 4g, then multipled the two numbers together and concatenated 'g' to the end of the string.
- remove 'g' and 'ml' suffixes from 'weight' values and convert the values to kg.
- remove 'oz' suffix from 'weight' values and convert the values to kg.
- renamed 'weight' column to 'weight_kg'.
- dropped 'Unnamed: 0' column.
- removed erroneous values from the 'removed' column.
- removed '£' character from 'product_price' values and renamed column to 'product_price_£'.
- converted 'date_added' to date type and 'product_price_£' and 'weight_kg' to float type.

**clean_orders_data()**:
- dropped 'level_0', 'first_name', 'last_name', '1' columns.

**clean_events_data**:
- removed records with erroneous values in the 'year' column.

<!-- SQL DATABASE -->
## SQL database

<!-- UPLOAD TO SQL DATABASE -->
### Upload to SQL database

At the end of each of the cleaning functions in data_cleaning.py, the cleaned dataframe is uploaded to the SQL database with the line:
```
dc('sql_creds.yaml').upload_to_db(df, <table name>)
```
where the \<table name\> is the name of the corresponding table in the SQL database.

<!-- SQL DATABASE STRUCTURE -->
### SQL database structure

We create a star-schema SQL database in pgadmin4, with **orders_table** as the fact table. After the cleaned dataframes are uploaded to the database, we set the data types of all the columns and establish the primary and foreign keys. The code to configure this is in the database_setup.sql file.

<img align="right" src="https://github.com/sgrayner/Retail-Data-Centralisation/blob/master/images/database.png" alt="Navigation bar" width="200"/>

- **orders_table**: index, date_uuid, user_uuid, card_number, store_code, product_code, product_quantity, cards_key, date_key, products_key, store_key, users_key
- **dim_users**: index, first_name, last_name, date_of_birth, company, email_address, address, country, country_code, phone_number, join_date, user_uuid (PK)
- **dim_card_details**: index, card_number (PK), expiry_date, card_provider, date_payment_confirmed, cards_key
- **dim_store_details**: index, address, longitude, locality, store_code (PK), staff_numbers, opening_date, store_type, latitude, country_code, continent, store_key
- **dim_products**: index, product_name, product_price_£, weight_kg, category, EAN, date_added, uuid, still_available, product_code (PK), products_key, weight_class
- **dim_date_times**: index, timestamp, month, year, day, time_period, date_uuid (PK), date_key

<!-- SQL QUERIES -->
## SQL queries

Once the database is built, we analyse the data to extract the following business insights. The code to extract the data is in the database_queries.sql file.

1. How many stores are operating in each country?
2. Which locations currently have the most stores?
3. Which months produce the highest amount from sales typically?
4. How many sales are made online and offline?
5. What percentage of sales come through each type of store?
6. Which month in each year produced the highest cost of sales?
7. What is our staff headcount?
8. Which German store type is selling the most?
9. How quickly is the company making sales?
