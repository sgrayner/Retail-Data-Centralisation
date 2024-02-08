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

## Usage instructions

1. Setup a SQL database to store the data. (See below section 'Creating the database in pgadmin4').
2. Run the data_cleaning.py file to extract, clean and transfer the data from the various sources into tables in the SQL database.
3. Run the SQL commands in the database_setup.sql file to finalise the database (correcting column types and creating keys).
4. Either run the queries in the database_queries.sql file, or feel free to query the database to extract your own insights!

## Creating the SQL database

1. Install pgadmin4. See link:
2. Setup a new database like so (pictures)
3. Save your database credentials in a yaml file and save it as **sql_creds.yaml** in the same directory as your cloned repository.

## Data cleaning transformations

## SQL database

## SQL queries
