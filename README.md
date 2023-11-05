## Data_centralisation_project

### Project description
This project extracts retail data from an AWS RDS database and an S3 bucket, cleans the data in Python and then queries the data in PostgreSQL to find information that would be useful to the retail company.

### Installation instructions

Clone the repository by running the following command inside a terminal:

git clone https://github.com/sgrayner/Retail-Data-Centralisation.git

### Usage instructions

- The data_cleaning.py file is the main file that runs the ETL process on the data tables. Run this file to clean the tables and upload them to the SQL server.
- The database_utils.py file contains the functionality to connect to the data sources and the SQL database. It is imported into the data_cleaning.py file.
- The data_extraction.py contains the functionality to extract data from the data sources. It is imported into the data_cleaning.py file.
- The SQL database, named 'sales_data' contains tables on users, payment card details, store data, product data, product orders, and order times.
- The milestone_3.sql and milestone_4.sql files contain the questions that I have answered on the database as well as the SQL code that runs the queries.
- Use SQL to query the database.

### File structure of the project

C:\Retail-Data-Centralisation
   - README.md
   - data_cleaning.py
   - data_extraction.py
   - database_utils.py
   - milestone_3.sql
   - milestone_4.sql
