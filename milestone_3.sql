'Changing the orders_table columns to the correct types'
ALTER TABLE orders_table
	ALTER COLUMN product_quantity TYPE SMALLINT,
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

'Changing the dim_users table columns to the correct types'
ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN join_date TYPE DATE;

'Changing the dim_store_details table columns to the correct types'
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
	ALTER COLUMN opening_date TYPE DATE,
	ALTER COLUMN store_type TYPE VARCHAR(225),
	ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);

'Creating a new column in the dim_products table to categorise weights into: "light", "mid-sized", "heavy" and "truck_required"'
ALTER TABLE dim_products
    ADD COLUMN weight_class VARCHAR(14)
    RENAME removed TO still_available

UPDATE dim_products
SET weight_class =
	CASE 
		WHEN weight_kg < 2 THEN 'Light'
		WHEN weight_kg >=2 AND weight_kg < 40 THEN 'Mid-Sized'
		WHEN weight_kg >=40 AND weight_kg < 140 THEN 'Heavy'
		WHEN weight_kg >= 140 THEN 'Truck_Required'	
	END;

'Changing the dim_product table columns to the correct types'
ALTER TABLE dim_products
	ALTER COLUMN product_price_£ TYPE FLOAT USING product_price_£::double precision,
	ALTER COLUMN weight_kg TYPE FLOAT USING weight_kg::double precision,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
	ALTER COLUMN still_available TYPE BOOL USING (still_available='Still_avaliable');

'Changing the dim_date_times table columns to the correct types'
ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(2),
	ALTER COLUMN year TYPE VARCHAR(4),
	ALTER COLUMN day TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

'Changing the dim_card_details table columns to the correct types'
ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN expiry_date TYPE VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE DATE

'Adding primary keys to the dimmension tables'
ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid)

ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code)

ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code)

ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid)

ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number)

'Adding foreign keys to the orders table'
ALTER TABLE orders_table
	ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
	ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid),
	ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number),
	ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
	ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
