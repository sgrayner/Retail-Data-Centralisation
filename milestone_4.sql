'Task 1: How many stores are operating in each country?'

SELECT 
	country_code, COUNT(*) as total_no_stores
FROM
	dim_store_details	
GROUP BY
	country_code	
ORDER BY
	total_no_stores DESC

'Task 2: Which locations currently have the most stores?'

SELECT 
	locality, COUNT(*) as total_no_stores
FROM
	dim_store_details	
GROUP BY
	locality	
ORDER BY
	total_no_stores DESC

'Task 3: Which months produce the highest amount from sales typically?'

SELECT 
	SUM(ot.product_quantity * dp.product_price_£) as total_sales, ddt.month
FROM
	orders_table as ot	
JOIN
	dim_date_times as ddt ON ot.date_uuid = ddt.date_uuid	
JOIN
	dim_products as dp ON ot.product_code = dp.product_code	
GROUP BY
	ddt.month	
ORDER BY
	total_sales DESC

'Task 4: How many sales are made online and offline?'

SELECT
	COUNT(*) as number_of_sales,
	SUM(product_quantity) as product_quantity_count,
	CASE
		WHEN store_code = 'WEB-1388012W' THEN 'online'
		ELSE 'offline'
		END AS location	
FROM
	orders_table	
GROUP BY
	(store_code = 'WEB-1388012W')

'Task 5: What percentage of sales come through each type of store?'

SELECT
	dsd.store_type,
	ROUND(CAST(SUM(product_quantity * product_price_£) AS numeric), 2) AS total_sales,
	ROUND(100. * count(*) / sum(count(*)) over (), 2) AS percentage	
FROM
	orders_table AS ot
JOIN
	dim_products AS dp ON dp.product_code = ot.product_code	
JOIN
	dim_store_details AS dsd ON dsd.store_code = ot.store_code	
GROUP BY
	dsd.store_type	
ORDER BY
	total_sales DESC

'Task 6: Which month in each year produced the highest cost of sales?'

SELECT
	ROUND(CAST(SUM(product_quantity * product_price_£) AS numeric), 2) AS total_sales,
	year,
	month	
FROM
	orders_table AS ot
JOIN
	dim_products AS dp ON dp.product_code = ot.product_code	
JOIN
	dim_date_times AS ddt ON ddt.date_uuid = ot.date_uuid	
GROUP BY
	month, year	
ORDER BY
	total_sales DESC

'Task 7: What is our staff headcount?'

SELECT
	SUM(staff_numbers) AS total_staff_numbers,
	country_code	
FROM
	dim_store_details
GROUP BY
	country_code	
ORDER BY
	total_staff_numbers DESC

'Task 8: Which German store type is selling the most?'

SELECT
	ROUND(CAST(SUM(product_quantity * product_price_£) AS numeric), 2) AS total_sales,
	store_type,
	country_code	
FROM
	orders_table AS ot
JOIN
	dim_products AS dp ON dp.product_code = ot.product_code	
JOIN
	dim_store_details AS dsd ON dsd.store_code = ot.store_code	
WHERE
	country_code = 'DE'	
GROUP BY
	store_type, country_code	
ORDER BY
	total_sales

'Task 9: How quickly is the company making sales?'

WITH date_times AS (
SELECT
year,
month,
day,
timestamp,
TO_TIMESTAMP(CONCAT(year, '/', month, '/', day, '/', timestamp), 'YYYY/MM/DD/HH24:MI:ss') as times
FROM dim_date_times d
JOIN orders_table o
ON d.date_uuid = o.date_uuid
JOIN dim_store_details s
ON o.store_code = s.store_code
ORDER BY times DESC),

next_times AS(
SELECT year,
timestamp,
times,
LEAD(times) OVER(ORDER BY times DESC) AS next_times
FROM date_times),

avg_times AS(
SELECT year,
(AVG(times - next_times)) AS avg_times
FROM next_times
GROUP BY year
ORDER BY avg_times DESC)

SELECT year, CONCAT('"hours": ', (EXTRACT(HOUR FROM avg_times)),','
' "minutes": ', (EXTRACT(MINUTE FROM avg_times)),','
' "seconds": ', ROUND((EXTRACT(SECOND FROM avg_times)), 0),','
' "milliseconds": ', (ROUND((EXTRACT( SECOND FROM avg_times)- FLOOR(EXTRACT(SECOND FROM avg_times)))*100))) as actual_time_taken
FROM avg_times