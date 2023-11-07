
-- number of stores business has in each country
SELECT country_code AS country, count(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- locations with most stores
SELECT locality, count(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

-- months that have highest average cost of sales
SELECT round( sum(product_price_pounds*product_quantity)::numeric, 2 ) AS total_sales ,  to_char(dt.date_time, 'MM') AS month
FROM orders_table AS ord
JOIN dim_date_times AS dt
ON ord.date_uuid = dt.date_uuid
JOIN dim_products AS prd
ON prd.product_code = ord.product_code
GROUP BY month
ORDER BY total_sales DESC;

-- number of sales and quantity of products coming from online vs offline
SELECT count(*) AS number_of_sales, sum(product_quantity) AS product_quantity_count, CASE 
	WHEN store_type = 'Web Portal' THEN 'Web'
	ELSE 'Offline' 
	END AS location
FROM orders_table
NATURAL JOIN dim_store_details
GROUP BY location
ORDER BY number_of_sales;

-- percentages of sales coming from each store type (outmost nesting is because over() cannot work with round)
SELECT store_type, round(total_sales::numeric, 2) AS total_sales, round(prs::numeric, 2) AS "percentage_total(%)"
FROM (
	SELECT store_type, total_sales, (total_sales*100) / sum(total_sales) over () AS prs 
	FROM (
		SELECT store_type,  sum(product_price_pounds * product_quantity) AS total_sales
		FROM orders_table AS ord
		JOIN dim_store_details AS srd
		ON ord.store_code = srd.store_code
		JOIN dim_products AS prd
		ON ord.product_code = prd.product_code
		GROUP BY store_type
		ORDER BY total_sales DESC
	) AS sales_by_store
) AS sales_by_store_prs;

-- month in years that produce highest cost of sales historically 
SELECT round((sum(product_price_pounds * product_quantity))::numeric, 2) AS total_sales, 
	   to_char(date_time, 'YYYY') AS year, 
	   to_char(date_time, 'MM') AS month 
FROM orders_table AS ord
JOIN dim_products AS prd
ON ord.product_code = prd.product_code
JOIN dim_date_times AS dt
ON ord.date_uuid = dt.date_uuid
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10;

-- staff headcount by country
SELECT sum(staff_numbers) AS total_staff_numbers, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- sales per German store type
SELECT round((sum(product_price_pounds * product_quantity))::numeric, 2) AS total_sales, sr.store_type, sr.country_code
FROM orders_table AS ord
JOIN dim_store_details AS srd
ON ord.store_code = srd.store_code
JOIN dim_products AS prd
ON ord.product_code = prd.product_code
JOIN dim_store_details AS sr
ON ord.store_code = sr.store_code
WHERE sr.country_code = 'DE'
GROUP BY sr.store_type, sr.country_code
ORDER BY total_sales;