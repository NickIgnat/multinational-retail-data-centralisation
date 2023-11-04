-- orders_table 
ALTER TABLE orders_table DROP COLUMN IF EXISTS index;

ALTER TABLE orders_table DROP COLUMN IF EXISTS level_0;

ALTER TABLE orders_table
    ALTER COLUMN store_code TYPE character varying(12), 
    ALTER COLUMN product_code TYPE character varying(11),
    ALTER COLUMN card_number TYPE character varying(19),
    ALTER COLUMN product_quantity TYPE smallint,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

-- dim_users
ALTER TABLE dim_users DROP COLUMN IF EXISTS level_0;

ALTER TABLE dim_users DROP COLUMN IF EXISTS index;

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE character varying(225),
    ALTER COLUMN last_name TYPE character varying(225),
    ALTER COLUMN date_of_birth TYPE date,
    ALTER COLUMN country_code TYPE character varying(2),
    ALTER COLUMN join_date TYPE date,
	ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;


-- dim_store_details
ALTER TABLE dim_store_details DROP COLUMN IF EXISTS index;

ALTER TABLE dim_store_details 
	ALTER COLUMN locality TYPE character varying(225),
	ALTER COLUMN store_code TYPE character varying(11),
	ALTER COLUMN staff_numbers TYPE smallint,
	ALTER COLUMN opening_date TYPE date,
	ALTER COLUMN store_type TYPE character varying(255),
	ALTER COLUMN store_type DROP NOT NULL,
	ALTER COLUMN country_code TYPE character varying(2),
	ALTER COLUMN continent TYPE character varying(225);

-- dim_products
ALTER TABLE dim_products DROP COLUMN IF EXISTS index;

ALTER TABLE dim_products
	ADD COLUMN weight_class character varying(14) GENERATED ALWAYS AS (
		CASE 
			WHEN weight_kg < 2 THEN 'Light'
			WHEN weight_kg < 40 THEN 'Mid_Sized'
			WHEN weight_kg < 140 THEN 'Heavy'
			ELSE 'Truck_Required'
		END
	) STORED;

ALTER TABLE dim_products
	ALTER COLUMN "EAN" TYPE character varying(17),
	ALTER COLUMN product_code TYPE character varying(11),
	ALTER COLUMN date_added TYPE date,
	ALTER COLUMN uuid TYPE uuid USING uuid::uuid;

-- dim_date_times
ALTER TABLE dim_date_times DROP COLUMN IF EXISTS index;

ALTER TABLE dim_date_times
	ALTER COLUMN time_period TYPE character varying(10),
	ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

-- dim_card_details
ALTER TABLE dim_card_details DROP COLUMN IF EXISTS index;

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE character varying(19),
	ALTER COLUMN expiry_date TYPE character varying(5),
	ALTER COLUMN date_payment_confirmed TYPE date;

