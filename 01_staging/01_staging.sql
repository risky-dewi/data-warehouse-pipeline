CREATE SCHEMA IF NOT EXISTS staging;

--Drop and recreate if needed
DROP TABLE IF EXISTS staging.stg_voucher            CASCADE;
DROP TABLE IF EXISTS staging.stg_products           CASCADE;
DROP TABLE IF EXISTS staging.stg_locations          CASCADE;
DROP TABLE IF EXISTS staging.stg_payment_methods    CASCADE;
DROP TABLE IF EXISTS staging.stg_users              CASCADE;
DROP TABLE IF EXISTS staging.stg_transactions       CASCADE;
DROP TABLE IF EXISTS staging.stg_transaction_items  CASCADE;
DROP TABLE IF EXISTS staging.stg_sessions           CASCADE;
DROP TABLE IF EXISTS staging.stg_events             CASCADE;

-- stg_voucher
CREATE TABLE  staging.stg_voucher AS 
SELECT
	voucher_id,
	TRIM(voucher) AS voucher_type
FROM raw.voucher
WHERE voucher_id IS NOT NULL;

-- stg_products
CREATE TABLE staging.stg_products AS 
SELECT
	product_id,
	TRIM(product_name) AS product_name,
	TRIM(product_category) AS product_category
FROM raw.products
WHERE product_id IS NOT NULL;

-- stg_locations
CREATE TABLE staging.stg_locations AS
SELECT 
	locations_id,
	TRIM(location) AS location
FROM raw.locations
WHERE locations_id IS NOT NULL;

-- stg_payment_methods
CREATE TABLE staging.stg_payment_methods AS
SELECT
	payment_method_id,
	TRIM(payment_method) AS payment_method
FROM raw.payment_methods
WHERE payment_method_id IS NOT NULL;

-- stg_users
CREATE TABLE staging.stg_users AS
SELECT
	user_id,
	TO_DATE(date, 'MM/DD/YY') AS registration_date,
	locations_id,
	age,
	TRIM(gender) AS gender,
	CASE
		WHEN age BETWEEN 10 AND 17 THEN 'Gen Z (Teen)'
		WHEN age BETWEEN 18 AND 24 THEN 'Gen Z (Adult)'
		WHEN age BETWEEN 25 AND 40 THEN 'Millennila'
		WHEN age BETWEEN 41 AND 56 THEN 'Gen X'
		WHEN age >= 57 THEN 'Boomer'
		ELSE 'Unknown'
	END AS age_group,
	FALSE AS is_sentinel
FROM raw.users
WHERE user_id IS NOT NULL

UNION ALL

SELECT
-1 AS user_id,
NULL AS registration_date,
NULL AS locations_id,
NULL AS age,
'Unknown' AS gender,
'Unknown' AS age_group,
TRUE AS is_sentinel;

-- stg_transaction
CREATE TABLE staging.stg_transactions AS
SELECT
	transactions_id,
	sessions_id,
	payment_method_id,
	total_amount,
	TO_TIMESTAMP(
		REPLACE(transactions_timestamps, ' UTC', ''),
		'YYYY-MM-DD HH24:MI:SS') AS transaction_timestamp,
	TRIM(status) AS status,
	COALESCE(voucher_id, 4) AS voucher_id
FROM raw.transactions
WHERE transactions_id IS NOT NULL;

-- stg_transaction_items
CREATE TABLE staging.stg_transaction_items AS
SELECT
	transactions_id,
	transaction_items_id,
	product_id,
	product_qty,
	product_price,
	product_amount
FROM raw.transaction_items
WHERE transaction_items_id IS NOT NULL AND product_qty > 0;

-- stg_sessions
CREATE TABLE staging.stg_sessions AS
SELECT
	sessions_id,
	user_id,
	TO_DATE(date, 'MM/DD/YY') AS session_date,
	COALESCE(NULLIF(TRIM(traffic_medium), ''), 'Unknown') AS traffic_medium,
	COALESCE(NULLIF(TRIM(traffic_source), ''), 'Unknown') AS traffic_source,
	traffic_name,
	FALSE AS is_ghost
FROM raw.sessions
WHERE sessions_id IS NOT NULL

UNION ALL

-- stg_sessions - backfill ghost sessions
SELECT 
	t.sessions_id,
	-1 AS user_id,
 	MIN(TO_DATE(e.date, 'MM/DD/YY')) AS session_date,
	'Untracked' AS traffic_medium,
	'Untracked' AS traffic_source,
	NULL AS traffic_name,
	TRUE AS is_ghost
FROM raw.transactions t
LEFT JOIN raw.events e ON t.sessions_id = e.sessions_id 
WHERE t.sessions_id NOT IN (SELECT sessions_id FROM raw.sessions)
GROUP BY t.sessions_id;

-- stg_event
CREATE TABLE staging.stg_events AS
SELECT
	sessions_id,
	event_id,
	TRIM(event) AS event,
	TO_DATE(date, 'MM/DD/YY') AS event_date
FROM raw.events
WHERE event_id IS NOT NULL;
