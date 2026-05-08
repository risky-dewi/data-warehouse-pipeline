CREATE SCHEMA IF NOT EXISTS dw;

-- DIMENSION TABLES --

-- dim_date
DROP TABLE IF EXISTS dw.dim_date CASCADE;
CREATE TABLE dw.dim_date AS
WITH date_series AS (
	SELECT CAST(generate_series(
		CAST('2019-01-01' AS DATE),
		CAST('2023-12-31' AS DATE),
		CAST('1 day' AS INTERVAL)
	) AS DATE) AS full_date
)
SELECT
	CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INTEGER) AS date_key,
	full_date,
	CAST(EXTRACT(ISOYEAR FROM full_date) AS INTEGER) AS year,
	CAST(EXTRACT(MONTH FROM full_date) AS INTEGER) AS month,
	TRIM(TO_CHAR(full_date, 'Month')) AS month_name,
	CAST(EXTRACT(DAY FROM full_date) AS INTEGER) AS day,
	CAST(EXTRACT(ISODOW FROM full_date) AS INTEGER) AS day_of_week,
	TRIM(TO_CHAR(full_date, 'Day')) AS day_name,
	TO_CHAR(full_date, '"Q"Q YYYY') AS quarter_label,
	CAST(EXTRACT(WEEK FROM full_date) AS INTEGER) AS week_of_year,
	CASE WHEN EXTRACT(ISODOW FROM full_date) IN (6,7)
		THEN TRUE ELSE FALSE END AS is_weekend
FROM date_series;

ALTER TABLE dw.dim_date ADD PRIMARY KEY (date_key);

-- dim_location
DROP TABLE IF EXISTS dw.dim_location CASCADE;
CREATE TABLE dw.dim_location AS
SELECT
	locations_id AS location_key,
	location
FROM staging.stg_locations;

ALTER TABLE dw.dim_location ADD PRIMARY KEY (location_key);

--dim_user
DROP TABLE IF EXISTS dw.dim_user CASCADE;
CREATE TABLE dw.dim_user AS 
SELECT 
	user_id AS user_key,
	registration_date,
	CASE
		WHEN registration_date IS NOT NULL
		THEN CAST(TO_CHAR(registration_date, 'YYYYMMDD') AS INTEGER)
		ELSE NULL
	END AS registration_date_key,
	age,
	age_group,
	gender,
	location
FROM staging.stg_users u
LEFT JOIN staging.stg_locations l ON u.locations_id = l.locations_id;

ALTER TABLE dw.dim_user ADD PRIMARY KEY (user_key);

--dim_product
DROP TABLE IF EXISTS dw.dim_product CASCADE;
CREATE TABLE dw.dim_product AS
SELECT 
	product_id AS product_key,
	product_name,
	product_category
FROM staging.stg_products;

ALTER TABLE dw.dim_product ADD PRIMARY KEY (product_key);

-- dim_payment_method
DROP TABLE IF EXISTS dw.dim_payment_method CASCADE;
CREATE TABLE dw.dim_payment_method AS
SELECT
    payment_method_id   AS payment_method_key,
    payment_method
FROM staging.stg_payment_methods;
 
ALTER TABLE dw.dim_payment_method ADD PRIMARY KEY (payment_method_key);
 
-- dim_voucher
DROP TABLE IF EXISTS dw.dim_voucher CASCADE;
CREATE TABLE dw.dim_voucher AS
SELECT
    voucher_id    AS voucher_key,
    voucher_type
FROM staging.stg_voucher;
 
ALTER TABLE dw.dim_voucher ADD PRIMARY KEY (voucher_key);

--dim_taffic
DROP TABLE IF EXISTS dw.dim_traffic CASCADE;
CREATE TABLE dw.dim_traffic AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY traffic_source, traffic_medium) AS traffic_key,
	traffic_source,
	traffic_medium
FROM (SELECT DISTINCT traffic_source, traffic_medium FROM staging.stg_sessions) AS t;

ALTER TABLE dw.dim_traffic ADD PRIMARY KEY (traffic_key);

-- FACT TABLES --

-- fact_sessions
DROP TABLE IF EXISTS dw.fact_sessions CASCADE;

-- total transactions per session
CREATE TABLE dw.fact_sessions AS
WITH trx AS(
SELECT
	sessions_id,
	COUNT(DISTINCT transactions_id) AS total_transactions
FROM staging.stg_transactions
GROUP BY sessions_id
),

-- total event per session
evt AS(
SELECT
	sessions_id,
	COUNT(DISTINCT event_id) AS total_events
FROM staging.stg_events
GROUP BY sessions_id
)

SELECT
	ss.sessions_id AS session_key,
	ss.user_id AS user_key,
	CAST(TO_CHAR(ss.session_date, 'YYYYMMDD') AS INTEGER) AS date_key,
	dt.traffic_key,
	COALESCE(trx.total_transactions,0) AS num_transactions,
	COALESCE(evt.total_events,0) AS num_events,
	CASE WHEN trx.total_transactions > 0 THEN 1 ELSE 0 END AS is_converted,
	CURRENT_TIMESTAMP AS created_at,
	CURRENT_TIMESTAMP AS updated_at,
	'batch_00' AS etl_batch_id,
	'ecommerce_app' AS source_system
FROM staging.stg_sessions ss
LEFT JOIN dw.dim_traffic dt ON ss.traffic_medium = dt.traffic_medium AND ss.traffic_source = dt.traffic_source 
LEFT JOIN trx ON ss.sessions_id = trx.sessions_id 
LEFT JOIN evt ON ss.sessions_id = evt.sessions_id;

ALTER TABLE dw.fact_sessions
ADD CONSTRAINT pk_fact_sessions
PRIMARY KEY (session_key),

ADD CONSTRAINT fk_fact_sessions_user
FOREIGN KEY (user_key)
REFERENCES dw.dim_user(user_key),

ADD CONSTRAINT fk_fact_sessions_date
FOREIGN KEY (date_key)
REFERENCES dw.dim_date(date_key),

ADD CONSTRAINT fk_fact_sessions_traffic
FOREIGN KEY (traffic_key)
REFERENCES dw.dim_traffic(traffic_key);

-- Index fact_sessions
CREATE INDEX idx_fact_sessions_date
ON dw.fact_sessions(date_key);
CREATE INDEX idx_fact_sessions_user
ON dw.fact_sessions(user_key);

-- fact_events
DROP TABLE IF EXISTS dw.fact_events CASCADE;
CREATE TABLE dw.fact_events AS
SELECT
    e.event_id AS event_key,
    e.sessions_id AS session_key,
    s.user_id AS user_key,
    CAST(TO_CHAR(e.event_date, 'YYYYMMDD') AS INTEGER) date_key,
    e.event,
    e.event_date,
    CURRENT_TIMESTAMP AS created_at,
	CURRENT_TIMESTAMP AS updated_at,
	'batch_00' AS etl_batch_id,
	'ecommerce_app' AS source_system
FROM staging.stg_events e
LEFT JOIN staging.stg_sessions s ON e.sessions_id = s.sessions_id;
 
ALTER TABLE dw.fact_events
ADD CONSTRAINT pk_fact_events
PRIMARY KEY (event_key),

ADD CONSTRAINT fk_fact_events_user
FOREIGN KEY (user_key)
REFERENCES dw.dim_user(user_key),

ADD CONSTRAINT fk_fact_events_date
FOREIGN KEY (date_key)
REFERENCES dw.dim_date(date_key);

-- Index fact_events
CREATE INDEX idx_fact_events_date
ON dw.fact_events(date_key);
CREATE INDEX idx_fact_events_user
ON dw.fact_events(user_key);
CREATE INDEX idx_fact_events_session
ON dw.fact_events(session_key);

-- fact_transactions
DROP TABLE IF EXISTS dw.fact_transactions CASCADE;
CREATE TABLE dw.fact_transactions AS 
SELECT 
	t.transactions_id AS transaction_key,
	t.sessions_id AS session_key,
	s.user_id AS user_key,
	t.payment_method_id AS payment_method_key,
	COALESCE(t.voucher_id, 4) AS voucher_key,
	CAST(TO_CHAR(t.transaction_timestamp, 'YYYYMMDD') AS INTEGER) AS date_key,
	t.status,
	t.total_amount,
	CASE WHEN t.status = 'completed' THEN t.total_amount ELSE 0 END AS revenue,
	CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END AS is_completed,
	CASE WHEN t.status = 'canceled' THEN 1 ELSE 0 END AS is_canceled,
	CAST(EXTRACT(HOUR FROM t.transaction_timestamp) AS INTEGER) AS transaction_hour,
	CURRENT_TIMESTAMP AS created_at,
	CURRENT_TIMESTAMP AS updated_at,
	'batch_00' AS etl_batch_id,
	'ecommerce_app' AS source_system
FROM staging.stg_transactions t
LEFT JOIN staging.stg_sessions s ON t.sessions_id = s.sessions_id;

ALTER TABLE dw.fact_transactions
ADD CONSTRAINT pk_fact_transactions
PRIMARY KEY (transaction_key),

ADD CONSTRAINT fk_fact_transactions_session
FOREIGN KEY (session_key)
REFERENCES dw.fact_sessions(session_key)
NOT VALID,

ADD CONSTRAINT fk_fact_transactions_user
FOREIGN KEY (user_key)
REFERENCES dw.dim_user(user_key),

ADD CONSTRAINT fk_fact_transactions_date
FOREIGN KEY (date_key)
REFERENCES dw.dim_date(date_key),

ADD CONSTRAINT fk_fact_transactions_payment_method
FOREIGN KEY (payment_method_key)
REFERENCES dw.dim_payment_method(payment_method_key),

ADD CONSTRAINT fk_fact_transactions_voucher
FOREIGN KEY (voucher_key)
REFERENCES dw.dim_voucher(voucher_key),

ADD CONSTRAINT chk_total_amount_positive
	CHECK (total_amount > 0);

-- Index fact_transactions
CREATE INDEX idx_fact_transactions_date
ON dw.fact_transactions(date_key);
CREATE INDEX idx_fact_transactions_user
ON dw.fact_transactions(user_key);
CREATE INDEX idx_fact_transactions_session
ON dw.fact_transactions(session_key);
CREATE INDEX idx_fact_transactions_status
ON dw.fact_transactions(status);
CREATE INDEX idx_fact_transactions_hour
ON dw.fact_transactions(transaction_hour);

-- fact_transaction_items
DROP TABLE IF EXISTS dw.fact_transaction_items CASCADE;
CREATE TABLE dw.fact_transaction_items AS
SELECT
	ti.transaction_items_id AS transaction_item_key,
	ti.transactions_id AS transaction_key,
	ti.product_id AS product_key,
	t.user_key,
	t.date_key,
	ti.product_qty,
	ti.product_price,
	ti.product_amount,
	t.voucher_key,
	t.payment_method_key,
	t.status,
	CURRENT_TIMESTAMP AS created_at,
	CURRENT_TIMESTAMP AS updated_at,
	'batch_00' AS etl_batch_id,
	'ecommerce_app' AS source_system
FROM staging.stg_transaction_items ti
INNER JOIN dw.fact_transactions t ON ti.transactions_id = t.transaction_key;

ALTER TABLE dw.fact_transaction_items
ADD CONSTRAINT pk_fact_transaction_items
PRIMARY KEY (transaction_item_key),

ADD CONSTRAINT fk_fact_transaction_items_transaction
FOREIGN KEY (transaction_key)
REFERENCES dw.fact_transactions(transaction_key),

ADD CONSTRAINT fk_fact_transaction_items_product
FOREIGN KEY (product_key)
REFERENCES dw.dim_product(product_key),

ADD CONSTRAINT fk_fact_transaction_items_user
FOREIGN KEY (user_key)
REFERENCES dw.dim_user(user_key),

ADD CONSTRAINT fk_fact_transaction_items_date
FOREIGN KEY (date_key)
REFERENCES dw.dim_date(date_key),

ADD CONSTRAINT fk_fact_transaction_items_payment_method
FOREIGN KEY (payment_method_key)
REFERENCES dw.dim_payment_method(payment_method_key),

ADD CONSTRAINT fk_fact_transaction_items_voucher
FOREIGN KEY (voucher_key)
REFERENCES dw.dim_voucher(voucher_key),

ADD CONSTRAINT chk_product_qty_positive
	CHECK (product_qty > 0),

ADD CONSTRAINT chk_product_price_positive
	CHECK (product_price > 0),

ADD CONSTRAINT chk_amount_equals_calc
	CHECK (product_amount = product_qty * product_price);

-- Index fact_transaction_items
CREATE INDEX idx_fact_transaction_items_transaction
ON dw.fact_transaction_items(transaction_key);
CREATE INDEX idx_fact_transaction_items_date
ON dw.fact_transaction_items(date_key);
CREATE INDEX idx_fact_transaction_items_user
ON dw.fact_transaction_items(user_key);
CREATE INDEX idx_fact_transaction_items_status
ON dw.fact_transaction_items(status);
CREATE INDEX idx_fact_transaction_items_product
ON dw.fact_transaction_items(product_key);
