	--Drop and recreate if needed
DROP TABLE IF EXISTS raw.voucher            CASCADE;
DROP TABLE IF EXISTS raw.products           CASCADE;
DROP TABLE IF EXISTS raw.locations          CASCADE;
DROP TABLE IF EXISTS raw.payment_methods    CASCADE;
DROP TABLE IF EXISTS raw.users              CASCADE;
DROP TABLE IF EXISTS raw.transactions       CASCADE;
DROP TABLE IF EXISTS raw.transaction_items  CASCADE;
DROP TABLE IF EXISTS raw.sessions           CASCADE;
DROP TABLE IF EXISTS raw.events             CASCADE;

CREATE TABLE raw.voucher (
voucher_id INTEGER,
voucher VARCHAR(50)
);

CREATE TABLE raw.products (
product_id INTEGER,
product_name VARCHAR(100),
product_category VARCHAR(100)
);

CREATE TABLE raw.locations(
locations_id INTEGER,
location VARCHAR(100)
);

CREATE TABLE raw.payment_methods(
payment_method_id INTEGER,
payment_method VARCHAR(100)
);

CREATE TABLE raw.users(
user_id INTEGER,
date VARCHAR(20),
locations_id INTEGER,
age INTEGER,
gender VARCHAR(10)
);

CREATE TABLE raw.sessions(
sessions_id INTEGER,
user_id INTEGER,
traffic_medium VARCHAR(50),
date VARCHAR(20),
traffic_source VARCHAR(50),
traffic_name VARCHAR(100)
);

CREATE TABLE raw.events(
sessions_id INTEGER,
event_id INTEGER,
event VARCHAR(50),
date VARCHAR(20)
);

CREATE TABLE raw.transaction_items(
transactions_id INTEGER,
transaction_items_id INTEGER,
product_id INTEGER,
product_qty INTEGER,
product_price BIGINT,
product_amount BIGINT
);

CREATE TABLE raw.transactions(
transactions_id INTEGER,
sessions_id INTEGER,
payment_method_id INTEGER,
total_amount BIGINT,
transactions_timestamps VARCHAR(50),
status VARCHAR(20),
voucher_id INTEGER
);



