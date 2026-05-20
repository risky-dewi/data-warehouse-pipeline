# E-Commerce Data Warehouse Pipeline

End-to-end data warehouse project built from 9 raw CSV files using Python and PostgreSQL, following a layered architecture pattern:

```text
RAW → Staging → Data Warehouse → Data Mart
```

This project transforms flat e-commerce CSV exports into a structured analytical data warehouse using a multi-fact star schema and business-ready data marts.

---

## Background & Problem Statement

An e-commerce company collects transactional and behavioral data across multiple systems, such as user registrations, browsing sessions, funnel events, payment methods, vouchers, products, and transactions.

However, the data is stored as flat CSV exports with no unified analytical structure. This makes it difficult to answer business questions directly from the raw files.

Business questions that could not be answered from raw files alone:
- What is the monthly revenue trend?
- What is the cancellation rate by month?
- Where do users drop off in the checkout funnel?
- Which customer segments generate the most revenue?
- Which traffic source brings the highest-value users?
- Which products contribute the most revenue?
- Do vouchers improve completed orders or increase cancellations?
- Which payment methods are most preferred by customers?

This project builds a structured data warehouse to answer these questions through clean, integrated, and business-ready data marts.

## Project Objectives

The main objectives of this project are:
- Load raw CSV files into PostgreSQL.
- Build a layered data warehouse architecture.
- Clean and standardize raw data in the staging layer.
- Model the data warehouse using facts and dimensions.
- Create analytical data marts for business reporting.
- Add data quality checks to validate pipeline output.
- Make the project reproducible and portfolio-ready.

---

## Tech Stack

- **PostgreSQL** — data warehouse engine
- **Python** (pandas, sqlalchemy, python-dotenv) — CSV ingestion pipeline
- **VS Code + SQLTools** — SQL development
- **Git + GitHub** — version control

---

| Table | Rows | Description |
|---|---|---|
| users | 9,156 | Customer profiles with age, gender, city |
| sessions | 83,486 | Browsing sessions with traffic source/medium |
| events | 565,527 | 9 funnel event types per session |
| transactions | ~19k | Orders with status and payment info |
| transaction_items | 59,520 | Line items with product, qty, price |
| products | 66 | 10 product categories |
| locations | 13 | Cities across Java (Jakarta, Surabaya, etc.) |
| payment_methods | 6 | Including E-Wallet, Credit Card, Paylater |
| voucher | 4 | Cashback, Direct Discount, Delivery Fee, None |

**Date range:** January 2020 – December 2022  
**Total rows:** ~750,000 across all tables

---

## Data Architecture

The project follows a layered architecture:

```text
CSV Files
   ↓
Python Loader
   ↓
Raw Layer
   ↓
Staging Layer
   ↓
Data Warehouse Layer
   ↓
Data Mart Layer
   ↓
Business Analysis
```


