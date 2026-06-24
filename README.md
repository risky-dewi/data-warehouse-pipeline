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
## Data Source

The project uses 9 CSV files as the source data.

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
```
---

## Project Structure

Recommended project structure:

```text
ecommerce-dwh/
│
├── sql/
│   ├── 00_raw/
│   │   └── 00_raw.sql
│   │
│   ├── 01_staging/
│   │   └── 01_staging.sql
│   │
│   ├── 02_dwh/
│   │   └── 02_dwh.sql
│   │
│   ├── 03_mart/
│   │   └── 03_mart.sql
│   │
│   └── 04_data_quality/
│       ├── 01_check_after_raw.sql
│       ├── 02_check_after_staging.sql
│       ├── 03_check_after_dwh.sql
│       └── 04_final_data_quality.sql
│
├── scripts/
│   ├── load_raw.py
│   ├── run_sql.py
│   └── run_pipeline.py
│
├── sample_data/
│   
│── diagrams/
│
│
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Pipeline Flow

The pipeline is designed to run sequentially because each layer depends on the previous layer.

1. Create raw schema and raw tables
2. Load CSV files into raw tables
3. Run raw data quality checks
4. Build staging tables from raw tables
5. Run staging data quality checks
6. Build DWH facts and dimensions from staging tables
7. Run DWH data quality checks
8. Build business data mart views
9. Run final data quality checks

---

## Data Integration
The data integration connects customer, behavioral, transactional, product, payment, voucher, traffic, and date data into one analytical model.
```text
locations 1 ──< users 1 ──< sessions 1 ──< events
                         │
                         └──< transactions 1 ──< transaction_items >── 1 products

transactions >── 1 payment_methods
transactions >── 1 voucher
sessions     >── 1 traffic
facts        >── 1 dim_date
```
---

## Data Model
The DWH layer uses a multi-fact star schema.

Instead of using only one fact table, this project models multiple business processes:
| Fact Table                  | Grain                        | Purpose                                                           |
| --------------------------- | ---------------------------- | ----------------------------------------------------------------- |
| `dw.fact_sessions`          | One row per session          | Analyze sessions, traffic, events, and conversion                 |
| `dw.fact_events`            | One row per event            | Analyze user behavior and funnel activity                         |
| `dw.fact_transactions`      | One row per transaction      | Analyze orders, revenue, cancellation, payment, and voucher usage |
| `dw.fact_transaction_items` | One row per transaction item | Analyze product-level sales performance                           |

| Dimension Table         | Description                                  |
| ----------------------- | -------------------------------------------- |
| `dw.dim_date`           | Calendar dimension for time-based analysis   |
| `dw.dim_user`           | Customer demographic and location attributes |
| `dw.dim_product`        | Product attributes                           |
| `dw.dim_payment_method` | Payment method reference                     |
| `dw.dim_voucher`        | Voucher or promotion reference               |
| `dw.dim_traffic`        | Traffic source and medium reference          |
| `dw.dim_location`       | Location reference dimension                 |

---

## Simplified Star Schema
<img width="923" height="690" alt="simplified_star_schema" src="https://github.com/user-attachments/assets/37d5050c-bf0f-4040-88c7-9e954964e929" />

---

## How to Run This Project

1. Clone Repository
2. Create Virtual Environment
3. Install Dependencies
4. Configure Environment Variables
5. Prepare PostgreSQL Database
 Create a PostgreSQL database:
```sql
CREATE DATABASE ecommerce_practice;
```
6. Run Pipeline or run each layer manually

---
## Key Learning & Design Decisions

During the development of this ecommerce data warehouse and analytical marts project, several improvements and design refinements were made to improve metric accuracy, warehouse consistency, and analytical reliability.

### Metric Validation & Aggregation Logic

* Refined Average Order Value (AOV) calculations after identifying discrepancies caused by `NULL` vs `0` behavior in aggregate functions
* Improved revenue calculations using filtered aggregation with `FILTER (WHERE status = 'completed')`
* Added defensive SQL handling using `COALESCE()` and `NULLIF()` to avoid unexpected `NULL` outputs and division-by-zero issues
* Validated business metrics by cross-checking multiple calculation approaches (e.g., `AVG(revenue)` vs filtered transaction averages)

### Grain & Data Modeling

* Improved understanding of fact table grain and aggregation behavior across:

  * session-level analytics
  * transaction-level analytics
  * item-level analytics
* Prevented potential double counting by pre-aggregating transaction metrics before joining session-level marts
* Differentiated between behavioral fact tables and descriptive dimension tables to improve warehouse design consistency

### Funnel Analytics Improvements

* Redesigned funnel modeling logic by treating `cancel` as an alternative outcome instead of a sequential funnel step
* Standardized funnel marts to use `dw.fact_events` instead of staging tables for better warehouse layering consistency
* Removed redundant joins from segmented funnel analysis to simplify session-level aggregation logic
* Implemented session-level binary funnel flags using conditional aggregation patterns

### Sentinel & Unknown Entity Handling

* Excluded sentinel users (`user_key = -1`) from customer-centric marts such as:

  * customer segmentation
  * cohort analysis
  * retention-style metrics
* Preserved sentinel rows within the warehouse layer to maintain referential integrity while filtering them from business-facing analytics

### Warehouse Architecture & Analytical Design

* Built layered warehouse architecture:

  * raw ingestion
  * staging layer
  * dimensional warehouse layer
  * business-facing marts
* Designed marts for multiple business stakeholders including:

  * management
  * marketing
  * product analytics
  * customer analytics
* Implemented reusable analytical patterns for:

  * funnel analysis
  * traffic performance
  * cohort analysis
  * product performance
  * payment behavior analysis

### Development Workflow Improvements

* Migrated SQL development workflow into VSCode with Git integration for:

  * version control
  * query organization
  * reproducible development workflow
* Improved commit structure using semantic commit messages (`feat`, `fix`, `refactor`, `docs`)
* Began organizing SQL scripts into modular project layers for maintainability and scalability

### Key Technical Concepts Reinforced

* Fact vs dimension modeling
* Session vs transaction grain awareness
* Aggregation correctness
* Defensive SQL patterns
* Filtered aggregation
* Join duplication risks
* Event-based behavioral analytics
* Business-oriented mart design

---

## Future Improvements

Potential improvements:
1. Add orchestration using Airflow or Prefect.
2. Add automated testing with dbt tests or Great Expectations.
3. Convert SQL transformations into dbt models.
4. Add incremental loading instead of full rebuild.
5. Add dashboard visualization using Power BI, Tableau, Metabase, or Looker Studio.
6. Add CI/CD checks for SQL syntax and data quality.
7. Add Docker setup for easier local environment replication.
8. Add source freshness checks.
9. Add data lineage documentation.
