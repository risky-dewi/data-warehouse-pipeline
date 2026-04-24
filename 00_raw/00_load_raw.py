import pandas as pd
from sqlalchemy import create_engine, inspect, text
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "Data"

# validate that Data folder exists
if not DATA_DIR.exists():
    raise FileNotFoundError(f"Data folder not found: {DATA_DIR}")

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@localhost:5432/ecommerce_practice"
    )

inspector = inspect(engine)

# mapping files into table
files = {
    "voucher1.csv": "voucher",
    "products1.csv": "products",
    "locations1.csv": "locations",
    "payment_methods1.csv": "payment_methods",
    "user1.csv": "users",
    "sessions1.csv": "sessions",
    "events1.csv": "events",
    "transactions_items1.csv": "transaction_items",
    "transactions1.csv": "transactions"
}

column_map = {
    "voucher": ["voucher_id", "voucher"],
    "products": ["product_id", "product_name", "product_category"],
    "locations": ["locations_id", "location"],
    "payment_methods": ["payment_method_id", "payment_method"],
    "users": ["user_id", "date", "locations_id", "age", "gender"],
    "sessions": ["sessions_id", "user_id", "traffic_medium", "date", "traffic_source", "traffic_name"],
    "events": ["sessions_id", "event_id", "event", "date"],
    "transaction_items": ["transactions_id", "transaction_items_id", "product_id", "product_qty", "product_price", "product_amount"],
    "transactions": ["transactions_id", "sessions_id", "payment_method_id", "total_amount", "transactions_timestamps", "status", "voucher_id"]
}

def transform(df, table):
    columns = column_map.get(table)
    
    if not columns:
        raise ValueError(f"No column mapping defined for table: {table}")
    
    return df[columns]

#-- validation step: number of row CVS vs PostgreSQL--
def validate_row_count(conn, table, csv_count):
    result=conn.execute(
        text(f"SELECT COUNT(*) FROM raw.{table}")
    )
    db_count = result.scalar()

    if csv_count == db_count:
        print(f"Validation OK : CSV {csv_count:,} rows = PostgreSQL {db_count:,} rows")
    else:
        raise ValueError(
            f" Validation failed: CSV CSV {csv_count:,} rows != PostgreSQL {db_count:,} rows"
        )

#-- check all table that exist before loading --
existing_tables = inspector.get_table_names(schema="raw")

for file, table in files.items():
    if table not in existing_tables:
        raise ValueError(f"Table raw.{table} not in database!"
                         f" Run DDL first!")

#-- check all file exist before loading --
for file in files.keys():
    file_path = DATA_DIR / file
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
print("All table and files found. Start loading...\n")

#-- Load process per table ---
for file, table in files.items():
    print(f"Loading {file} -> raw.{table}")

    file_path = DATA_DIR / file

    df = pd.read_csv(
        file_path,
        keep_default_na=True,
        na_values=['', ' ', 'NULL', 'null', 'None', 'none', 'NA', 'N/A']
    )

    df = transform(df, table)
    csv_count = len(df)

    
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE raw.{table}"))

        df.to_sql(
            name=table,
            con=conn,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi"
        )

        validate_row_count(conn, table, csv_count)

    print(f" {csv_count:,} line -> raw.{table} done.\n")

print("=" * 50)
print("All files uploaded.")
print("=" * 50)