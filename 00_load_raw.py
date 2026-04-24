import pandas as pd
from sqlalchemy import create_engine, inspect, text
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"

engine = create_engine("postgresql://postgres:PostgreSQL_1999@localhost:5432/ecommerce_practice")

inspector = inspect(engine)

# mapping file ke table
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

def transform(df, table):
    if table == "voucher":
        return df[["voucher_id", "voucher"]]

    elif table == "products":
        return df[["product_id", "product_name", "product_category"]]

    elif table == "locations":
        return df[["locations_id", "location"]]

    elif table == "payment_methods":
        return df[["payment_method_id", "payment_method"]]

    elif table == "users":
        return df[["user_id", "date", "locations_id", "age", "gender"]]

    elif table == "sessions":
        return df[["sessions_id", "user_id", "traffic_medium", "date", "traffic_source", "traffic_name"]]

    elif table == "events":
        return df[["sessions_id", "event_id", "event", "date"]]

    elif table == "transaction_items":
        return df[["transactions_id", "transaction_items_id", "product_id", "product_qty", "product_price", "product_amount"]]

    elif table == "transactions":
        return df[["transactions_id", "sessions_id", "payment_method_id", "total_amount", "transactions_timestamps", "status", "voucher_id"]]

    return df

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
            f" Validation failed: CVS CSV {csv_count:,} rows != PostgreSQL {db_count:,} rows"
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