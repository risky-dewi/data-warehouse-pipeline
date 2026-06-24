from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import subprocess
import sys
import os

BASE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = BASE_DIR / "sql"
SCRIPT_DIR = BASE_DIR / "scripts"

load_dotenv(BASE_DIR / ".env")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def run_sql_file(relative_path):
    path = SQL_DIR / relative_path

    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    print(f"\nRunning SQL: {relative_path}")
    print("=" * 60)

    sql = path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"Done: {relative_path}")


def run_python_file(filename):
    path = SCRIPT_DIR / filename

    if not path.exists():
        raise FileNotFoundError(f"Python file not found: {path}")

    print(f"\nRunning Python: {filename}")
    print("=" * 60)

    subprocess.run([sys.executable, str(path)], check=True)

    print(f"Done: {filename}")


if __name__ == "__main__":
    run_sql_file("00_raw/00_raw.sql")
    run_python_file("load_raw.py")
    run_sql_file("04_data_quality/01_check_after_raw.sql")

    run_sql_file("01_staging/01_staging.sql")
    run_sql_file("04_data_quality/02_check_after_staging.sql")

    run_sql_file("02_dwh/02_dwh.sql")
    run_sql_file("04_data_quality/03_check_after_dwh.sql")

    run_sql_file("03_mart/03_mart.sql")
    run_sql_file("04_data_quality/04_final_data_quality.sql")

    print("\nPIPELINE COMPLETED SUCCESSFULLY")