from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv(BASE_DIR / ".env")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_practice")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

BASE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = BASE_DIR / "Sql"

def run_sql_file(filename):
    path = SQL_DIR / filename

    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    print(f"Running {filename}...")

    sql = path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"Done: {filename}\n")