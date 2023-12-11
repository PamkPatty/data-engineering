import duckdb
import os
import glob
import shutil

DATA_FOLDER = "data"
DB_FILE = "electro_cars.db"
TABLE_NAME = "table_cars"
PARQUET_FOLDER = "parquet_files"

def get_csv(data):
    csv_files = glob.glob(os.path.join(data, "*.csv"))
    for csv_file in csv_files:
        return csv_file


def create_duckdb(conn, csv_path): 
    conn.execute(f"""
        CREATE TABLE {TABLE_NAME} AS 
        SELECT * 
        FROM read_csv_auto('{csv_path}')
    """)
    print(conn.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 5;").fetchall())


def make_first(conn):
    result = conn.execute(f"""
        SELECT City, COUNT(*) as num_cars
        FROM {TABLE_NAME}
        GROUP BY City
    """).fetchdf()
    print(result)
    result.to_parquet(os.path.join(PARQUET_FOLDER,'reault1.parquet'))

def make_second(conn):
    result = conn.execute(f"""
        SELECT Make, COUNT(*) as num_cars
        FROM {TABLE_NAME}
        GROUP BY Make
        ORDER BY num_cars DESC
        LIMIT 3
    """).fetchdf()
    print(result)
    result.to_parquet(os.path.join(PARQUET_FOLDER,'reault2.parquet'))

def make_third(conn):
    result = conn.execute(f"""
        SELECT "Postal Code" as PostalCode, Make, COUNT(*) as num_cars
        FROM {TABLE_NAME}
        GROUP BY PostalCode, Make
        QUALIFY ROW_NUMBER() OVER(PARTITION BY PostalCode ORDER BY num_cars DESC) = 1
    """).fetchdf()
    print(result)
    result.to_parquet(os.path.join(PARQUET_FOLDER,'reault3.parquet'))

def make_fourth(conn):
    result = conn.execute(f"""
        SELECT "Model Year" as Model_Year, COUNT(*) as num_cars
        FROM {TABLE_NAME}
        GROUP BY Model_Year
    """).fetchdf()
    print(result)
    result.to_parquet(os.path.join(PARQUET_FOLDER,'reault4.parquet'))


def main():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    conn = duckdb.connect(DB_FILE)
    create_duckdb(conn, get_csv(DATA_FOLDER))    
    if os.path.exists(PARQUET_FOLDER):
        shutil.rmtree(PARQUET_FOLDER)
    os.makedirs(PARQUET_FOLDER)
    make_first(conn)
    make_second(conn)
    make_third(conn)
    make_fourth(conn)
    conn.close()


if __name__ == "__main__":
    main()
