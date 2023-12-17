import pandas
from sqlalchemy import create_engine
import sqlite3
import duckdb
import psycopg2
from src.data_loader import upload_dataset
from src.data_boot import boot_file
from src.executor_query import execute_queries, ress
from src.quer import querSQl, sqlite_quer, pandas_quer


def main():
    upload_dataset()

    fp = boot_file()
    if 'Airport_fee' in fp.columns:
        fp = fp.drop(columns=['Airport_fee'])

    eng = create_engine('postgresql://postsql:postsql@localhost:5432/postsql')
    connect = psycopg2.connect(
        dbname='postsql',
        user='postsql',
        password='postsql',
        host='localhost',
        port='5432'
    )
    cursor = connect.cursor()

    sqlite_connect = sqlite3.connect('file/sqlite_db.db')
    sqlite_cursor = sqlite_connect.cursor()

    duckDB_connect = duckdb.connect(database='file/duckdb_db.db', read_only=False)
    duckDB_cursor = duckDB_connect.cursor()

    SQL_ress = ress(execute_queries(connect, cursor, querSQl))
    SQLite_ress = ress(execute_queries(sqlite_connect, sqlite_cursor, sqlite_quer))
    duckDB_ress = ress(execute_queries(duckDB_connect, duckDB_cursor, querSQl))
    pandas_ress = ress(execute_queries(None, fp, pandas_quer))
    SQLalchemy_ress = ress(execute_queries(eng, None, querSQl))

    res1 = {
        'Psycopg2': SQL_ress,
        'SQLite': SQLite_ress,
        'DuckDB': duckDB_ress,
        'Pandas': pandas_ress,
        'SQLAlchemy': SQLalchemy_ress
    }
    fp = pandas.DataFrame(res1)
    fp.index = [{i+1} for i in range(len(execute_queries(connect, cursor, querSQl)))]
    fp.to_csv('res.csv', index=False)

if __name__ == "__main__":
    main()