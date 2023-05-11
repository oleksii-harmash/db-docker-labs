import sys
import time
import psycopg2
import sqlalchemy
import pandas as pd
import settings
from query import compare_region


TRANSACTION_SIZE = 100
TABLE_NAME = 'open_data_zno'
SUBJECT = 'eng'
STATUS = 'Зараховано'


def connect() -> psycopg2.connect:
    try:
        connection = psycopg2.connect(host=settings.host, database=settings.database, user=settings.user,
                                      password=settings.password)
        print('Database connection successful.')
        return connection
    except Exception:
        print('Database connection error. Trying to reconnect...')
        time.sleep(2)
        return connect()


def process_csv(df: pd.DataFrame, year: int) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    ball_cols = [col for col in df.columns if 'ball' in col]
    for col in ball_cols:
        df[col] = df[col].apply(lambda x: str(x).replace(',', '.')).astype('float64')
    df[['birth', 'ukradaptscale']] = df[['birth', 'ukradaptscale']].astype('int64')
    df.insert(3, 'year', [year for _ in range(len(df))])
    return df


def read_csv() -> pd.DataFrame:
    try:
        print('Reading .csv files...')
        df19 = process_csv(pd.read_csv('data/Odata2019File.csv', sep=';', encoding='cp1251', dtype='object'), 2019)
        df20 = process_csv(pd.read_csv('data/Odata2020File.csv', sep=';', encoding='cp1251', dtype='object'), 2020)
        print('Successful processed .csv files.')
        return pd.concat([df19, df20])
    except Exception:
        sys.exit('Something went wrong with reading .csv file...')


def create_table(name: str, df: pd.DataFrame, connection: psycopg2.connect):
    try:
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS {} ({});'.format(name, ','.join([f'{k} {v}' for k, v in column_types.items()])))
        cursor.execute(f'CREATE TABLE IF NOT EXISTS {name}_logs (rows_added INT, rows_left INT);')
        cursor.execute(f'INSERT INTO {name}_logs (rows_added, rows_left) SELECT 0, {len(df)} '
                                                f'WHERE NOT EXISTS (SELECT * FROM {name}_logs);')
        connection.commit()
        cursor.close()
        connection.close()
    except psycopg2.OperationalError:
        print("Lost database connection. Table hasn't created.")
        create_table(name, df, connect())
    print(f'Table "{name}" successfully created/connected.')


def export_time(start_time: float):
    with open('processing_time.txt', 'w') as f:
        f.write(f"{round(time.time() - start_time, 3) / 60} minutes")


def print_progress(rows_added: int, df_length: int, name: str) -> bool:
    progress = rows_added * 100 / df_length
    for percent in progress_bar:
        if progress >= percent:
            print(f'{percent}% of data committed into "{name}"')
            return True
    return False


def insert_into(name: str, df: pd.DataFrame, connection: psycopg2.connect):
    global progress_bar
    t_size = TRANSACTION_SIZE
    try:
        while True:
            cursor = connection.cursor()
            cursor.execute(f'SELECT * FROM {name}_logs LIMIT 1;')
            rows_added, rows_left = cursor.fetchone()

            if rows_left < t_size:
                t_size = rows_left
            for _, row in df[rows_added:rows_added+t_size].iterrows():
                query_insert = 'INSERT INTO {} ({}) VALUES ({});'.format(name, ','.join(column_types.keys()),
                                                                         ', '.join(['%s' for _ in column_types.keys()]))
                cursor.execute(query_insert, list(row))
            cursor.execute(f'UPDATE {name}_logs SET rows_added = {rows_added + t_size}, rows_left = {rows_left - t_size};')
            connection.commit()
            cursor.close()

            if print_progress(rows_added + t_size, len(df), name):
                progress_bar = progress_bar[1:]
            if t_size == rows_left:
                break
        connection.close()
    except psycopg2.OperationalError:
        connection.rollback()
        insert_into(name, df, connect())
    print(f'All data successfully inserted.')


if __name__ == '__main__':
    progress_bar = [_ for _ in range(10, 101, 10)]
    sql_types = {'object': 'TEXT', 'int64': 'INT', 'float64': 'DOUBLE PRECISION'}
    start_time = time.time()

    df = read_csv()
    column_types = {col: sql_types[df[col].dtype.name] for col in df.columns}
    create_table(TABLE_NAME, df, connect())
    insert_into(TABLE_NAME, df, connect())
    compare_region(SUBJECT, STATUS, TABLE_NAME)

    export_time(start_time)
