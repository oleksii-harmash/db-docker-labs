import time
import settings
import sqlalchemy
import pandas as pd


def compare_region(subject: str, status: str, table: str):
    try:
        engine = sqlalchemy.create_engine(f'postgresql://{settings.user}:{settings.password}'
                                      f'@{settings.host}:5432/{settings.database}')
        query = f'SELECT regname, year, ROUND(AVG({subject}ball100), 2) AS avg_mark FROM {table} ' \
                f"WHERE {subject}teststatus = '{status}' " \
                'GROUP BY regname, year ' \
                'ORDER BY regname, year'
        df = pd.read_sql(query, engine)
        df.to_csv(f'avg_{subject}_by_region.csv', index=False)
        print(f'Comparative statistics successfully saved as "avg_{subject}_by_region.csv" file.')
        print('Connection closed.')
    except Exception:
        print('Database connection error. Trying to reconnect...')
        time.sleep(2)
        compare_region(subject, status, table)
