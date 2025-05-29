import psycopg2
import pandas as pd
from common.database.postgres_database import PostgresDatabase
from common.database.db_task_manager import TaskManagerRepository

def calculate_technical(task_manager, companyid = 32307, start_date = '2018-01-01', end_date = '2023-06-01', rolling_window = 252):
    #   FOR ONE STOCK
    # step 1: pull out daily return of one individual stock
    price = task_manager.get_hist_miadj_pricing(start_date, end_date, [companyid, ])[['divadjclose', 'pricedate']]
    if len(price) <= rolling_window:
        return 1 # price history too short 

    price['divadjclose'] = price['divadjclose'].astype(float)
    price['pricedate'] = pd.to_datetime(price['pricedate'])
    price['stock_ret'] = 100*(price['divadjclose'].pct_change())

    # step 2: calculate technical indicators
    #       divadjclose  pricedate  stock_ret
    # 0        4.930643 2018-01-02        NaN
    # 1        5.255148 2018-01-03   6.581390
    # 2        5.282849 2018-01-04   0.527133
    # 3        5.327617 2018-01-05   0.847418
    # 4        5.490859 2018-01-08   3.064067
    # ...           ...        ...        ...
    # 1358    37.964704 2023-05-25  24.369638
    # 1359    38.930315 2023-05-26   2.543444
    # 1360    40.094846 2023-05-30   2.991321
    # 1361    37.818763 2023-05-31  -5.676747
    print(price)
    

    return 0

if __name__ == "__main__":
    database = PostgresDatabase(
    dbname="targetdb",
    user="ubuntu",
    )
    print("Connected to database")

    task_manager = TaskManagerRepository(database)

    calculate_technical(task_manager)