import psycopg2
from common.database.postgres_database import PostgresDatabase
from common.database.db_task_manager import TaskManagerRepository

database = PostgresDatabase(
    dbname="targetdb",
    user="ubuntu",
)
print("Connected to database")
task_manager = TaskManagerRepository(database)



print(task_manager.get_hist_miadj_pricing(
    start="2020-05-05",
    end="2020-08-06",
    ls_ids=[24937]
))