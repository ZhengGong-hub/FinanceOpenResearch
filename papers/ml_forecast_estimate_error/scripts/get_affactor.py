"""
This script retrieves and processes alpha factors data from a database.

The script:
1. Connects to a PostgreSQL database
2. Loads universe data from a CSV file
3. Loads alpha factor definitions from a CSV file
4. Processes factors in chunks of 50 to avoid memory issues
5. Retrieves monthly factor data for the specified date range
6. Saves the results to separate CSV files

Input files:
- big500_data_2012_2022.csv: Contains universe company IDs
- affactor.csv: Contains factor definitions

Output:
- Multiple CSV files in the affactor directory, each containing data for 50 factors
"""

import os
import psycopg2
import numpy as np
import pandas as pd
from common.database.postgres_database import PostgresDatabase
from common.database.db_task_manager import TaskManagerRepository

# Initialize database connection
database = PostgresDatabase(
    dbname="targetdb",
    user="ubuntu",
)
print("Connected to database")
task_manager = TaskManagerRepository(database)

# Load universe data and extract unique company IDs
universe = pd.read_csv("papers/ml_forecast_estimate_error/data/universe/big500_data_2012_2022.csv", index_col=0)
cids = universe.companyid.unique()

# Load factor definitions
affactor = pd.read_csv("papers/ml_forecast_estimate_error/data/affactor.csv")

# Split factors into chunks of 50 for processing
affactor_group = [affactor.factorid.unique()[i:i+50] for i in range(0, len(affactor.factorid.unique()), 50)]

# Create output directory
os.makedirs("papers/ml_forecast_estimate_error/data/affactor", exist_ok=True)

# Process each group of factors
ind = 0
for group in affactor_group:
    print(f"Processing factor group {ind}: {group}")
    affactor_df = task_manager.get_afl_factor_monthly_period(
        begin="2011-01-01",
        end="2023-01-01",
        factorids=group,
        ls_ids=cids
    )
    affactor_df.to_csv(f"papers/ml_forecast_estimate_error/data/affactor/affactor_{ind}.csv", index=False)
    ind += 1