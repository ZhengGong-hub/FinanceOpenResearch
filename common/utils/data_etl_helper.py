from common.utils.logging import get_logger, log_execution_time, log_api_call
import pandas as pd
import duckdb
from pathlib import Path

# Initialize logger and paths
logger = get_logger(__name__)

@log_execution_time
def load_data(path: Path):
    """Load data from parquet file."""
    logger.info(f"Loading data from {path}")
    
    # if it is a parquet file, load it using pandas
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
        logger.info(f"Loaded data from {path}")
        logger.info(f"Data shape: {df.shape}")
        # start a new line and then print the first 5 rows of the dataframe, within the same logger.info
        print(f"\n{df.head()}")
        return df
    # if it is a duckdb file, load it using duckdb
    elif path.suffix == ".duckdb":
        return duckdb.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")


@log_execution_time
def save_df_to_duckdb(df: pd.DataFrame, db_path: str, table_name: str):
    """
    Save data to a DuckDB database file that both Python and R can access.
    
    Args:
        data: DataFrame to save
        db_path: Path to the DuckDB database file, which can include a table name
                   in the format 'path/to/database.duckdb:table_name'
        table_name: Name of the table in the DuckDB database
    """
    logger.info(f"Saving data to DuckDB database: {db_path} with table name: {table_name}")

    # Create a DuckDB database file
    con = duckdb.connect(database=db_path)
    
    # Register the DataFrame as a table in the database
    con.register('data_table', df)
    
    # Create a permanent table from the DataFrame
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM data_table")
    
    # Close the connection
    con.close()
    
    logger.info(f"Data saved successfully to DuckDB database: {db_path} with table name: {table_name}")
