import pandas as pd
import numpy as np
import subprocess
import os
import shutil
from pathlib import Path
import tempfile
import time
import duckdb

from common.r_helper.r_package_helper import check_r_package
from config.paths import ProjectPaths
from common.utils.logging import get_logger, log_execution_time, log_api_call
from common.utils.data_etl_helper import load_data, save_df_to_duckdb

# Initialize logger and paths
logger = get_logger(__name__)
paths = ProjectPaths().get_paper_paths("paper1_identifier")

@log_execution_time
def run_r_script(r_script_path: Path, db_path: Path, table_name: str, output_dir: str):
    """
    Run the R script with the DuckDB database path.
    
    Args:
        r_script_path: Path to the R script
        db_path: Path to the DuckDB database
        table_name: Name of the table in the DuckDB database
        output_dir: Path to the output directory
    """
    logger.info(f"Running R script: {r_script_path}")
    
    # Run the R script with the database path and table name as command line arguments
    cmd = ["Rscript", str(r_script_path), str(db_path), table_name, str(output_dir)]
    logger.info(f"Running command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
        )
        logger.info("R script executed successfully")
        logger.info(result.stdout)
        
        if result.stderr:
            logger.warning(f"R script stderr: {result.stderr}")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"R script failed with error: {e}")
        raise

@log_execution_time
def run():
    """
    Main execution function.
    """
    # Setup parameters for the run
    duckdb_path = paths["data"]["processed"] / "temp.duckdb"
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    rsrc_path = Path(__file__).parent / "Rsrc"
    r_script_path = rsrc_path / "msci.R"

    # table_name = "refinitiv_analysis_data"
    table_name = "msci_analysis_data"

    # Check for required R packages
    required_packages = ["fixest", "dplyr", "duckdb", "modelsummary"]
    for package in required_packages:
        check_r_package(package)

    # Process data
    data = load_data(paths["data"]["processed"] / 
                    "ibes_recs_monthly_with_analyst_esg_score_with_msci_esg_v2_trimmed.parquet")

    print(data.head())
    print(data.columns)
    
    save_df_to_duckdb(data, db_path=str(duckdb_path), table_name=table_name)

    # Run the R script
    run_r_script(r_script_path, db_path=str(duckdb_path), table_name=table_name, output_dir=paths["data"]["regression"])
    
    logger.info("Analysis pipeline completed successfully")


if __name__ == "__main__":
    run()
