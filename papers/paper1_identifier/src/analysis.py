import pandas as pd
import numpy as np
import subprocess
import os
import shutil
from pathlib import Path
import tempfile
import time
import duckdb

from config.paths import ProjectPaths
from common.utils.logging import get_logger, log_execution_time, log_api_call

# Initialize logger and paths
logger = get_logger(__name__)
paths = ProjectPaths().get_paper_paths("paper1_identifier")

@log_execution_time
def load_data(path: Path):
    """Load data from parquet file."""
    logger.info(f"Loading data from {path}")
    try:
        con = duckdb.connect(database=':memory:')
        data = con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
        logger.info(f"Successfully loaded data with shape {data.shape}")
        return data
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

@log_execution_time
def prepare_data(data: pd.DataFrame):
    """Calculate heterogeneity measures."""
    logger.info("Preparing data - calculating heterogeneity measures")
    try:
        con = duckdb.connect(database=':memory:')
        con.register('data_table', data)
        
        query = """
        SELECT *,
            esg_esg_pctg * msci_esg_WEIGHTED_SCORE AS hetero_esg,
            esg_e_pctg * msci_esg_ENVIRONMENTAL_PILLAR_SCORE AS hetero_esg_e,
            esg_s_pctg * msci_esg_SOCIAL_PILLAR_SCORE AS hetero_esg_s,
            esg_g_pctg * msci_esg_GOVERNANCE_PILLAR_SCORE AS hetero_esg_g
        FROM data_table
        """
        
        return con.execute(query).fetchdf()
    except Exception as e:
        logger.error(f"Error in data preparation: {str(e)}")
        raise

@log_execution_time
def save_data_to_duckdb(data: pd.DataFrame, db_path: Path):
    """Save data to a DuckDB database file that both Python and R can access."""
    logger.info(f"Saving data to DuckDB database: {db_path}")
    try:
        # Create a DuckDB database file
        con = duckdb.connect(database=str(db_path))
        
        # Register the DataFrame as a table in the database
        con.register('data_table', data)
        
        # Create a permanent table from the DataFrame
        con.execute("CREATE TABLE IF NOT EXISTS analysis_data AS SELECT * FROM data_table")
        
        # Close the connection
        con.close()
        
        logger.info(f"Data saved successfully to DuckDB database: {db_path}")
        return db_path
    except Exception as e:
        logger.error(f"Error saving data to DuckDB: {str(e)}")
        raise

@log_execution_time
def check_r_package(package_name):
    """Check if an R package is installed and install it if not."""
    logger.info(f"Checking if R package '{package_name}' is installed")
    try:
        # Create a temporary R script to check for package
        temp_dir = Path(tempfile.mkdtemp())
        check_script = temp_dir / "check_package.R"
        
        with open(check_script, 'w') as f:
            f.write(f"""
            if (!require("{package_name}", quietly = TRUE)) {{
                install.packages("{package_name}", repos="https://cloud.r-project.org")
                if (!require("{package_name}", quietly = TRUE)) {{
                    stop(paste("Failed to install package:", "{package_name}"))
                }}
            }}
            cat("Package {package_name} is available\\n")
            """)
        
        # Run the check script
        result = subprocess.run(
            ['Rscript', str(check_script)],
            capture_output=True,
            text=True
        )
        
        # Clean up
        shutil.rmtree(temp_dir)
        
        if result.returncode != 0:
            logger.error(f"Error checking/installing R package '{package_name}': {result.stderr}")
            return False
        
        logger.info(f"R package '{package_name}' is available")
        return True
    except Exception as e:
        logger.error(f"Error checking R package '{package_name}': {str(e)}")
        return False

@log_execution_time
def run_r_script(r_script_path: Path, db_path: Path, output_dir: Path):
    """
    Run the R script with the DuckDB database path.
    
    Args:
        r_script_path: Path to the R script
        db_path: Path to the DuckDB database
        output_dir: Directory for output files
    """
    logger.info(f"Running R script: {r_script_path}")
    
    # Ensure the output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run the R script with the database path as a command line argument
    cmd = ["Rscript", str(r_script_path), str(db_path)]
    logger.info(f"Running command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("R script executed successfully")
        logger.info(result.stdout)
        
        if result.stderr:
            logger.warning(f"R script stderr: {result.stderr}")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"R script failed with error: {e}")
        logger.error(f"R script stdout: {e.stdout}")
        logger.error(f"R script stderr: {e.stderr}")
        raise

@log_execution_time
def run():
    """Main execution function."""
    logger.info("Starting analysis pipeline")
    try:
        # Setup output directory
        output_dir = paths["data"]["processed"] / "analysis_output"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process data
        data = load_data(paths["data"]["processed"] / 
                        "ibes_recs_monthly_with_analyst_esg_score_with_msci_esg_v2_trimmed.parquet")
        data = prepare_data(data)
        
        # Save data to DuckDB and run R analysis
        db_path = save_data_to_duckdb(data, output_dir / "analysis_data.duckdb")
        r_script_path = Path(__file__).parent / "Rsrc" / "msci.R"
        r_output = run_r_script(r_script_path, db_path, output_dir)
        
        print(r_output)
        logger.info("Analysis pipeline completed successfully")
    except Exception as e:
        logger.critical(f"Analysis pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    run()
