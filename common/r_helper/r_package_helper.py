import subprocess
import tempfile
import shutil
from pathlib import Path
from common.utils.logging import get_logger, log_execution_time

logger = get_logger(__name__)

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