import pandas as pd
import numpy as np
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
from rpy2.robjects.packages import importr, isinstalled

from config.paths import ProjectPaths
from pathlib import Path
paths = ProjectPaths().get_paper_paths("paper1_identifier")

# Add this function to check and install package if needed
def ensure_r_package(package_name):
    if not isinstalled(package_name):
        utils = importr('utils')
        utils.install_packages(package_name)

def load_data(path: Path):
    return pd.read_parquet(path)

def prepare_data(data: pd.DataFrame):
    data['hetero_esg'] = data['esg_esg_pctg'] * data['msci_esg_WEIGHTED_SCORE']
    data['hetero_esg_e'] = data['esg_e_pctg'] * data['msci_esg_ENVIRONMENTAL_PILLAR_SCORE']
    data['hetero_esg_s'] = data['esg_s_pctg'] * data['msci_esg_SOCIAL_PILLAR_SCORE']
    data['hetero_esg_g'] = data['esg_g_pctg'] * data['msci_esg_GOVERNANCE_PILLAR_SCORE']
    return data

def run_regression(data: pd.DataFrame):
    # Check and install fixest if needed
    ensure_r_package('fixest')
    
    # Convert pandas DataFrame to R dataframe
    with localconverter(robjects.default_converter + pandas2ri.converter):
        r_data = pandas2ri.py2rpy(data)
    
    # Inject the dataframe into R's environment
    robjects.globalenv['data'] = r_data
    
    # Run fixed effects regression in R
    robjects.r('''
        library(fixest)
        model1 <- feols(recs_panel_rec_code ~ msci_esg_WEIGHTED_SCORE + esg_esg_pctg + hetero_esg
                      + 1 | isin + recs_panel_beg_date + recs_panel_AMASKCD, data = data)
        model1_summary <- summary(model1)
    ''')
    
    # Get the model summary back into Python
    model1_summary = robjects.r('model1_summary')
    
    return model1_summary

def run():
    data = load_data(paths["data"]["processed"] / "ibes_recs_monthly_with_analyst_esg_score_with_msci_esg_v2_trimmed.parquet")
    data = prepare_data(data)
    model1_summary = run_regression(data)
    print(model1_summary)

if __name__ == "__main__":
    run()

