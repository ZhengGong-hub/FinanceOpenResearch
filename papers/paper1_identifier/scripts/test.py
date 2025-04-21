# testing out the rpy2 library

import pandas as pd
import numpy as np
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

# Enable conversion of pandas to R data.frame
pandas2ri.activate()

# Prepare Python dataframe
df = pd.DataFrame({
    'y': np.random.randn(100),
    'x1': np.random.randn(100),
    'x2': np.random.randn(100),
})

# Inject the dataframe into R's environment
with localconverter(robjects.default_converter + pandas2ri.converter):
    robjects.globalenv['df'] = pandas2ri.py2rpy(df)

# Run regression in R
robjects.r('''
    model <- lm(y ~ x1 + x2, data = df)
    summary_model <- summary(model)
    coef_matrix <- as.data.frame(summary_model$coefficients)
''')

# Get coefficients back into Python
with localconverter(robjects.default_converter + pandas2ri.converter):
    coefs_df = robjects.r('coef_matrix')

print(coefs_df)