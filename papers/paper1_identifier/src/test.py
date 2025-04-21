import pandas as pd
import numpy as np
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri

# Enable conversion of pandas to R data.frame
pandas2ri.activate()

# Prepare Python dataframe
df = pd.DataFrame({
    'y': np.random.randn(100),
    'x1': np.random.randn(100),
    'x2': np.random.randn(100),
})

# Inject the dataframe into R's environment
robjects.globalenv['df'] = pandas2ri.py2rpy(df)

# Run regression in R
robjects.r('''
    model <- lm(y ~ x1 + x2, data = df)
    summary_model <- summary(model)
''')

# Get coefficients back into Python
coefs = robjects.r('summary_model$coefficients')
coefs_df = pandas2ri.rpy2py(coefs)

print(coefs_df)