import glob
import pandas as pd

data_path = "papers/ml_forecast_estimate_error/data/universe/big500_data_per_year/*.csv"

universe_per_year = []
for universe_per_year_path in glob.glob(data_path):
    universe_per_year.append(pd.read_csv(universe_per_year_path, index_col=0))

universe = pd.concat(universe_per_year)
print(universe.tail(50))

universe.to_csv("papers/ml_forecast_estimate_error/data/universe/big500_data_2012_2022.csv", index=False)






