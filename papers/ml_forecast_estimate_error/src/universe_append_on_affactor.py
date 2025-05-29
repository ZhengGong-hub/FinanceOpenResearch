import pandas as pd
import glob

universe_path = "papers/ml_forecast_estimate_error/data/output_data/universe_df/universe_df.csv"
affactor_paths = glob.glob("papers/ml_forecast_estimate_error/data/affactor/*.csv")
ref_affactor_path = "papers/ml_forecast_estimate_error/data/ref_affactor.csv"

output_path = "papers/ml_forecast_estimate_error/data/output_data/universe_df/universe_with_affactor.parquet"

universe = pd.read_csv(universe_path)
# drop universe earningsdate that is none 
universe = universe.dropna(subset = ['EPS_actual_et'])
print("the length of universe is ", len(universe))

universe['EPS_actual_et'] = pd.to_datetime(universe['EPS_actual_et'], utc=True)
universe['EPS_actual_et'] = universe['EPS_actual_et'].dt.tz_convert('America/New_York')  # 若你确实想保留 ET
universe['EPS_actual_et'] = universe['EPS_actual_et'].dt.tz_localize(None)

# sort universe by EPS_et
universe.sort_values(by='EPS_actual_et', ascending=True, inplace=True)

print("the length of universe is ", len(universe))

counter = 0
for affactor_path in affactor_paths:
    print(f"this is the {counter} file to be appended! the file is {affactor_path}")

    affactor = pd.read_csv(affactor_path)
    affactor['asofdate'] = pd.to_datetime(affactor['asofdate'])
    print("the length of affactor is ", len(affactor))

    ref_affactor = pd.read_csv(ref_affactor_path)

    affactor = pd.merge(affactor, ref_affactor[['factorabbreviation', 'factorid']], on="factorid", how="left")
    affactor['factorvalue'] = affactor['factorvalue'].round(3)

    # affactor 
    #        factorvalue  factorid  objectid    asofdate  securityid   gvkey  iid  companyid  factorabbreviation
    # 0          NaN       914   2610835  2022-01-31     2610834    4517    1     270586  PB_PatentstoMktCap
    # 1    -0.367379       460   2666094  2021-12-31     2666093  137310    1      31158             OCFStab
    # 2     0.253532       502   2666094  2021-12-31     2666093  137310    1      31158                BVEV
    # 3          NaN       914   2666094  2021-12-31     2666093  137310    1      31158  PB_PatentstoMktCap
    # 4          NaN       921   2666094  2021-12-31     2666093  137310    1      31158     HF_FacilitiesGr

    
    # drop dubplicates on companyid, asofdate, factorabbreviation
    affactor = affactor.drop_duplicates(subset=['companyid', 'asofdate', 'factorabbreviation'])

    # show duplicates
    # print(affactor[affactor.duplicated(subset=['companyid', 'asofdate', 'factorabbreviation'], keep=False)])

    # pivot the affactor so that columns are companyid, asofdate, many factorabbreviation as columns and then value is factorvalue
    affactor = affactor.pivot(index=['companyid', 'asofdate'], columns='factorabbreviation', values='factorvalue').reset_index()
    affactor.sort_values(by='asofdate', ascending=True, inplace=True)

    # we should find a way to append the affactor to the universe
    # we append the affactor when affactor asofdate is smaller than EPS_et, they both are datetime
    universe = pd.merge_asof(universe, affactor, left_on='EPS_actual_et', right_on='asofdate', by='companyid', tolerance=pd.Timedelta("30 days"))
    universe.rename(columns = {"asofdate": f"affactor_asofdate_{counter}"}, inplace=True)

    print(universe)

    counter = counter + 1 


universe.to_parquet(output_path)
