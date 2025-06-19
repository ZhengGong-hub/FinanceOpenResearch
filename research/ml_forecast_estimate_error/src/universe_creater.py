import pandas as pd
import os

ADDR = "papers/ml_forecast_estimate_error/data/output_data/"

def load_universe(fys):
    res = []
    for fy in fys:   
        fy = int(fy)
        universe = pd.read_csv(f'papers/ml_forecast_estimate_error/data/universe/1b_2008_2022/universe_fy{fy}.csv', index_col = 0)
        res.append(universe)
    universe = pd.concat(res).drop_duplicates(subset=['companyid'], keep = 'first')
    print('universe loaded! The universe has '+str(len(universe))+' companies!')
    return universe

# -------- Helper function to load and process data -------- #
def load_and_process_data(filename, date_col, new_col_name, verbose=False, add_date=False):
    """Helper function to load, process and merge data files"""
    # Load and sort data
    df = pd.read_csv(ADDR + filename, index_col=0).sort_values(by=[date_col], ascending=True)
    print(df.head())
    print(f"the length of {filename} is: ", len(df))

    if verbose:
        print(f"some of the examples of duplicates in appending {filename}:")
        print(df[df.duplicated(subset=["fiscalyear", "fiscalquarter", "companyid"], keep=False)]
                .sort_values(by=["fiscalyear", "fiscalquarter", "companyid"]))
    
    # Drop duplicates and rename columns
    df = df.drop_duplicates(subset=["companyid", "fiscalyear", "fiscalquarter"], keep="last")
    df.rename(columns={"dataitemvalue": new_col_name}, inplace=True)
    
    print(f"the length of {filename} after dropping duplicates is: ", len(df))

    if add_date:
        df[f'{new_col_name}_et'] = pd.to_datetime(df[date_col]).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    else:
        df.drop(columns=[date_col], inplace=True)
        
    merge_cols = ['companyid', 'fiscalyear', 'fiscalquarter', new_col_name]
    if add_date:
        merge_cols.append(f'{new_col_name}_et')
        
    return df[merge_cols]


def load_and_process_EPS(universe_df, verbose=False):

    # Process EPS actual data
    EPS_actual = load_and_process_data("EPS.csv", "effectivedate", "EPS_actual", verbose, add_date=True)
    universe_df = universe_df.merge(EPS_actual, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # Process EPS diff data
    EPS_diff = load_and_process_data("EPSDiff.csv", "asofdate", "EPSDiff", verbose)
    universe_df = universe_df.merge(EPS_diff, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS_surprise
    EPS_surprise = load_and_process_data("EPS_Surprise.csv", "asofdate", "EPS_surprise", verbose)
    universe_df = universe_df.merge(EPS_surprise, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS count 
    EPS_count = load_and_process_data("EPS_count.csv", "effectivedate", "EPS_count", verbose)
    universe_df = universe_df.merge(EPS_count, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS std
    EPS_std = load_and_process_data("EPS_std.csv", "effectivedate", "EPS_std", verbose)
    universe_df = universe_df.merge(EPS_std, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS_guidance
    EPS_guidance_high = load_and_process_data("EPS_guidance_high.csv", "effectivedate", "EPS_guidance_high", verbose)
    universe_df = universe_df.merge(EPS_guidance_high, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS_guidance_low
    EPS_guidance_low = load_and_process_data("EPS_guidance_low.csv", "effectivedate", "EPS_guidance_low", verbose)
    universe_df = universe_df.merge(EPS_guidance_low, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")
    return universe_df


def load_and_process_EPSNormalized(universe_df, verbose=False):

    # Process EPS actual data
    EPSNormalized_actual = load_and_process_data("EPSNormalized.csv", "effectivedate", "EPSNormalized_actual", verbose, add_date=True)
    universe_df = universe_df.merge(EPSNormalized_actual, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # Process EPS diff data
    EPSNormalized_diff = load_and_process_data("EPSNormalizedDiff.csv", "asofdate", "EPSNormalized_diff", verbose)
    universe_df = universe_df.merge(EPSNormalized_diff, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS_surprise
    EPSNormalized_surprise = load_and_process_data("EPSNormalized_Surprise.csv", "asofdate", "EPSNormalized_surprise", verbose)
    universe_df = universe_df.merge(EPSNormalized_surprise, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS count 
    EPSNormalized_count = load_and_process_data("EPSNormalized_count.csv", "effectivedate", "EPSNormalized_count", verbose)
    universe_df = universe_df.merge(EPSNormalized_count, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS std
    EPSNormalized_std = load_and_process_data("EPSNormalized_std.csv", "effectivedate", "EPSNormalized_std", verbose)
    universe_df = universe_df.merge(EPSNormalized_std, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS_guidance
    EPSNormalized_guidance_high = load_and_process_data("EPSNormalized_guidance_high.csv", "effectivedate", "EPSNormalized_guidance_high", verbose)
    universe_df = universe_df.merge(EPSNormalized_guidance_high, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS_guidance_low
    EPSNormalized_guidance_low = load_and_process_data("EPSNormalized_guidance_low.csv", "effectivedate", "EPSNormalized_guidance_low", verbose)
    universe_df = universe_df.merge(EPSNormalized_guidance_low, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")
    return universe_df

def load_and_process_revenue(universe_df, verbose=False):
    # Process EPS actual data
    revenue_actual = load_and_process_data("revenue.csv", "effectivedate", "revenue_actual", verbose, add_date=True)
    universe_df = universe_df.merge(revenue_actual, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # Process EPS diff data
    revenue_diff = load_and_process_data("revenueDiff.csv", "asofdate", "revenueDiff", verbose)
    universe_df = universe_df.merge(revenue_diff, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS_surprise
    revenue_surprise = load_and_process_data("revenue_Surprise.csv", "asofdate", "revenue_surprise", verbose)
    universe_df = universe_df.merge(revenue_surprise, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS count 
    revenue_count = load_and_process_data("revenue_count.csv", "effectivedate", "revenue_count", verbose)
    universe_df = universe_df.merge(revenue_count, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS std
    revenue_std = load_and_process_data("revenue_std.csv", "effectivedate", "revenue_std", verbose)
    universe_df = universe_df.merge(revenue_std, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS_guidance
    revenue_guidance_high = load_and_process_data("revenue_guidance_high.csv", "effectivedate", "revenue_guidance_high", verbose)
    universe_df = universe_df.merge(revenue_guidance_high, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")

    # EPS_guidance_low
    revenue_guidance_low = load_and_process_data("revenue_guidance_low.csv", "effectivedate", "revenue_guidance_low", verbose)
    universe_df = universe_df.merge(revenue_guidance_low, on=["companyid", "fiscalyear", "fiscalquarter"], how="left")
    return universe_df

def run(verbose=False):
    # load the universe
    start_year = 2008
    end_year = 2023
    fys = range(start_year, end_year)
    universe = load_universe(fys)['companyid'].tolist()
    # create df with columns: companyid, fiscalyear, fiscalquarter
    # the fiscal year is not exactly the same as the calendar year so we need to subtract 1 from the start year and add 1 to the end year
    idx = pd.MultiIndex.from_product([universe, range(start_year-1, end_year+1), range(1, 5)], 
                                   names=['companyid', 'fiscalyear', 'fiscalquarter'])
    universe_df = pd.DataFrame(index=idx).reset_index()
    print(universe_df.head())
    print("the length of universe_df is: ", len(universe_df))

    # load and process EPS
    universe_df = load_and_process_EPS(universe_df, verbose)
    # load and process EPSNormalized
    universe_df = load_and_process_EPSNormalized(universe_df, verbose)
    # load and process revenue
    universe_df = load_and_process_revenue(universe_df, verbose)

    
    # drop rows that none of the EPS_actual_et, EPSNormalized_actual_et, revenue_actual_et exist
    universe_df = universe_df[universe_df['EPS_actual_et'].notna() | universe_df['EPSNormalized_actual_et'].notna() | universe_df['revenue_actual_et'].notna()]
    os.makedirs(ADDR + "universe_df", exist_ok=True)
    universe_df.to_csv(ADDR + "universe_df/universe_df.csv")
    
    








if __name__ == "__main__":
    run(verbose=True)