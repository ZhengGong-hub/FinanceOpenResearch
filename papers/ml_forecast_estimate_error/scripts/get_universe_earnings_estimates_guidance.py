# need proper logging!
import json
import pandas as pd 
import datetime
from datetime import date, timedelta
import logging
import os
from common.database.postgres_database import PostgresDatabase
from common.database.db_task_manager import TaskManagerRepository

DATA_OUTPUT_DIR = 'papers/ml_forecast_estimate_error/data/output_data'
os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)

## ---------- Get the data for earnings estimates and guidance ---------- ##
def get_universe_earnings_estimates_guidance(task_manager):
    with open('papers/ml_forecast_estimate_error/data/dataitemid.json', 'r') as f:
        dataitemid = json.load(f)

    fys = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]

    regenerate = False
    if regenerate:
        for fy in fys:
            fy = int(fy)
            print('the year being processed: '+str(fy))

            ## ---------- Get the company universe ---------- ##
            universe = task_manager.query_global_market_cap(asofdate=f'{fy}-01-01', mktcap_thres=1e3).sort_values(by='marketcap').drop_duplicates(subset=['companyid'], keep = 'last') # sort from small to large
            universe = universe.query("currency == 'USD'")
            universe = universe.query("exchange != 'OTCPK'")
            universe.drop(columns = ['currency', 'country'], inplace = True)
            universe.to_csv(f'papers/ml_forecast_estimate_error/data/universe/1b_2008_2022/universe_fy{fy}.csv')
            print('universe loaded! The universe has '+str(len(universe))+' companies!')
    else:
        res = []
        for fy in fys:   
            fy = int(fy)
            universe = pd.read_csv(f'papers/ml_forecast_estimate_error/data/universe/1b_2008_2022/universe_fy{fy}.csv', index_col = 0)
            res.append(universe)
        universe = pd.concat(res).drop_duplicates(subset=['companyid'], keep = 'first')
        print(universe)
        print('universe loaded! The universe has '+str(len(universe))+' companies!')

    # -------- Helper function to load or fetch data -------- #
    def get_data(dataitem_key, analysts_estimate=False):
        """Helper function to load or fetch data (estimates or guidance)"""
        filename = f'{dataitem_key}.csv'
        if os.path.exists(os.path.join(DATA_OUTPUT_DIR, filename)):
            return pd.read_csv(os.path.join(DATA_OUTPUT_DIR, filename), index_col=0)
        else:
            if analysts_estimate:
                data = task_manager.get_estimatediff_ref_co(
                    universe['companyid'].values, 
                    [dataitemid[dataitem_key]], 
                    '2008-01-01', 
                    '2022-12-31'
                )
            else:
                data = task_manager.get_act_q_ref_co(
                    universe['companyid'].values, 
                    [dataitemid[dataitem_key]], 
                    '2008-01-01'
                )
            data.to_csv(os.path.join(DATA_OUTPUT_DIR, filename))
            return data

    # -------- EPS normalized estimates --------- #
    EPSnormalized = get_data('EPSNormalized')
    EPSnormalizedDiff = get_data('EPSNormalizedDiff', analysts_estimate=True)
    EPSNormalized_Surprise = get_data('EPSNormalized_Surprise', analysts_estimate=True)
    EPSNormalized_count = get_data('EPSNormalized_count')
    EPSNormalized_std = get_data('EPSNormalized_std')

    # -------- EPS estimates --------- #
    EPS = get_data('EPS')    
    EPSDiff = get_data('EPSDiff', analysts_estimate=True)    
    EPS_Surprise = get_data('EPS_Surprise', analysts_estimate=True)
    EPS_count = get_data('EPS_count')
    EPS_std = get_data('EPS_std')

    # -------- revenue estimates --------- #
    revenue = get_data('revenue')    
    revenueDiff = get_data('revenueDiff', analysts_estimate=True)    
    revenue_Surprise = get_data('revenue_Surprise', analysts_estimate=True)
    revenue_count = get_data('revenue_count')
    revenue_std = get_data('revenue_std')

    # -------- Revenue guidance --------- #
    revenue_guidance_high = get_data('revenue_guidance_high')
    revenue_guidance_mid = get_data('revenue_guidance_mid')
    revenue_guidance_low = get_data('revenue_guidance_low')

    # -------- EPS normalized guidance --------- #
    EPSNormalized_guidance_high = get_data('EPSNormalized_guidance_high')
    EPSNormalized_guidance_mid = get_data('EPSNormalized_guidance_mid')
    EPSNormalized_guidance_low = get_data('EPSNormalized_guidance_low')

    # -------- EPS guidance --------- #
    EPS_guidance_high = get_data('EPS_guidance_high')
    EPS_guidance_mid = get_data('EPS_guidance_mid')
    EPS_guidance_low = get_data('EPS_guidance_low')

    # et_ref = pd.merge(et_ref, EPSnormalized[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
    # et_ref['EPSnormalized_et'] = pd.to_datetime(et_ref['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    # et_ref.drop(columns = ['effectivedate'], inplace = True)
    # et_ref.rename(columns = {'dataitemvalue': 'EPS_normalized'}, inplace = True)
    # et_ref.drop_duplicates(subset=['keydevid', 'EPS_normalized', 'EPSnormalized_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
    # # print(et_ref)


if __name__ == "__main__":
    database = PostgresDatabase(
    dbname="targetdb",
    user="ubuntu",
    )
    print("Connected to database")

    task_manager = TaskManagerRepository(database)

    get_universe_earnings_estimates_guidance(task_manager)