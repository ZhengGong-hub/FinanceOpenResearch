import pandas as pd

addr = "/Users/zg/FinanceOpenResearch/papers/paper1_identifier/data/processed/ibes_recs_monthly_with_analyst_esg_score_refinitiv_esg_v2_trimmed.parquet"

df = pd.read_parquet(addr)

print(df.head())
print(df.columns)

print(df[['recs_panel_month', 'recs_panel_year', 'year', 'analyst_esg_date', 'recs_panel_beg_date', 'analyst_esg_date', 'refi_esg_date']])

# show column where column year is not the same as recs_panel_year
print(df[['recs_panel_month', 'recs_panel_year', 'year', 'analyst_esg_date', 'recs_panel_beg_date', 'analyst_esg_date', 'refi_esg_date']][df['recs_panel_year']-df['year'] == 1])