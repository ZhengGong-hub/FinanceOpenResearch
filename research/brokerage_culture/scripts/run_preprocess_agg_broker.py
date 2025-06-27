# in contrast, this script is for aggregating the questions by broker-quarter. instead of having the questions one-by-one going through the LLM.
# 1. we aim to attain a bigger variance between brokers 
# 2. we aim to avoid the cases where some questions are too short and not representative of the broker's culture.

import pandas as pd
from openai_batch_wrapper.preprocess import preprocess_dataframe

# load in the data
df = pd.read_parquet('research/brokerage_culture/input_data/question_transcripts_with_employer.parquet')
print('before dropping null companyid', len(df))
# drop the row where companyid is null
df = df[df['companyid'].notna()]
df.rename(columns={'companyid': 'brokerid'}, inplace=True)

# get rid of the questions that are too short
df = df[df['componenttext'].str.len() > 100]

# by year 
et_ref = pd.read_csv('research/brokerage_culture/input_data/us_et_ref_v2.csv', index_col=0)
et_ref.sort_values('marketcap', ascending=False, inplace=True)

# merge on year
df = df.merge(et_ref, left_on='transcriptid', right_on='transcriptid', how='left')
df.dropna(subset=['YearQTR'], inplace=True)

# group by broker-quarter
df = df.groupby(['brokerid', 'YearQTR']).agg({'componenttext': '(end of question) '.join, 'companyname': 'first'}).reset_index()
df['char_count'] = df['componenttext'].apply(lambda x: len(x))

# drop those who have less than 10k in char count 
df = df[df['char_count'] > 10000]

# keep only first 15000 char per row 
df['componenttext'] = df['componenttext'].apply(lambda x: x[:15000])
df['char_count'] = df['componenttext'].apply(lambda x: len(x))

# preprocess the data
preprocess_dataframe(
    df=df, 
    guiding_prompt=open('research/brokerage_culture/input_data/prompts_question_agg.txt').read(), 
    content_col='componenttext',
    chunk_size=10000,
    output_dir='research/brokerage_culture/output_data/agg_broker_quarter/',
    llm_model='gpt-4o',
    structured_output_path='research/brokerage_culture/input_data/strucutred_output.json')
