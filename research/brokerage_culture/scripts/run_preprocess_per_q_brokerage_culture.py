import pandas as pd
from openai_batch_wrapper.preprocess import preprocess_dataframe

# load in the data
df = pd.read_parquet('research/brokerage_culture/input_data/question_transcripts_with_employer.parquet')
print('before dropping null companyid', len(df))
# get rid of the questions that are too short
df = df[df['componenttext'].str.len() > 100]
# drop the row where companyid is null
df = df[df['companyid'].notna()]
print('after dropping null companyid and too short questions', len(df))
# get top 10000 questions
df = df.sample(2000, random_state=42, replace=False)
print(df.head())

# preprocess the data
preprocess_dataframe(
    df=df, 
    guiding_prompt=open('research/brokerage_culture/input_data/prompts.txt').read(), 
    content_col='componenttext',
    chunk_size=400,
    output_dir='research/brokerage_culture/output_data/small_scale_random_200k/',
    llm_model='gpt-4o',
    structured_output_path='research/brokerage_culture/input_data/strucutred_output.json')
