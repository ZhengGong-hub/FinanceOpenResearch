import pandas as pd
from openai_batch_wrapper.preprocess import preprocess_dataframe

# Constants
BASE_PATH = 'research/brokerage_culture'
INPUT_DATA_PATH = f'{BASE_PATH}/input_data'
OUTPUT_DATA_PATH = f'{BASE_PATH}/output_data'

# load in the data
df = pd.read_parquet(f'{INPUT_DATA_PATH}/question_transcripts_with_employer.parquet')
print('before dropping null companyid', len(df))
# get rid of the questions that are too short
df = df[df['componenttext'].str.len() > 100]
# drop the row where companyid is null
df = df[df['companyid'].notna()]
print('after dropping null companyid and too short questions', len(df))

# by year 
et_ref = pd.read_csv(f'{INPUT_DATA_PATH}/us_et_ref_v2.csv', index_col=0)

# merge on year
df = df.merge(et_ref, left_on='transcriptid', right_on='transcriptid', how='left')
df.dropna(subset=['YearQTR'], inplace=True)
df['year'] = df['YearQTR'].str[:4].astype(int)
print('after merging with year data', len(df))

# drop those who does not have proid
df = df[df['proid'].notna()]
print('after dropping null proid', len(df))

# add a column that is called count, counting how many question a proid ask
df['q_count'] = df.groupby('proid')['proid'].transform('count')

q_count_threshold = 1000
# drop those who have less than 500 questions
df = df[df['q_count'] > q_count_threshold]
print(f'after dropping analysts with less than {q_count_threshold} questions', len(df))

# choose per analyst randomly 100 questions
df = df.groupby('proid').sample(300, random_state=42, replace=False)
print('after sampling 300 questions per analyst', len(df))

guiding_prompt = open(f'{INPUT_DATA_PATH}/prompts_analyst_charisma.txt').read()
print(guiding_prompt)
structured_output_path = f'{INPUT_DATA_PATH}/strucutred_output_analyst_charisma.json'

# preprocess the data
preprocess_dataframe(
    df=df, 
    guiding_prompt=guiding_prompt, 
    content_col='componenttext',
    chunk_size=40000,
    output_dir=f'{OUTPUT_DATA_PATH}/analysts_charisma/',
    llm_model='gpt-4o',
    structured_output_path=structured_output_path)

# save structured_output_path and guiding_prompt to separate files
with open(f'{OUTPUT_DATA_PATH}/analysts_charisma/prompt.txt', 'w') as f:
    f.write(guiding_prompt)

with open(f'{OUTPUT_DATA_PATH}/analysts_charisma/schema.json', 'w') as f:
    f.write(structured_output_path)
