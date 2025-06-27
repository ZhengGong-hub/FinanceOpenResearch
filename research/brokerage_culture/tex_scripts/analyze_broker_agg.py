import pandas as pd
import glob 

output_files = glob.glob('research/brokerage_culture/output_data/agg_broker_quarter/output/output_job_*.csv')

# combine all the output files into one dataframe
df = pd.concat([pd.read_csv(file) for file in output_files])
print(df.head())

# merge on 
indexed = pd.read_parquet('research/brokerage_culture/output_data/agg_broker_quarter/indexed_input_data/indexed_df.parquet')
print(indexed.head())

df = df.merge(indexed, left_on='custom_id', right_on='bash_custom_id', how='left')
print(df.head())


# innovation  integrity  quality  respect  teamwork  take mean and std, proid takes count

df_by_employer = df.groupby('brokerid').agg({'innovation': ['mean', 'std'], 'integrity': ['mean', 'std'], 'quality': ['mean', 'std'], 'respect': ['mean', 'std'], 'teamwork': ['mean', 'std'], 'companyname': 'first', 'bash_custom_id': 'count'})
# flatten column name
df_by_employer.columns = ['_'.join(col).strip() for col in df_by_employer.columns.values]

df_by_employer.rename(columns={'companyname_first': 'companyname', 'bash_custom_id_count': 'repeat_count'}, inplace=True)

df_by_employer.set_index('companyname', inplace=True)

# round the result to 3 decimal places
df_by_employer = df_by_employer.round(3)

# sort by repeat_count
df_by_employer.sort_values(by='repeat_count', ascending=False, inplace=True)
df_by_employer = df_by_employer.query('repeat_count > 20')

metrics = ['innovation', 'integrity', 'quality', 'respect', 'teamwork']

# Create mean ± std formatted strings (rounded to 3 decimals)
formatted = {
    m: df_by_employer[f"{m}_mean"].round(2).astype(str) + ' ± ' + df_by_employer[f"{m}_std"].round(2).astype(str)
    for m in metrics
}
formatted = pd.DataFrame(formatted, index=df_by_employer.index)
print(formatted)

# Clean the index by escaping LaTeX special chars
escaped_index = formatted.index.to_series().apply(
    lambda s: s.replace('&', r'\&').replace("Crédit Suisse", r"Cr\'edit Suisse").replace("Research Division", "")
)
formatted.index = escaped_index

# Split the dataframe into chunks of 60 rows
chunk_size = 50
num_chunks = (len(formatted) + chunk_size - 1) // chunk_size

with open('research/brokerage_culture/tex_scripts/table_broker_agg.tex', 'w') as f:
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(formatted))
        chunk = formatted.iloc[start_idx:end_idx]
        
        # Generate LaTeX for this chunk
        latex = chunk.to_latex(
            index=True,
            header=True,
            escape=False,                # prevent pandas from escaping LaTeX
            column_format='l' + 'r' * len(metrics),  # alignment: left for index, right for metrics
        )
        
        # Write chunk to file
        f.write(latex)
        
        # Add page break between tables (except for the last one)
        if i < num_chunks - 1:
            f.write('\n\\newpage\n\n')