import pandas as pd
import dask.dataframe as dd
import modin.pandas as mpd
import ray
import yaml
import os

# Read CSV file with Pandas
df_pandas = pd.read_csv('effects-of-covid-19.csv')

# Read CSV file with Dask
df_dask = dd.read_csv('effects-of-covid-19.csv')

# Read CSV file with Modin
df_modin = mpd.read_csv('effects-of-covid-19.csv')

# Check if Ray is already initialized
if not ray.is_initialized():
    # Start Ray
    ray.init()

    # Read CSV file with Ray
    df_ray = ray.read_csv('effects-of-covid-19.csv')

# Basic Validation: Remove special characters and whitespaces from column names
df_pandas.columns = df_pandas.columns.str.replace('[^\w\s]', '').str.strip()

# Write to YAML
schema = df_pandas.columns.tolist()
separator = '|'

with open('schema.yaml', 'w') as file:
    yaml.dump({'columns': schema, 'separator': separator}, file)

# Validate with YAML
with open('schema.yaml', 'r') as file:
    yaml_data = yaml.safe_load(file)

if len(df_pandas.columns) == len(yaml_data['columns']) and set(df_pandas.columns) == set(yaml_data['columns']):
    print("Validation successful!")
else:
    print("Validation failed.")

# Write to Pipe-Separated Gzip File
df_pandas.to_csv('output_file.txt.gz', sep='|', compression='gzip', index=False)

# Create Summary
total_rows = len(df_pandas)
total_columns = len(df_pandas.columns)
file_size = os.path.getsize('output_file.txt.gz')

print(f'Total number of rows: {total_rows}')
print(f'Total number of columns: {total_columns}')
print(f'File size: {file_size} bytes')
