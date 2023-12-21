import dask.dataframe as dd
import timeit

# Specify the file path
file_path = 'effects-of-covid-19.csv'

# Measure time taken to execute the Dask computation
timeit_result = timeit.timeit(lambda: dd.read_csv(file_path), number=1)

print(f'Time taken: {timeit_result} seconds')
