import ray
import modin.pandas as mpd
import timeit

# Initialize Ray
ray.init()

file_path = 'effects-of-covid-19.csv'

# Measure time taken to read the file using Ray and Modin
timeit_result = timeit.timeit(lambda: mpd.read_csv(file_path), number=1)

print(f'Time taken: {timeit_result} seconds')
