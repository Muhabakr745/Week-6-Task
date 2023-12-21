import ray
import modin.config as mc
import modin.pandas as mpd
import timeit

# Initialize Ray
ray.init()

# Specify the file path
file_path = 'effects-of-covid-19.csv'

# Set the Modin engine to Ray
mc.engine = 'ray'

# Measure time taken to execute the Modin computation
timeit_result = timeit.timeit(lambda: mpd.read_csv(file_path), number=1)

print(f'Time taken: {timeit_result} seconds')
