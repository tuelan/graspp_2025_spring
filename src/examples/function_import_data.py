import os
import pandas as pd

# Print file path
print('This file is run from the following path:')
print("\n") # This line provides space
print(os.getcwd())

# Print the location
print("\n") # This line provides space
file_location = "data/examples/module_1/"
print(os.listdir(file_location))

print()
df = pd.read_csv("data/examples/module_1/world_bank_data.csv")
print('Look mom this function imports data to pandas')
print(df.head(5))

