# Find unoccupied systems
# within 15 LY of occupied systems
# within 500 LY from Sol
# With bodyCount > 30
# With landable planets > 20
# With rings > 3
#
#  unoccupied systems can be found by the property "population" = 0 while occupied systems have a population > 0
#  distance can be calculated by comparing the coordinates of the systems found in the "coords_x", "coords_y", "coords_z" columns


import pandas as pd
import duckdb

# Load CSV with Pandas
df = pd.read_csv("./src/data/galaxy_1day.csv")

# Run SQL on the Pandas DataFrame
query = """
SELECT 
    name, 
    distance, 
    total_bodies, 
    landable_bodies 
FROM df 
WHERE distance < 50 
  AND total_bodies > 5 
  AND landable_bodies > 1
"""
filtered = duckdb.query(query).to_df()

# Further processing or exporting with Pandas
print(filtered.head())
