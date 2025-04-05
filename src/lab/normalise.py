import pandas as pd
import json

# Load the data
with open("./src/data/galaxy_1day.json") as f:
    systems = json.load(f)

df = pd.json_normalize(systems, sep="_", max_level=3)

# Dump the normalised data to a CSV file with index
df.to_csv("./src/data/galaxy_1day.csv", index=False)

# Load the CSV file with DuckDB
# conn = duckdb.connect(database=":memory:")
# conn.execute(
#     "CREATE TABLE df AS SELECT * FROM read_csv_auto('./src/data/galaxy_1day.csv')"
# )
#
# # Find the center of the bubble.
# sol = conn.execute("""
#     SELECT *
#     FROM df
#     WHERE name = 'Sol'
# """).fetchdf()
#
# # Find unoccupied systems
# # within 15 LY of occupied systems
# # within 500 LY from Sol
# # With bodyCount > 30
# # With landable planets > 20
