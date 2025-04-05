import json
import pandas as pd

# Load the JSON data
with open("./src/data/sol.json") as f:
    data = json.load(f)

# Check if the top-level is a list or dict
if isinstance(data, dict):
    data = [data]  # Make it a list of one dict if needed

# Flatten nested dictionaries and lists up to a certain depth
df = pd.json_normalize(
    data,
    record_path=["bodies"],  # Go into the 'bodies' list
    meta=[
        "name",
        "id",
        "coords",
        "primary_star",
        ["faction", "name"],
        ["faction", "state"],
    ],
    sep="_",
    errors="ignore",
)

# Display the flattened DataFrame
import ace_tools as tools

tools.display_dataframe_to_user(name="Flattened Sol System Data", dataframe=df)
