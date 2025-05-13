# Read a JSON array from a file and print the first n objects
# Usage
# head -n 5 source.json

import ijson


def read_n_lines_from_json_array_file(file_path, n):
    """
    Read the first n lines from a JSON array file.

    Args:
        file_path (str): Path to the JSON file.
        n (int): Number of lines to read.

    Returns:
        list: List of the first n objects from the JSON array.
    """
    with open(file_path, "r") as f:
        objects = ijson.items(f, "item")
        return [obj for _, obj in zip(range(n), objects)]


fp = "./data/galaxy_1day.json"
lines = 4
result = read_n_lines_from_json_array_file(fp, lines)

# filter result for systems with stations
result = [
    system
    for system in result
    if system.get("stations") and len(system["stations"]) > 0
]
print(result)
