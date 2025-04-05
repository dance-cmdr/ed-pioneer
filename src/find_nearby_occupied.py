import pandas as pd
import argparse
from scipy.spatial import KDTree
import numpy as np


def build_kdtree(df):
    coords = df[["x", "y", "z"]].values
    return KDTree(coords), coords


def find_nearby_with_sources(candidates_df, occupied_df, radius_ly=15):
    tree, occupied_coords = build_kdtree(occupied_df)
    candidate_coords = candidates_df[["x", "y", "z"]].values

    nearby_sources = []
    for i, point in enumerate(candidate_coords):
        indices = tree.query_ball_point(point, radius_ly)
        if indices:
            sources = occupied_df.iloc[indices]["system_name"].tolist()
            nearby_sources.append(", ".join(sources))
        else:
            nearby_sources.append("")

    candidates_df = candidates_df.copy()
    candidates_df["source_systems_within_15ly"] = nearby_sources
    return candidates_df[candidates_df["source_systems_within_15ly"] != ""]


def main(input_candidates, input_all_systems, output_csv):
    candidates_df = pd.read_csv(input_candidates)
    all_systems_df = pd.read_csv(input_all_systems)

    occupied_df = all_systems_df[all_systems_df["occupation_status"] == "occupied"]

    final_df = find_nearby_with_sources(candidates_df, occupied_df)
    final_df = final_df[
        [
            "system_name",
            "x",
            "y",
            "z",
            "distance_from_sol",
            "body_count",
            "landable_count",
            "ring_count",
            "source_systems_within_15ly",
        ]
    ]
    final_df.sort_values(
        by=["body_count", "landable_count", "ring_count"], ascending=False, inplace=True
    )
    final_df.to_csv(output_csv, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find colonisation candidates near occupied systems with sources"
    )
    parser.add_argument("candidates", help="CSV with candidate systems")
    parser.add_argument("systems", help="CSV with all system stats")
    parser.add_argument("output", help="Output CSV path")
    args = parser.parse_args()

    main(args.candidates, args.systems, args.output)
