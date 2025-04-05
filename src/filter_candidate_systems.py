import pandas as pd
import argparse


def filter_candidates(df):
    return df[
        (df["occupation_status"] == "uncolonised")
        & (df["distance_from_sol"] <= 500)
        & (df["body_count"] > 30)
        & (df["landable_count"] > 20)
        & (df["ring_count"] > 3)
    ].copy()


def main(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    candidates = filter_candidates(df)
    candidates.sort_values(
        by=["body_count", "landable_count", "ring_count"], ascending=False, inplace=True
    )
    candidates.to_csv(output_csv, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter systems suitable for colonisation"
    )
    parser.add_argument("input", help="Path to input CSV from extract_system_stats.py")
    parser.add_argument("output", help="Path to output CSV for candidate systems")
    args = parser.parse_args()

    main(args.input, args.output)
