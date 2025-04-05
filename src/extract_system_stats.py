import json
import csv
import argparse
from math import sqrt


def distance_from_sol(x, y, z):
    return sqrt(x**2 + y**2 + z**2)


def detect_occupation_status(stations):
    if not stations:
        return "uncolonised"

    has_colonised = False
    has_colonising = False
    has_regular_station = False

    for s in stations:
        market_id = str(s.get("market_id", ""))
        station_type = s.get("type", "").lower()

        if market_id.startswith("42") or "construction type" in station_type:
            has_colonised = True
        elif market_id.startswith("395") or market_id.startswith("396"):
            if "construction" in station_type:
                has_colonising = True
        else:
            has_regular_station = True

    if has_regular_station:
        return "occupied"
    elif has_colonised:
        return "colonised"
    elif has_colonising:
        return "colonising"
    return "uncolonised"


def extract_system_stats(system):
    coords = system.get("coords", {})
    bodies = system.get("bodies", [])
    stations = system.get("stations", [])

    landable_count = sum(1 for b in bodies if b.get("isLandable") is True)
    ring_count = sum(1 for b in bodies if b.get("rings"))
    body_count = len(bodies)

    return {
        "system_name": system.get("name"),
        "x": coords.get("x", 0),
        "y": coords.get("y", 0),
        "z": coords.get("z", 0),
        "distance_from_sol": distance_from_sol(
            coords.get("x", 0), coords.get("y", 0), coords.get("z", 0)
        ),
        "body_count": body_count,
        "landable_count": landable_count,
        "ring_count": ring_count,
        "has_station": len(stations) > 0,
        "occupation_status": detect_occupation_status(stations),
    }


def main(input_path, output_path):
    with open(input_path, "r", encoding="utf-8") as f:
        systems = json.load(f)

    output_rows = [extract_system_stats(system) for system in systems]

    fieldnames = [
        "system_name",
        "x",
        "y",
        "z",
        "distance_from_sol",
        "body_count",
        "landable_count",
        "ring_count",
        "has_station",
        "occupation_status",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract system stats from Spansh galaxy JSON"
    )
    parser.add_argument("input", help="Path to input galaxy JSON file")
    parser.add_argument("output", help="Path to output CSV file")
    args = parser.parse_args()

    main(args.input, args.output)
