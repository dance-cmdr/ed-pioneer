import duckdb
import os
import sys
from datetime import datetime
import ijson
import pprint

# Path to the DuckDB database file
DB_PATH = "spansh_data.db"

# Backup directory
BACKUP_DIR = "backups"


def get_connection():
    """
    Establish and return a connection to the DuckDB database.
    """
    return duckdb.connect(DB_PATH)


def initialize_database():
    """
    Create tables in the DuckDB database if they don't already exist.
    """
    conn = get_connection()
    create_systems_table = """
    CREATE TABLE IF NOT EXISTS systems (
        system_id BIGINT PRIMARY KEY,
        name TEXT,
        x DECIMAL,
        y DECIMAL,
        z DECIMAL,
        allegiance TEXT,
        government TEXT,
        primary_economy TEXT,
        secondary_economy TEXT,
        security TEXT,
        population BIGINT,
        date TIMESTAMP
    );
    """
    create_bodies_table = """
    CREATE TABLE IF NOT EXISTS bodies (
        body_id BIGINT PRIMARY KEY,
        system_id BIGINT REFERENCES systems(system_id),
        name TEXT,
        type TEXT,
        sub_type TEXT,
        distance_to_arrival DECIMAL,
        main_star BOOLEAN,
        age BIGINT,
        spectral_class TEXT,
        luminosity TEXT,
        absolute_magnitude DECIMAL,
        solar_masses DECIMAL,
        solar_radius DECIMAL,
        surface_temperature DECIMAL,
        rotational_period DECIMAL,
        axial_tilt DECIMAL,
        orbital_period DECIMAL,
        semi_major_axis DECIMAL,
        orbital_eccentricity DECIMAL,
        orbital_inclination DECIMAL,
        arg_of_periapsis DECIMAL,
        mean_anomaly DECIMAL,
        ascending_node DECIMAL,
        update_time TIMESTAMP
    );
    """
    create_stations_table = """
    CREATE TABLE IF NOT EXISTS stations (
        station_id BIGINT PRIMARY KEY,
        system_id BIGINT REFERENCES systems(system_id),
        name TEXT,
        type TEXT,
        controlling_faction TEXT,
        controlling_faction_state TEXT,
        distance_to_arrival DECIMAL,
        primary_economy TEXT,
        government TEXT,
        update_time TIMESTAMP,
        latitude DECIMAL,
        longitude DECIMAL
    );
    """
    conn.execute(create_systems_table)
    conn.execute(create_bodies_table)
    conn.execute(create_stations_table)
    conn.close()


def create_backup():
    """
    Create a backup of the database file if it contains data.
    """
    conn = get_connection()
    # Check if there is data in the database
    result = conn.execute("SELECT COUNT(*) FROM systems").fetchone()[0]
    conn.close()

    if result > 0:
        # Create the backup directory if it doesn't exist
        os.makedirs(BACKUP_DIR, exist_ok=True)
        # Create a backup file with the current date appended
        backup_file = os.path.join(
            BACKUP_DIR, f"systems_data_backup_{datetime.now().strftime('%Y-%m-%d')}.db"
        )
        if not os.path.exists(backup_file):
            os.system(f"cp {DB_PATH} {backup_file}")
            print(f"Backup created: {backup_file}")
        else:
            print("Backup already exists for today.")


def update_from_json(json_file):
    """
    Update records in the database from a large JSON data dump file using incremental parsing.
    """
    conn = get_connection()

    # Open the JSON file and parse it incrementally
    with open(json_file, "r") as f:
        # Parse the JSON array incrementally
        systems = ijson.items(
            f, "item"
        )  # Assumes the JSON file contains an array of systems

        for system in systems:
            # Insert or update systems
            system_query = """
            INSERT INTO systems (system_id, name, x, y, z, allegiance, government, primary_economy, secondary_economy, security, population, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (system_id) DO UPDATE SET
                name=excluded.name,
                x=excluded.x,
                y=excluded.y,
                z=excluded.z,
                allegiance=excluded.allegiance,
                government=excluded.government,
                primary_economy=excluded.primary_economy,
                secondary_economy=excluded.secondary_economy,
                security=excluded.security,
                population=excluded.population,
                date=excluded.date;
            """
            conn.execute(
                system_query,
                (
                    system["id64"],
                    system["name"],
                    system["coords"]["x"],
                    system["coords"]["y"],
                    system["coords"]["z"],
                    system.get("allegiance", None),
                    system.get("government", None),
                    system.get("primaryEconomy", None),
                    system.get("secondaryEconomy", None),
                    system.get("security", None),
                    system.get("population", 0),
                    system["date"],
                ),
            )

            # Insert or update bodies
            for body in system.get("bodies", []):
                body_query = """
                INSERT INTO bodies (body_id, system_id, name, type, sub_type, distance_to_arrival, main_star, age, spectral_class, luminosity, absolute_magnitude, solar_masses, solar_radius, surface_temperature, rotational_period, axial_tilt, orbital_period, semi_major_axis, orbital_eccentricity, orbital_inclination, arg_of_periapsis, mean_anomaly, ascending_node, update_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (body_id) DO UPDATE SET
                    name=excluded.name,
                    type=excluded.type,
                    sub_type=excluded.sub_type,
                    distance_to_arrival=excluded.distance_to_arrival,
                    main_star=excluded.main_star,
                    age=excluded.age,
                    spectral_class=excluded.spectral_class,
                    luminosity=excluded.luminosity,
                    absolute_magnitude=excluded.absolute_magnitude,
                    solar_masses=excluded.solar_masses,
                    solar_radius=excluded.solar_radius,
                    surface_temperature=excluded.surface_temperature,
                    rotational_period=excluded.rotational_period,
                    axial_tilt=excluded.axial_tilt,
                    orbital_period=excluded.orbital_period,
                    semi_major_axis=excluded.semi_major_axis,
                    orbital_eccentricity=excluded.orbital_eccentricity,
                    orbital_inclination=excluded.orbital_inclination,
                    arg_of_periapsis=excluded.arg_of_periapsis,
                    mean_anomaly=excluded.mean_anomaly,
                    ascending_node=excluded.ascending_node,
                    update_time=excluded.update_time;
                """

                conn.execute(
                    body_query,
                    (
                        body["id64"],
                        system["id64"],
                        body["name"],
                        body["type"],
                        body.get("subType"),
                        body.get("distanceToArrival"),
                        body.get("mainStar", False),
                        body.get("age"),
                        body.get("spectralClass"),
                        body.get("luminosity"),
                        body.get("absoluteMagnitude"),
                        body.get("solarMasses"),
                        body.get("solarRadius"),
                        body.get("surfaceTemperature"),
                        body.get("rotationalPeriod"),
                        body.get("axialTilt"),
                        body.get("orbitalPeriod"),
                        body.get("semiMajorAxis"),
                        body.get("orbitalEccentricity"),
                        body.get("orbitalInclination"),
                        body.get("argOfPeriapsis"),
                        body.get("meanAnomaly"),
                        body.get("ascendingNode"),
                        body["updateTime"],
                    ),
                )

            # Insert or update stations
            for station in system.get("stations", []):
                station_query = """
                INSERT INTO stations (station_id, system_id, name, type, controlling_faction, controlling_faction_state, distance_to_arrival, primary_economy, government, update_time, latitude, longitude)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (station_id) DO UPDATE SET
                    name=excluded.name,
                    type=excluded.type,
                    controlling_faction=excluded.controlling_faction,
                    controlling_faction_state=excluded.controlling_faction_state,
                    distance_to_arrival=excluded.distance_to_arrival,
                    primary_economy=excluded.primary_economy,
                    government=excluded.government,
                    update_time=excluded.update_time,
                    latitude=excluded.latitude,
                    longitude=excluded.longitude;
                """

                if (
                    station.get("distanceToArrival") is None
                    and station.get("latitude") is None
                    and station.get("longitude") is None
                ):
                    print(
                        f"Station {station['name']} in system {system['name']} has not distanceToArrival, latitude, or longitude value."
                    )
                    pprint.pprint(station)
                    break

                conn.execute(
                    station_query,
                    (
                        station["id"],
                        system["id64"],
                        station["name"],
                        station.get("type"),
                        station.get("controllingFaction"),
                        station.get("controllingFactionState"),
                        station.get("distanceToArrival"),
                        station.get("primaryEconomy"),
                        station.get("government"),
                        station["updateTime"],
                        station.get("latitude"),
                        station.get("longitude"),
                    ),
                )

                # @TODO Process the stations's market, services and economies

    conn.close()
    print("Database updated from JSON file.")


if __name__ == "__main__":
    # Initialize the database
    initialize_database()

    # Create a backup if necessary
    if os.path.exists(DB_PATH):
        create_backup()

    # Check for the JSON file path as a command-line argument
    if len(sys.argv) > 1:
        json_file_path = sys.argv[1]
        if os.path.exists(json_file_path):
            update_from_json(json_file_path)
        else:
            print(f"JSON data dump file '{json_file_path}' not found.")
    else:
        print(
            "Please provide the path to the JSON data dump file as a command-line argument."
        )
