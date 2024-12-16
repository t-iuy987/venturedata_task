import os
import logging
import json
import pandas as pd
import duckdb
from fastavro import writer, parse_schema
from config import PROCESSED_DATA_DIR, COMBINED_DATA_DIR, AVRO_SCHEMA_FILE

logger = logging.getLogger(__name__)

# combine all processed parquet files into a single dataset
def load_data(export_csv, export_avro):
    combined_files = [os.path.join(PROCESSED_DATA_DIR, f) for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith(".parquet")]

    if not combined_files:
        logger.warning("No processed files found to load.")
        return

    combined_df = pd.concat([pd.read_parquet(file) for file in combined_files], ignore_index=True)
    final_path = os.path.join(COMBINED_DATA_DIR, "all_taxi_data.parquet")
    combined_df.to_parquet(final_path, index=False)
    logger.info(f"Combined all processed data into one file at {final_path}")
    if export_csv:
        export_to_csv()
    elif export_avro:
        export_to_avro()


def export_to_avro():
    combined_file = os.path.join(COMBINED_DATA_DIR, "all_taxi_data.parquet")
    avro_output_file = os.path.join(COMBINED_DATA_DIR, "all_taxi_data.avro")

    # read the combined parquet into a DataFrame
    df = pd.read_parquet(combined_file)
    
    # convert datetime columns to strings before writing to Avro.
    # avro does not have a native datetime type by default, and we defined these fields as strings in the schema.
    # that's why, we must convert pandas Timestamp objects into ISO8601-formatted strings
    # to ensure they match the ["null", "string"] Avro type and avoid type mismatch errors.
    df['pickup_datetime'] = df['pickup_datetime'].astype(str)
    df['dropoff_datetime'] = df['dropoff_datetime'].astype(str)

    # loading and parse the Avro schema
    with open(AVRO_SCHEMA_FILE, "r") as f:
        schema_dict = json.load(f)
    parsed_schema = parse_schema(schema_dict)

    # converting DataFrame to a list of dictionaries
    records = df.to_dict(orient="records")

    # writing records to Avro file
    with open(avro_output_file, 'wb') as out:
        writer(out, parsed_schema, records)

    print(f"Exported combined data to Avro format at {avro_output_file}")
    

    
def export_to_csv():
    # if user requested output compatible with Excel, then provide the argument --export-csv
    df = pd.read_parquet(f"{COMBINED_DATA_DIR}/all_taxi_data.parquet")
    csv_file = f"{COMBINED_DATA_DIR}/all_taxi_data.csv"
    df.to_csv(csv_file, index=False)    
    logger.info(f"Exported combined data to {csv_file}")



def analyze_data():
    # setup DuckDB for running queries
    con = duckdb.connect()
    con.execute(f"CREATE TABLE IF NOT EXISTS trips AS SELECT * FROM parquet_scan('{COMBINED_DATA_DIR}/all_taxi_data.parquet')")

    # queries
    avg_trip_distance = con.execute("""
        SELECT taxi_type, pickup_hour, AVG(trip_distance) as avg_distance
        FROM trips
        GROUP BY taxi_type, pickup_hour
        ORDER BY taxi_type, pickup_hour
    """).fetchdf()
    logger.info(avg_trip_distance)

    lowest_single_trips = con.execute("""
        SELECT pickup_dayofweek, COUNT(*) AS single_rider_trips
        FROM trips
        WHERE passenger_count = 1 AND EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
        GROUP BY pickup_dayofweek
        ORDER BY single_rider_trips 
        LIMIT 1
    """).fetchdf()
    logger.info(lowest_single_trips)

    busiest_hours = con.execute("""
        SELECT pickup_hour, COUNT(*) AS trip_count
        FROM trips
        GROUP BY pickup_hour
        ORDER BY trip_count DESC
        LIMIT 3
    """).fetchdf()
    logger.info(busiest_hours)