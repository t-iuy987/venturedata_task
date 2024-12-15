import os
import logging
import pandas as pd
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR

logger = logging.getLogger(__name__)

# find the  taxi type from the file name
def get_taxi_type(filename: str) -> str:
    if "yellow" in filename:
        return "yellow"
    elif "green" in filename:
        return "green"
    return "unknown"

# transform raw parquet files by standardizing columns and adding derived fields
def transform_data():
    files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".parquet")]
    for f in files:
        full_path = os.path.join(RAW_DATA_DIR, f)
        taxi_type = get_taxi_type(f)

        df = pd.read_parquet(full_path)

        # standardize columns
        if 'tpep_pickup_datetime' in df.columns:
            df['pickup_datetime'] = df.pop('tpep_pickup_datetime')
        elif 'lpep_pickup_datetime' in df.columns:
            df['pickup_datetime'] = df.pop('lpep_pickup_datetime')

        if 'tpep_dropoff_datetime' in df.columns:
            df['dropoff_datetime'] = df.pop('tpep_dropoff_datetime')
        elif 'lpep_dropoff_datetime' in df.columns:
            df['dropoff_datetime'] = df.pop('lpep_dropoff_datetime')

        # validate presence of pickup_datetime
        if 'pickup_datetime' not in df.columns:
            logger.warning(f"Skipping {f} due to missing pickup_datetime")
            continue

        # convert to datetime
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
        
        # remove rows without a valid pickup_datetime
        df = df.dropna(subset=['pickup_datetime'])

        #  derived columns
        df['pickup_hour'] = df['pickup_datetime'].dt.hour
        df['pickup_dayofweek'] = df['pickup_datetime'].dt.dayofweek

        # add taxi_type
        df['taxi_type'] = taxi_type

        # save processed file
        processed_file = os.path.join(PROCESSED_DATA_DIR, f"{taxi_type}_processed_{f}")
        df.to_parquet(processed_file, index=False)
        logger.info(f"Processed {f} and saved as {processed_file}")
