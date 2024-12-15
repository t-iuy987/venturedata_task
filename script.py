# script.py
import logging
import argparse
import os
from config import LOG_LEVEL, METADATA_FILE
from datetime import datetime, timedelta
from extract_data import download_data
from transform_data import transform_data
from load_data import load_data

# setup logging
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# setup argument parser
parser = argparse.ArgumentParser(description="Run the NYC taxi data pipeline.")
parser.add_argument("--export-csv", action="store_true", help="Exports combined data to CSV format.")
parser.add_argument("--export-avro", action="store_true", help="Exports combined data to Avro format.")
args = parser.parse_args()

def get_last_downloaded():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            line = f.read().strip()
            if line:
                year, month = line.split('-')
                return int(year), int(month)
    # if no metadata, download data from the past 3 years
    now = datetime.now()
    return now.year - 3, 1

def save_last_downloaded(year, month):
    with open(METADATA_FILE, 'w') as f:
        f.write(f"{year}-{month:02d}")
    logger.info(f"Updated last_downloaded.txt to {year}-{month:02d}")

# helper to get the next month
def next_month(year, month):
    dt = datetime(year, month, 1)
    next_dt = dt + timedelta(days=32)  # jump ahead to ensure next month
    return next_dt.year, next_dt.month


def main():
    # get the last processed month
    last_year, last_month = get_last_downloaded()

    # determine what months to download next
    current_year = datetime.now().year
    current_month = datetime.now().month

    # if there are new months available
    # If current_year == last_year and current_month <= last_month, no new data
    if (current_year < last_year) or (current_year == last_year and current_month <= last_month):
        logger.info("No new months to download. The pipeline is up-to-date.")
    else:
        # start getting data from the month after the last downloaded
        start_year, start_month = next_month(last_year, last_month)
        end_year, end_month = current_year, current_month

        # extract the new data
        download_data(start_year, start_month, end_year, end_month)

        transform_data()
        load_data(args.export_csv, args.export_avro)

        # update last_downloaded.txt
        # latest processed month is end_year-end_month
        save_last_downloaded(end_year, end_month)

if __name__ == "__main__":
    main()

