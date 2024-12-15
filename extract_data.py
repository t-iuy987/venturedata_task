import os
import requests
import logging
from config import RAW_DATA_DIR, BASE_URL_YELLOW, BASE_URL_GREEN

logger = logging.getLogger(__name__)

# download a file from URL and save it locally
def download_file(url, local_filename):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded {local_filename}")
    else:
        logger.error(f"Failed to download {url}: HTTP {response.status_code}")

# downloads yellow and green taxi data for specified years and months
# i.e., download data from start_year/start_month up to end_year/end_month inclusive
# if start_year=2023, start_month=1 and end_year=2023, end_month=3 this will download jan, feb, mar 2023 data if it is available

def download_data(start_year, start_month, end_year, end_month):
    current_year, current_month = start_year, start_month
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        yellow_url = BASE_URL_YELLOW.format(year=current_year, month=current_month)
        green_url = BASE_URL_GREEN.format(year=current_year, month=current_month)

        yellow_file = os.path.join(RAW_DATA_DIR, f"yellow_tripdata_{current_year}-{current_month:02d}.parquet")
        green_file = os.path.join(RAW_DATA_DIR, f"green_tripdata_{current_year}-{current_month:02d}.parquet")

        download_file(yellow_url, yellow_file)
        download_file(green_url, green_file)

        # move to next month
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    logger.info("Data download completed.")
