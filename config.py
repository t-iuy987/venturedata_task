import os
from dotenv import load_dotenv

# load variables from .env file
load_dotenv()

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")
COMBINED_DATA_DIR = os.getenv("COMBINED_DATA_DIR")
AVRO_SCHEMA_FILE = os.getenv("AVRO_SCHEMA_FILE")
METADATA_FILE = os.getenv("METADATA_FILE")
BASE_URL_YELLOW = os.getenv("BASE_URL_YELLOW")
BASE_URL_GREEN = os.getenv("BASE_URL_GREEN")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Ensure directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(COMBINED_DATA_DIR, exist_ok=True)
