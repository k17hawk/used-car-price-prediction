import os,sys
from dataclasses import dataclass
from datetime import datetime

from car_price.exception import CarPriceException
from car_price.constant import TIMESTAMP
#Dataingestion constants
DATA_INGESTION_DIR = "data_ingestion"
DATA_INGESTION_DOWNLOADED_DATA_DIR = "downloaded_files"
DATA_INGESTION_FILE_NAME = "car_price"
DATA_INGESTION_FEATURE_STORE_DIR = "feature_store"
DATA_INGESTION_FAILED_DIR = "failed_downloaded_files"

DATA_INGESTION_DATA_SOURCE_URL = f""

