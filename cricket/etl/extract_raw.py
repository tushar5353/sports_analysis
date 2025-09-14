import logging
import os
from zipfile import ZipFile

from sports_analysis.utils import utils, logs
from sports_analysis.utils.config import Config

config_obj = Config()
config = config_obj.get_config("environment")

# Initialize the logger
logger = logging.getLogger(os.environ["RUN_TYPE"])

zip_files = f"{config['RAW_FILE_PATH']}/all_json.zip"
extraction_path = f"{config['RAW_FILE_PATH']}/raw_info"

def download_raw_files():
    """
    Downloads the raw cricket data ZIP file from the cricsheet.org URL,
    extracts its contents into the configured raw_info directory, 
    and logs the extraction completion.

    The ZIP file path and extraction path are defined by environment configuration.

    Uses:
        - utils.download_file_urllib for downloading the file.
        - Python's ZipFile for extraction.
        - Logger to record extraction status.
    """
    utils.download_file_urllib("https://cricsheet.org/downloads/all_json.zip", zip_files)

    with ZipFile(zip_files, 'r') as zip_ref:
        zip_ref.extractall(extraction_path)

    logger.info(f"All files extracted from {zip_files} to {extraction_path}")
