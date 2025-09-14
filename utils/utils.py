import os
import urllib.request
import logging
import re

from psycopg2.extras import execute_values

from sports_analysis.utils.config import Config
from sports_analysis.utils import database

# Initialize the logger
logger = logging.getLogger(os.environ["RUN_TYPE"])
config_obj = Config()
config = config_obj.get_config("environment")
extraction_path = f"{config['RAW_FILE_PATH']}/raw_info"

def download_file_urllib(url, local_filename):
    """
    Download a file from the specified URL to a local file path using urllib.

    Logs informational messages about the download start and success,
    and logs an error message if the download fails due to a URLError.

    Args:
        url (str): The URL of the file to download.
        local_filename (str): The file path where the downloaded file should be saved.
    """
    try:
        logger.info(f"Downloading started... {url}")
        urllib.request.urlretrieve(url, local_filename)
        logger.info(f"File downloaded successfully to: {local_filename}")
    except urllib.error.URLError as e:
        logger.error(f"Error downloading file: {e}")

def get_matches_info():
    """
    Parses the README.txt file in the extraction path to extract match information.

    The function matches lines formatted as '<date> - <match details>' using a regex pattern,
    then organizes these lines into a dictionary keyed by match types, with values as lists of
    JSON file paths constructed from the matched lines.

    Returns:
        dict: A dictionary mapping match type strings to lists of corresponding JSON file paths.
    """
    pattern = re.compile(r"^\d{4}-\d{2}-\d{2}(?: - [^-]+)+$")

    filename = f'{extraction_path}/README.txt'

    matched_lines = []

    with open(filename, 'r') as file:
        for line in file:
            if pattern.match(line.strip()):
                matched_lines.append(line.strip())
    info = {}
    for line in matched_lines:
        columns = line.split(' - ')
        if columns[2] not in info:
            info[columns[2]] = []
        info[columns[2]].append(f'{extraction_path}/{columns[4]}.json')

    return info

def get_processed_files():
    """
    Fetches a list of filenames that have already been processed successfully.

    Queries the 'silver.files_processed' table filtering rows with status 'FINISHED' 
    and returns a list of the processed file names.

    Returns:
        list: List of file name strings that have status 'FINISHED'.
    """
    logger.info("Getting already processed files")
    processed_files = []
    info = database.fetch_info(f"SELECT file_name FROM silver.files_processed WHERE status='FINISHED'")
    for i in info:
        processed_files.append(i['file_name'])
    return processed_files

def insert_data(data, table_name, del_match_id=True):
    """
    Inserts data into the specified database table, optionally deleting existing rows
    with the same match_id as the first element in the data.

    Performs batched inserts using psycopg2's execute_values for efficiency.

    Args:
        data (list): List of dictionaries representing rows to insert.
        table_name (str): Target table name in the database.
        del_match_id (bool, optional): If True, deletes existing entries matching match_id before insert. Default is True.
    """
    if del_match_id:
        match_id = data[0]["match_id"]
        database.execute_statement(f"DELETE FROM {table_name} WHERE match_id='{match_id}'")
    batches = list(create_batches(data, batch_size=100))
    for batch in batches:
        cols = batch[0].keys()
        query = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
        values = [[d[col] for col in cols] for d in batch]
        conn = database.get_psql_conn()
        cursor = conn.cursor()
        execute_values(cursor, query, values)
        conn.commit()

def create_batches(data, batch_size=50):
    """
    Yields successive batches from the input data list of the specified batch size.

    Args:
        data (list): List of data elements to batch.
        batch_size (int, optional): Size of each batch. Defaults to 50.

    Yields:
        list: Next batch of data elements.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

def get_unprocessed_match_stats(process_name):
    """
    Retrieves match IDs from 'silver.matches' that have not been processed successfully
    for a specific gold process type.

    Performs a left join with 'gold.stats_processed' filtering for missing finished statuses.

    Args:
        process_name (str): Process type to filter by (e.g., 'batting_stats').

    Returns:
        list of dict: List containing dicts with unprocessed match IDs.
    """
    query = f"""
    SELECT m.match_id AS match_id
    FROM   silver.matches m
    LEFT JOIN   gold.stats_processed sp
    ON     m.match_id=sp.match_id
    AND    sp.status='FINISHED'
    AND    sp.process_name='{process_name}'
    WHERE  sp.match_id IS NULL
    """
    info = database.fetch_info(query)
    return info
