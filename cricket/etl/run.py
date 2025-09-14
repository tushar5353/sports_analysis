import argparse
from datetime import datetime
import glob
import logging
import os
import sys
import json

from psycopg2.extras import execute_values

from sports_analysis.utils import utils, logs
from sports_analysis.utils.config import Config
from sports_analysis.cricket.etl import transform_to_silver
from sports_analysis.cricket.etl import transform_to_gold
from sports_analysis.cricket.etl import extract_raw

config_obj = Config()
config = config_obj.get_config("environment")
extraction_path = f"{config['RAW_FILE_PATH']}/raw_info"

def main():
    """
    Main entry point for running the sports analysis cricket pipeline via command-line interface.

    Parses command line arguments to trigger specific operations:
    - '--extract': Download raw JSON files.
    - '--load_silver': Load silver schema with optional 'start_over' flag to recreate tables.
    - '--load_gold': Load gold schema with specified processing type (match_info, batting_stats, bowling_stats, fielding_stats).

    Workflow details:
    - If extract is selected, downloads raw files.
    - If load_silver is selected, processes match files by loading them into silver layer tables.
      Optionally drops and recreates silver tables if 'start_over' is 'Y'.
    - If load_gold is selected, processes gold layer stats.
      For match_info, processes without match_id.
      For other stats types, processes all unprocessed match IDs with detailed logging.

    This function serves as the orchestrator based on CLI input.

    Usage example:
        python script.py --extract
        python script.py --load_silver ODI-Test --start_over Y
        python script.py --load_gold batting_stats
    """
    parser = argparse.ArgumentParser(description="Trigger for running sports analysis for cricket")

    # Main options as mutually exclusive or independent flags
    parser.add_argument('--extract', action='store_true', help='Download raw json files')

    # load_silver option with optional suboption start_over
    parser.add_argument('--load_silver', type=str, help='Option to load silver schema')
    parser.add_argument('--start_over', type=str, choices=['Y', 'N'], default='N',
                        help='Optional suboption start_over (Y or N) for load_silver default(N)')
    parser.add_argument('--load_gold', type=str, choices=['match_info', 'batting_stats', 'bowling_stats', 'fielding_stats'],
                    help='Option to load gold schema with specified choice')

    args = parser.parse_args()

    if args.extract:
        logger.info("Extract option selected")
        extract_raw.download_raw_files()

    if args.load_silver:
        processed_files = utils.get_processed_files()
        if args.start_over=='Y':
            transform_to_silver.drop_tables()
        transform_to_silver.create_tables()
        info = utils.get_matches_info()
        match_types = args.load_silver.split("-")
        files = []
        for match_type in match_types:
            files = files + info[match_type]
        for file in files:
            if file in processed_files:
                logger.info(f"file :: {file} Already processed")
                continue
            process_silver_obj = transform_to_silver.ProcessSilver(file)
            process_silver_obj.process()
    if args.load_gold:
        transform_to_gold_obj = transform_to_gold.ProcessGold(args.load_gold)
        if args.load_gold=="match_info":
            transform_to_gold_obj.process(None)
        else:
            unprocessed_matches = utils.get_unprocessed_match_stats(args.load_gold)
            logger.info(f"Unprocessed Matches - {len(unprocessed_matches)}")
            for i in unprocessed_matches:
                logger.info(f"Processing for match id - {i['match_id']}")
                transform_to_gold_obj.process(i['match_id'])

if __name__ == "__main__":
    logs.configure_logging()
    logger = logging.getLogger(os.environ["RUN_TYPE"])
    main()
