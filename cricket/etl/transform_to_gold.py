from datetime import datetime
import logging
import os
import sys
import json

from sports_analysis.utils import database
from sports_analysis.utils import utils, logs
from sports_analysis.utils.config import Config

config_obj = Config()
config = config_obj.get_config("environment")
queries = config_obj.get_config("cricket")

# Initialize the logger
logger = logging.getLogger(os.environ["RUN_TYPE"])

class ProcessGold:
    """
    Handles processing and insertion of 'gold' layer cricket statistics data
    for different processing types such as match_info, batting, bowling, and fielding stats.

    Attributes:
        process_type (str): The type of processing to perform (e.g., 'match_info', 'batting_stats', etc.).
    """

    def __init__(self, process_type):
        """
        Initializes the ProcessGold instance, creates the gold schema and the stats_processed table if not exists.

        Args:
            process_type (str): Processing type indicator.
        """
        self.process_type = process_type
        database.execute_statement("CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION postgres")
        database.execute_statement(queries["gold.stats_processed"])

    def process(self, match_id):
        """
        Executes the processing flow for the given match_id based on the process_type.

        Deletes existing entries for match_id and process_type, sets process status, executes the relevant 
        processing method, and updates the status accordingly. Handles exceptions by logging, setting status 
        to FAILED, and exiting.

        Args:
            match_id (str): Identifier for the match to process.
        """
        try:
            database.execute_statement("DELETE FROM gold.stats_processed WHERE match_id='{match_id}' AND process_name='{self.process_type}'")
            self.set_gold_process_status(self.process_type, match_id, "STARTED")
            if self.process_type == "match_info":
                self.process_match_info()
            if self.process_type == "batting_stats":
                self.process_batting_stats(match_id)
            if self.process_type == "bowling_stats":
                self.process_bowling_stats(match_id)
            if self.process_type == "fielding_stats":
                self.process_fielding_stats(match_id)
            self.set_gold_process_status(self.process_type, match_id, "FINISHED")
        except Exception as e:
            logger.info(f"Exception::{e}", exc_info=True)
            self.set_gold_process_status(self.process_type, match_id, "FAILED")
            sys.exit(1)

    def process_match_info(self):
        """
        Processes match level metadata by executing match_info queries and inserting results into gold.match_info table.
        """
        database.execute_statement(queries["gold.match_info"], debug=True)
        info = database.fetch_info(queries["gold.match_info_query"], debug=True)
        utils.insert_data(info, "gold.match_info", del_match_id=False)

    def process_batting_stats(self, match_id):
        """
        Processes batting statistics for the specified match and inserts into gold.batting_stats table.

        Args:
            match_id (str): The match identifier for which batting stats are processed.
        """
        database.execute_statement(queries["gold.batting_stats"])
        info = database.fetch_info(queries["gold.batting_stats_query"].format(match_id=match_id))
        if not len(info):
            logger.info("No Data")
            return
        utils.insert_data(info, "gold.batting_stats")

    def process_bowling_stats(self, match_id):
        """
        Processes bowling statistics for the specified match and inserts into gold.bowling_stats table.

        Args:
            match_id (str): The match identifier for which bowling stats are processed.
        """
        database.execute_statement(queries["gold.bowling_stats"])
        info = database.fetch_info(queries["gold.bowling_stats_query"].format(match_id=match_id))
        if not len(info):
            logger.info("No Data")
            return
        utils.insert_data(info, "gold.bowling_stats")

    def process_fielding_stats(self, match_id):
        """
        Processes fielding statistics for the specified match and inserts into gold.fielding_stats table.

        Args:
            match_id (str): The match identifier for which fielding stats are processed.
        """
        database.execute_statement(queries["gold.fielding_stats"])
        info = database.fetch_info(queries["gold.fielding_stats_query"].format(match_id=match_id))
        if not len(info):
            logger.info("No Data")
            return
        utils.insert_data(info, "gold.fielding_stats")

    def set_gold_process_status(self, process_name, match_id, status):
        """
        Records the current processing status in the gold.stats_processed table.

        Args:
            process_name (str): Name of the processing step (e.g., match_info, batting_stats).
            match_id (str): Match identifier associated with the processing.
            status (str): Processing status (e.g., STARTED, FINISHED, FAILED).
        """
        database.execute_statement(f"INSERT INTO gold.stats_processed(process_name, match_id, status) VALUES('{process_name}', '{match_id}', '{status}')")
