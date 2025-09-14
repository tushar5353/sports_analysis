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

class ProcessSilver:
    """
    Processes cricket match JSON files into structured 'silver' layer data and loads into database tables.

    Attributes:
        json_file_path (str): Path to the JSON file to process.
    """

    def __init__(self, json_file_path):
        """
        Initialize with the JSON file path to be processed.

        Args:
            json_file_path (str): File path of the cricket match JSON.
        """
        self.json_file_path = json_file_path

    def process(self):
        """
        Orchestrates the full processing of the JSON file.

        This method logs the start, sets file status to 'STARTED', processes match and innings data,
        inserts the data into silver tables, and updates the file status to 'FINISHED'.
        On exceptions, logs the error, sets status to 'FAILED', and exits the program.
        """
        try:
            logger.info(f"Processing File :: {self.json_file_path}")
            self.set_file_status(self.json_file_path, "STARTED")
            match_info = self.process_match_info()
            innings_info = self.process_innings_deliveries(match_info[0])
            utils.insert_data(match_info, "silver.matches")
            utils.insert_data(innings_info, "silver.innings_deliveries")
            self.set_file_status(self.json_file_path, "FINISHED")
        except Exception as e:
            logger.error(f"Exception :: {e}", exc_info=True)
            self.set_file_status(self.json_file_path, "FAILED")
            sys.exit(1)

    def process_match_info(self):
        """
        Extracts and processes match-level metadata from the JSON file.

        Parses match details such as match_id, event, dates, type, officials, outcome, venue,
        player of match, and more, converting nested structures to JSON strings.

        Returns:
            list of dict: Processed match information as a single-element list containing a dictionary.
        """
        processed_match_info = []
        info = json.load(open(self.json_file_path))["info"]
        match_type_number = info.get('match_type_number', self.json_file_path.split("/")[-1].replace(".json", ""))
        dates = [datetime.strptime(d, '%Y-%m-%d').date() for d in info.get('dates', ['0000-00-00'])]
        processed_match_info.append({
            "match_id": "-".join([info['match_type'], str(match_type_number)]),
            "event": json.dumps(info.get('event', {"name": "NA"})),
            "match_number": match_type_number,
            "dates": dates,
            "match_type": info.get('match_type', "NA"),
            "officials": json.dumps(info.get('officials', {"NA": "NA"})),
            "outcome": json.dumps(info.get('outcome', {"NA", "NA"})),
            "overs": info.get('overs', 0),
            "players": json.dumps(info.get('players', {})),
            "match_type_number": match_type_number,
            "season": info.get('season', "NA"),
            "team_type": info.get('team_type', "NA"),
            "toss_winner": info.get('toss', {"winner": "NA"}).get('winner'),
            "toss_winner_decision": info.get('toss', {"decision", "NA"}).get('decision'),
            "venue": info.get('venue', "NA"),
            "city": info.get('city', "NA"),
            "source": self.json_file_path,
            "player_of_match": info.get('player_of_match', []),
            "gender": info.get('gender', 'male')
        })
        return processed_match_info

    def process_innings_deliveries(self, match_info):
        """
        Processes delivery-level details for each innings from the JSON file.

        Extracts each delivery's over number, ball number, batter, bowler, runs, extras, wickets,
        and associates with the given match ID and inning number.

        Args:
            match_info (dict): Dictionary containing match metadata, used to get the match_id.

        Returns:
            list of dict: List of dictionaries representing each delivery's processed data.
        """
        processed_innings_info = []
        innings_info = json.load(open(self.json_file_path))["innings"]
        inning_num = 1
        for inning in innings_info:
            if "overs" not in inning:
                print(inning)
                continue
            for overs in inning['overs']:
                over_num = overs['over'] + 1
                ball_count = 1
                for delivery in overs['deliveries']:
                    extras = delivery.get("extras", {})
                    wickets = delivery.get("wickets", [])
                    processed_innings_info.append({
                        "over_num": over_num,
                        "ball_num": ball_count,
                        "batter": delivery['batter'],
                        "bowler": delivery['bowler'],
                        "runs": json.dumps(delivery['runs']),
                        "extras": json.dumps(extras),
                        "wickets": json.dumps(wickets),
                        "match_id": match_info["match_id"],
                        "source": self.json_file_path,
                        "inning_num": inning_num
                    })
                    if "extras" not in delivery:
                        ball_count += 1
            inning_num += 1
        return processed_innings_info

    def set_file_status(self, file_name, status):
        """
        Inserts or updates the processing status of a file in the silver.files_processed table.

        Args:
            file_name (str): The file path or file name being tracked.
            status (str): Processing status - expected values include 'STARTED', 'FINISHED', 'FAILED'.
        """
        database.execute_statement(f"INSERT INTO silver.files_processed(file_name, status) VALUES('{file_name}', '{status}')")

def create_tables():
    """
    Creates the silver schema and required tables if they do not exist.

    Tables created:
        - silver.matches
        - silver.innings_deliveries
        - silver.files_processed
    """
    database.execute_statement("CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION postgres")
    database.execute_statement(queries["silver.matches"])
    database.execute_statement(queries["silver.innings_deliveries"])
    database.execute_statement(queries["silver.files_processed"])

def drop_tables():
    """
    Drops all tables in the silver schema if they exist, including cascading dependencies.

    Tables dropped:
        - silver.matches
        - silver.innings_deliveries
        - silver.files_processed
    """
    database.execute_statement("DROP TABLE IF EXISTS silver.matches CASCADE")
    database.execute_statement("DROP TABLE IF EXISTS silver.innings_deliveries CASCADE")
    database.execute_statement("DROP TABLE IF EXISTS silver.files_processed CASCADE")

