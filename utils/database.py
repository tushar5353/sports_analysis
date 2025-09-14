import os
import logging

import psycopg2

from sports_analysis.utils.config import Config

config_obj = Config()
db_config = config_obj.get_config("environment")['db']

# Initialize the logger
logger = logging.getLogger(os.environ["RUN_TYPE"])


def get_psql_conn():
    """
    Creates and returns a new connection to the PostgreSQL database
    using the configuration parameters from the environment.

    Returns:
        psycopg2.extensions.connection: A connection object to the PostgreSQL database.
    """
    psql_config = db_config["psql"]
    conn = psycopg2.connect(
        user=psql_config['user'],
        password=psql_config['password'],
        host=psql_config['host'],
        port=psql_config['port']
    )
    return conn


def execute_statement(statement, debug=False):
    """
    Executes a single SQL statement on the PostgreSQL database.

    Opens a connection, executes the given statement, commits the transaction,
    and then closes the connection. Optionally logs the statement when debug is True.

    Args:
        statement (str): The SQL statement to execute.
        debug (bool, optional): Flag to log the statement for debugging. Defaults to False.
    """
    if debug:
        logger.info(statement)
    conn = get_psql_conn()
    cursor = conn.cursor()
    cursor.execute(statement)
    conn.commit()
    conn.close()


def fetch_info(statement, debug=False):
    """
    Executes a SQL query and returns all results as a list of dictionaries.

    Opens a connection, executes the query, fetches all rows using a dict cursor,
    closes the connection, and returns the results. Optionally logs the query.

    Args:
        statement (str): The SQL query to execute.
        debug (bool, optional): Flag to log the query for debugging. Defaults to False.

    Returns:
        list of dict: The query results as a list of dictionaries where each dict represents a row.
    """
    if debug:
        logger.info(statement)
    conn = get_psql_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(statement)
    info = cursor.fetchall()
    conn.close()
    return info
