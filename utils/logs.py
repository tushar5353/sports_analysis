
import logging
from logging.config import dictConfig
import sys

from sports_analysis.utils.config import Config

config_obj = Config()
log_file_name = config_obj.get_config('environment')['log_file']

def configure_logging() -> None:
    dictConfig(
        {
            'version': 1,
            'disable_existing_loggers': True,
            'formatters': {
                'console': {
                    'class': 'logging.Formatter',
                    'datefmt': '%H:%M:%S',
                    # formatter decides how our console logs look, and what info is included.
                    'format': '%(levelname)s:\t%(asctime)s %(name)s:%(lineno)d [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s',
                    "datefmt": "%Y-%m-%d %H:%M:%S"
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    # Filter must be declared in the handler, otherwise it won't be included
                    'stream': sys.stdout,
                    'formatter': 'console',
                },
                'file': {
                    'class': 'logging.handlers.TimedRotatingFileHandler',
                    'level': 'INFO',
                    'formatter': 'console',
                    'filename': log_file_name,
                    'when': 'midnight',
                    'interval': 1,
                    'backupCount': 15,
                },
            },
            # Loggers can be specified to set the log-level to log, and which handlers to use
            'loggers': {
                # project logger
                'app': {'handlers': ['console'], 'level': 'DEBUG', 'propagate': False},
                # third-party package loggers
                'databases': {'handlers': ['console'], 'level': 'WARNING'},
                'httpx': {'handlers': ['console'], 'level': 'INFO'},
                'asgi_correlation_id': {'handlers': ['console'], 'level': 'WARNING'},
            },
        }
    )

    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        """
        Modifying log message to replace next line with <N> to easily
        grep the logs on same line
        """
        message = ""
        record = old_factory(*args, **kwargs) # get the unmodified record
        record.msg = record.msg.replace("\n", "<N>") # change the original `lineno` attribute
        return record

    logging.setLogRecordFactory(record_factory)
