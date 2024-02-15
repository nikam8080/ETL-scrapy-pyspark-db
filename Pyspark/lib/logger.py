import logging
import os.path
import sys


def setup_logging(filename=None):
    if filename is None:
        filename = os.path.basename((sys.argv[0]))

    log_format = "%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logfile.log')
    ])
    logging.info(f"Running {filename}")


def log_info(message):
    logging.info(message)


def log_error(message):
    logging.error(message)


def log_warning(message):
    logging.warning(message)


def log_debug(message):
    logging.debug(message)
