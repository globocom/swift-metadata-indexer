import logging
import os


def logger(name):
    log_level = logging.INFO
    # TODO: setar no conf o log level (?)
    if os.environ.get('DEBUG', 'false').lower() == 'true':
        log_level = logging.DEBUG
    log_format = '%(asctime)s - %(levelname)s - [%(name)s]: %(message)s'

    logging.basicConfig(level=log_level,
                        format=log_format,
                        datefmt='%m/%d/%y %H:%M:%S')

    logger = logging.getLogger(name)

    return logger
