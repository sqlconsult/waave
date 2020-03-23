#!/usr/bin/env python3

import datetime
import logging


def start_logger(app_name):
    # Create logger with 'spam_application'
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)

    # Create file handler which logs debug messages
    log_fil_nm = 'logs/monitor_log_{date:%Y%m%d_%H%M%S}.log'.format(date=datetime.datetime.now())
    fh = logging.FileHandler(log_fil_nm)
    fh.setLevel(logging.DEBUG)

    # Create console handler with a higher log level, error
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
                                  datefmt="%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

