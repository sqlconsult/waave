#!/usr/bin/env python3
import argparse
import datetime
import logging
import sys

import logger
from import_airports import AirportImporter
from get_all_airlines import GetAirlines
from get_flight_schedules import FlightInfo

def cmd_line_parse():
    parser = argparse.ArgumentParser(description='Get airline flight data using FlightAware API')

    parser.add_argument(
        '--flights',
        '-f',
        required=False,
        type=str,
        dest='flights',
        help='Comma separated airport list (example, KJFK,KLGA,KEWR). Default=KJFK',
        default='KJFK')

    parser.add_argument(
        '--airports',
        '-p',
        required=False,
        action='store_true',
        dest='airports',
        help='Refill airports database table using airports.csv')    

    parser.add_argument(
        '--airlines',
        '-l',
        required=False,
        action='store_true',
        dest='airlines',
        help='Refill airports database table using AllAirlines and AirlineInfo API')

    args = parser.parse_args()

    return args


def main():
    # Start logger
    app_name = __file__.split('.')[0]
    logger.start_logger(app_name)

    module_logger = logging.getLogger('{app_name}.controller'.format(app_name=app_name))
    module_logger.info('<<<<< Starting >>>>>')

    # get command line args
    cmd_line_args = cmd_line_parse()

    if cmd_line_args.airlines:
        obj = GetAirlines(module_logger)
        obj.run()

    if cmd_line_args.airports:
        obj = AirportImporter(module_logger)
        obj.run()

    if cmd_line_args.flights:
        icao_codes = cmd_line_args.flights.split(',')
        obj = FlightInfo(module_logger, icao_codes)
        obj.run()

    module_logger.info('<<<<< Done >>>>>')


if __name__ == '__main__':
    main()
    sys.exit(0)