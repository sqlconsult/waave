import argparse
import datetime
import logging
import sys

import logger

from get_events import GetEvent


# http://api.eventful.com/json/categories/list?app_key=BwndJZPhjvnDbKX5
categories = [
    {"name":"Concerts & Tour Dates","event_count":None,"id":"music"},
    {"name":"Conferences & Tradeshows","event_count":None,"id":"conference"},
    {"name":"Comedy","event_count":None,"id":"comedy"},
    {"name":"Education","event_count":None,"id":"learning_education"},
    {"name":"Kids & Family","event_count":None,"id":"family_fun_kids"},
    {"name":"Festivals","event_count":None,"id":"festivals_parades"},
    {"name":"Film","event_count":None,"id":"movies_film"},
    {"name":"Food & Wine","event_count":None,"id":"food"},
    {"name":"Fundraising & Charity","event_count":None,"id":"fundraisers"},
    {"name":"Art Galleries & Exhibits","event_count":None,"id":"art"},
    {"name":"Health & Wellness","event_count":None,"id":"support"},
    {"name":"Holiday","event_count":None,"id":"holiday"},
    {"name":"Literary & Books","event_count":None,"id":"books"},
    {"name":"Museums & Attractions","event_count":None,"id":"attractions"},
    {"name":"Neighborhood","event_count":None,"id":"community"},
    {"name":"Business & Networking","event_count":None,"id":"business"},
    {"name":"Nightlife & Singles","event_count":None,"id":"singles_social"},
    {"name":"University & Alumni","event_count":None,"id":"schools_alumni"},
    {"name":"Organizations & Meetups","event_count":None,"id":"clubs_associations"},
    {"name":"Outdoors & Recreation","event_count":None,"id":"outdoors_recreation"},
    {"name":"Performing Arts","event_count":None,"id":"performing_arts"},
    {"name":"Pets","event_count":None,"id":"animals"},
    {"name":"Politics & Activism","event_count":None,"id":"politics_activism"},
    {"name":"Sales & Retail","event_count":None,"id":"sales"},
    {"name":"Science","event_count":None,"id":"science"},
    {"name":"Religion & Spirituality","event_count":None,"id":"religion_spirituality"},
    {"name":"Sports","event_count":None,"id":"sports"},
    {"name":"Technology","event_count":None,"id":"technology"},
    {"name":"Other & Miscellaneous","event_count":None,"id":"other"}]


def cmd_line_parse():
    parser = argparse.ArgumentParser(description='Get events using Eventful API')

    parser.add_argument(
        '--cities',
        '-c',
        required=False,
        type=str,
        dest='cities',
        help='Comma separated city list (example, "New York,Boston"). Default=New York',
        default='New York')

    parser.add_argument(
        '--categories',
        '-g',
        required=False,
        type=str,
        dest='categories',
        help='Comma separated category list (example, "music,sports"). Default=None',
        default=None)

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

    # remove leading and trailing spaces from each input city argument
    tmp_city_list = cmd_line_args.cities.split(',')
    city_list = [x.strip(' ') for x in tmp_city_list]

    # get valid categories
    valid_categories = set()
    for cat in categories:
        valid_categories.add(cat['id'])

    category_list = None
    if cmd_line_args.categories:
        input_categories = set(cmd_line_args.categories.split(','))
        tmp_category_list = list(valid_categories.intersection(input_categories))
        category_list = [x.strip(' ') for x in tmp_category_list]

    # display input cities and categories
    msg = 'Cities: {0}'.format(','.join(city_list))
    module_logger.info(msg)

    if category_list:
        msg = 'Categories: {0}'.format(','.join(category_list))
    else:
        msg = 'Categories: All'
    module_logger.info(msg)

    event_obj = GetEvent(city_list, category_list, module_logger)

    event_obj.run()

    module_logger.info('<<<<< Done >>>>>')
    

if __name__ == '__main__':
    main()
    sys.exit(0)