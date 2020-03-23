#!/usr/bin/env python3

import datetime
import os
import string
import sys

from air_transport_api import Air_Transport_Api

def test_all_airlines():
    flight_api = Air_Transport_Api()

    # request all airlines
    response = flight_api.get_api_data('AllAirlines')

    # successful request status?
    assert response.status_code == 200

    response_json = response.json()

    # was data returned?
    assert 'AllAirlinesResult' in response_json


def test_airline_info():
    airline = 'DAL'    # Delta
    payload = {'airlineCode': airline}
    flight_api = Air_Transport_Api()

    # request Delta airline info
    response = flight_api.get_api_data('AirlineInfo', payload)

    # successful?
    assert response.status_code == 200

    response_json = response.json()

    # was data returned?
    assert 'AirlineInfoResult' in response_json

def test_flight_schedule():
    # request 15 arrival flights at JFK for today

    # get today (23:59:59) as end date in epoch time
    today = datetime.datetime.today()
    endDate = today.strftime('%Y-%m-%d 23:59:59')
    endDateTime = datetime.datetime.strptime(endDate, '%Y-%m-%d %H:%M:%S')
    endDate = int(endDateTime.timestamp())

    # get t-1 (23:59:59) as end start date in epoch time
    t_minus_one = today - datetime.timedelta(days=1)
    startDate = t_minus_one.strftime('%Y-%m-%d 23:59:59')
    startDateTime = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
    startDate = int(startDateTime.timestamp())

    airport = 'KJFK'
    payload = { 'destination': airport,
                'startDate': startDate,
                'endDate': endDate,
                'offset': 0,
                'howMany': 15}

    flight_api = Air_Transport_Api()
    response = flight_api.get_api_data('AirlineFlightSchedules', payload)

    # successful?
    assert response.status_code == 200

    response_json = response.json()

    # was data returned?
    assert 'AirlineFlightSchedulesResult' in response_json

