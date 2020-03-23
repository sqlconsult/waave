import datetime
import json
import os
import string
import sys
import time

from air_transport_api import Air_Transport_Api
from myDb import myDb

# sample browser url
# http://flightxml.flightaware.com/json/FlightXML2/AirlineFlightSchedules?origin=KJFK&startDate=1582001999&endDate=1582088399&howMany=15&offset=0

class FlightInfo():
    def __init__(self, module_logger, icao_codes):
        self.logger = module_logger
        self.icao_codes = icao_codes
        self.myDb = myDb()


    def run(self):
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

        process_date = time.strftime('%Y-%m-%d', time.localtime(endDate))

        # open connection to d/b
        cxn = self.myDb.cxn_open()

        # clear airports table before repopulating
        self.clear_process_date(process_date, cxn)


        # airport code
        # airport_codes = ['KJFK', 'KEWR', 'KLGA']
        for airport in self.icao_codes:
            msg = 'Retrieving flight data for {airport} between {startDate} and {endDate}'.\
                format(airport=airport, startDate=startDate, endDate=endDate)
            self.logger.info(msg)

            # arrivals
            payload = { 'destination': airport,
                        'startDate': startDate,
                        'endDate': endDate,
                        'offset': 0,
                        'howMany': 15}

            arrival_flighs = self.get_flight_info(
                airport, startDate, endDate, payload, 'arrival')
            msg = 'Found {0:,} arriving flights for {1}'.\
                format(len(arrival_flighs), airport)
            self.logger.info(msg)

            self.save_flight_info_to_db(cxn, arrival_flighs)
            msg = 'Saved {0} arriving flights to database'.format(airport)
            self.logger.info(msg)
            #
            # departures
            #
            payload = { 'origin': airport,
                        'startDate': startDate,
                        'endDate': endDate,
                        'offset': 0,
                        'howMany': 15}

            departure_flighs = self.get_flight_info(
                airport, startDate, endDate, payload, 'departure')
            msg = 'Found {0:,} departing flights for {1}'.\
                format(len(arrival_flighs), airport)
            self.logger.info(msg)

            self.save_flight_info_to_db(cxn, departure_flighs)
            msg = 'Saved {0} departing flights to database'.format(airport)
            self.logger.info(msg)

        # all done, commit everything and close d/b connection
        cxn.commit()
        self.myDb.cxn_close()


    def clear_process_date(self, process_date, cxn):
        # clear airports table before repopulating
        cursor = cxn.cursor()
        # remove any data for this process date
        cursor.execute(
            """
            DELETE FROM flights WHERE process_date = '{0}'
            """.format(process_date))
        cursor.close
        msg = 'Cleared my_db.flights for process date = {0}'.format(process_date)
        self.logger.info(msg)


    def get_flight_info(self, airport, startDate, endDate, payload, direction):
        # processing date = end date
        process_date = time.strftime('%Y-%m-%d', time.localtime(endDate))

        prev_offset = -1
        printable = set(string.printable)

        request_num = 1

        flight_api = Air_Transport_Api()
        response = flight_api.get_api_data('AirlineFlightSchedules', payload)

        ret_val = []

        while response.status_code == 200 and \
            payload['offset'] >= 0 and \
            request_num < 200:
            msg = 'Request {0:,} successful, offset = {1:,}'.format(
                request_num, payload['offset'])
            self.logger.info(msg)

            response_json = response.json()

            if 'AirlineFlightSchedulesResult' not in response_json:
                self.logger.error('=== response.json ===')
                msg = response.json()
                self.logger.error(msg)
                sys.exit(0)

            flight_schedule = response_json['AirlineFlightSchedulesResult']        

            prev_offset = payload['offset']
            payload['offset'] = flight_schedule['next_offset']

            data = flight_schedule['data']
            for flight_info in data:
                # remove non-ascii characters from response
                ident = ''.join(filter(lambda x: x in printable, flight_info['ident']))
                origin = ''.join(filter(lambda x: x in printable, flight_info['origin']))
                destination = ''.join(filter(lambda x: x in printable, flight_info['destination']))
                schedule_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(flight_info['departuretime']))
                aircraft_type = ''.join(filter(lambda x: x in printable, flight_info['aircrafttype']))
                seats_cabin_first = flight_info['seats_cabin_first']
                seats_cabin_business = flight_info['seats_cabin_business']
                seats_cabin_coach = flight_info['seats_cabin_coach']

                tmp_tuple = (process_date,
                            airport,
                            ident,
                            origin,
                            destination,
                            schedule_time,
                            aircraft_type,
                            direction,
                            seats_cabin_first,
                            seats_cabin_business,
                            seats_cabin_coach,
                            request_num, 
                            prev_offset)

                ret_val.append(tmp_tuple)
        
            # get next data set
            response = flight_api.get_api_data('AirlineFlightSchedules', payload)
            request_num += 1
                
        return ret_val


    def save_flight_info_to_db(self, cxn, flights):
        sql = """INSERT INTO flights (
                    process_date,      airport,              ident,             origin,
                    destination,       schedule_time,        aircraft_type,     direction,
                    seats_cabin_first, seats_cabin_business, seats_cabin_coach, request_num,
                    offset ) 
                    VALUES ( %s, %s, %s, %s, 
                             %s, %s, %s, %s,
                             %s, %s, %s, %s,
                             %s )
                """
        affected_rows = self.myDb.exec_many_qry(sql, flights)
        print("Number of flights inserted: {0:,}".format(affected_rows))

"""
query:

select f.process_date,
       airport,
       left(f.ident,3) as airline_code,
       a.short_name,
       sum(f.seats_cabin_first),
       sum(f.seats_cabin_business), 
       sum(seats_cabin_coach) 
from   flights f
left join airlines a on left(f.ident,3) = airline_icao_code
where  f.process_date = '2020-03-04'
and    f.direction = 'departure'
and    hour(f.schedule_time) = 9
group by f.process_date, airport, left(f.ident,3), a.short_name
order by left(ident,3);
"""
