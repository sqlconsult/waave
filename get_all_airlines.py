import csv
import string
import sys

from air_transport_api import Air_Transport_Api
from myDb import myDb

class GetAirlines:
    def __init__(self, module_logger):
        self.file_path = '/home/steve/waave/airline_info.csv'
        self.logger = module_logger

    def run(self):
        # get airline icao and iata codes using api
        airline_icao_codes = self.get_airline_icao_ids()
        
        # write to file for archive
        with open(self.file_path, 'w+') as f:
            f.write('airline,name,shortName,callSign,country,airlineUrl,phone\n')
            for i, t in enumerate(airline_icao_codes):
                out_line = '|'.join(t) + '\n'
                f.write(out_line)

        # save to d/b
        self.save_airline_info_to_db(airline_icao_codes)

    def get_airline_icao_ids(self):
        # get list of all airlines
        flight_api = Air_Transport_Api()
        response = flight_api.get_api_data('AllAirlines')
        response_json = response.json()

        all_airlines = response_json['AllAirlinesResult']
        data = all_airlines['data']

        num_airlines = len(data)
        print('Found {0:,} icao airline codes'.format(num_airlines))

        # printable character set for filtering api results
        printable = set(string.printable)

        # list of tuples tht are returned
        ret_val = []
        
        # loop over returned api airline data
        for i, airline in enumerate(data):

            if i % 100 == 0:
                msg = 'Processing {0:,} of {1:,} airline icao codes'.\
                    format(i, num_airlines)
                self.logger.info(msg)

            # get more detailed airline info using api
            payload = {'airlineCode': airline}
            response = flight_api.get_api_data('AirlineInfo', payload)
            response_json = response.json()

            # parse response
            if 'AirlineInfoResult' in response_json:
                # only save printable characters
                airline_info = response_json['AirlineInfoResult']
                name = ''.join(filter(lambda x: x in printable, airline_info['name']))
                shortName = ''.join(filter(lambda x: x in printable, airline_info['shortname']))
                callSign = ''.join(filter(lambda x: x in printable, airline_info['callsign']))
                country  = ''.join(filter(lambda x: x in printable, airline_info['country']))
                airlineUrl = ''.join(filter(lambda x: x in printable, airline_info['url']))
                phone = ''.join(filter(lambda x: x in printable, airline_info['phone']))

                # create tuple and add to return list
                tmp_tuple = (airline, name, shortName, callSign, country, airlineUrl, phone)
                ret_val.append(tmp_tuple)

        return ret_val


    def save_airline_info_to_db(self, airlines):
        sql = """INSERT INTO airlines (
                    airline_icao_code, airline_name, short_name, call_sign,
                    country,           airline_url,  phone) 
                    VALUES ( %s, %s, %s, %s,
                            %s, %s, %s )
                """
        # open connection to d/b
        db_obj = myDb()
        cxn = db_obj.cxn_open()

        # clear airlines table before repopulating
        db_obj.exec_qry('TRUNCATE TABLE airlines')
            
        affected_rows = db_obj.exec_many_qry(sql, airlines)

        # close connection to d/b
        cxn.commit()
        db_obj.cxn_close()

        print("Number of airlines inserted: {0:,}".format(affected_rows))


def import_from_file():
    file_path = '/home/steve/waave/airline_info.csv'
    printable = set(string.printable)

    airlines = []
    with open(file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter='|')

        # skip header row
        next(csv_reader)

        line_count = 0
        for input_row in csv_reader:

            line_count += 1
            if line_count % 100 == 0:
                print('Processing input line # {0:}'.format(line_count))

            airline = input_row[0]
            name = ''.join(filter(lambda x: x in printable, input_row[1]))
            shortName = ''.join(filter(lambda x: x in printable, input_row[2]))
            callSign = ''.join(filter(lambda x: x in printable, input_row[3]))
            country  = ''.join(filter(lambda x: x in printable, input_row[4]))
            airlineUrl = ''.join(filter(lambda x: x in printable, input_row[5]))
            phone = ''.join(filter(lambda x: x in printable, input_row[6]))

            # create tuple and add to return list
            tmp_tuple = (airline, name, shortName, callSign, country, airlineUrl, phone)
            airlines.append(tmp_tuple)

    sql = """INSERT INTO airlines (
                airline_icao_code, airline_name, short_name, call_sign,
                country,           airline_url,  phone) 
                VALUES ( %s, %s, %s, %s,
                        %s, %s, %s )
            """
    # open connection to d/b
    db_obj = myDb()
    cxn = db_obj.cxn_open()

    # clear airlines table before repopulating
    db_obj.exec_qry('TRUNCATE TABLE airlines')

    for i in range(10):
        print(airlines[i])

    sys.exit(0)
        
    affected_rows = db_obj.exec_many_qry(sql, airlines)

    # close connection to d/b
    cxn.commit()
    db_obj.cxn_close()

    print("Number of airlines inserted: {0:,}".format(affected_rows))
            

if __name__ == '__main__':
    print('import_from_file')
    import_from_file()
    sys.exit(0)