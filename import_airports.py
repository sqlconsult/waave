import csv
import os
import string
import sys

from myDb import myDb


class AirportImporter():
    def __init__(self, module_logger):
        self.logger = module_logger
        self.myDb = myDb()
        self.file_nm = '/home/steve/waave/airports.csv'


    def run(self):
        # open connection to d/b
        cxn = self.myDb.cxn_open()

        # clear airports table before repopulating
        self.clear_airports_table(cxn)
        
        # populate airports table
        self.fill_airports_table(cxn)

        # close connection to d/b
        self.myDb.cxn_close()


    def clear_airports_table(self, cnx):
        # clear airports table before repopulating
        cursor = cnx.cursor()
        cursor.execute('TRUNCATE TABLE airports')
        cursor.close
        print('Truncated my_db.airports')


    def fill_airports_table(self, cxn):
        # read airports.csv and populate my_db.airports table
        printable = set(string.printable)

        cursor = cxn.cursor()
        with open(self.file_nm, 'r') as f:
            csv_reader = csv.reader(f, delimiter=',')

            # skip header row
            next(csv_reader)

            line_count = 0
            for input_row in csv_reader:

                line_count += 1
                if line_count % 500 == 0:
                    print('Processing input line # {0:}'.format(line_count))
                # remove apostrophes from any input column
                clean_row = '~'.join(input_row)
                clean_row = clean_row.replace("'", "")
                row = clean_row.split('~')

                name = ''.join(filter(lambda x: x in printable, row[3]))

                latitude = row[4] if len(row[4]) > 0 else 0
                longitude = row[5] if len(row[5]) > 0 else 0
                elevation = row[6] if len(row[6]) > 0 else 0

                municipality = ''.join(filter(lambda x: x in printable, row[10]))

                try:
                    cursor.execute(
                        """
                        INSERT INTO airports (
                            id,           ident,         airport_type, name, 
                            latitude_deg, longitude_deg, elevation_ft, continent, 
                            iso_country,  iso_region,    municipality, scheduled_service,
                            icao_code,    iata_code ) VALUES (
                            {0}, '{1}', '{2}', '{3}',
                            {4}, {5}, {6}, '{7}',
                            '{8}', '{9}', '{10}', '{11}',
                            '{12}', '{13}' )
                        """.format(
                        row[0], row[1], row[2], name,
                        latitude, longitude, elevation, row[7],
                        row[8], row[9], municipality, row[11],
                        row[12], row[13] ))
                except Exception as ex:
                    msg = 'Exception: {ex}'.format(ex=ex)
                    self.logger.error(msg)
                    msg = 'Row: [', row, ']'
                    self.logger.error(msg)

            msg = 'Inserted {0:,} rows'.format(line_count)
            self.logger.info(msg)

        cxn.commit()
        cursor.close
