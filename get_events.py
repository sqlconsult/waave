import requests
import datetime
import string

from myDb import myDb

class GetEvent:
    def __init__(self, city_list, category_list, logger):
        self.city_list = city_list
        self.category_list = category_list
        self.logger = logger


    def clear_events_table(self, cxn):
        # clear venues table before repopulating
        cursor = cxn.cursor()
        cursor.execute('TRUNCATE TABLE events')
        cursor.close
        self.logger.info('Truncated my_db.events')


    def fill_events_table(self, cxn, db_obj):
        # printable character set for filtering api results
        printable = set(string.printable)

        # A list of categories may be specified separated by commas
        # http://api.eventful.com/json/events/search?&app_key=BwndJZPhjvnDbKX5&l="New York, NY"&t=Today
        # http://api.eventful.com/json/events/search?&app_key=BwndJZPhjvnDbKX5&l=%22Albany,%20NY%22&t=Today
        # http://api.eventful.com/json/events/search?&app_key=BwndJZPhjvnDbKX5&l=New+York+City&t=Today

        app_key = 'BwndJZPhjvnDbKX5'
        search_param = 'l=New+York+City&t=Today&'\
            'page_size=100&sort_order=relevance&sort_direction=ascending'
        
        base_url = 'http://api.eventful.com/json/events/search?&app_key={app_key}'.\
            format(app_key=app_key)
        
        # TODO: Only 5 pages for testing
        page_count = 5
        page_num = 1
        events = []
        while page_num <= page_count:
            # request this page number
            page_num_param = 'page_number={0}'.format(page_num)

            # build url parameters
            url_params = '&{search}&{page_num}'.\
                format(search=search_param, page_num=page_num_param)
            url = base_url + url_params

            # request data from server
            data = requests.get(url).json()
    
            if page_num % 25 == 0:
                msg = 'Retrieved page {page_number:,} of {page_count:,}'.\
                    format(page_number=page_num, page_count=page_count)
                self.logger.info(msg)

            # update total number of pages from return data on 1st request
            # TODO: Uncomment after testing
            if page_num == 1:
                page_count = int(data['page_count'])

            for i in range(len(data['events']['event'])):
                evt = data['events']['event'][i]

                # filter non-ascii chars from 
                # venue name and address
                title = None
                if evt['title']:
                    tmp_title = ''.join(filter(lambda x: x in printable, evt['title']))
                    title = tmp_title[:255]

                venue_address = None
                if evt['venue_address']:
                    tmp_address = ''.join(filter(lambda x: x in printable, evt['venue_address']))
                    venue_address = tmp_address[:255]

                venue_name = None
                if evt['venue_name']:
                    tmp_name = ''.join(filter(lambda x: x in printable, evt['venue_name']))
                    venue_name = tmp_name[:255]

                start_time = None
                if evt['start_time']:
                    start_time = str(evt['start_time'])

                stop_time = None
                if evt['start_time']:
                    start_time = str(evt['start_time'])


                evt_tuple = (str(evt['region_abbr']),
                    str(evt['postal_code']),
                    float(evt['latitude']),
                    str(evt['id']),
                    str(evt['city_name']),
                    float(evt['longitude']),
                    str(evt['country_name']),
                    str(evt['country_abbr']),
                    str(evt['region_name']),
                    start_time,
                    title,
                    venue_address,
                    str(evt['venue_id']),
                    stop_time,
                    venue_name)

                events.append(evt_tuple)

                if len(events) >= 200:
                    self.save_events_to_db(cxn, db_obj, events)
                    cxn.commit()
                    events = []

            # increment page number to get next page from server
            page_num += 1

        # save any excess
        if events:
            self.save_events_to_db(cxn, db_obj, events)
            cxn.commit()
            events = []

    def save_events_to_db(self, cxn, db_obj, events):
        sql = """INSERT INTO events (
                        region_abbr, postal_code, latitude,     id,
                        city_name,   longitude,   country_name, country_abbr,
                        region_name, start_time,  title,        venue_address,
                        venue_id,    stop_time,   venue_name) 
                    VALUES ( %s, %s, %s, %s, 
                            %s, %s, %s, %s, 
                            %s, %s, %s, %s,
                            %s, %s, %s)
                """
        try:
            affected_rows = db_obj.exec_many_qry(sql, events)
        except Exception as ex:
            msg = 'Exception: {ex}'.format(ex=ex)
            self.logger.error(msg)
            for i, t in enumerate(events):
                msg = '{0} {1}'.format(i, t)
                self.logger.error(msg)

        self.logger.info("Number of events inserted: {0:,}".format(affected_rows))


    def run(self):
        # open connection to d/b
        db_obj = myDb()
        cxn = db_obj.cxn_open()

        # clear airports table before repopulating
        self.clear_events_table(cxn)
        
        # populate airports table
        self.fill_events_table(cxn, db_obj)

        # close connection to d/b
        db_obj.cxn_close()
