import requests
import datetime
import string

from myDb import myDb

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


def clear_events_table(cxn):
    # clear venues table before repopulating
    cursor = cxn.cursor()
    cursor.execute('TRUNCATE TABLE events')
    cursor.close
    print('Truncated my_db.events')


def fill_events_table(cxn, db_obj):
    # printable character set for filtering api results
    printable = set(string.printable)

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
            print(msg)

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
                save_events_to_db(cxn, db_obj, events)
                cxn.commit()
                events = []

        # increment page number to get next page from server
        page_num += 1

    # save any excess
    if events:
        save_events_to_db(cxn, db_obj, events)
        cxn.commit()
        events = []

def save_events_to_db(cxn, db_obj, events):
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
        print('Exception:', ex)
        for i, t in enumerate(events):
            print(i, t)

    print("Number of events inserted: {0:,}".format(affected_rows))


def run():
    # open connection to d/b
    db_obj = myDb()
    cxn = db_obj.cxn_open()

    # clear airports table before repopulating
    clear_events_table(cxn)
    
    # populate airports table
    fill_events_table(cxn, db_obj)

    # close connection to d/b
    db_obj.cxn_close()


if __name__  == '__main__':
    run()
