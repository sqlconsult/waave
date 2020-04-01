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


def clear_venues_table(cxn):
    # clear venues table before repopulating
    cursor = cxn.cursor()
    cursor.execute('TRUNCATE TABLE venues')
    cursor.close
    print('Truncated my_db.venues')


def fill_venues_table(cxn, db_obj):
    # printable character set for filtering api results
    printable = set(string.printable)

    app_key = 'BwndJZPhjvnDbKX5'
    search_param = 'l=New+York+City&t=Today'
    
    base_url = 'http://api.eventful.com/json/venues/search?&app_key={app_key}'.\
        format(app_key=app_key)
    
    # TODO: Only 5 pages for testing
    page_count = 5
    page_num = 1
    venues = []
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

        for i in range(len(data['venues']['venue'])):
            ven = data['venues']['venue'][i]

            # filter non-ascii chars from 
            # venue name and address
            venue_address = None
            if ven['venue_name']:
                tmp_name = ''.join(filter(lambda x: x in printable, ven['venue_name']))
                venue_name = tmp_name[:255]

            venue_address = None
            if ven['address']:
                tmp_address = ''.join(filter(lambda x: x in printable, ven['address']))
                venue_address = tmp_address[:100]

            venue_description = None
            if ven['description']:
                tmp_description = ''.join(filter(lambda x: x in printable, ven['description']))
                venue_description = tmp_description[:255]

            ven_tuple = (str(ven['id']),
                         str(ven['region_name']),
                         float(ven['latitude']),
                         float(ven['longitude']),
                         venue_address,
                         str(ven['city_name']),
                         str(ven['country_abbr']),
                         venue_name,
                         venue_description,
                         str(ven['venue_type']))

            venues.append(ven_tuple)

            if len(venues) >= 200:
                save_venues_to_db(cxn, db_obj, venues)
                cxn.commit()
                venues = []

        # increment page number to get next page from server
        page_num += 1

    # save any excess
    if venues:
        save_venues_to_db(cxn, db_obj, venues)
        cxn.commit()
        venues = []

def save_venues_to_db(cxn, db_obj, venues):
    sql = """INSERT INTO venues (
                venue_id,          venue_region,  venue_latitude,     venue_longitude,
                venue_address,     venue_city,    venue_country_abbr, venue_name,
                venue_description, venue_type ) 
                VALUES ( %s, %s, %s, %s, 
                         %s, %s, %s, %s,
                         %s, %s)
            """
    try:
        affected_rows = db_obj.exec_many_qry(sql, venues)
    except Exception as ex:
        print('Exception:', ex)
        for i, t in enumerate(venues):
            print(i, t)

    print("Number of venues inserted: {0:,}".format(affected_rows))


def run():
    # open connection to d/b
    db_obj = myDb()
    cxn = db_obj.cxn_open()

    # clear airports table before repopulating
    clear_venues_table(cxn)
    
    # populate airports table
    fill_venues_table(cxn, db_obj)

    # close connection to d/b
    db_obj.cxn_close()


if __name__  == '__main__':
    run()
