"""Trunctate and refill states and cities tables from cities.txt"""
import csv
import string
import sys

from myDb import myDb


def clear_states_and_cities_tables(cxn):
    """
    Clear states and cities tables before repopulating
    :param cxn   Database connection
    """
    cursor = cxn.cursor()
    cursor.execute('TRUNCATE TABLE states')
    cursor.execute('TRUNCATE TABLE cities')
    cursor.close()
    print('Truncated my_db.states and cities')


def get_states_and_cities():
    """
    Retrieve states and cities from input file
    """
    file_nm = '/home/steve/waave/cities.txt'
    printable = set(string.printable)

    state_tuples = []
    city_tuples = []
    with open(file_nm, 'r') as file_hdl:
        csv_reader = csv.reader(file_hdl, delimiter='~')

        # skip header row
        next(csv_reader)

        line_count = 0
        for input_row in csv_reader:

            line_count += 1
            if line_count % 10 == 0:
                print('Processing input line # {0:}'.format(line_count))

            state_abbr = ''.join(filter(lambda x: x in printable, input_row[0]))
            state_name = ''.join(filter(lambda x: x in printable, input_row[1]))
            state_capital = ''.join(filter(lambda x: x in printable, input_row[2]))
            major_cities = ''.join(filter(lambda x: x in printable, input_row[3]))

            tmp_tuple = (state_abbr, state_name, state_capital)
            state_tuples.append(tmp_tuple)

            major_city_list = major_cities.split(',')
            for city in major_city_list:
                tmp_tuple = (state_abbr, city)
                city_tuples.append(tmp_tuple)

    return state_tuples, city_tuples


def save_states_and_cities_to_db(db_obj, state_tuples, city_tuples):
    """
    Save state and city lists to database tables states & cities
    :param db_obj           Database object
    :param state_tuples     List of states to save
    :param city_tuples      List of cities to save
    """
    # insert states into my_db.states
    sql = """INSERT INTO states (state_abbr, state_name, state_capital)
                VALUES ( %s, %s, %s )
          """
    affected_rows = db_obj.exec_many_qry(sql, state_tuples)
    print('Number of states inserted: {0:,}'.format(affected_rows))

    # insert cities into my_db.cities
    sql = """INSERT INTO cities (state_abbr, city_name)
                VALUES ( %s, %s )
            """
    affected_rows = db_obj.exec_many_qry(sql, city_tuples)
    print('Number of cities inserted: {0:,}'.format(affected_rows))

    # close connection to d/b
    db_obj.cxn.commit()


def run():
    """
    Main entry point
    """
    # open connection to d/b
    db_obj = myDb()
    cxn = db_obj.cxn_open()

    # clear airports table before repopulating
    clear_states_and_cities_tables(cxn)

    # get states and cities from input file
    states, cities = get_states_and_cities()

    # save to database
    save_states_and_cities_to_db(db_obj, states, cities)

    # close connection to d/b
    db_obj.cxn_close()


if __name__ == '__main__':
    run()
    sys.exit(0)
