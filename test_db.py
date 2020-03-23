from myDb import myDb

def test_cb_conn():
    # open connection to d/b
    myDb_obj = myDb()
    cxn = myDb_obj.cxn_open()

    assert cxn

    cursor = cxn.cursor()
    cursor.execute('SELECT * FROM airports LIMIT 25;')
    rows = cursor.fetchall()

    assert len(rows) == 25

    cursor.close
    myDb_obj.cxn_close()