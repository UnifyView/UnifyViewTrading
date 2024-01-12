# import modules
from datetime import datetime, timedelta
import yaml
import sqlite3
"""
    This function get all Environment Variabled from yml file.
"""



def get_env(config_file):

    try:
        with open(f'Config/{config_file}', 'r') as file:
            config = yaml.safe_load(file)
    except Exception as err:
        print(' Exception From get_env')
        print(f"Unexpected {err=}, {type(err)=}")

    return config

def get_sqlite_db_connection(config):

    sqlite_database = config['database']
    sqlite_conn = sqlite3.connect(sqlite_database,isolation_level=None)
    sqlite_conn.execute('pragma journal_mode=wal;')

    return sqlite_conn

def get_sqlite_db_connection_np(config):
    try:
        sqlite_database = config['database']
        sqlite_conn = sqlite3.connect(sqlite_database)
        # sqlite_conn.execute('pragma journal_mode=wal;')
        return sqlite_conn
    except Exception as err:
        print(' Exception From get_sqlite_db_connection_np')
        print(f"Unexpected {err=}, {type(err)=}")

# def get_sqlite_db_conn_string(string):

#     sqlite_database = string
#     sqlite_conn = sqlite3.connect(sqlite_database)

#     return sqlite_conn

def get_last_working_day(currentDate):
    holidays = ('2023-06-28','2023-08-15','2023-09-19','2023-10-02','2023-10-24','2023-11-14','2023-11-27','2023-12-25')

    dayInt = currentDate.weekday()
    if dayInt >= 1 and dayInt <= 4:
        previousDay = currentDate - timedelta(days=1)
    elif dayInt == 0:
        previousDay = currentDate - timedelta(days=3)
    elif dayInt == 6:
        previousDay = currentDate - timedelta(days=2)
    elif dayInt == 5:
        previousDay = currentDate - timedelta(days=1)                

    for d in holidays:
        if datetime.strptime(d, "%Y-%m-%d") == previousDay:
            if dayInt >= 1 and dayInt <= 4:
                previousDay = previousDay - timedelta(days=1)
            elif dayInt == 0:
                previousDay = previousDay - timedelta(days=3)
            elif dayInt == 6:
                previousDay = previousDay - timedelta(days=2)
            elif dayInt == 5:
                previousDay = previousDay - timedelta(days=1) 

    return datetime.strftime(previousDay, '%Y-%m-%d')

def get_expiry_date(d=datetime.now()):
    # d = datetime.now()

    if d.strftime('%a') == 'Fri':
        d += timedelta(1)
        
    while d.strftime('%a') != 'Fri':
        d += timedelta(1)
    
    pd = get_last_working_day(d)
    return datetime.strftime(datetime.strptime(pd,'%Y-%m-%d'), '%d%b%y').upper()
    # print(datetime.strptime(pd,'%d%M%y'))


def get_timestamp(config, token, timestamp=None):
    """
        This function takes token name as argument and returns all timestamp associated with that token.

        return list of timestamps
    """

    try:
        # connect to sqlite database and open a cursor
        # sqlite_database = config['database']
        sqliteConnection = get_sqlite_db_connection_np(config)
        cursor = sqliteConnection.cursor()

        # create query with a token parameter, execute the query with parameter, fetch all record to a list
        if timestamp == None:
            sqlite_select_query = f"""SELECT TimeStamp FROM script_tick WHERE Token = {token} ORDER BY TimeStamp"""
            cursor.execute(sqlite_select_query)
        else:
            sqlite_select_query = f"""SELECT TimeStamp FROM script_tick WHERE Token = {token} and TimeStamp >= '{timestamp}' ORDER BY timestamp"""
            cursor.execute(sqlite_select_query)
        # print(sqlite_select_query)
        record = [x[0] for x in cursor.fetchall()]
        cursor.close()

        # return token for the instrument
        return record

    except sqlite3.Error as error:
        print("get_timestamp:Failed to get timestamp from sqlite table", error)
    finally:
        # Finally close database connection
        if sqliteConnection:
            sqliteConnection.close()    