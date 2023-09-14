import numpy as np
import json
from typing import List, Tuple, Union, Any, Set, Dict
import os
import shutil
from datetime import timedelta
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
import decimal
import sqlite3
import pyodbc
import socket
import configparser
import functools
import inspect
import random
import socket
import sqlite3
import time
import logging
import firebirdsql
import holidays
import time
from functools import wraps
import fdb

def connections_set_up(fbd_db_name: str = 'database_developer_archevani'):
    # connect to sqlite database
    # get paths from config.ini
    # For different FDB files, we pass relevant keyword argument, name of the database file from config.ini

    sql_lite_final = configs_get_section_dict('database_sqlite')['path_final']
    sql_lite_raw = configs_get_section_dict('database_sqlite')['path_raw']
    sql_lite_processed = configs_get_section_dict('database_sqlite')['path_processed']

    # connect to sqlite databases
    conn_sqlite_final = sqlite3.connect(sql_lite_final, detect_types=sqlite3.PARSE_DECLTYPES)
    conn_sqlite_raw = sqlite3.connect(sql_lite_raw, detect_types=sqlite3.PARSE_DECLTYPES)
    conn_sqlite_processed = sqlite3.connect(sql_lite_processed, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor_sqlite_final = conn_sqlite_final.cursor()
    cursor_sqlite_raw = conn_sqlite_raw.cursor()
    cursor_sqlite_processed = conn_sqlite_processed.cursor()

    # connect to firebird database
    conn_firebird_ = connect_firebird(db_name=fbd_db_name)
    cursor_firebird_ = conn_firebird_.cursor()

    # create a dict to save connection and cursor objects
    conn_cursor_dict = {
        'conn_firebird': conn_firebird_,
        'cursor_firebird': cursor_firebird_,
        'conn_sqlite_final': conn_sqlite_final,
        'cursor_sqlite_final': cursor_sqlite_final,
        'conn_sqlite_raw': conn_sqlite_raw,
        'cursor_sqlite_raw': cursor_sqlite_raw,
        'conn_sqlite_processed': conn_sqlite_processed,
        'cursor_sqlite_processed': cursor_sqlite_processed
    }

    return conn_cursor_dict