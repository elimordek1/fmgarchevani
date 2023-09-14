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


# %% FUNCTIONS

def _00_timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f'Function {func.__name__} took {end - start} seconds')
        return result

    return wrapper


def _00_time_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} took {elapsed_time:.6f} seconds to execute.")
        return result

    return wrapper


def _00_logger_decorator(func):
    def wrapper(*args, **kwargs):
        """
        A decorator to log the start and end of a function.
        USAGE: @logger_decorator. Add it above the function definition.
        use logger.info('some text or fstring') and logger.error('some text or fstring')
        to log messages instead of print.

        :param args:
        :param kwargs:
        :return:
        """
        logging.info(' ')
        start_time = time.time()
        logging.info(f'### START----{func.__name__.upper()} at {time.ctime(time.time())}')
        result = func(*args, **kwargs)
        logging.info(f'*** END------{func.__name__.upper()} at {time.ctime(time.time())}')
        # add total time of the function
        # define start_time before the function call
        logging.info(f'*** TOTAL TIME: {time.time() - start_time}')
        # add a blank line to the log file
        logging.info(' ')
        return result

    return wrapper


def _00_clear_log_file(filename: str = 'app.log'):
    """Clear the log file if it is bigger than 1MB."""
    if os.path.exists(filename):
        if os.path.getsize(filename) > 1000000:
            open(filename, 'w').close()


def _00_initialize_logger(filename: str = 'app.log', log_level=logging.INFO):
    """
    Initialize the logger
    :param filename: the file name for log
    :param log_level: the log level (e.g. logging.ERROR or DEBUG or INFO)
    """
    logging.basicConfig(filename=filename, level=log_level,
                        format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


def adapt_decimal(value):
    return str(value)


def convert_decimal(value):
    return decimal.Decimal(value)


def decimals_handling():
    sqlite3.register_adapter(decimal.Decimal, adapt_decimal)
    sqlite3.register_converter("decimal", convert_decimal)


def get_existing_entries_tbl_col_sql(table_name_: str,
                                     column_name: str,
                                     conn_dict_key_val: dict) -> list[list[int] | set[int]] | None:
    """
    Get existing entries from SQLite table / col.
    (to read only new data later on)
    :param conn_dict_key_val: conn_dict['conn_firebird']
    :param table_name_:
    :param column_name:
    :return: entries list and tuple
    """

    print('/' * 100)
    print(f'Getting existing entries from SQLite processed for table {table_name_}, column: {column_name} ...')
    existing_entries_list = pd.read_sql_query(f'SELECT {column_name} FROM {table_name_}',
                                              conn_dict_key_val)[column_name].tolist()

    # take only unique values
    existing_entries_list: list[int] = list(set(existing_entries_list))

    existing_entries_tuple = set(existing_entries_list)

    print(f'Existing entries len: {len(existing_entries_list)} in table {table_name_}, column: {column_name}')

    if len(existing_entries_list) != 0:
        return [existing_entries_list, existing_entries_tuple]
    else:
        return None


def new_entries_list(existing_elements: list | tuple,
                     incoming_elements: list | tuple,
                     list_or_tuple: str = 'list') -> list | tuple | None:
    """
    Compare incoming and existing entries in two lists,
    leave only new elements.
    :return:
    """
    if len(existing_elements) == 0 or len(incoming_elements) == 0:
        print(f'LEN EXISTING: {len(existing_elements)}, LEN INCOMING: {len(incoming_elements)}!!!')
        return None
    if list_or_tuple == 'list':
        result = [entry for entry in incoming_elements if entry not in existing_elements]
    elif list_or_tuple == 'tuple':
        result = (entry for entry in incoming_elements if entry not in existing_elements)
    else:
        print('Incorrect DType!')
        return None
    return result


def get_file_size_mb(file_path):
    size_in_bytes = os.path.getsize(file_path)
    size_in_mb = size_in_bytes / (1024 * 1024)
    return size_in_mb


def copy_large_file(src, dst):
    try:
        shutil.copy2(src, dst)
        result = f"File '{src}' successfully copied to '{dst}'"
        print(result)
        return True, result
    except Exception as e:
        result = f"Error: {e}"
        print(result)
        return False, result


@_00_logger_decorator
def clean_the_data(
        tables_columns_details_dict_df: dict | pd.DataFrame = None,
        data_dict_or_df: dict | pd.DataFrame = None) -> dict | pd.DataFrame:
    """
    Get data from data_dict_ (or pd.Dataframe, convert it back to dict)
    and clean it according to tables_columns_details_dict_.

    :param tables_columns_details_dict_df:
        metadata (data about data) - what columns are in what tables,
        what are their data types, which one is needed, etc.

        this dict key is table name, value is list of lists:
        [select statement, columns, data types, translation yes/no]

    :type tables_columns_details_dict_df: dict | pd.DataFrame
    :param data_dict_or_df: the data itself
    :return: dict with cleaned data
    """

    # if tables_columns_details_dict_df is a dataframe, convert it to dict
    if isinstance(tables_columns_details_dict_df, pd.DataFrame):
        data_columns = tables_columns_details_dict_df['RDB$FIELD_NAME'].tolist()
        data_types = tables_columns_details_dict_df['PD_DTYPE'].tolist()
        data_translation = tables_columns_details_dict_df['TRANSLATION_YES_NO'].tolist()
        table_name_00 = tables_columns_details_dict_df['TABLE_NAME'].iloc[0]
        tables_columns_details_dict_df = {
            table_name_00: [
                [x for x in range(len(data_columns))],
                data_columns,
                data_types,
                data_translation
            ]}
        # if data_dict_or_df is a dataframe, convert it to dict
        if isinstance(data_dict_or_df, pd.DataFrame):
            data_dict_or_df = {table_name_00: data_dict_or_df}

    data_dict_cleaned_ = {}
    for table_name_, data in data_dict_or_df.items():

        # get columns from tables_columns_details_dict_ VALUE[1]
        col_names_ = tables_columns_details_dict_df[table_name_][1]

        # print table name
        print('\n' + '-' * 100)
        print(f'Table name: {table_name_.upper()}', end='\n\n')

        # initialize dataframe
        df = pd.DataFrame(data, columns=col_names_)

        # extract column names and types from sql_strings_dict_
        cols_dtypes_dict_ = dict(zip(tables_columns_details_dict_df[table_name_][1],
                                     tables_columns_details_dict_df[table_name_][2]))

        # print cols_dtypes_dict_ properly indented
        print(json.dumps(cols_dtypes_dict_, indent=4, sort_keys=True))

        # create dataframe and convert columns to relevant types
        try:
            print(f'Creating dataframe for table {table_name_} ...', end='\n\n')
            for col_, dtype_ in cols_dtypes_dict_.items():
                print(f'Converting column {col_} with type {df[col_].dtype} to {dtype_} ...')
                if dtype_.startswith('float') or dtype_.startswith('int'):
                    try:
                        df[col_] = pd.to_numeric(df[col_])
                    except (Exception, ValueError, TypeError) as err:
                        print(err)
                        print(f'Error converting column {col_} to {dtype_}.', '\n')
                else:
                    df[col_] = df[col_].astype(dtype_)
        except (Exception, ValueError, TypeError) as err:
            print(err)

        # create dict with column names and translate_need flag
        col_translate_dict = dict(zip(tables_columns_details_dict_df[table_name_][1],
                                      tables_columns_details_dict_df[table_name_][3]))

        print('TRANSLATE: ', json.dumps(col_translate_dict, indent=4, sort_keys=True))

        # convert to Georgian Unicode
        for col_, translate_flag in col_translate_dict.items():
            if df[col_].dtype == 'object' and translate_flag == 'YES':
                print(f'Translating column {col_} ...')
                try:
                    df[col_] = [translate_unicode_to_georgian(x) if isinstance(x, str) else 'NA' for x in df[col_]]
                except Exception as err:
                    print(err)
                    print(f'Column {col_} not translated to Georgian.', '\n')
                    continue

        data_dict_cleaned_[table_name_] = df

    if len(data_dict_or_df) == 1:
        return data_dict_cleaned_[table_name_]
    else:
        return data_dict_cleaned_


def read_firebird_with_cursor(table_name_: str,
                              column_name: str,
                              read_one: bool = True,
                              fbd_db_name: str = 'database_developer_archevani') -> pd.DataFrame:
    """
    Read Firebird table with cursor:
    FETCH ONE or FETCH ALL.
    :param fbd_db_name: default is ARCHEVANI2021.FDB for Firebird database connections
    :param table_name_:
    :param column_name:
    :param read_one:
    :return:
    """

    print('/' * 100)
    print(f'Reading Firebird table {table_name_}, column: {column_name} ...')
    connection_dict = connections_set_up(fbd_db_name=fbd_db_name)

    query = f'SELECT {column_name} FROM {table_name_};'
    cursor = connection_dict['cursor_firebird']
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]

    rows = []
    if read_one:
        try:
            # Read rows and handle exceptions for each row
            i = 0
            while True:
                try:
                    row = cursor.fetchone()
                    if row is None:
                        print(f"None row: {i}")
                        break
                    rows.append(row)
                except Exception as e:
                    print(f"Skipping invalid row due to error: {e}")
                i += 1
        except Exception as e:
            print(f"Error executing query: {e}")
    else:
        try:
            rows = cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")

    # Create a DataFrame from the retrieved data
    df_ = pd.DataFrame(rows, columns=columns)
    return df_


def create_date_table(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Generates a comprehensive date table with various date-related columns.

    Args:
    start_date (str): The start date in 'YYYY-MM-DD' format.
    end_date (str): The end date in 'YYYY-MM-DD' format.

    Returns:
    pd.DataFrame: A date table DataFrame with date-related columns.
    """

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_table = pd.DataFrame(date_range, columns=['Date'])

    georgian_holidays = holidays.country_holidays('GE',
                                                  years=[pd.to_datetime(start_date).year,
                                                         pd.to_datetime(end_date).year])

    date_table['Holiday'] = date_table['Date'].apply(lambda x: georgian_holidays.get(x))
    date_table['IsHoliday'] = date_table['Holiday'].notnull()

    date_table['Year'] = date_table['Date'].dt.year
    date_table['Month'] = date_table['Date'].dt.month
    date_table['Day'] = date_table['Date'].dt.day
    date_table['DayOfWeek'] = date_table['Date'].dt.dayofweek
    date_table['DayName'] = date_table['Date'].dt.day_name()
    date_table['WeekNumber'] = date_table['Date'].dt.isocalendar().week
    date_table['Quarter'] = date_table['Date'].dt.quarter
    date_table['IsWeekend'] = date_table['DayOfWeek'].isin([5, 6])
    date_table['IsMonthStart'] = date_table['Date'].dt.is_month_start
    date_table['IsMonthEnd'] = date_table['Date'].dt.is_month_end

    return date_table


# %% FIX WHEN THERE'S A SALE BUT NO PURCHASE
def _04_fix_before_purchase_sales(df_param: pd.DataFrame | DataFrameGroupBy) -> pd.DataFrame | None:
    """
    APPLY THIS FUNCTION TO GROUPBY OBJECT or df sliced by GOODS_KOD
    If there was sales before purchase, we need to fix it

    Iterate through the dataframe until there are no more negative values in the Diff column
    and the negative value in DIFF_PQC_USC column means that there was sales before purchase

    :param df_param: dataframe

    :return: dataframe
    """

    df_ = df_param.copy()

    del df_param

    if len(df_) == 0:
        return None

    if len(df_) == 1:
        return df_

    # reset index
    df_.reset_index(drop=True, inplace=True)

    # add cumulative sum to Purchased Qtty
    df_['PQC'] = df_['Purchased Qtty'].cumsum()
    # add cumulative sum to Total Usage Qtty
    df_['TUC'] = df_['Total Usage Qtty'].cumsum()
    df_['DIFF_PQC_TUC'] = df_['PQC'] - df_['TUC']

    # if sum of either qtty or amt is zero, we don't need to fix anything, i.e. it is an obvious error
    if df_['Purchased Qtty'].sum() == 0 or df_['Purchased Amount'].sum() == 0:
        # _00_error_codes_json(file_name='error_codes.json',
        #                      goods_kod=df_['GOODS_KOD'].iloc[0],
        #                      time_stamp=pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
        return df_

    # if line 1 qtty and sum are zero and usage is negative, find first nonzero indices for them and copy to first row
    if df_.loc[0, 'Purchased Qtty'] == 0 and df_.loc[0, 'Purchased Amount'] == 0 and df_.loc[0, 'Total Usage Qtty'] < 0:
        # save first row Total Usage Qtty (in case we need to add it to same day purchase)
        first_row_usage = df_.loc[0, 'Total Usage Qtty']

        # set first row Total Usage Qtty to zero to avoid double counting
        df_.loc[0, 'Total Usage Qtty'] = 0

        # find first nonzero indices for qtty and amt
        first_nonzero_qtty_idx = df_['Purchased Qtty'].ne(0).idxmax()
        first_nonzero_amt_idx = df_['Purchased Amount'].ne(0).idxmax()

        # for total usage qtty, we take corresponding index from either the first nonzero qtty index or amt index
        first_nonzero_total_usage_idx = first_nonzero_qtty_idx

        # copy first nonzero qtty and amt to line 1
        df_.loc[0, 'Purchased Qtty'] = df_.loc[first_nonzero_qtty_idx, 'Purchased Qtty']
        df_.loc[0, 'Purchased Amount'] = df_.loc[first_nonzero_amt_idx, 'Purchased Amount']
        df_.loc[0, 'Total Usage Qtty'] = df_.loc[first_nonzero_total_usage_idx, 'Total Usage Qtty'] + first_row_usage

        # set first_nonzero_qtty_idx and first_nonzero_amt_idx locations to 0
        df_.loc[first_nonzero_qtty_idx, 'Purchased Qtty'] = 0
        df_.loc[first_nonzero_amt_idx, 'Purchased Amount'] = 0
        df_.loc[first_nonzero_qtty_idx, 'Total Usage Qtty'] = 0

        # UPDATE cumulative sum to Purchased Qtty
        df_['PQC'] = df_['Purchased Qtty'].cumsum()
        # add cumulative sum to Total Usage Qtty
        df_['TUC'] = df_['Total Usage Qtty'].cumsum()
        df_['DIFF_PQC_TUC'] = df_['PQC'] - df_['TUC']

    # Here we process cases where sales are before purchase excluding the cases above
    # if i more than 0, we need next loop
    i = 0
    while df_['DIFF_PQC_TUC'].lt(0).any():

        i += 1

        # get the first negative value in the Diff column
        first_neg_idx = df_['DIFF_PQC_TUC'].lt(0).idxmax()

        # if there are still negative values, i.e. i>=2,  in the Diff column after the first correction,
        # begin operation from previous first negative value
        if i >= 2:
            first_pos_idx = df_['DIFF_PQC_TUC'].iloc[first_neg_idx:].gt(0).idxmax()
        else:
            first_pos_idx = df_['DIFF_PQC_TUC'].iloc[first_neg_idx:].gt(0).idxmax()
            # first_pos_idx = df_['DIFF_PQC_TUC'].gt(0).idxmax() # this is the same as above TEMP!!!

        # if from this point on there are no more purchases, but we have returns, we move those
        # returns to the first_pos_idx as if they were purchased
        no_more_purchases: bool = df_['Purchased Qtty'].iloc[first_neg_idx:].eq(0).all()

        # now check if there are returns after the first_pos_idx
        any_returns: bool = df_['Total Usage Qtty'].iloc[first_neg_idx:].lt(0).any()
        if any_returns:
            next_return_idx = df_['Total Usage Qtty'].iloc[first_neg_idx:].lt(0).idxmax()

        # if both are true, we move the next return to the first_pos_idx
        if no_more_purchases and any_returns:
            # correct the negative sign to positive
            df_.loc[first_neg_idx, 'Purchased Qtty'] = df_.loc[next_return_idx, 'Total Usage Qtty'] * -1

            # we also need to move the corresponding amount
            # for this we need last purchase amount and qtty to calculate the ratio
            last_purchase_idx = df_[df_['Purchased Amount'] != 0].last_valid_index()
            last_purchase_amt = df_.loc[last_purchase_idx, 'Purchased Amount']
            last_purchase_qtty = df_.loc[last_purchase_idx, 'Purchased Qtty']
            ratio = last_purchase_amt / last_purchase_qtty

            # now calculate the Purchase Amount with ratio
            df_.loc[first_neg_idx, 'Purchased Amount'] = df_.loc[first_neg_idx, 'Purchased Qtty'] * ratio

            # then set the return to zero
            df_.loc[next_return_idx, 'Total Usage Qtty'] = 0

            # recalculate the cumulative sums
            df_['PQC'] = df_['Purchased Qtty'].cumsum()
            df_['TUC'] = df_['Total Usage Qtty'].cumsum()
            df_['DIFF_PQC_TUC'] = df_['PQC'] - df_['TUC']

            # TODO: seems that's it with returns.
            # TODO: but need to also accommodate for the cases when there are no more purchases but still selling
            # then maybe we add same amount to those that they are selling without purchase

            continue

        # if no_more_purchases:
        #     pass

        first_line_total_usage = df_.loc[first_neg_idx, 'Total Usage Qtty']
        # copy first positive value to the first negative value from columns Purchased Qtty and Purchased Amount
        df_.loc[first_neg_idx, 'Purchased Qtty'] = df_.loc[first_pos_idx, 'Purchased Qtty']
        df_.loc[first_neg_idx, 'Purchased Amount'] = df_.loc[first_pos_idx, 'Purchased Amount']
        df_.loc[first_neg_idx, 'Total Usage Qtty'] = df_.loc[first_pos_idx, 'Total Usage Qtty'] + first_line_total_usage

        # after we copy in the above step
        # set first positive value to zero for columns Purchased Qtty and Purchased Amount
        df_.loc[first_pos_idx, 'Purchased Qtty'] = 0
        df_.loc[first_pos_idx, 'Purchased Amount'] = 0
        df_.loc[first_pos_idx, 'Total Usage Qtty'] = 0

        # again
        df_['PQC'] = df_['Purchased Qtty'].cumsum()
        df_['TUC'] = df_['Total Usage Qtty'].cumsum()
        df_['DIFF_PQC_TUC'] = df_['PQC'] - df_['TUC']

    # if purchased amount, purchased qtty, total usage qtty equal to zero, remove the row(s)
    df_ = df_[(df_['Purchased Amount'] != 0) |
              (df_['Purchased Qtty'] != 0) |
              (df_['Total Usage Qtty'] != 0)].copy()

    # reset index
    df_.reset_index(drop=True, inplace=True)

    return df_


@_00_logger_decorator
def configs_get_section_dict(section: str = None, config_file: str = 'config.ini') -> dict or None:
    """
    Get config.ini sections as dictionary
    :param section:
    :param config_file:
    :return: section as dictionary or None if no such section
    """
    config = configparser.ConfigParser()
    config.read(config_file)

    try:
        r = dict(config[section])
    except KeyError:
        r = None
        print(f'No such section: {section}')

    return r


# %%

# db file path: client machine or developer machine
def get_db_path() -> str:
    if socket.gethostname() == configs_get_section_dict('machine')['name_client']:
        return configs_get_section_dict('database_firebird')['database_main_copy_client']
    else:
        return configs_get_section_dict('database_firebird')['database_developer_archevani']


# TEST
# print(_02_get_db_path())

# %% set up connection to main database and sqlite database

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


def connections_close(conn_cursor_dict: dict):
    conn_cursor_dict['conn_firebird'].close()
    conn_cursor_dict['conn_sqlite_final'].close()
    conn_cursor_dict['conn_sqlite_raw'].close()
    conn_cursor_dict['conn_sqlite_processed'].close()


# %% get machine name with os module
def _02_03_get_machine_name() -> str:
    machine_name_ = socket.gethostname()
    print("Machine name:", machine_name_)
    return machine_name_


@_00_logger_decorator
def required_tables_list_df(path_to_config_excel: str) -> Tuple[pd.DataFrame, Dict[Any, Any]]:
    """
    The list of tables that are required for the project (specified in config_tables.xlsx)

    :param path_to_config_excel: path to config_tables.xlsx
    :type path_to_config_excel: str

    :return: tuple with two elements:
        1. dataframe with required tables
        2. dictionary with required tables and their id columns
    :rtype: Tuple[pd.DataFrame, Dict[Any, Any]]

    """

    # noinspection PyTypeChecker
    df_ = pd.read_excel(path_to_config_excel,
                        sheet_name='required_tables',
                        usecols='A:C')

    # filter out rows with 'main_db' in 'source' column
    df_ = df_[df_['source'] == 'dwh']

    # convert df_ to dict
    df_dict = dict(zip(df_['tables'], df_['id_column']))

    return df_, df_dict


def get_required_tables(df_or_dict: str = 'dict') -> pd.DataFrame or dict:
    cfs = configs_get_section_dict(section='PATHS')
    config_excel_path = cfs['config_excel_file']

    if df_or_dict == 'df':
        return required_tables_list_df(config_excel_path)[0]
    elif df_or_dict == 'dict':
        return required_tables_list_df(config_excel_path)[1]


# %% create firebird connection
def connect_firebird(db_name: str = 'database_developer_archevani') -> firebirdsql.Connection:
    """
    :return: firebird connection
    """

    # Get DB file path from config.ini
    database_path = configs_get_section_dict('database_firebird')[db_name]

    # Make sure the path is an absolute path
    database_path = os.path.abspath(database_path)

    c_ = configs_get_section_dict('database_firebird')

    conn_ = firebirdsql.connect(
        host=c_['host'],
        database=database_path,
        user=c_['username'],
        password=c_['password'],
        charset=c_['charset'],
        port=c_['port'])

    return conn_


def convert_to_relevant_dtype(df: pd.DataFrame, col_: str, dtype_: str) -> None:
    if dtype_.startswith('float') or dtype_.startswith('int'):
        try:
            df[col_] = pd.to_numeric(df[col_])
        except (Exception, ValueError, TypeError) as err:
            logging.error(f"Error converting column {col_} to {dtype_}: {err}")
    else:
        df[col_] = df[col_].astype(dtype_)


def translate_column(df_: pd.DataFrame, col_: str | list) -> None:
    try:
        # if col_ is a list, translate each column in the list
        if isinstance(col_, list):
            for col in col_:
                df_[col] = [translate_unicode_to_georgian(x) if isinstance(x, str) else 'NA' for x in df_[col]]
        else:
            df_[col_] = [translate_unicode_to_georgian(x) if isinstance(x, str) else 'NA' for x in df_[col_]]
    except Exception as err:
        logging.error(f"Column {col_} not translated to Georgian: {err}")


def _01_02_read_firebird_with_cursor(table_name_: str,
                                     column_name: str,
                                     read_one: bool = True) -> pd.DataFrame:
    """
    Read Firebird table with cursor
    :param table_name_:
    :param column_name:
    :param read_one:
    :return:
    """
    # TODO: add to utils

    print('/' * 100)
    print(f'Reading Firebird table {table_name_}, column: {column_name} ...')
    connection_dict = connections_set_up()

    query = f'SELECT {column_name} FROM {table_name_};'
    cursor = connection_dict['cursor_firebird']
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]

    rows = []
    if read_one:
        try:
            # Read rows and handle exceptions for each row
            i = 0
            while True:
                try:
                    row = cursor.fetchone()
                    if row is None:
                        print(f"None row: {i}")
                        break
                    rows.append(row)
                except Exception as e:
                    print(f"Skipping invalid row due to error: {e}")
                i += 1
        except Exception as e:
            print(f"Error executing query: {e}")
    else:
        try:
            rows = cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")

    # Create a DataFrame from the retrieved data
    df_ = pd.DataFrame(rows, columns=columns)
    return df_


def translate_unicode_to_georgian(string: str) -> str:
    if not isinstance(string, object):
        exit()

    if string is None:
        return 'NA'
    else:

        mapping = {
            "»": "წ",
            "¼": "ჭ",
            "—": "ე",
            "ª": "რ",
            "¬": "ტ",
            "²": "ყ",
            "›": "თ",
            "\xad": "უ",
            "¤": "ო",
            "¥": "პ",
            "‹": "ა",
            "«": "ს",
            "³": "შ",
            "–": "დ",
            "¯": "ფ",
            "•": "გ",
            "¿": "ჰ",
            "¾": "ჯ",
            "Ÿ": "კ",
            "¡": "ლ",
            "š": "ზ",
            "º": "ძ",
            "½": "ხ",
            "µ": "ც",
            "˜": "ვ",
            "Œ": "ბ",
            "£": "ნ",
            "¢": "მ",
            "´": "ჩ",
            "±": "ღ",
            "œ": "ი",
            "¦": "ჟ",
            "°": "ქ",
            "ў": "მ",
            "Ѕ": "ხ",
            "\x00": "ვ",
            "Ј": "ნ",
            "Њ": "ბ",
            "њ": "ი",
            "Є": "რ",
            "Ў": "ლ",
            "џ": "კ",
            "Ґ": "პ",
            "і": "შ",
            "ѕ": "ჯ",
            "є": "ძ",
            "Ї": "ფ",
            "ј": "ჭ",
            "љ": "ზ",
            "І": "ყ",
            "ї": "ჰ",
            "ґ": "ჩ"
        }

        result = ''
        try:
            for c in string:
                if c in mapping:
                    result += mapping[c]
                else:
                    result += c
        except Exception as e:
            print(e)
            return 'NA'

        return ' '.join(result.split())


# Function to generate random dates
def random_date(start, end):
    return start + timedelta(days=random.randint(0, int((end - start).days)))


def save_dataframes_to_excel(filename):
    # Get the current frame and its local variables
    frame = inspect.currentframe().f_back
    local_vars = frame.f_locals

    # Filter out the local variables that are pandas DataFrames
    dataframes = {name: var for name, var in local_vars.items() if isinstance(var, pd.DataFrame)}

    # Save all the DataFrames into a single Excel file with sheet names based on variable names
    with pd.ExcelWriter(filename) as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"DataFrames saved to {filename}")


def save_to_excel(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        output_file = f"final_exam/{func.__name__}.xlsx"
        df.to_excel(output_file, index=False)
        print(f"Data saved to {os.path.abspath(output_file)}")
        return df

    return wrapper


def copy_table_from_main_to_wh(table_name_: str, fbd_conn: bool = False):
    """
     read table from main db with read_firebird_with_cursor or pd.read_sql

     NOTE: C_GOODS has problems with pd.read_sql, as there seems to be
        a problem with the column data type. we generally use read_firebird_with_cursor
        with fetch one.
    :param table_name_:
    :param fbd_conn:
    :return:
    """

    c = connections_set_up()

    fbd_conn = True
    if fbd_conn:
        decimals_handling()
        # connections are set up within the function
        df_ = read_firebird_with_cursor(table_name_=table_name_, column_name='*')
    else:
        df_ = pd.read_sql(f'SELECT * FROM {table_name_}', c['conn_firebird'])

    # write to wh db
    df_.to_sql(table_name_,
               c['conn_sqlite_processed'],
               if_exists='replace',
               index=False)

    connections_close(c)


def align_columns(df):
    col1 = sorted(set(df.iloc[:, 0].dropna()))
    col2 = sorted(set(df.iloc[:, 1].dropna()))

    combined_set = sorted(list(set(col1 + col2)))

    aligned_col1 = []
    aligned_col2 = []

    for val in combined_set:
        if val in col1:
            aligned_col1.append(val)
        else:
            aligned_col1.append(None)

        if val in col2:
            aligned_col2.append(val)
        else:
            aligned_col2.append(None)

    aligned_df = pd.DataFrame({df.columns[0]: aligned_col1, df.columns[1]: aligned_col2})

    return aligned_df


def _00_setup_logging(to_file=False, log_file_name='app.log'):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Clear all existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # File handler
    if to_file:
        file_handler = logging.FileHandler(log_file_name)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
    else:
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.DEBUG)
        logger.addHandler(console_handler)


if __name__ == '__main__':
    print('Hello World from UTILS!')
    c = connections_set_up()

    # df = pd.read_clipboard(sep=',')
    df = pd.read_sql_query('select distinct oper_type_id, oper_name from TS_OPERATIONS',
                           c['conn_sqlite_processed'])
    df_01 = pd.read_sql_query('select distinct oper_type_id, oper_name from RS_CHEQS',
                              c['conn_firebird'])

    # df_01 = align_columns(df).copy()
    translate_column(df_=df, col_=['OPER_NAME'])

    # df_01.to_clipboard(index=False)

    df_goods = pd.read_sql('SELECT * FROM C_GOODS', c['conn_sqlite_processed'])
    df_goods_copy = df_goods.copy()

    # translate
    translate_column(df_=df_goods_copy,
                     col_=['NAME', 'BREND', 'UNIT_NAME', 'CLIENT_NAME', 'VENDOR_NAME', 'GROUP_NAME'])

    # copy_table_from_main_to_wh(table_name_='C_GOODS', fbd_conn=True)

    # cfs_sqlite = configs_get_section_dict(section='database_sqlite')
    # cfs_fbd = configs_get_section_dict(section='database_firebird')
    # cfs_fbd['database_developer_archevani']

    # c = connections_set_up()
    #
    # sql_lite_final = configs_get_section_dict('database_sqlite')['path_final']
    #
    # fbd_conn = connect_firebird()

    # print(socket.gethostname())
    # print(_02_get_db_path())
    #
    # # print and assign machine name with walrus operator
    # if machine_name := _02_03_get_machine_name():
    #     print(machine_name)
    #
    # conn = _03_get_firebird_connection()
    #
    # x = translate_unicode_to_georgian('ўµњЄ—Ї‹«‹Јњ  «‹јЄ—Ўњ њЈ«¬Є\xadў—Ј¬—Њњ')
    # print(x)

    # df = pd.read_clipboard()
    # df['NAME'] = df['ITEM'].apply(translate_unicode_to_georgian)

    # y = _04_translate('«‹°¤ЈЎњ« –‹«‹Ў‹•—Њ—Ўњ ЄџњЈњ« «¬њЎ‹¦—Њњ (»—Є—›—Ўњ)')
    # a = _04_translate('°‹Ўњ« Ґ‹Ў¬¤')
    # print(y, x, a)
    #
    # # goods_df = pd.read_sql('select * from C_GOODS', conn)
    # # goods_df = pd.read_sql_query('select * from C_GOODS', conn)
    #
    # goods_df = read_firebird_with_cursor('C_GOODS', '*', read_one=True)
    # goods_df['NAME'] = goods_df['NAME'].apply(_04_translate)
    # goods_df['UNIT_NAME'] = goods_df['UNIT_NAME'].apply(_04_translate)
    #
    # goods_df = connect_to_firebird_with_fdb('C_GOODS')

    # TEST FIX FOR SALES BEFORE PURCHASES
    # sales_df = pd.read_excel('tt_00.xlsx')
    # s = _04_fix_before_purchase_sales(sales_df)
    # print(s)

# %%
