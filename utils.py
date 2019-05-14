import requests
import os
import csv
import itertools
import time
from functools import wraps
from io import StringIO
from sqlalchemy import create_engine
from pandas import read_csv
from time import sleep
from requests.exceptions import (
    ChunkedEncodingError, ConnectionError, Timeout, ProxyError
)

# example proxy without login
proxies = [
    '103.248.219.26:53634',
    '94.141.120.116:42292'
]
proxy_list = [{'http': p} for p in proxies]

base_url = 'http://download.macrotrends.net/assets/php/stock_data_export.php?t='
path = os.path.join(os.getcwd(), 'files/csv/')
tickers_path = os.path.join(os.getcwd(),'ticker.csv')
# db_connection = 'postgresql://ticker_price:ticker_price@localhost:5432/ticker_price'
db_connection = 'sqlite:////home/m/web-dev/ticker/foo.db'


def retry(exceptions, tries=3, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = f'{e}, Retrying in {mdelay} seconds...'
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


@retry((ChunkedEncodingError, ConnectionError, Timeout, ProxyError))
def save_csv(url, path, file_name, session, proxies):
    print(file_name)
    print(proxies)
    # build directory path
    file_name = file_name.lower()
    full_path = os.path.join(path, file_name[0])
    if not os.path.exists(full_path):
        os.makedirs(full_path)

    # build file path
    file_path = os.path.join(full_path, file_name + '.csv')
    if os.path.isfile(file_path):
        return True
        

    # download file and save on local
    r = session.get(url, stream=True, proxies=proxies)
    with open(file_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    if os.path.isfile(file_path):
        with open(file_path, "r") as f:
            if len(f.readlines()) < 11:
                os.remove(file_path)
                print("You have reached your monthly export limit of 50 files.")
                return False
            elif len(f.readlines()) < 14:
                os.remove(file_path)
                print("DNS resolving failed<.")
                return False
    return True


def get_ticker_list(path):
    # get table names
    with open(tickers_path) as f:
        reader = csv.DictReader(f, delimiter=';')
        return [row['ticker'] for row in itertools.islice(reader, 3)]
        # return [row['ticker'] for row in reader]
    
# '/home/mark/web-dev/ticker_price/files/A.csv'
def create_sql_from_csv(csv_path, db_connection, table_name):
    # create sql table from csv using pandas and sqlalchemy
    with open(csv_path, 'r+') as f:
        engine = create_engine(db_connection)
        lines = f.readlines()
        tmp = StringIO()
        for line in lines[10:]:
            tmp.write(line)
        tmp.seek(0)
        data = read_csv(tmp)
        data.to_sql(
            table_name, con=engine, index=True, index_label='id', if_exists='append'
        )


def get_folder_names(path):
    files = []
    for f in os.listdir(os.fsencode(path)):
        filename = os.fsdecode(f)
        files.append(filename)
    return files


def get_file_names(path, endswith):
    files = []
    for f in os.listdir(os.fsencode(path)):
        filename = os.fsdecode(f)
        if filename.endswith(endswith):
            files.append(filename)
    return files


def remove_n_rows(path, n=10):
    nfirstlines = []
    with open(path) as f, open("bigfiletmp.txt", "w") as out:
        for x in xrange(n):
            nfirstlines.append(next(f))
        for line in f:
            out.write(line)

    # NB : it seems that `os.rename()` complains on some systems
    # if the destination file already exists.
    os.remove("bigfile.txt")
    os.rename("bigfiletmp.txt", "bigfile.txt")


# GET ALL FILES CHANGE IN get_ticker_list return value without itertools

# CREATE TABLES 

# folders = get_folder_names(path)
# for folder in folders:
#     print("Folder name")
#     print(folder)
#     file_path = os.path.join(path, folder)
#     files = get_file_names(file_path, '.csv')
#     for f in files:
#         print("Table name")
#         print(f)
#         create_sql_from_csv(os.path.join(file_path, f), db_connection, f.replace('.csv', ''))


# DOWNLOAD FILES

tickers = get_ticker_list(tickers_path)
proxy_iterator = iter(proxy_list)
current_proxy = next(proxy_iterator)
for ticker in tickers:
    session = requests.session()
    sleep(0.1)
    saved = save_csv(base_url + ticker, path, ticker, session, current_proxy)
    if saved:
        continue
    while True:
        current_proxy = next(proxy_iterator, None)
        saved = save_csv(base_url + ticker, path, ticker, session, current_proxy)
        if saved:
            break
        if not current_proxy:
            break
    if not current_proxy:
        break