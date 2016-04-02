#!/usr/bin/env python
# coding=utf-8
import csv

import json
import os
from glob import glob
from zipfile import ZipFile, ZIP_DEFLATED, ZIP_STORED

from requests import HTTPError

try:  # use cache_requests if installed
    from cache_requests import Session

    requests = Session(ex=60 * 60)
except ImportError:  # fallback to requests
    from requests import Session

    requests = Session()

try:  # use zip compression if zlib is available (should be available)
    import zlib

    compression = ZIP_DEFLATED
except ImportError:  # fallback to default
    compression = ZIP_STORED

# CONSTANTS
# ============================================================================

BASE_URL = 'http://www.oecdbetterlifeindex.org/bli/rest/indexes/responses;offset={offset};limit={limit}'
LIMIT = 1000  # number of records per chunk
PAGE_LIMIT = 10  # number of chunks per page

PWD = os.path.dirname(__file__)
JSON_DIR = os.path.join(PWD, 'json')
CSV_DIR = os.path.join(PWD, 'csv')

WEIGHTS_KEYS = ['housing', 'income', 'jobs', 'community', 'education', 'environment', 'civic engagement', 'health',
                'life_satisfaction', 'safety', 'work_life_balance']


# CORE :: MAIN
# ============================================================================
def main():
    chunk = 0

    # Initialize generator
    dump = dump_results()
    next(dump)  # prevent weird generator 'nonNone' error

    # Walk through limits/offsets
    while True:
        # get LIMIT (1000) responses
        response = get_data_chunk(chunk)
        chunk += 1

        # group and write chunks
        dump.send((chunk, response))

        # data collection complete
        if len(response) < LIMIT:
            break

    # signal to generator to dump remaining data
    dump.send((None, None))

    # create a zip file of data
    zip_data()


# CORE :: REQUEST
# ============================================================================
def get_data_chunk(page, retry=False):
    """GET request data by page number."""
    offset = page * LIMIT
    limit = LIMIT

    # build URL
    url = BASE_URL.format(offset=offset, limit=limit)

    # get response
    response = requests.get(url)

    try:
        response.raise_for_status()
        return [prepare_row(row) for row in response.json()]

    except HTTPError:
        if retry:
            return {}

        # Retry request or return empty data set.
        return get_data_chunk(page, retry=True)


# CORE :: WRITE FILES
# ============================================================================
def dump_results(file_name='data_page'):
    """Generator, Combine and dump chunks into files."""

    stack = []
    page = 0

    while True:
        # Receive chunk number and data
        chunk, results = yield

        if results:
            # add data to stack
            stack.extend(results)

        # If enough data is collected to merit a page dump ...
        page_is_filled = (chunk or 0) > 0 and chunk % PAGE_LIMIT is 0
        end_of_data = (chunk, results) == (None, None)
        if page_is_filled or end_of_data:
            page += 1

            # write data files
            print('Dumping %d records to %s (csv & json)' % (len(stack), '%s_%03d' % (file_name, page)))
            write_json(stack, page, file_name)
            write_csv(stack, page, file_name)

            # Reset the current stack
            stack.clear()

        if end_of_data:
            yield  # prevent StopIteration error
            break


def write_json(data, page, file_name='data_page', directory=JSON_DIR):
    # ensure directory
    if not os.path.exists(directory):
        os.makedirs(directory)

    # build filename
    file_basename = '{file_name}_{page:03d}.json'.format(file_name=file_name, page=page)
    file_path = os.path.join(directory, file_basename)

    # Dump the data
    with open(file_path, 'w') as fp:
        json.dump(data, fp)


def write_csv(data, page, file_name='data_page', directory=CSV_DIR):
    # ensure directory
    if not os.path.exists(directory):
        os.makedirs(directory)

    # create field names
    fieldnames = ['id', 'gender', 'age', 'country']
    fieldnames.extend(WEIGHTS_KEYS)
    fieldnames.append('comments')

    # build filename
    file_basename = '{file_name}_{page:03d}.csv'.format(file_name=file_name, page=page)
    file_path = os.path.join(directory, file_basename)

    # Dump the data
    with open(file_path, 'w') as fp:
        writer = csv.DictWriter(fp, fieldnames)
        writer.writeheader()

        writer.writerows(data)
        # for row in data:
        #     writer.writerow(row)


# CORE :: CREATE ZIP
# ============================================================================
def zip_data(name='data', zip_json_directory=JSON_DIR, zip_csv_directory=CSV_DIR, pwd=PWD):
    """Create a zip file from data."""

    if zip_json_directory:
        zip_json_file_name = os.path.join(pwd, '%s_json.zip' % name)

        with ZipFile(zip_json_file_name, 'w', compression) as zf:
            # for each JSON file ...
            for file in sorted(glob('%s/*.json' % zip_json_directory)):
                # add to archive
                zf.write(file, os.path.basename(file))

        # print archive manifest
        with ZipFile(zip_json_file_name) as zf:
            zf.printdir()

    if zip_csv_directory:
        zip_csv_file_name = os.path.join(pwd, '%s_csv.zip' % name)
        with ZipFile(zip_csv_file_name, 'w', compression) as zf:
            # for each JSON file ...
            for file in sorted(glob('%s/*.csv' % zip_csv_directory)):
                # add to archive
                zf.write(file, os.path.basename(file))

        # print archive manifest
        with ZipFile(zip_csv_file_name) as zf:
            zf.printdir()


# UTILS
# ============================================================================
def prepare_row(row):
    # remove keys
    del row['timestamp']

    # split weights into questions
    weights = row.pop('weights')
    for key, value in zip(WEIGHTS_KEYS, weights):
        row[key] = value

    return row


# RUN
# ============================================================================
if __name__ == '__main__':
    main()
