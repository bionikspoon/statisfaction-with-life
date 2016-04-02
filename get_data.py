#!/usr/bin/env python
# coding=utf-8
import os
from glob import glob
from itertools import cycle
from zipfile import ZipFile, ZIP_DEFLATED, ZIP_STORED

from requests import HTTPError

import csv
import json

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
JSON_PAGE_LIMIT = 10  # number of chunks per JSON page
CSV_PAGE_LIMIT = 107  # number of chunks per CSV page

PWD = os.path.dirname(__file__) or '.'
print('PWD', PWD)
JSON_DIR = os.path.join(PWD, 'json')
CSV_DIR = os.path.join(PWD, 'csv')

WEIGHTS_KEYS = ['housing', 'income', 'jobs', 'community', 'education', 'environment', 'civic engagement', 'health',
                'life_satisfaction', 'safety', 'work_life_balance']


# CORE :: MAIN
# ============================================================================
def main():
    clean_dirs()
    chunk = 0

    # Initialize generator
    dump = dump_results()
    next(dump)

    # Walk through limits/offsets
    while True:
        # get LIMIT (1000) responses
        response = get_data_chunk(chunk)

        # group and write chunks
        try:
            dump.send(response)
        except StopIteration:
            break

        chunk += 1

    # create a zip file of data
    zip_data()


# CORE :: REQUEST
# ============================================================================
def get_data_chunk(page, retry=False):
    """GET request data by page number."""

    # build URL
    offset = page * LIMIT
    limit = LIMIT
    url = BASE_URL.format(offset=offset, limit=limit)

    # get response
    response = requests.get(url)

    # return data or retry
    try:
        response.raise_for_status()  # check errors
        return (prepare_row(row) for row in response.json())  # clean data

    except HTTPError:
        if retry:
            return {}

        # Retry request or return empty data set.
        return get_data_chunk(page, retry=True)


# CORE :: WRITE FILES
# ============================================================================
def dump_results(file_name='data_page'):
    """Generator, Combine and dump chunks into files."""

    json_stack = []
    csv_stack = []

    json_page = 0
    csv_page = 0

    iter_json_chunk = cycle(range(JSON_PAGE_LIMIT))
    iter_csv_chunk = cycle(range(CSV_PAGE_LIMIT))

    while True:
        json_chunk = next(iter_json_chunk)
        csv_chunk = next(iter_csv_chunk)

        # Receive chunk number and data
        results = list((yield))

        # add data to stacks
        json_stack.extend(results)
        csv_stack.extend(results)

        is_end_of_data = len(results) < LIMIT

        # If enough data is collected to merit a JSON page dump ...
        json_page_is_filled = json_chunk is JSON_PAGE_LIMIT - 1
        if (json_page_is_filled or is_end_of_data) and json_stack:
            # write data files
            print('Dumping %d records to %s.json' % (len(json_stack), '%s_%03d' % (file_name, json_page)))
            write_json(json_stack, json_page, file_name)

            # Reset the current stack
            json_stack.clear()

            json_page += 1

        # If enough data is collected to merit a CSV page dump ...
        csv_page_is_filled = csv_chunk is CSV_PAGE_LIMIT - 1
        if (csv_page_is_filled or is_end_of_data) and csv_stack:
            # write data files
            print('Dumping %d records to %s.csv' % (len(csv_stack), '%s_%03d' % (file_name, csv_page)))
            write_csv(csv_stack, csv_page, file_name)

            # Reset the current stack
            csv_stack.clear()

            csv_page += 1

        if is_end_of_data:
            break


def write_json(data, page, file_name='data_page', directory=JSON_DIR):
    # ensure directory
    if not os.path.exists(directory):
        os.makedirs(directory)

    # build filename
    file_basename = '{file_name}_{page:03d}.json'.format(file_name=file_name, page=page)
    file_path = os.path.join(directory, file_basename)

    # Dump the data
    with open(file_path, 'w', encoding='utf-8') as fp:
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
    with open(file_path, 'w', encoding='utf-8') as fp:
        writer = csv.DictWriter(fp, fieldnames)
        writer.writeheader()
        writer.writerows(data)


# CORE :: CREATE ZIP
# ============================================================================
def zip_data(name='data', zip_json_directory=JSON_DIR, zip_csv_directory=CSV_DIR, pwd=PWD):
    """Create a zip file from data."""

    if zip_json_directory:
        # build zip filename
        zip_json_file_name = os.path.join(pwd, '%s_json.zip' % name)

        print('Writing %s' % os.path.basename(zip_json_file_name))

        # create an archive
        with ZipFile(zip_json_file_name, 'w', compression) as zf:
            # for each JSON file ...
            for file in sorted(glob('%s/*.json' % zip_json_directory)):
                # add to archive
                zf.write(file, os.path.basename(file))

        # print archive manifest
        with ZipFile(zip_json_file_name) as zf:
            zf.printdir()

    if zip_csv_directory:
        # build zip filename
        zip_csv_file_name = os.path.join(pwd, '%s_csv.zip' % name)

        print('Writing %s' % os.path.basename(zip_csv_file_name))

        # create an archive
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

    if row['comments']:  # potentially null
        row['comments'] = row['comments'].replace('\r', ' ').replace('\n', ' ')

    # split weights into questions
    weights = row.pop('weights')
    for key, value in zip(WEIGHTS_KEYS, weights):
        row[key] = value

    return row


def clean_dirs():
    """Clean files of interest."""

    files = {
        'json': glob('%s/*.json' % JSON_DIR),

        'csv': glob('%s/*.csv' % CSV_DIR),

        'zip': glob('%s/*.zip' % PWD)
    }

    for key, files in files.items():
        print('Removing %s %s files.' % (len(files), key))
        for file in files:
            os.remove(file)


# RUN
# ============================================================================
if __name__ == '__main__':
    main()
