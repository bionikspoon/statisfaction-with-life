#!/usr/bin/env python
# coding=utf-8
import json
import os
from glob import glob

try:
    from cache_requests import Session

    requests = Session(ex=60 * 60)
except ImportError:
    from requests import Session

    requests = Session()

from requests import HTTPError
from zipfile import ZipFile, ZIP_DEFLATED, ZIP_STORED

try:
    import zlib

    compression = ZIP_DEFLATED
except ImportError:
    compression = ZIP_STORED

BASE_URL = 'http://www.oecdbetterlifeindex.org/bli/rest/indexes/responses;offset={offset};limit={limit}'
LIMIT = 1000
PAGE_LIMIT = 10
PWD = os.path.dirname(__file__)
DATA_DIR = os.path.join(PWD, 'data')


def main():
    chunk = 0
    dump = dump_results()
    next(dump)

    while True:
        # get LIMIT (1000) responses
        response = get_data_chunk(chunk)
        chunk += 1

        # group and write chunks
        dump.send((chunk, response))

        if len(response) < LIMIT:
            break

    dump.send((None, None))

    zip_data()


def get_data_chunk(page, retry=False):
    offset = page * LIMIT
    limit = LIMIT

    url = BASE_URL.format(offset=offset, limit=limit)
    response = requests.get(url)
    try:
        response.raise_for_status()
        return response.json()
    except HTTPError:
        if retry:
            return {}

        return get_data_chunk(page, retry=True)


def dump_results(file_name_template='data_page_%03d.json', directory=DATA_DIR):
    data = []
    page = 0

    if not os.path.exists(directory):
        os.makedirs(directory)

    while True:
        chunk, results = yield

        if results:
            data.extend(results)

        page_is_filled = (chunk or 0) > 0 and chunk % PAGE_LIMIT is 0
        end_of_data = (chunk, results) == (None, None)
        if page_is_filled or end_of_data:
            page += 1
            file_name = os.path.join(directory, file_name_template % page)
            print('Dumping %d records to %s' % (len(data), os.path.basename(file_name)))
            with open(file_name, 'w') as fp:
                json.dump(data, fp)

            data.clear()

        if end_of_data:
            yield
            break


def zip_data(name='data', zip_directory=DATA_DIR, pwd=PWD):
    zip_file_name = os.path.join(pwd, '%s.zip' % name)

    with ZipFile(zip_file_name, 'w', compression) as zf:
        for file in sorted(glob('%s/*.json' % DATA_DIR)):
            zf.write(file, os.path.basename(file))

    with ZipFile(zip_file_name) as zf:
        zf.printdir()


if __name__ == '__main__':
    main()
