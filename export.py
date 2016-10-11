#!/usr/bin/env python

from pymongo import MongoClient
from io import BytesIO
from urlparse import urlparse
import os
from bson.binary import Binary
import logging
import sys

# logging.basicConfig(
#     level=logging.DEBUG,
#     format='[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s')

client = MongoClient()
collection = client.imagenet.urls
cursor = collection.find({'data': {'$ne': None}}).skip(int(sys.argv[2]))
folder = sys.argv[1]

info = {'count': 0., 'total': 14197121.}
row = None
while True:
    try:
        info['count'] += 1
        row = cursor.next()
    except StopIteration:
        logging.debug('StopIteration')
        break
    url = row['url']
    id_ = row['id']
    data = row['data']

    try:

        # get extension
        ext = os.path.splitext(urlparse(url).path)[1]
        # construct file name
        filename = "{}/{}{}".format(folder, id_, ext)

        if os.path.isfile(filename):
            continue

        with open(filename, 'wb+') as f:
            f.write(data)

    except Exception:
        print 'error: ', id_

    progress = info['count']/info['total']*100
    print 'progress {}/{} {}%'.format(info['count'], info['total'], progress)

print 'done'
