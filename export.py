#!/usr/bin/env python

from pymongo import MongoClient
from io import BytesIO
from urlparse import urlparse
import os
from bson.binary import Binary
import logging
import sys


client = MongoClient()
collection = client.imagenet.urls
cursor = collection.find({'data': {'$ne': None}}).sort("_id").skip(int(sys.argv[2]))
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

    # get extension
    ext = os.path.splitext(urlparse(url).path)[1]
    # construct file name
    filename = "{}/{}{}".format(folder, id_, ext)

    with open(filename, 'wb+') as f:
        f.write(data)

    progress = info['count']/info['total']*100
    logging.debug('progress {}/{} {}%'.format(
        info['count'],
        info['total'],
        progress))


logging.debug('done')
