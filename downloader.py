#!/usr/bin/env python

import threading
import logging
from pymongo import MongoClient
import requests
from io import BytesIO
from bson.binary import Binary
import time
import sys

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s')

info = {'count': 0., 'total': 14197121.}


def worker(cur, cur_lock, collection, run_event, info):
    logging.debug('start')
    row = None
    while run_event.is_set():
        with cursor_lock:
            try:
                row = cur.next()
            except StopIteration:
                break
        if 'status' in row and row['status'] == 200:
            continue
        try:
            r = requests.get(row['url'])
            if (r.status_code == 200):
                b = Binary(BytesIO(r.content).getvalue())
                collection.update_one({'_id': row['_id']},
                                      {"$set": {'data': b,
                                                'status': 200}})
                logging.debug('{} {}'.format(row['id'], 'done'))
            else:
                collection.update_one({'_id': row['_id']},
                                      {"$set": {'data': None,
                                                'status': r.status_code}})
                logging.debug('{} {}'.format(row['id'], 'failed'))
        except requests.exceptions.RequestException as e:
            collection.update_one({'_id': row['_id']},
                                  {"$set": {'data': None,
                                            'status': -1}})
            logging.debug('{} {}'.format(row['id'], str(e)))
        logging.debug('progress {}/{} {}%'.format(
            info['count'],
            info['total'],
            info['count']/info['total']*100))
        info['count'] += 1
    logging.debug('stop')

client = MongoClient()
collection = client.imagenet.urls
cursor = collection.find().sort("_id")
cursor_lock = threading.Lock()

run_event = threading.Event()
run_event.set()

threads = []
for i in range(int(sys.argv[1])):
    t = threading.Thread(name='worker_{}'.format(i),
                         target=worker,
                         args=(cursor,
                               cursor_lock,
                               collection,
                               run_event,
                               info, ))
    threads.append(t)
    t.start()

try:
    while 1:
        time.sleep(.1)
except KeyboardInterrupt:
    logging.debug('attempting to close threads')
    run_event.clear()
    for t in threads:
        t.join()
    logging.debug('threads successfully closed')

logging.debug('all done')
