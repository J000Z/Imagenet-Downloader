#!/usr/bin/env python

import threading
import logging
from pymongo import MongoClient
import requests
from io import BytesIO
from bson.binary import Binary
import time
import sys
from requests.adapters import HTTPAdapter

s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=1))
s.mount('https://', HTTPAdapter(max_retries=1))

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s')

info = {'count': 0., 'total': 14197121., 'cursor_valid': True}


def process(row, collection, retry=0):
    if retry == 2:
        collection.update_one({'_id': row['_id']},
                              {"$set": {'data': None,
                                        'status': -2}})
        logging.debug('{} {}'.format(row['id'], "max retry"))
        return
    try:
        r = requests.get(row['url'], timeout=1)
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
    except requests.exceptions.ConnectionError:
        time.sleep(60)
        process(row, collection, retry+1)
    except Exception as e:
        collection.update_one({'_id': row['_id']},
                              {"$set": {'data': None,
                                        'status': -1}})
        logging.debug('{} {}'.format(row['id'], str(e)))


def worker(cur, cur_lock, collection, run_event, info):
    logging.debug('start')
    row = None
    while run_event.is_set():
        with cursor_lock:
            info['count'] += 1
            try:
                row = cur.next()
            except StopIteration:
                logging.debug('StopIteration')
                info['cursor_valid'] = False
                break
        if 'status' in row:
            continue
        process(row, collection)
        logging.debug('progress {}/{} {}%'.format(
            info['count'],
            info['total'],
            info['count']/info['total']*100))
    logging.debug('stop with run_event={}'.format(run_event.is_set()))

client = MongoClient()
collection = client.imagenet.urls
cursor = collection.find({'status': {'$exists': False}}).sort("_id")
cursor_lock = threading.Lock()

run_event = threading.Event()
run_event.set()


def genThread(i):
    return threading.Thread(name='worker_{}'.format(i),
                            target=worker,
                            args=(cursor,
                                  cursor_lock,
                                  collection,
                                  run_event,
                                  info, ))
threads = {}
for i in range(int(sys.argv[1])):
    threads[i] = genThread(i)
    threads[i].start()

try:
    while 1:
        time.sleep(1)
        if not info['cursor_valid']:
            with cursor_lock:
                cursor = collection
                .find({'status': {'$exists': False}})
                .sort("_id")
                info['cursor_valid'] = True
        for k, t in threads.items():
            if not t.isAlive():
                threads[k] = genThread(k)
                threads[k].start()
except KeyboardInterrupt:
    logging.debug('attempting to close threads')
    run_event.clear()
    for t in threads.values():
        t.join()
    logging.debug('threads successfully closed')

logging.debug('all done')
