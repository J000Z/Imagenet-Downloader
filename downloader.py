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

info = {'count': 0., 'total': 14197121.}


def process(row, collection, retry=0):
    id_, url = row.split()
    item = collection.find_one({"id": id_})
    if 'status' in item and item['status'] != -1:
        logging.debug('{} {}'.format(id_, "skip with status"))
        return
    if retry == 2:
        collection.update_one({'id': id_},
                              {"$set": {'data': None,
                                        'status': -2}})
        logging.debug('{} {}'.format(id_, "max retry"))
        return
    try:
        r = requests.get(url, timeout=1)
        if (r.status_code == 200):
            b = Binary(BytesIO(r.content).getvalue())
            collection.update_one({'id': id_},
                                  {"$set": {'data': b,
                                            'status': 200}})
            logging.debug('{} {}'.format(id_, 'done'))
        else:
            collection.update_one({'id': id_},
                                  {"$set": {'data': None,
                                            'status': r.status_code}})
            logging.debug('{} {}'.format(id_, 'failed'))
    except requests.exceptions.ConnectionError:
        time.sleep(60)
        process(row, collection, retry+1)
    except Exception as e:
        # collection.update_one({'id': id_},
        #                      {"$set": {'data': None,
        #                                'status': -1}})
        logging.debug('{} {}'.format(id_, str(e)))


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
                break
        process(row, collection)
        with cursor_lock:
            logging.debug('progress {}/{} {}%'.format(
                info['count'],
                info['total'],
                info['count']/info['total']*100))
    logging.debug('stop with run_event={}'.format(run_event.is_set()))

client = MongoClient(maxPoolSize=None)
collection = client.imagenet.urls
cursor = open('/root/imagenet/fall11_urls.txt', 'r')
# cursor = collection.find({'status': {'$exists': False}}).sort("_id")
cursor_lock = threading.Lock()
skip = int(sys.argv[2])
info['count'] = skip
logging.debug('skip {}'.format(skip))
for _ in xrange(skip):
    cursor.next()
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
threads_count = int(sys.argv[1])
for i in range(threads_count):
    threads[i] = genThread(i)
    threads[i].start()
    time.sleep(0.5)

try:
    while 1:
        time.sleep(1)
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

cursor.close()
logging.debug('all done')
