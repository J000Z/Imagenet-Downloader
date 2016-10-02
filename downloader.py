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
from datadog import statsd
from datadog.api.constants import CheckStatus
import random

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.8 (KHTML, like Gecko) Version/9.1.3 Safari/601.7.8',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:48.0) Gecko/20100101 Firefox/48.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Safari/602.1.50',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'
]

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
        statsd.increment('url.failed')
        collection.update_one({'id': id_},
                              {"$set": {'data': None,
                                        'status': -2}})
        logging.debug('{} {}'.format(id_, "max retry"))
        return
    try:

        r = requests.get(url,
                         timeout=1,
                         headers={'User-Agent': random.choice(user_agents)})
        if (r.status_code == 200):
            b = Binary(BytesIO(r.content).getvalue())
            collection.update_one({'id': id_},
                                  {"$set": {'data': b,
                                            'status': 200}})
            logging.debug('{} {}'.format(id_, 'done'))
            statsd.increment('url.success')
        else:
            statsd.increment('url.failed')
            collection.update_one({'id': id_},
                                  {"$set": {'data': None,
                                            'status': r.status_code}})
            logging.debug('{} {}'.format(id_, 'failed'))
    except requests.exceptions.ConnectionError:
        time.sleep(60)
        process(row, collection, retry+1)
    except Exception as e:
        statsd.increment('url.failed')
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
        statsd.increment('url.process')
        process(row, collection)
        with cursor_lock:
            progress = info['count']/info['total']*100
            logging.debug('progress {}/{} {}%'.format(
                info['count'],
                info['total'],
                progress))
            statsd.gauge('url.downloader.progress', progress)
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
        time.sleep(5)
        statsd.service_check(check_name='url.downloader',
                             status=CheckStatus.OK,
                             message='heart beat ok')
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
