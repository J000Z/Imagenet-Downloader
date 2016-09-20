import threading
import logging
from pymongo import MongoClient
import requests
from io import BytesIO
from bson.binary import Binary

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s',
                    )


def worker(cur, cur_lock, collection):
    logging.debug('start')
    row = None
    while True:
        with cursor_lock:
            try:
                row = cur.next()
            except StopIteration:
                break
        if 'status' in row and row['status'] == 200:
            continue
        try:
            r = requests.get(row.url)
            if (r.status_code == 200):
                b = Binary(BytesIO(r.content).getvalue())
                collection.update_one({'_id': row['_id']},
                                      {"$set": {'data': b,
                                                'status': 200}})
                logging.debug('{} {}'.format(row.id, 'done'))
            else:
                collection.update_one({'_id': row['_id']},
                                      {"$set": {'data': None,
                                                'status': r.status_code}})
                logging.debug('{} {}'.format(row.id, 'failed'))
        except requests.exceptions.RequestException e:
            collection.update_one({'_id': row['_id']},
                                  {"$set": {'data': None,
                                            'status': -1}})
            logging.debug('{} {}'.format(row.id, str(e)))
    logging.debug('stop')

client = MongoClient()
collection = client.imagenet.urls
cursor = collection.find().sort("_id")
cursor_lock = threading.Lock()

threads = []
for i in range(10):
    t = threading.Thread(name='worker_{}'.format(i),
                         target=worker,
                         args=(cursor, cursor_lock, collection, ))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

logging.debug('all done')
