import threading
import logging
from pymongo import MongoClient
import requests
from io import BytesIO

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s',
                    )


def worker(cur, cur_lock):
    logging.debug('start')
    while True:
        with cursor_lock:
            row = None
            try:
                row = cur.next()
            except StopIteration:
                break
            r = requests.get(row.url)
            if (r.status_code == 200):
                # TODO
                BytesIO(r.content)
            else:
                pass
                # TODO retry and log failed
    logging.debug('stop')

client = MongoClient()
collection = client.imagenet.urls
cursor = collection.find().sort("_id")
cursor_lock = threading.Lock()

threads = []
for i in range(5):
    t = threading.Thread(name='worker_{}'.format(cursor, cursor_lock),
                         target=worker,
                         args=(cursor,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

logging.debug('all done')
