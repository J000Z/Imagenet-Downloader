import sqlite3
from threading import Lock
import os
import io
import pickle


def synchronized(func):
    def func_wrapper(*args, **kwargs):
        result = None
        with args[0].mutex:
            result = func(*args, **kwargs)
        return result
    return func_wrapper


def to_bytes(func):
    def func_wrapper(*args, **kwargs):
        r = func(*args, **kwargs)
        if r is None:
            return None
        else:
            return pickle.loads(r)
    return func_wrapper


class FifoSQLiteQueue(object):

    _sql_create = (
        'CREATE TABLE IF NOT EXISTS queue '
        '(id INTEGER PRIMARY KEY AUTOINCREMENT, item BLOB)'
    )
    _sql_size = 'SELECT COUNT(*) FROM queue'
    _sql_push = 'INSERT INTO queue (item) VALUES (?)'
    _sql_pop = 'SELECT id, item FROM queue ORDER BY id LIMIT 1'
    _sql_del = 'DELETE FROM queue WHERE id = ?'
    _sql_get = 'SELECT id, item FROM queue WHERE id = ?'

    def __init__(self, path):
        self._path = os.path.abspath(path)
        self._db = sqlite3.Connection(self._path, check_same_thread=False)
        self._db.text_factory = bytes
        self.mutex = Lock()
        with self._db as conn:
            conn.execute(self._sql_create)
        self.size = self.size()

    @synchronized
    def push(self, item):
        if not isinstance(item, bytes):
            item = pickle.dumps(item)

        with self._db as conn:
            conn.execute(self._sql_push, (item,))
        self.size += 1

    # @synchronized
    # def pop(self):
    #     with self._db as conn:
    #         for id_, item in conn.execute(self._sql_pop):
    #             conn.execute(self._sql_del, (id_,))
    #             return (id_, item)
    #         conn.execute(self._sql_del, (id_,))

    @to_bytes
    @synchronized
    def peek(self, id_=None):
        with self._db as conn:
            if id_ is None:
                for result in conn.execute(self._sql_pop):
                    return result
            else:
                for result in conn.execute(self._sql_get, (id_,)):
                    return result

    @synchronized
    def delete(self, id_):
        with self._db as conn:
            conn.execute(self._sql_del, (id_,))
            self.size -= 1

    @synchronized
    def close(self):
        size = len(self)
        self._db.close()
        if not size:
            os.remove(self._path)

    @synchronized
    def size(self):
        with self._db as conn:
            return next(conn.execute(self._sql_size))[0]

    def __getitem__(self, id_):
        return self.peek(id_)

    def __delitem__(self, id_):
        self.delete(id_)

    @synchronized
    def __len__(self):
        return self.size
