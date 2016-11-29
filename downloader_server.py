from wsgiref.simple_server import make_server
from ws4py.websocket import WebSocket
from ws4py.server.wsgirefserver import WSGIServer, WebSocketWSGIRequestHandler
from ws4py.server.wsgiutils import WebSocketWSGIApplication
from queue import FifoSQLiteQueue
import pickle
import logging
import argparse
import time


parser = argparse.ArgumentParser(description='urls downloader server')
parser.add_argument('queue_path', help='The buffer queue path')
parser.add_argument('port', help='socket port', type=int)
parser.add_argument('key', help='password')
args = parser.parse_args()
queue = FifoSQLiteQueue(args.queue_path)
key = args.key

FLAG = 0
DATA = 1
ID = 2
KEY = 3

FLAG_ACK = 0
FLAG_NEXT = 1
FLAG_DATA = 2
FLAG_AUTH = 3

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s')


class Handler(WebSocket):

    def opened(self):
        self.auth = False
        logging.debug('connection opened from {}'.format(self.peer_address))

    def receiveNext(self):
        row = queue.peek()
        while row is None:
            row = queue.peek()
            time.sleep(10)
        id_, data = row
        payload = {FLAG: FLAG_DATA, ID: id_, DATA: data}
        self.send(pickle.dumps(payload), True)
        logging.debug('send {}'.format(id_))

    def receiveAck(self, id_):
        del queue[id_]
        logging.debug('pop {}'.format(id_))
        receiveNext()

    def received_message(self, message):
        message = pickle.loads(message.data)
        if message[FLAG] == FLAG_AUTH:
            logging.debug('auth {}?={}'.format(id_, key))
            if message[KEY] == key:
                self.auth = True
                return receiveNext()
        if not self.auth:
            logging.debug('not authenticated request')
            return
        if message[FLAG] == FLAG_ACK:
            id_ = message[ID]
            self.receiveAck(id_)
        elif message[FLAG] == FLAG_NEXT:
            self.receiveNext()
        else:
            logging.debug('unsupportted message flag {}'.format(message[FLAG]))

try:
    server = make_server('', args.port, server_class=WSGIServer,
                         handler_class=WebSocketWSGIRequestHandler,
                         app=WebSocketWSGIApplication(handler_cls=Handler))
    server.initialize_websockets_manager()
    server.serve_forever()
except KeyboardInterrupt:
    queue.close()
