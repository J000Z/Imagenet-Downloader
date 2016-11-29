from ws4py.async_websocket import EchoWebSocket
from queue import FifoSQLiteQueue
import pickle
import logging
import argparse

loop = asyncio.get_event_loop()

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

    def received_message(self, message):
        message = pickle.loads(message.data)
        if message[FLAG] == FLAG_AUTH:
            if message[KEY] == key:
                self.auth = True
        if not self.auth:
            logging.debug('not authenticated request')
        if message[FLAG] == FLAG_ACK:
            id_ = message[ID]
            del queue[id_]
            logging.debug('pop {}'.format(id_))
        elif message[FLAG] == FLAG_NEXT:
            id_, data = queue.peek()
            payload = {FLAG: FLAG_DATA, ID: id_, DATA: data}
            self.send(pickle.dumps(payload), True)
            logging.debug('send {}'.format(id_))
        else:
            logging.debug('unsupportted message flag {}'.format(message[FLAG]))


def start_server():
    proto_factory = lambda: WebSocketProtocol(Handler)
    return loop.create_server(proto_factory, '', args.port)

try:
    s = loop.run_until_complete(start_server())
    logging.debug('serving on {}'.format(s.sockets[0].getsockname()))
    loop.run_forever()
except KeyboardInterrupt:
    queue.close()
