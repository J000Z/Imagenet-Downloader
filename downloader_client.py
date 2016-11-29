from ws4py.client.threadedclient import WebSocketClient
import pickle
import logging
import argparse

parser = argparse.ArgumentParser(description='urls downloader server')
parser.add_argument('folder', help='folder to put images')
parser.add_argument('address', help='remote address')
parser.add_argument('key', help='password')

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s')

FLAG = 0
DATA = 1
ID = 2
KEY = 3

FLAG_ACK = 0
FLAG_NEXT = 1
FLAG_DATA = 2
FLAG_AUTH = 3


class DownloaderClient(WebSocketClient):

    def requestNext(self):
        payload = {FLAG: FLAG_NEXT}
        self.send(pickle.dumps(payload), True)

    def processData(self, data):
        filename, data = pickle.loads(data)
        path = '{}/{}'.format(args.folder, filename)
        if os.path.isfile(path):
            continue
        with open(path, 'wb+') as f:
            f.write(data)

    def requestAck(self, id_):
        payload = {FLAG: FLAG_ACK, ID: id_}
        self.send(pickle.dumps(payload), True)

    def opened(self):
        payload = {FLAG: FLAG_AUTH, KEY: args.key}
        self.send(pickle.dumps(payload), True)

    def closed(self, code, reason=None):
        logging.debug("Closed down {} {}".format(code, reason))

    def received_message(self, message):
        message = pickle.loads(message.data)
        if message[FLAG] == FLAG_DATA:
            self.processData(message[DATA])
            self.requestAck(message[ID])
            self.requestNext()
        else:
            logging.debug('unsupportted message flag {}'.format(message[FLAG]))

if __name__ == '__main__':
    try:
        ws = DownloaderClient(args.address)  # 'ws://localhost:9000/'
        ws.connect()
        ws.run_forever()
    except KeyboardInterrupt:
        ws.close()
