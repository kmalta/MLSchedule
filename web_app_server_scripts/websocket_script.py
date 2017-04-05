import websocket
import thread
import time
import json

import sys
sys.path.insert(0,'.')

from profile_dataset import *



def on_message(ws, message):
    message = json.loads(message)
    print repr(message)
    if message['message'] == 'profile data':
        s3url = message['url']
        json_return = get_data_profile(s3url)
        json_to_send = json.dumps(json_return)
        ws.send(json_to_send)


def on_error(ws, error):
    print error

def on_close(ws):
    print "### closed ###"

def on_open(ws):
    hello_message = {}
    hello_message['message'] = 'hello'
    hello_message['client'] = 'python'
    json_to_send = json.dumps(hello_message)
    ws.send(json_to_send)
    print "Connecting!"



def main():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("ws://localhost:5000",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()



if __name__ == "__main__":
    main()

