#!/usr/bin/env python3

import dmq

qs = dmq.DMQ("q", dmq.MODE_NEWLINES)
qs.send("this is test 1")
qs.send("this is test 2")
qs.send("this is test 3")
qs.send("this is test 4")

try:
    qs.receive()
except dmq.DMQError:
    print("Correctly received exception")
else:
    print("Missing exception")

qr = dmq.DMQ("q")
for message in qr.receive_iter(nowait=True):
    print('Message is:', message)

