#!/usr/bin/env python3

# Create independent reader and writer threads, dmq supplies the buffer
# Then stdout can be piped to a simple shell script that does whatever it wants to process each line without worrying about whether it is falling behind the producer
# Obviously the buffer size is limited by the filesystem size

import argparse
import dmq
import os
import sys
import threading


ENDMARKER = b'****EOF****'


parser = argparse.ArgumentParser(description='DMQ Buffer: write existing contents of buffer and stdin to line-buffered stdout; reads until EOF on stdin')
parser.add_argument('-q', '--queuename', help='Use the queue files in /var/iofs/QUEUENAME.')
parser.add_argument('-c', '--clean', action='store_true', help='Clean up (remove) the queue files when done')
parser.add_argument('-i', '--input', action='store_true', help='Do NOT read from stdin just consume the queue and print to stdout')

args = parser.parse_args()

def reader(q):
    while True:
        line = sys.stdin.readline().encode()
        if line == b'':
            break
        linelen = len(line)
        if line[-1] == ord(b'\n'):
            linelen -= 1
        q.send(line, linelen)
    q.send(ENDMARKER, len(ENDMARKER))

def writer(q):
    while True:
        message = q.receive()
        if message == ENDMARKER:
             break
        try:
            print('%s' % message.decode(), flush=True)
        except:
            print('%s' % message, flush=True)

if args.queuename and args.queuename[0] != '/':
    args.queuename = '/var/iofs/%s' % args.queuename

os.makedirs(args.queuename, exist_ok=True)

qrecv = dmq.DMQ(args.queuename)
if not args.input:
    qsend = dmq.DMQ(args.queuename)
    threading.Thread(target=reader, args=(qsend,), name='Reader', daemon=True).start()
writer(qrecv)
# Only clean on normal exit
if args.clean:
    qrecv.clean()
