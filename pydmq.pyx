# Copyright 2020 Rayan Zachariassen. All rights reserved.

# cython: language_level=3
# cython: c_string_type=unicode, c_string_encoding=ascii

from cpython.bytes cimport PyBytes_FromStringAndSize

# Use: dmq = DMQ("q", MODE_NEWLINES)
# dmq.send("foo")
# dmq.receive()
# dmq.info()

cimport cdmq

cdef extern from "dmq.h":
    cdef int _MODE_NULLS "MODE_NULLS"
    cdef int _MODE_NEWLINES "MODE_NEWLINES"
    cdef int _MODE_FRAMES "MODE_FRAMES"
    cdef int _MODE_RAW "MODE_RAW"

MODE_NULLS = _MODE_NULLS
MODE_NEWLINES = _MODE_NEWLINES
MODE_FRAMES = _MODE_FRAMES
MODE_RAW = _MODE_RAW

class DMQError(Exception):

    def __init__(self, message="exception"):
        self.message = message

class DMQ:

    def __init__(self, basepath, mode=-1):
        cdef char *s = basepath
        self.desc = cdmq.dmq_init(s, mode)

    def free(self):
        cdmq.dmq_free(self.desc)

    def send(self, char* s, int slen=(-1)):
        cdef int sdesc, rval;
        if slen < 0:
            slen = len(s)
        sdesc = self.desc
        with nogil:
            rval = <int> cdmq.dmq_send(sdesc, s, slen)
        return rval

    def receive(self, int nowait=0):
        cdef int error, sdesc;
        sdesc = self.desc
        with nogil:
            rec = cdmq.dmq_receive(sdesc, nowait, &error)
        if rec == NULL:
            if nowait and error == 0:
                return None
            raise DMQError(<str> cdmq.dmq_errmsg)
        return PyBytes_FromStringAndSize(rec.begin,rec.end - rec.begin)

    def receive_iter(self, int nowait=0):
        cdef int error, sdesc;
        sdesc = self.desc
        while True:
            with nogil:
                rec = cdmq.dmq_receive(sdesc, nowait, &error)
            if rec == NULL:
                if nowait and error == 0:
                    raise StopIteration
                raise DMQError(<str> cdmq.dmq_errmsg)
            yield PyBytes_FromStringAndSize(rec.begin,rec.end - rec.begin)

    def info(self):
        return cdmq.dmq_info(self.desc)

    def clean(self):
        return cdmq.dmq_clean(self.desc)

dmq = DMQ
