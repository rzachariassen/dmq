
cdef extern from "dmq.h":

    cdef struct dmq_iovbuf:
        int      len
        char*    buf

    cdef struct dmq_record:
        char*    begin
        char*    end

    cdef int dmq_init(char* basepath, int) nogil;
    cdef void dmq_free(int) nogil;
    cdef int dmq_send(int, char*, int) nogil;
    cdef dmq_record* dmq_receive(int, int, int*) nogil;
    cdef void dmq_info(int) nogil;
    cdef void dmq_clean(int) nogil;
    cdef char *dmq_errmsg;
