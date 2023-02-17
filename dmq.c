/*
 * Copyright 2020 Rayan Zachariassen. All rights reserved.
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/errno.h>
#include <sys/uio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <linux/limits.h>

#include "dmq.h"

int mode = MODE_NEWLINES;	// 0 means '\n' is the record separator

/*

This is a persistent durable message queue.

Its implemented using 2 files that create a circular buffer.

The reader (consumer) controls which file the writer (producer) writes to.

When reader hits EOF on a file being written to, or after a certain size,
the writer is redirected to the other file.

The main program options are:
-p print contents of map file
-r run a reader
-w run a writer

*/


struct common {
	int readtarget;			// 0 or 1 - where reader is reading
	int writetarget;		// 0 or 1 - where reader wants it to write
	int writedelay;			// minimum time between writes to throttle production, in ns TODO XXX
	int mode;			// framing mode (NULs, newlines, frames, raw)
	size_t readoffset;		// last seek offset in readtarget
	size_t position;		// next record position in readtarget
	char qfile[3][PATH_MAX];	// file names
};

struct statedescriptor {
	struct common *sharedstate;
	int qfd[2];			// index is target fd, value is actual fd
	int direction;			// uncommitted is 0, send is 1, receive is 2
};

// We use a statedescriptor array so we can pass the descriptor (array index) around in Cython, instead of passing a sharedstate void * which is more problematic
struct statedescriptor sdarray[20] = { { NULL, { 0 }, 0 } };

char *dmq_errmsg;

#define CHECK_SDESC(errarg)	\
	struct statedescriptor *sdp;												\
																\
	if (sdesc < 0 || sdesc >= (int)(sizeof sdarray / sizeof sdarray[0]) || (sdp = &sdarray[sdesc])->sharedstate == NULL) {	\
		sprintf(dmq_errmsg, "Invalid descriptor");									\
		return (errarg);												\
	}

#define QF_Q0	0
#define QF_Q1	1
#define QF_MAP	2

int dmq_init(char *basepath, int mode)
{
	int fd;
	int sdesc;
	struct common *shared;
	char path[PATH_MAX];

	if (dmq_errmsg == NULL)
		dmq_errmsg = malloc(2*PATH_MAX);
	for (sdesc = 0; sdesc < (int)(sizeof sdarray / sizeof sdarray[0]); ++sdesc)
		if (sdarray[sdesc].sharedstate == NULL)
			break;
	if (sdesc >= (int)(sizeof sdarray / sizeof sdarray[0])) {
		sprintf(dmq_errmsg, "No descriptors available");
		return -1;
	}
	sprintf(path, "%smap", basepath);
	if ((fd = open(path, O_RDWR)) == -1) {
		// initialize qmap file
		if ((fd = open(path, O_RDWR|O_CREAT, 0600)) == -1) {
			sprintf(dmq_errmsg, "Cannot open map file %s: error %d [%s]", path, errno, strerror(errno));
			return -1;
		}
		// printf("Initializing map file\n");
		ftruncate(fd, sizeof (struct common));
		shared = (struct common *)mmap(0, sizeof (struct common), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
		shared->readtarget = 0;
		shared->writetarget = 0;
		shared->readoffset = 0;
		shared->position = 0;
		shared->mode = mode < 0 ? MODE_NEWLINES : mode;
		sprintf(shared->qfile[QF_Q0], "%s0", basepath);
		sprintf(shared->qfile[QF_Q1], "%s1", basepath);
		strcpy(shared->qfile[QF_MAP], path);
	} else {
		shared = (struct common *)mmap(0, sizeof (struct common), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
		// The file existed beforehand, don't touch contents yet
	}
	close(fd);
	if ((sdarray[sdesc].qfd[0] = open(shared->qfile[QF_Q0], O_RDWR|O_APPEND|O_CREAT, 0600)) == -1) {
		sprintf(dmq_errmsg, "Cannot open queue file %s: error %d [%s]", shared->qfile[QF_Q0], errno, strerror(errno));
		return -1;
	}
	if ((sdarray[sdesc].qfd[1] = open(shared->qfile[QF_Q1], O_RDWR|O_APPEND|O_CREAT, 0600)) == -1) {
		sprintf(dmq_errmsg, "Cannot open queue file %s: error %d [%s]", shared->qfile[QF_Q1], errno, strerror(errno));
		return -1;
	}
	// printf("setting readoffset to %ld\n", shared->readoffset);
	lseek(sdarray[sdesc].qfd[shared->readtarget], shared->position, SEEK_SET);
	shared->readoffset = shared->position;
	sdarray[sdesc].sharedstate = shared;
	sdarray[sdesc].direction = 0;
	return sdesc;
}

void dmq_free(int sdesc)
{
	CHECK_SDESC((void)0);

	(void) munmap(sdp->sharedstate, sizeof (struct common));
	close(sdp->qfd[0]);
	close(sdp->qfd[1]);
	sdp->sharedstate = NULL;
	sdp->direction = 0;
}

int dmq_send(int sdesc, char *s, int slen)
{
	int r, n;
	ushort len;
	char c;
	struct iovec iov[10];

	CHECK_SDESC(-1);

	switch (sdp->direction) {
	case 0:
		sdp->direction = 1;
	case 1:
		break;
	case 2:
		sprintf(dmq_errmsg, "Send: not after receive");
		return -1;
	}

	len = (ushort)slen;
	switch (sdp->sharedstate->mode) {
	case MODE_NULLS:
		c = '\0';
		iov[0].iov_base = s; iov[0].iov_len = len;
		iov[1].iov_base = &c; iov[1].iov_len = 1;
		n = 2;
		len += 1;
		break;
	case MODE_NEWLINES:
		c = '\n';
		iov[0].iov_base = s; iov[0].iov_len = len;
		iov[1].iov_base = &c; iov[1].iov_len = 1;
		n = 2;
		len += 1;
		break;
	case MODE_FRAMES:
		iov[0].iov_base = &len; iov[0].iov_len = sizeof len;
		iov[1].iov_base = s; iov[1].iov_len = len;
		n = 2;
		len += sizeof len;
		break;
	case MODE_RAW:
		iov[0].iov_base = s; iov[0].iov_len = len;
		n = 1;
		break;
	default:
		n = 0;
		break;
	}
	if ((r = writev(sdp->qfd[sdp->sharedstate->writetarget], iov, n)) == -1) {
		sprintf(dmq_errmsg, "Send: write(%d, buf, %d) errno %d [%s]", sdp->qfd[sdp->sharedstate->writetarget], len, errno, strerror(errno));
		return -1;
	} else if (r != len) {
                sprintf(dmq_errmsg, "Send: short write(%d, buf, %d) returned %d", sdp->qfd[sdp->sharedstate->writetarget], len, r);
		return -1;
	}
	return r;
}

struct dmq_iovbuf *dmq_read(struct statedescriptor *sdp, struct dmq_iovbuf *iovp, int nowait, int *errorp)
{
	struct common *shared;
	int r;

	shared = sdp->sharedstate;
	while ((r = read(sdp->qfd[shared->readtarget], iovp->buf, iovp->len)) == 0) {
		// hit EOF
		if (shared->readtarget != shared->writetarget) {
			// we are not expecting any more data in this file until writetarget flips
			// printf("flipping readtarget from %d to %d\n", shared->readtarget, shared->writetarget);
			ftruncate(sdp->qfd[shared->readtarget], 0);
			shared->readtarget = shared->writetarget;
			lseek(sdp->qfd[shared->readtarget], 0, SEEK_SET);
			shared->readoffset = 0;
			shared->position = 0;
		} else {	// readtarget == writetarget and we just missed a read
			if (shared->readoffset > 100) {
				// printf("flipping writetarget from %d to %d\n", shared->writetarget, 1 - shared->writetarget);
				shared->writetarget = 1 - shared->writetarget;
			}
			// wait for a write to happen
			// printf("sleeping\n");
			// if we have to wait for a write to happen, tell caller
			if (nowait) {
				*errorp = 0;
				return NULL;
			}
			sleep(1);
		}
	}
	if (r < 0) {
		sprintf(dmq_errmsg, "Read: error %d: %s", errno, strerror(errno));
		*errorp = 1;
		return NULL;
	}
	shared->readoffset += r;
	// printf("read offset = %ld\n", shared->readoffset);
	iovp->len = r;
	return iovp;
}

char *dmq_findeor(int mode, char *begin, char *end)
{
	ushort shortint;
	char *cp;

	// RAW mode is not allowed for reads
	switch (mode) {
	case MODE_NULLS:	// '\0' is record separator
		for (cp = begin; cp < end; ++cp) {
			if (*cp == '\0')
				return cp;
		}
		break;
	case MODE_NEWLINES:	// '\n' is record separator
		for (cp = begin; cp < end; ++cp) {
			if (*cp == '\n')
				return cp;
		}
		break;
	case MODE_FRAMES:	// the first 2 bytes are record length including frame header
		memcpy(&shortint, begin, sizeof shortint);
		if ((begin + shortint) < end)
			return begin + shortint;
		break;
	default:
		abort();
	}
	return 0;
}

struct dmq_record *dmq_receive(int sdesc, int nowait, int *errorp)
{
	char *eor;
	static int pos = 0;
	static struct dmq_record res;
	static struct dmq_iovbuf readiov = { .len = 0 };
	static struct dmq_iovbuf *iov = &readiov;
	static char buffer[2*BUFSIZ];

	CHECK_SDESC(NULL);

	switch (sdp->direction) {
	case 0:
		sdp->direction = 2;
	case 2:
		break;
	case 1:
		sprintf(dmq_errmsg, "Receive: cannot after send");
		*errorp = 1;
		return NULL;
	}

	// printf("pos = %d\n", pos);
	if (pos == iov->len) {
		readiov.len = BUFSIZ;
		readiov.buf = buffer + BUFSIZ;
		iov = dmq_read(sdp, &readiov, nowait, errorp);
		if (iov == NULL)
			return NULL;
		pos = 0;
	}
	if (pos >= iov->len)
		return NULL;
	// find the end of the record
	eor = dmq_findeor(sdp->sharedstate->mode, iov->buf + pos, iov->buf + iov->len);
	// if its not in this buffer, save the partial record as a residue and get another buffer
	if (eor == 0) {
		// copy from pos to len to the end of the previous buffer and adjust pos
		memcpy(iov->buf + pos - BUFSIZ, iov->buf + pos, iov->len - pos);
		pos -= BUFSIZ;
		readiov.len = BUFSIZ;
		readiov.buf = buffer + BUFSIZ;
		iov = dmq_read(sdp, &readiov, nowait, errorp);
		if (iov == NULL)
			return NULL;
		// printf("read returned offset = %ld\n", sdp->sharedstate->readoffset);
		eor = dmq_findeor(sdp->sharedstate->mode, iov->buf + pos, iov->buf + iov->len);
		if (eor == 0) {
			// record too big for buffer
			sprintf(dmq_errmsg, "Receive: record too big for internal buffer");
			if (errorp)
				*errorp = 1;
			return NULL;
		}
	}
	res.begin = iov->buf + pos + (sdp->sharedstate->mode == MODE_FRAMES ? sizeof (ushort) : 0);
	res.end = eor;
	pos = eor - iov->buf + 1;
	sdp->sharedstate->position = (sdp->sharedstate->readoffset - iov->len) + pos;
	// printf("position offset = %ld\n", sdp->sharedstate->position);
	return &res;
}

void dmq_info(int sdesc)
{
	struct common *shared;
	CHECK_SDESC((void)0);

	shared = sdp->sharedstate;
	printf("iofs.log.readtarget = %d\n", shared->readtarget);
	printf("iofs.log.writetarget = %d\n", shared->writetarget);
	printf("iofs.log.writedelay = %d\n", shared->writedelay);
	printf("iofs.log.readoffset = %ld\n", shared->readoffset);
	printf("iofs.log.position = %ld\n", shared->position);
	printf("iofs.log.logfile0 = %s\n", shared->qfile[0]);
	printf("iofs.log.logfile1 = %s\n", shared->qfile[1]);
}

void dmq_clean(int sdesc)
{
	struct common *shared;
	CHECK_SDESC((void)0);

	shared = sdp->sharedstate;
	unlink(shared->qfile[QF_Q0]);
	unlink(shared->qfile[QF_Q1]);
	unlink(shared->qfile[QF_MAP]);
}

#ifdef STANDALONE
int main(int argc, char *argv[])
{
	int i = 0, count, sd;
	struct dmq_record *rec;
	char *cp, buf[80];

	sd = dmq_init("q", MODE_NEWLINES);
	// printf("read from %d write to %d\n", qfd[sharedstate->readtarget], qfd[sharedstate->writetarget]);
	// Writeable is now set to whichever one that is
	if (argc == 1) {
		fprintf(stderr, "Error, no -r or -w option\n");
	} else if (strcmp(argv[1], "-r") == 0) {
		// reader
		count = 0;
		while (rec = dmq_receive(sd, 0, NULL)) {
#if 0
			printf("read %d bytes from %s (%d): <", rec->end - rec->begin, sharedstate->qfile[sharedstate->readtarget], count);
			for (cp = rec->begin; cp < rec->end; ++cp)
				putchar(*cp);
			putchar('>');
			putchar('\n');
#endif
			if ((++count % 100000) == 0)
				printf("count = %d\n", count);
			// sleep(1);
		}
	} else if (strcmp(argv[1], "-w") == 0) {
		// writer
		while (++i) {
			sprintf(buf, "%d", i);
			dmq_send(sd, buf, strlen(buf));
			// sleep(1);
		}
	} else if (strcmp(argv[1], "-p") == 0) {
		dmq_info(sd);
	} else {
		fprintf(stderr, "Error, unknown option\n");
	}
	return 0;
}

#endif
