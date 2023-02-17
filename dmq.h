/*
 * Copyright 2020 Rayan Zachariassen. All rights reserved.
 */

#define MODE_NULLS      0
#define MODE_NEWLINES   1
#define MODE_FRAMES     2
#define MODE_RAW        3

struct dmq_iovbuf {
	int	len;
	char	*buf;
};

struct dmq_record {
	char	*begin;
	char	*end;
};

extern int dmq_init(char *, int);
extern void dmq_free(int);
extern int dmq_send(int, char *, int);
extern struct dmq_record *dmq_receive(int, int, int *);
extern void dmq_info(int);
extern void dmq_clean(int);
extern char *dmq_errmsg;
