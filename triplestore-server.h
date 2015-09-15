#ifndef _TRIPLESTORE_SERVER_H
#define _TRIPLESTORE_SERVER_H

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#include <pcre.h>
// #include <dispatch/dispatch.h>
#include <sys/stat.h>
#include <ck_ring.h>
#include <pthread.h>
#include "triplestore.h"

typedef struct triplestore_server_s {
	short port;
	int fd;
	int use_http;
	int buffer_size;
// 	dispatch_queue_t queue;
// 	dispatch_queue_t sync_queue;
	ck_ring_t *ring;
	int nthr;
	int size;
	int max;
	ck_ring_buffer_t *buffer;
	pthread_t* threads;
	triplestore_t* t;
} triplestore_server_t;

triplestore_server_t* triplestore_new_server(short port, int use_http, triplestore_t* t);
int triplestore_free_server(triplestore_server_t* s);

int triplestore_run_server(triplestore_server_t* s);
int triplestore_read_and_run_query(triplestore_server_t* s, FILE* in, FILE* out);

#endif
