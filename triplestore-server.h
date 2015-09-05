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
#include <dispatch/dispatch.h>
#include <sys/stat.h>
#include "triplestore.h"

struct server_runtime_ctx_s {
	int error;
	char* error_message;
	double start;
	query_t* query;
	int constructing;
	void(^result_block)(query_t* query, nodeid_t* final_match);
};

typedef struct triplestore_server_s {
	short port;
	dispatch_queue_t queue;
	dispatch_queue_t sync_queue;
	int fd;
	int use_http;
	int buffer_size;
} triplestore_server_t;

triplestore_server_t* triplestore_new_server(short port, int use_http);
int triplestore_free_server(triplestore_server_t* s);

int triplestore_run_server(triplestore_server_t* s, triplestore_t* t);
int triplestore_read_and_run_query(triplestore_server_t* s, triplestore_t* t, FILE* in, FILE* out);

#endif
