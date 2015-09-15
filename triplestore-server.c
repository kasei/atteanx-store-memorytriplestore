#include <time.h>
#include "triplestore-server.h"
#include "commands.h"

#pragma mark -

static int server_ctx_set_error(struct command_ctx_s* ctx, char* message) {
	ctx->error++;
	if (!ctx->error_message) {
		ctx->error_message	= message;
	}
	if (0) {
		fprintf(stderr, "Error: %s\n", message);
	}
	return 1;
}

static int triplestore_output_op(triplestore_t* t, struct command_ctx_s* ctx, int argc, char** argv) {
	if (argc == 0) {
		return 0;
	}
	
	const char* op			= argv[0];
	if (ctx->constructing) {
		if (!strcmp(op, "end")) {
			return 1;
		}
		if (!strcmp(op, "agg")) {
			return 1;
		}
		return 0;
	} else {
		if (!strcmp(op, "begin")) {
			return 0;
		}
		return 1;
	}
}

// static int _triplestore_run_query(triplestore_t* t, query_t* query, struct command_ctx_s* ctx, FILE* f) {
// 	__block int count	= 0;
// 	if (!query) {
// 		return 1;
// 	}
// 	triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
// 		count++;
// 		ctx->result_block(query, final_match);
// 		return 0;
// 	});
// 	return 0;
// }

static int64_t _triplestore_query_get_variable_id(query_t* query, const char* var) {
	int64_t v	= 0;
	char* p		= (char*) var;
	if (p[0] == '?') {
		p++;
	}
	for (int x = 1; x <= query->variables; x++) {
		const char* vname	= query->variable_names[x];
		if (vname) {
			if (!strcmp(p, vname)) {
				v	= -x;
				break;
			}
		}
	}
	return v;
}

// int64_t query_node_id(triplestore_t* t, query_t* query, const char* ts) {
// 	int64_t id	= 0;
// 	if (isdigit(ts[0])) {
// 		return 0;
// 	} else if (ts[0] == '<') {
// 		char* p	= strstr(ts, ">");
// 		if (!p) {
// 			return 0;
// 		}
// 		long len	= p - ts;
// 		char* value	= malloc(1 + len);
// 		snprintf(value, len, "%s", ts+1);
// // 		fprintf(stderr, "IRI: (%d) <%s>\n", len, value);
// 		rdf_term_t* term = triplestore_new_term(t, TERM_IRI, value, NULL, 0);
// 		id = triplestore_get_termid(t, term);
// 		free(value);
// 	} else if (ts[0] == '"') {
// 		char* p	= strstr(ts+1, "\"");
// 		if (!p) {
// 			fprintf(stderr, "cannot parse literal value\n");
// 			return 0;
// 		}
// 		long len	= p - ts;
// 		char* value	= malloc(1 + len);
// 		snprintf(value, len, "%s", ts+1);
// 		rdf_term_t* term	= NULL;
// 		if (p[1] == '^') {
// 			p += 4;
// 			char* q	= strstr(p, ">");
// 			if (!q) {
// 				fprintf(stderr, "cannot parse datatype IRI\n");
//                 free(value);
// 				return 0;
// 			}
// 			long len	= q - p + 1;
// 			char* dt	= malloc(1 + len);
// 			snprintf(dt, len, "%s", p);
// 			rdf_term_t* dtterm = triplestore_new_term(t, TERM_IRI, dt, NULL, 0);
// 			int64_t dtid = triplestore_get_termid(t, dtterm);
// 			term = triplestore_new_term(t, TERM_TYPED_LITERAL, value, NULL, (nodeid_t) dtid);
// 			free(dt);
// 		} else if (p[1] == '@') {
// 			p += 2;
// 			term = triplestore_new_term(t, TERM_LANG_LITERAL, value, p, 0);
// // 			char* s	= triplestore_term_to_string(t, term);
// // 			fprintf(stderr, "Term: %s\n", s);
// // 			free(s);
// 		} else {
// 			term = triplestore_new_term(t, TERM_XSDSTRING_LITERAL, value, NULL, 0);
// 		}
// 		
// 		id = triplestore_get_termid(t, term);
// // 		fprintf(stderr, "%"PRId64" Literal: (%d) \"%s\"\n", id, len, value);
// 		free(value);
// 	} else {
// 		id	= _triplestore_query_get_variable_id(query, ts);
// 		if (id == 0) {
// // 			id			= -(next_var++);
// // 				fprintf(stderr, "Setting variable ?%s ID %"PRId64"\n", ts, id);
// 			char* p		= (char*) ts;
// 			if (p[0] == '?') {
// 				p++;
// 			}
// 			id	= triplestore_query_add_variable(query, p);
// // 			triplestore_ensure_variable_capacity(query, -id);
// // 			triplestore_query_set_variable_name(query, -id, ts);
// // 				bgp.variable_names[-id]		= calloc(1,2+strlen(ts));
// // 				sprintf(bgp.variable_names[-id], "?%s", ts);
// 		}
// 	}
// 	
// 	if (id == 0) {
// 		fprintf(stderr, "Unrecognized term string %s\n", ts);
// 	}
// 	return id;
// }

static query_t* construct_bgp_query(triplestore_t* t, struct command_ctx_s* ctx, int argc, char** argv, int i) {
	int count		= argc - i - 1;
	if (count % 3) {
		return NULL;
	}
	int triples		= count / 3;
	int variables	= 3 * triples;
// 	int next_var	= 1;
	query_t* query	= triplestore_new_query(t, 0);
	bgp_t* bgp		= triplestore_new_bgp(t, variables, triples);
	int j			= 0;
	int64_t* ids	= calloc(sizeof(int64_t), variables);
	while (i+1 < argc) {
		int index			= j++;
		const char* ts		= argv[++i];
		int64_t id			= query_node_id(t, ctx, query, ts);
		if (!id) {
			ctx->set_error(-1, "No node ID found for BGP term");
// 			fprintf(stderr, "No node ID found for BGP term %s\n", ts);
			triplestore_free_query(query);
			triplestore_free_bgp(bgp);
			free(ids);
			return NULL;
		}
		ids[index]	= id;
	}
	
	int possible_variables	= query->variables + 3*triples;
	int* seen				= calloc(possible_variables, sizeof(int));
	for (j = 0; j < triples; j++) {
		int64_t s	= ids[3*j + 0];
		int64_t p	= ids[3*j + 1];
		int64_t o	= ids[3*j + 2];
		if (j > 0) {
			int joinable	= 0;
			if (s < 0 && seen[-s]) { joinable++; }
			if (p < 0 && seen[-p]) { joinable++; }
			if (o < 0 && seen[-o]) { joinable++; }
			if (joinable == 0) {
				free(ids);
				free(seen);
				triplestore_free_query(query);
				triplestore_free_bgp(bgp);
				ctx->set_error(-1, "BGP with cartesian products are not allowed");
// 				server_ctx_set_error(ctx, "BGP with cartesian products are not allowed");
				return NULL;
			}
		}
		if (s < 0) { seen[-s]++; }
		if (p < 0) { seen[-p]++; }
		if (o < 0) { seen[-o]++; }
		triplestore_bgp_set_triple_nodes(bgp, j, s, p, o);
	}
	free(ids);
	free(seen);

	triplestore_query_add_op(query, QUERY_BGP, bgp);
	
	return query;
}

// static int triplestore_match_terms(triplestore_t* t, const char* pattern, int(^block)(nodeid_t id)) {
// 	const char *error;
// 	int erroffset;
// 	pcre* re = pcre_compile(
// 		pattern,		/* the pattern */
// 		0,				/* default options */
// 		&error,			/* for error message */
// 		&erroffset,		/* for error offset */
// 		NULL			/* use default character tables */
// 	);
// 	if (re == NULL) {
// 		printf("PCRE compilation failed at offset %d: %s\n", erroffset, error);
// 		exit(1);
// 	}
// 
// 	int64_t count	= 0;
// 	for (nodeid_t s = 1; s < t->nodes_used; s++) {
// 		char* string		= triplestore_term_to_string(t, t->graph[s]._term);
// // 			fprintf(stderr, "matching %s =~ %s\n", string, pattern);
// 		int OVECCOUNT	= 30;
// 		int ovector[OVECCOUNT];
// 		int rc = pcre_exec(
// 			re,							/* the compiled pattern */
// 			NULL,						/* no extra data - we didn't study the pattern */
// 			string,						/* the subject string */
// 			strlen(string),				/* the length of the subject */
// 			0,							/* start at offset 0 in the subject */
// 			0,							/* default options */
// 			ovector,					/* output vector for substring information */
// 			OVECCOUNT					/* number of elements in the output vector */
// 		);
// 		if (rc < 0) {
// 			switch(rc) {
// 				case PCRE_ERROR_NOMATCH: break;
// 				default: printf("Matching error %d\n", rc); break;
// 			}
// 			free(string);
// 			continue;
// 		}
// 		if (rc == 0) {
// 			rc = OVECCOUNT/3;
// 			printf("ovector only has room for %d captured substrings\n", rc - 1);
// 			free(string);
// 			continue;
// 		}
// 		
// 		count++;
// 		int r	= block(s);
// 		free(string);
// 		
// 		if (r) {
// 			break;
// 		}
// 	}
// 	pcre_free(re);     /* Release memory used for the compiled pattern */
// 	return 0;
// }
// 
// #pragma mark -
// 
// static int triplestore_print_triple(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o, FILE* f) {
// 	rdf_term_t* subject		= t->graph[s]._term;
// 	rdf_term_t* predicate	= t->graph[p]._term;
// 	rdf_term_t* object		= t->graph[o]._term;
// 
// 	if (subject == NULL) assert(0);
// 	if (predicate == NULL) assert(0);
// 	if (object == NULL) assert(0);
// 
// 	char* ss		= triplestore_term_to_string(t, subject);
// 	char* sp		= triplestore_term_to_string(t, predicate);
// 	char* so		= triplestore_term_to_string(t, object);
// 	fprintf(f, "%s %s %s .\n", ss, sp, so);
// 	free(ss);
// 	free(sp);
// 	free(so);
// 	return 0;
// }
// 
// static int triplestore_print_ntriples(triplestore_t* t, FILE* f) {
// 	uint32_t count	= 0;
// 	for (nodeid_t s = 1; s <= t->nodes_used; s++) {
// 		nodeid_t idx	= t->graph[s].out_edge_head;
// 		while (idx != 0) {
// 			nodeid_t p	= t->edges[idx].p;
// 			nodeid_t o	= t->edges[idx].o;
// 			triplestore_print_triple(t, s, p, o, f);
// 			idx			= t->edges[idx].next_out;
// 			count++;
// 		}
// 	}
// 	return 0;
// }
// 
// static int triplestore_node_dump(triplestore_t* t, FILE* f) {
// 	fprintf(f, "# %"PRIu32" nodes\n", t->nodes_used);
// 	for (nodeid_t s = 1; s <= t->nodes_used; s++) {
// 		char* ss		= triplestore_term_to_string(t, t->graph[s]._term);
// 		fprintf(f, "N %07"PRIu32" %s (%"PRIu32", %"PRIu32")\n", s, ss, t->graph[s].in_degree, t->graph[s].out_degree);
// 		free(ss);
// 	}
// 	return 0;
// }
// 
// static int triplestore_edge_dump(triplestore_t* t, FILE* f) {
// 	fprintf(f, "# %"PRIu32" edges\n", t->edges_used);
// 	int64_t count	= 0;
// 	for (nodeid_t s = 1; s <= t->nodes_used; s++) {
// 		nodeid_t idx	= t->graph[s].out_edge_head;
// 		while (idx != 0) {
// 			nodeid_t p	= t->edges[idx].p;
// 			nodeid_t o	= t->edges[idx].o;
// 			fprintf(f, "E %07"PRIu32" %07"PRIu32" %07"PRIu32"\n", s, p, o);
// 			idx			= t->edges[idx].next_out;
// 			count++;
// 		}
// 	}
// 	return 0;
// }
// 
// static int triplestore_print_data(triplestore_t* t, FILE* f) {
// 	triplestore_node_dump(t, f);
// 	triplestore_edge_dump(t, f);
// 	return 0;
// }
// 
// static nodeid_t triplestore_print_match(triplestore_t* t, int64_t s, int64_t p, int64_t o, FILE* f) {
// 	__block nodeid_t count	= 0;
// 	triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o) {
// 		count++;
// 		if (f != NULL) {
// 			triplestore_print_triple(t, s, p, o, f);
// 		}
// 		return 0;
// 	});
// 	return count;
// }

#pragma mark -

triplestore_server_t* triplestore_new_server(short port, int use_http) {
	triplestore_server_t* server	= calloc(1, sizeof(triplestore_server_t));
	server->use_http	= use_http;
	server->port		= port;
	server->queue		= dispatch_queue_create("us.kasei.triplestore.workers", DISPATCH_QUEUE_CONCURRENT);
	server->sync_queue	= dispatch_queue_create("us.kasei.triplestore.sync", NULL);
	server->buffer_size	= 4096;
	return server;
}

int triplestore_free_server(triplestore_server_t* s) {
	dispatch_barrier_sync(s->queue, ^{});
	dispatch_barrier_sync(s->sync_queue, ^{});	

	dispatch_release(s->queue);
	dispatch_release(s->sync_queue);
	close(s->fd);
	free(s);
	return 0;
}

static int write_tsv_results_header(FILE* f, query_t* query) {
	int vars	= query->variables;
	for (int j = 1; j <= vars; j++) {
		if (j < vars) {
			fprintf(f, "?%s\t", query->variable_names[j]);
		} else {
			fprintf(f, "?%s\n", query->variable_names[j]);
		}
	}
	return 0;
}

static int write_http_header(FILE* out, int code, const char* message, char* contenttype) {
// 	fprintf(stderr, "writing HTTP header %03d\n", code);
	time_t now		= time(0);
	struct tm tm	= *gmtime(&now);
	char* buf		= alloca(256);
	strftime(buf, 256, "%a, %d %b %Y %H:%M:%S %Z", &tm);
	
	if (!contenttype) {
		contenttype	= "text/plain";
	}
	
	fprintf(out, "HTTP/1.1 %03d %s\r\n"
				"Content-Type: %s\r\n"
				"Date: %s\r\n"
				"Server: MemoryTripleStore\r\n"
				"\r\n", code, message, contenttype, buf);
	return 0;
}

static int write_http_error_header(struct command_ctx_s* ctx, FILE* out, int code, const char* message) {
	if (write_http_header(out, code, message, "text/plain")) {
		return 1;
	}
	if (ctx->error_message) {
		fprintf(out, "%s\r\n", ctx->error_message);
	} else {
		fprintf(out, "%s\r\n", message);
	}
	return 0;
}

static int read_http_header(triplestore_server_t* server, FILE* in, int* length) {
// 	fprintf(stderr, "Reading HTTP header:\n");
// 	fprintf(stderr, "-------------------------\n");
	int cl					= 0;
	size_t len	= 1024;
	char* line	= calloc(1, 1024);
	ssize_t bytes;
	while ((bytes = getline(&line, &len, in)) != -1) {
		if (bytes == 2 && !strcmp(line, "\r\n")) {
			free(line);
			return 0;
		}
// 		fprintf(stderr, "[%zu] %s\n", bytes, line);
		char* ptr	= strcasestr(line, "Content-Length:");
		if (ptr) {
			ptr	+= 15;
			while (*ptr == ' ') {
				ptr++;
			}
			cl	= atoi(ptr);
			if (length) {
// 				fprintf(stderr, "Content-Length: %d\n", cl);
				*length	= cl;
			}
		}
	}
	free(line);
	return 1;
}

static int new_socket ( short port ) {
	int fd	= socket(PF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		perror("Socket creation");
		return -1;
	}
	
	struct sockaddr_in myname;
	myname.sin_family	= AF_INET;
	myname.sin_addr.s_addr	= INADDR_ANY;
	myname.sin_port	= htons(port);
	
	int reuse = 1;
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0) {
		perror("Socket options error");
		return -1;
	}	
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
		perror("Socket options error");
		return -1;
	}	
	
	int status;
	status	= bind(fd, (struct sockaddr*) &myname, sizeof(myname));
	if (status < 0) {
		perror("Bind error");
		return -1;
	}
	
	return fd;
}

int triplestore_run_server(triplestore_server_t* s, triplestore_t* t) {
	s->fd			= new_socket(s->port);
	if (s->fd < 0) {
		return 1;
	}

	struct sockaddr_in peer;
	int status		= listen(s->fd, 1024);
	if (status < 0) {
		perror("Listen error");
		return 1;
	} else {
		if (s->use_http) {
			fprintf(stderr, "Listening on http://localhost:%d/\n", s->port);
		} else {
			fprintf(stderr, "Listening on localhost:%d\n", s->port);
		}
	}
	
	int sd;
	socklen_t addrlen;
	
	fd_set set;
	struct timeval timeout;
	timeout.tv_sec = 0;
	timeout.tv_usec = 250000;
	
    uint64_t count	= 0;
    __block int highwater = 0;
    __block int outstanding = 0;
	while (1) {
		FD_ZERO(&set);
		FD_SET(s->fd, &set);
		int ready	= select(s->fd+1, &set, NULL, NULL, &timeout);
		if (ready > 0) {
// 			fprintf(stderr, "Peer available. trying to accept\n");
			uint64_t request_id  = ++count;
			addrlen	= sizeof(peer);
//            double start    = triplestore_current_time();
			sd	= accept(s->fd, (struct sockaddr*) &peer, &addrlen);
			if (sd < 0) {
				perror("Wrong connection");
				exit(1);
			}
            
			int set	= 1;
			setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, (void *)&set, sizeof(int));

//            double accept_time    = triplestore_elapsed_time(start);
            outstanding++;
            if (outstanding > highwater) {
                fprintf(stderr, "[%"PRIu64"] Starting block with new highwater mark at %d\n", request_id, highwater);
                highwater   = outstanding;
            }
// 			fprintf(stderr, "Accepted connection on fd %d\n", sd);
			dispatch_async(s->queue, ^{
//                double block_start_time    = triplestore_elapsed_time(start);
				FILE* f	= fdopen(sd, "r+");
				if (f == NULL) {
                    close(sd);
					perror("");
					return;
				}

				triplestore_read_and_run_query(s, t, f, f);
				fclose(f);
                outstanding--;
//                double total_time  = triplestore_elapsed_time(start);
//                fprintf(stdout, "R%"PRIu64",%lf,%lf,%lf,%lf\n", request_id, start, accept_time, block_start_time, total_time);
			});
		} else {
			usleep(100000);
		}
	}
	return 0;
}

static size_t fwrite_tsv(const char* ptr, size_t size, size_t nitems, FILE *restrict stream) {
	// TODO: escape values
	int needs_escape	= 0;
	int bytes	= size * nitems;
	for (int i = 0; i < size * bytes; i++) {
		if (ptr[i] == '\r' || ptr[i] == '\n' || ptr[i] == '\t') {
			needs_escape++;
		}
	}
	
	if (needs_escape) {
		size_t bytes	= 0;
		for (int i = 0; i < size * bytes; i++) {
			if (ptr[i] == '\r') {
				bytes	+= fwrite("\\r", 1, 2, stream);
			} else if (ptr[i] == '\n') {
				bytes	+= fwrite("\\n", 1, 2, stream);
			} else if (ptr[i] == '\t') {
				bytes	+= fwrite("\\t", 1, 2, stream);
			} else {
				fputc(ptr[i], stream);
				bytes++;
			}
		}
		return bytes;
	} else {
		return fwrite(ptr, size, nitems, stream);
	}
}

static int triplestore_print_tsv_term(triplestore_server_t* s, struct command_ctx_s* ctx, triplestore_t* t, nodeid_t id, FILE* f) {
	if (id > t->nodes_used) {
		ctx->set_error(-1, "Undefined term ID found in query result");
		return 1;
	}
	rdf_term_t* term		= t->graph[id]._term;
	const char* datatype	= NULL;
	if (term == NULL) assert(0);
	switch (term->type) {
		case TERM_IRI:
			fwrite("<", 1, 1, f);
			fwrite_tsv(term->value, 1, strlen(term->value), f);
			fwrite(">", 1, 1, f);
			return 0;
		case TERM_BLANK:
			fprintf(f, "_:b%"PRIu32"b%s", (uint32_t) term->vtype.value_id, term->value);
			return 0;
		case TERM_XSDSTRING_LITERAL:
			fwrite("\"", 1, 1, f);
			fwrite_tsv(term->value, 1, strlen(term->value), f);
			fwrite("\"", 1, 1, f);
			return 0;
		case TERM_LANG_LITERAL:
			// TODO: handle escaping
			fwrite("\"", 1, 1, f);
			fwrite_tsv(term->value, 1, strlen(term->value), f);
			fprintf(f, "\"@%s", (char*) &(term->vtype.value_type));
			return 0;
		case TERM_TYPED_LITERAL:
			datatype	= t->graph[term->vtype.value_id]._term->value;
			if (!strcmp(datatype, "http://www.w3.org/2001/XMLSchema#decimal")) {
				fwrite_tsv(term->value, 1, strlen(term->value), f);
			} else if (!strcmp(datatype, "http://www.w3.org/2001/XMLSchema#integer")) {
				fwrite_tsv(term->value, 1, strlen(term->value), f);
			} else if (!strcmp(datatype, "http://www.w3.org/2001/XMLSchema#double")) {
				fwrite_tsv(term->value, 1, strlen(term->value), f);
			} else {
				fwrite("\"", 1, 1, f);
				fwrite_tsv(term->value, 1, strlen(term->value), f);
				fwrite("\"^^<", 1, 4, f);
				fwrite_tsv(datatype, 1, strlen(datatype), f);
				fwrite(">", 1, 1, f);
			}
			return 0;
	}
}

int serialize_result(triplestore_server_t* s, struct command_ctx_s* ctx, FILE* f, triplestore_t* t, query_t* query, nodeid_t* result) {
	if (f != NULL) {
		int vars	= query->variables;
		for (int j = 1; j <= vars; j++) {
			nodeid_t id	= result[j];
			if (id > 0) {
				triplestore_print_tsv_term(s, ctx, t, id, f);
				if (j < vars) {
					fprintf(f, "\t");
				}
			}
		}
		fprintf(f, "\n");
	}
	return 0;
}

int triplestore_run_query(triplestore_server_t* s, triplestore_t* t, char* query, FILE* out) {
	__block struct command_ctx_s ctx	= {
		.error				= 0,
		.error_message		= NULL,
		.start				= triplestore_current_time(),
		.constructing		= 0,
		.query				= NULL,
	};
	
	ctx.preamble_block		= ^(query_t* query){
		write_tsv_results_header(out, query);
	};
	
	ctx.result_block		= ^(query_t* query, nodeid_t* final_match){
		serialize_result(s, &ctx, out, t, query, final_match);
	};
	
	ctx.set_error	= ^(int code, const char* message){
		if (0) {
			fprintf(stderr, "*** set_error called: %s\n", message);
		}
		server_ctx_set_error(&ctx, (char*) message);
	};
	
	int output	= 0;
	char* ptr	= query;
	
// 	fprintf(stderr, "query: «%s»\n", query);
	while (1) {
		char* end;
		if ((end = strstr(ptr, "\r"))) {
			*end	= '\0';
		} else if ((end = strstr(ptr, "\n"))) {
			*end	= '\0';
		} else {
			end		= strstr(ptr, "\0");
		}
		if (!end) {
			return 1;
		}
		
		long linelen	= end - ptr + 1;
// 		fprintf(stderr, "line length: %d\n", linelen);

		int argc_max	= 16;
		char** argv		= calloc(sizeof(char*), argc_max);
		int argc		= 0;
		argv[argc++]	= ptr;
		for (int i = 1; i < (linelen-1); i++) {
			if (ptr[i] == '\0') {
				free(argv);
				if (0) {
					fprintf(stderr, "Unexpected NULL byte in triplestore_run_query\n");
				}
				write_http_error_header(&ctx, out, 400, "Bad Request");
				return 1;
			} else if (ptr[i] == ' ') {
				ptr[i]	= '\0';
			} else if (ptr[i-1] == '\0') {
				if (argc > argc_max) {
					argc_max	*= 2;
					argv	= realloc(argv, sizeof(char*) * argc_max);
					if (!argv) {
						write_http_error_header(&ctx, out, 500, "Internal Server Error");
						return 1;
					}
				}
				char* p	= &(ptr[i]);
				argv[argc++]	= p;
				if (*p == '"') {
					while (ptr[i]) {
						if (ptr[i] == '\\') {
							i++;
							if (ptr[i] == '\0') {
								break;
							}
						}
						i++;
						if (ptr[i] == '"') {
							break;
						}
					}
				}
			}
		}
		
		if (triplestore_output_op(t, &ctx, argc, argv)) {
			if (s->use_http) {
				write_http_header(out, 200, "OK", "text/tab-separated-values; charset=utf-8");
			}
			output++;
		}
		
		if (argc == 1 && !strcmp(argv[0], "")) {
			goto loop_cleanup;
		}
		int r	= triplestore_op(t, &ctx, argc, argv);
		if (r) {
			if (0) {
				fprintf(stderr, "triplestore_op failed in triplestore_run_query\n");
			}
			write_http_error_header(&ctx, out, 400, "Bad Request");
			free(argv);
			return 1;
		}
		
		if (ctx.constructing == 0) {
			free(argv);
			break;
		}
		
loop_cleanup:
		ptr	+= linelen;
		free(argv);
	}

	if (!output) {
		if (0) {
			fprintf(stderr, "No output in triplestore_run_query\n");
		}
		write_http_error_header(&ctx, out, 400, "Bad Request");
		return 1;
	}
	
	return 0;
}

int triplestore_read_and_run_query(triplestore_server_t* server, triplestore_t* t, FILE* in, FILE* out) {
	int length	= 0;
	if (server->use_http) {
		if (read_http_header(server, in, &length)) {
			return 1;
		}
	} else {
		length	= server->buffer_size-1;
	}
	
	size_t total		= 0;
	assert(length < server->buffer_size);
	
	const int needed	= length;
    char* buffer		= calloc(1, server->buffer_size);
	while (total < needed) {
		size_t bytes	= fread(&(buffer[total]), 1, needed-total, in);
// 		fprintf(stderr, "- %zu\n", bytes);
		if (bytes == 0) {
			break;
		}
		total			+= bytes;
	}
	
// 	fprintf(stderr, "read %zu query bytes\n", total);
	if ((total == 0) || buffer[total] != '\0') {
		free(buffer);
		return 1;
	}
	
	int r	= triplestore_run_query(server, t, buffer, out);
	free(buffer);
	return r;
}
