#include "triplestore-server.h"
#include <time.h>

#pragma mark -

int server_ctx_set_error(struct server_runtime_ctx_s* ctx, char* message) {
	ctx->error++;
	if (!ctx->error_message) {
		ctx->error_message	= message;
	}
	fprintf(stderr, "Error: %s\n", message);
	return 1;
}

static int triplestore_output_op(triplestore_t* t, struct server_runtime_ctx_s* ctx, int argc, char** argv) {
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

static int _triplestore_run_query(triplestore_t* t, query_t* query, struct server_runtime_ctx_s* ctx, FILE* f) {
	__block int count	= 0;
	if (!query) {
		return 1;
	}
	triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
		count++;
		ctx->result_block(query, final_match);
		return 0;
	});
	return 0;
}

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

static int64_t query_node_id(triplestore_t* t, query_t* query, const char* ts) {
	int64_t id	= 0;
	if (isdigit(ts[0])) {
		return 0;
	} else if (ts[0] == '<') {
		char* p	= strstr(ts, ">");
		if (!p) {
			return 0;
		}
		long len	= p - ts;
		char* value	= malloc(1 + len);
		snprintf(value, len, "%s", ts+1);
// 		fprintf(stderr, "IRI: (%d) <%s>\n", len, value);
		rdf_term_t* term = triplestore_new_term(t, TERM_IRI, value, NULL, 0);
		id = triplestore_get_term(t, term);
		free(value);
	} else if (ts[0] == '"') {
		char* p	= strstr(ts+1, "\"");
		if (!p) {
			fprintf(stderr, "cannot parse literal value\n");
			return 0;
		}
		long len	= p - ts;
		char* value	= malloc(1 + len);
		snprintf(value, len, "%s", ts+1);
		rdf_term_t* term	= NULL;
		if (p[1] == '^') {
			p += 4;
			char* q	= strstr(p, ">");
			if (!q) {
				fprintf(stderr, "cannot parse datatype IRI\n");
                free(value);
				return 0;
			}
			long len	= q - p + 1;
			char* dt	= malloc(1 + len);
			snprintf(dt, len, "%s", p);
			rdf_term_t* dtterm = triplestore_new_term(t, TERM_IRI, dt, NULL, 0);
			int64_t dtid = triplestore_get_term(t, dtterm);
			term = triplestore_new_term(t, TERM_TYPED_LITERAL, value, NULL, (nodeid_t) dtid);
			free(dt);
		} else if (p[1] == '@') {
			p += 2;
			term = triplestore_new_term(t, TERM_LANG_LITERAL, value, p, 0);
// 			char* s	= triplestore_term_to_string(t, term);
// 			fprintf(stderr, "Term: %s\n", s);
// 			free(s);
		} else {
			term = triplestore_new_term(t, TERM_XSDSTRING_LITERAL, value, NULL, 0);
		}
		
		id = triplestore_get_term(t, term);
// 		fprintf(stderr, "%"PRId64" Literal: (%d) \"%s\"\n", id, len, value);
		free(value);
	} else {
		id	= _triplestore_query_get_variable_id(query, ts);
		if (id == 0) {
// 			id			= -(next_var++);
// 				fprintf(stderr, "Setting variable ?%s ID %"PRId64"\n", ts, id);
			char* p		= (char*) ts;
			if (p[0] == '?') {
				p++;
			}
			id	= triplestore_query_add_variable(query, p);
// 			triplestore_ensure_variable_capacity(query, -id);
// 			triplestore_query_set_variable_name(query, -id, ts);
// 				bgp.variable_names[-id]		= calloc(1,2+strlen(ts));
// 				sprintf(bgp.variable_names[-id], "?%s", ts);
		}
	}
	
	if (id == 0) {
		fprintf(stderr, "Unrecognized term string %s\n", ts);
	}
	return id;
}

static query_t* construct_bgp_query(triplestore_t* t, struct server_runtime_ctx_s* ctx, int argc, char** argv, int i) {
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
		int64_t id			= query_node_id(t, query, ts);
		if (!id) {
			fprintf(stderr, "No node ID found for BGP term %s\n", ts);
			free(ids);
			return NULL;
		}
		ids[index]	= id;
	}
	
	int* seen				= calloc(3*triples, sizeof(int));
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
				server_ctx_set_error(ctx, "BGP with cartesian products are not allowed");
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

int triplestore_match_terms(triplestore_t* t, const char* pattern, int(^block)(nodeid_t id)) {
	const char *error;
	int erroffset;
	pcre* re = pcre_compile(
		pattern,		/* the pattern */
		0,				/* default options */
		&error,			/* for error message */
		&erroffset,		/* for error offset */
		NULL			/* use default character tables */
	);
	if (re == NULL) {
		printf("PCRE compilation failed at offset %d: %s\n", erroffset, error);
		exit(1);
	}

	int64_t count	= 0;
	for (nodeid_t s = 1; s < t->nodes_used; s++) {
		char* string		= triplestore_term_to_string(t, t->graph[s]._term);
// 			fprintf(stderr, "matching %s =~ %s\n", string, pattern);
		int OVECCOUNT	= 30;
		int ovector[OVECCOUNT];
		int rc = pcre_exec(
			re,							/* the compiled pattern */
			NULL,						/* no extra data - we didn't study the pattern */
			string,						/* the subject string */
			strlen(string),				/* the length of the subject */
			0,							/* start at offset 0 in the subject */
			0,							/* default options */
			ovector,					/* output vector for substring information */
			OVECCOUNT					/* number of elements in the output vector */
		);
		if (rc < 0) {
			switch(rc) {
				case PCRE_ERROR_NOMATCH: break;
				default: printf("Matching error %d\n", rc); break;
			}
			free(string);
			continue;
		}
		if (rc == 0) {
			rc = OVECCOUNT/3;
			printf("ovector only has room for %d captured substrings\n", rc - 1);
			free(string);
			continue;
		}
		
		count++;
		int r	= block(s);
		free(string);
		
		if (r) {
			break;
		}
	}
	pcre_free(re);     /* Release memory used for the compiled pattern */
	return 0;
}

#pragma mark -

int triplestore_print_triple(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o, FILE* f) {
	rdf_term_t* subject		= t->graph[s]._term;
	rdf_term_t* predicate	= t->graph[p]._term;
	rdf_term_t* object		= t->graph[o]._term;

	if (subject == NULL) assert(0);
	if (predicate == NULL) assert(0);
	if (object == NULL) assert(0);

	char* ss		= triplestore_term_to_string(t, subject);
	char* sp		= triplestore_term_to_string(t, predicate);
	char* so		= triplestore_term_to_string(t, object);
	fprintf(f, "%s %s %s .\n", ss, sp, so);
	free(ss);
	free(sp);
	free(so);
	return 0;
}

int triplestore_print_ntriples(triplestore_t* t, FILE* f) {
	uint32_t count	= 0;
	for (nodeid_t s = 1; s <= t->nodes_used; s++) {
		nodeid_t idx	= t->graph[s].out_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->edges[idx].p;
			nodeid_t o	= t->edges[idx].o;
			triplestore_print_triple(t, s, p, o, f);
			idx			= t->edges[idx].next_out;
			count++;
		}
	}
	return 0;
}

int triplestore_node_dump(triplestore_t* t, FILE* f) {
	fprintf(f, "# %"PRIu32" nodes\n", t->nodes_used);
	for (nodeid_t s = 1; s <= t->nodes_used; s++) {
		char* ss		= triplestore_term_to_string(t, t->graph[s]._term);
		fprintf(f, "N %07"PRIu32" %s (%"PRIu32", %"PRIu32")\n", s, ss, t->graph[s].in_degree, t->graph[s].out_degree);
		free(ss);
	}
	return 0;
}

int triplestore_edge_dump(triplestore_t* t, FILE* f) {
	fprintf(f, "# %"PRIu32" edges\n", t->edges_used);
	int64_t count	= 0;
	for (nodeid_t s = 1; s <= t->nodes_used; s++) {
		nodeid_t idx	= t->graph[s].out_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->edges[idx].p;
			nodeid_t o	= t->edges[idx].o;
			fprintf(f, "E %07"PRIu32" %07"PRIu32" %07"PRIu32"\n", s, p, o);
			idx			= t->edges[idx].next_out;
			count++;
		}
	}
	return 0;
}

int triplestore_print_data(triplestore_t* t, FILE* f) {
	triplestore_node_dump(t, f);
	triplestore_edge_dump(t, f);
	return 0;
}

nodeid_t triplestore_print_match(triplestore_t* t, int64_t s, int64_t p, int64_t o, FILE* f) {
	__block nodeid_t count	= 0;
	triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o) {
		count++;
		if (f != NULL) {
			triplestore_print_triple(t, s, p, o, f);
		}
		return 0;
	});
	return count;
}

#pragma mark -

triplestore_server_t* triplestore_new_server(short port, int use_http) {
	triplestore_server_t* server	= calloc(1, sizeof(triplestore_server_t));
	server->use_http	= use_http;
	server->port		= port;
	server->queue		= dispatch_queue_create("us.kasei.triplestore.workers", DISPATCH_QUEUE_CONCURRENT);
	return server;
}

int triplestore_free_server(triplestore_server_t* s) {
	dispatch_barrier_sync(s->queue, ^{});
	dispatch_release(s->queue);
	close(s->fd);
	free(s);
	return 0;
}

static int write_http_header(FILE* out, int code, const char* message) {
	time_t now		= time(0);
	struct tm tm	= *gmtime(&now);
	char* buf		= alloca(256);
	strftime(buf, 256, "%a, %d %b %Y %H:%M:%S %Z", &tm);
	
	fprintf(out, "HTTP/1.1 %03d %s\r\n"
				"Content-Type: text/plain\r\n"
				"Date: %s\r\n"
				"Server: MemoryTripleStore\r\n"
				"\r\n", code, message, buf);
	return 0;
}

static int write_http_error_header(struct server_runtime_ctx_s* ctx, FILE* out, int code, const char* message) {
	if (write_http_header(out, code, message)) {
		return 1;
	}
	if (ctx->error_message) {
		fprintf(out, "%s\r\n", ctx->error_message);
	} else {
		fprintf(out, "%s\r\n", message);
	}
	return 0;
}

static int read_http_header(FILE* in) {
	int state	= 0;
	int cur;
// 	fprintf(stderr, "Reading HTTP header:\n");
// 	fprintf(stderr, "-------------------------\n");
	while ((cur = fgetc(in)) != EOF) {
// 		fprintf(stderr, "%c", cur);
		char expect	= (state % 2) ? '\n' : '\r';
		if (cur == expect) {
			state++;
			if (state == 4) {
				return 0;
			}
		} else {
			state	= 0;
		}
	}
// 	fprintf(stderr, "\n--- EOF FOUND -------------------------\n");
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
	uint64_t count	= 0;
	while (1) {
		count++;
		// if (count > 1000) { break; } // XXXXXXXXXXXXXX
		addrlen	= sizeof(peer);
		sd	= accept(s->fd, (struct sockaddr*) &peer, &addrlen);
		if (sd < 0) {
			perror("Wrong connection");
			exit(1);
		}

// 		fprintf(stderr, "Accept connection\n");
		dispatch_async(s->queue, ^{
			FILE* f	= fdopen(sd, "r+");
			if (f == NULL) {
				perror("");
				return;
			}

			if (s->use_http) {
				read_http_header(f);
			}
			triplestore_run_query(s, t, f, f);
			fclose(f);
		});
	}
	return 0;
}

static int triplestore_op(triplestore_t* t, struct server_runtime_ctx_s* ctx, int argc, char** argv) {
	if (argc == 0) {
		return server_ctx_set_error(ctx, "No arguments given");
	}
	
	int i	= 0;
	FILE* f	= stdout;
	const char* op			= argv[i];
	if (!strcmp(op, "") || op[0] == '#') {
// 	} else if (!strcmp(op, "size")) {
// 		uint32_t count	= triplestore_size(t);
// 		fprintf(f, "%"PRIu32" triples\n", count);
	} else if (!strcmp(op, "begin")) {
		ctx->constructing	= 1;
		ctx->query			= NULL;
		if (argc > i+1) {
			ctx->query	= construct_bgp_query(t, ctx, argc, argv, i);
			if (!ctx->query) {
				return server_ctx_set_error(ctx, "Failed to build query object in BEGIN");
			}
		}
	} else if (!strcmp(op, "end")) {
		query_t* query	= ctx->query;
		if (!query) {
			return server_ctx_set_error(ctx, "No query object present in END");
		}
		// triplestore_print_query(t, query, stderr);
		ctx->query	= NULL;
		ctx->constructing	= 0;
		if (query) {
			_triplestore_run_query(t, query, ctx, f);
			triplestore_free_query(query);
		} else {
			fprintf(stderr, "No query available\n");
		}
	} else if (!strcmp(op, "bgp")) {
		if (ctx->constructing && ctx->query) {
			ctx->constructing	= 0;
			ctx->query			= NULL;
			return server_ctx_set_error(ctx, "Cannot add a BGP to an existing query");
		}
		
		query_t* query	= construct_bgp_query(t, ctx, argc, argv, i);
		if (!query) {
			return server_ctx_set_error(ctx, "No query object present in BGP");
		}
		
		if (ctx->constructing) {
			ctx->query	= query;
			return 0;
		}
		
		_triplestore_run_query(t, query, ctx, f);
		triplestore_free_query(query);
	} else if (!strcmp(op, "path")) {
		int64_t var	= -1;
		const char* ss	= argv[++i];
// 		int64_t s	= atoi(argv[++i]);
		int64_t p	= atoi(argv[++i]);
		const char* os	= argv[++i];
// 		int64_t o	= atoi(argv[++i]);

		query_t* query;
		if (ctx->constructing && ctx->query) {
			query	= ctx->query;
		} else {
			query	= triplestore_new_query(t, 0);
		}
		
		int64_t s, o;
		if (isdigit(ss[0])) {
			s	= atoi(ss);
		} else {
			s	= var--;
			triplestore_ensure_variable_capacity(query, -s);
			triplestore_query_set_variable_name(query, -s, ss);
		}

		if (isdigit(os[0])) {
			o	= atoi(os);
		} else {
			o	= var--;
			triplestore_query_set_variable_name(query, -o, os);
		}

		path_t* path	= triplestore_new_path(t, PATH_PLUS, s, (nodeid_t) p, o);
		triplestore_query_add_op(query, QUERY_PATH, path);
		if (ctx->constructing) {
			ctx->query	= query;
			return 0;
		}

		_triplestore_run_query(t, query, ctx, f);
		triplestore_free_query(query);
	} else if (!strcmp(op, "unique")) {
		if (ctx->constructing == 0) {
			return server_ctx_set_error(ctx, "UNIQUE can only be used during query construction");
		}
		query_t* query	= ctx->query;
		if (!query) {
			return server_ctx_set_error(ctx, "No query object present in UNIQUE");
		}
		sort_t* sort	= triplestore_new_sort(t, query->variables, query->variables, 1);
		for (int j = 1; j <= query->variables; j++) {
			int64_t v	= -j;
// 			const char* var	= query->variable_names[j];
// 			fprintf(stderr, "setting sort variable #%d to ?%s (%"PRId64")\n", j-1, var, v);
			triplestore_set_sort(sort, j-1, v);
		}
		triplestore_query_add_op(ctx->query, QUERY_SORT, sort);
	} else if (!strcmp(op, "sort")) {
		if (ctx->constructing == 0) {
			return server_ctx_set_error(ctx, "SORT can only be used during query construction");
		}
		query_t* query	= ctx->query;
		if (!query) {
			return server_ctx_set_error(ctx, "No query object present in SORT");
		}
		int svars		= argc-i-1;
// 		fprintf(stderr, "%d sort variables\n", svars);
		sort_t* sort	= triplestore_new_sort(t, query->variables, svars, 0);
		for (int j = 0; j < svars; j++) {
			const char* var	= argv[j+i+1];
			int64_t v	= _triplestore_query_get_variable_id(query, var);
			if (v == 0) {
				return server_ctx_set_error(ctx, "No such term or variable in SORT");
			}
// 			fprintf(stderr, "setting sort variable #%d to ?%s (%"PRId64")\n", j, var, v);
			triplestore_set_sort(sort, j, v);
		}
		triplestore_query_add_op(ctx->query, QUERY_SORT, sort);
	} else if (!strcmp(op, "project")) {
		if (ctx->constructing == 0) {
			fprintf(stderr, "project can only be used during query construction\n");
			return 1;
		}
		query_t* query	= ctx->query;
		if (!query) {
			return server_ctx_set_error(ctx, "No query object present in PROJECT");
		}
		project_t* project	= triplestore_new_project(t, query->variables);
		for (int j = i+1; j < argc; j++) {
			const char* var	= argv[j];
			int64_t v	= _triplestore_query_get_variable_id(query, var);
			if (v == 0) {
				return server_ctx_set_error(ctx, "No such term or variable in PROJECT");
			}
			triplestore_set_projection(project, v);
// 			fprintf(stderr, "setting project variable %s (%"PRId64")\n", var, v);
		}
		triplestore_query_add_op(ctx->query, QUERY_PROJECT, project);
	} else if (!strcmp(op, "filter")) {
		const char* op	= argv[++i];
		const char* vs	= argv[++i];
		query_t* query;
		if (ctx->constructing) {
			query	= ctx->query;
		} else {
			query	= construct_bgp_query(t, ctx, argc, argv, i);
		}
		
		if (!query) {
			return server_ctx_set_error(ctx, "No query object present in FILTER");
		}
		
		int64_t var		= _triplestore_query_get_variable_id(query, vs);
		if (var == 0) {
			return server_ctx_set_error(ctx, "No such term or variable in FILTER");
		}
		
		query_filter_t* filter;
		if (!strncmp(op, "is", 2)) {
			if (!strcmp(op, "isiri")) {
				filter	= triplestore_new_filter(FILTER_ISIRI, var);
			} else if (!strcmp(op, "isliteral")) {
				filter	= triplestore_new_filter(FILTER_ISLITERAL, var);
			} else if (!strcmp(op, "isblank")) {
				filter	= triplestore_new_filter(FILTER_ISBLANK, var);
			} else if (!strcmp(op, "isnumeric")) {
				filter	= triplestore_new_filter(FILTER_ISNUMERIC, var);
			} else {
				return server_ctx_set_error(ctx, "Unrecognized FILTER operation");
			}
		} else {
			const char* pat	= argv[++i];
			if (!strcmp(op, "starts")) {
				filter	= triplestore_new_filter(FILTER_STRSTARTS, var, pat, TERM_XSDSTRING_LITERAL);
			} else if (!strcmp(op, "ends")) {
				filter	= triplestore_new_filter(FILTER_STRENDS, var, pat, TERM_XSDSTRING_LITERAL);
			} else if (!strcmp(op, "contains")) {
				filter	= triplestore_new_filter(FILTER_CONTAINS, var, pat, TERM_XSDSTRING_LITERAL);
			} else if (!strncmp(op, "re", 2)) {
				filter	= triplestore_new_filter(FILTER_REGEX, var, pat, "i");
			} else {
				return server_ctx_set_error(ctx, "Unrecognized FILTER operation");
			}
		}
		
		if (ctx->constructing) {
			triplestore_query_add_op(ctx->query, QUERY_FILTER, filter);
			return 0;
		}
		
		triplestore_query_add_op(query, QUERY_FILTER, filter);
		_triplestore_run_query(t, query, ctx, f);
		triplestore_free_query(query);
// 	} else if (!strcmp(op, "match")) {
// 		const char* pattern	= argv[++i];
// 		triplestore_match_terms(t, pattern, ^(nodeid_t id) {
// 			if (f != NULL) {
// 				char* string		= triplestore_term_to_string(t, t->graph[id]._term);
// 				fprintf(f, "%-7"PRIu32" %s\n", id, string);
// 				free(string);
// 			}
// 			return 0;
// 		});
// 	} else if (!strcmp(op, "ntriples")) {
// 		triplestore_print_ntriples(t, stdout);
// 	} else if (!strcmp(op, "load")) {
// 		const char* filename	= argv[++i];
// 		triplestore_load(t, filename, 0);
// 	} else if (!strcmp(op, "dump")) {
// 		const char* filename	= argv[++i];
// 		triplestore_dump(t, filename);
// 	} else if (!strcmp(op, "import")) {
// 		const char* filename	= argv[++i];
// 		if (triplestore__load_file(t, filename, 0)) {
// 			fprintf(stderr, "Failed to import file %s\n", filename);
// 		}
// 	} else if (!strcmp(op, "debug")) {
// 		fprintf(stdout, "Triplestore:\n");
// 		fprintf(stdout, "- Nodes: %"PRIu32"\n", t->nodes_used);
// 		for (uint32_t i = 1; i <= t->nodes_used; i++) {
// 			char* s	= triplestore_term_to_string(t, t->graph[i]._term);
// 			fprintf(stdout, "       %4d: %s (out head: %"PRIu32"; in head: %"PRIu32")\n", i, s, t->graph[i].out_edge_head, t->graph[i].in_edge_head);
// 			free(s);
// 			nodeid_t idx	= t->graph[i].out_edge_head;
// 			while (idx != 0) {
// 				nodeid_t s	= t->edges[idx].p;
// 				nodeid_t p	= t->edges[idx].p;
// 				nodeid_t o	= t->edges[idx].o;
// 				fprintf(stdout, "       -> %"PRIu32" %"PRIu32" %"PRIu32"\n", s, p, o);
// 				idx			= t->edges[idx].next_out;
// 			}
// 		}
// 		fprintf(stdout, "- Edges: %"PRIu32"\n", t->edges_used);
// 	} else if (!strcmp(op, "data")) {
// 		triplestore_print_data(t, stdout);
// 	} else if (!strcmp(op, "nodes")) {
// 		triplestore_node_dump(t, stdout);
// 	} else if (!strcmp(op, "edges")) {
// 		triplestore_edge_dump(t, stdout);
// 	} else if (!strcmp(op, "triple")) {
// 		int64_t s	= atoi(argv[++i]);
// 		int64_t p	= atoi(argv[++i]);
// 		int64_t o	= atoi(argv[++i]);
// 		triplestore_print_match(t, s, p, o, f);
	} else if (!strcmp(op, "count")) {
		query_t* query		= ctx->query;
		if (!query) {
			return server_ctx_set_error(ctx, "No query object present in COUNT");
		}
		
		__block int count	= 0;
		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
			count++;
			return 0;
		});
		return 0;
// 	} else if (!strcmp(op, "agg")) {
// 		const char* gs		= argv[++i];
// 		const char* op		= argv[++i];
// //		const char* vs		= argv[++i];
// 		query_t* query;
// 		if (ctx->constructing) {
// 			query		= ctx->query;
// 		} else {
// 			query		= construct_bgp_query(t, ctx, argc, argv, i);
// 		}
// 		
// 		if (!query) {
// 			return server_ctx_set_error(ctx, "No query object present in AGG");
// 		}
// 		
// 		int64_t groupvar	= _triplestore_query_get_variable_id(query, gs);
// 		if (groupvar == 0) {
// 			return server_ctx_set_error(ctx, "No such term or variable in AGG");
// 		}
// // 		int64_t var			= strcmp(vs, "*") ? _triplestore_query_get_variable_id(query, vs) : 0;
// // 		int aggid			= triplestore_query_add_variable(query, ".agg");
// 		
// 		if (strcmp(op, "count")) {
// 			fprintf(stderr, "Unrecognized aggregate operation. Assuming count.\n");
// 		}
// 		uint32_t* counts	= calloc(sizeof(uint32_t), 1+t->nodes_used);
// 		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
// 			nodeid_t group	= 0;
// 			if (groupvar != 0) {
// 				group	= final_match[-groupvar];
// // 				fprintf(stderr, "aggregating in group %"PRIu32" ", group);
// // 				triplestore_print_term(t, group, stderr, 1);
// 			}
// 			counts[group]++;
// 			return 0;
// 		});
// 		
// 		// TODO: unify this with the result handler
// 		__block int count	= 0;
// 		for (uint32_t j = 0; j <= t->nodes_used; j++) {
// 			count++;
// 			if (counts[j] > 0) {
// 				if (f != NULL) {
// 					fprintf(f, "%"PRIu32"", counts[j]);
// 					if (j == 0) {
// 						fprintf(f, "\n");
// 					} else {
// 						fprintf(f, " => ");
// 						triplestore_print_term(t, j, f, 1);
// 					}
// 				}
// 			}
// 		}
// 		free(counts);
// 		triplestore_free_query(query);
// 		if (ctx->constructing) {
// 			ctx->constructing	= 0;
// 			ctx->query			= NULL;
// 		}
	} else {
		fprintf(stderr, "Unrecognized operation '%s'\n", op);
		return server_ctx_set_error(ctx, "Unrecognized operation");
	}
	return 0;
}

int triplestore_run_query(triplestore_server_t* s, triplestore_t* t, FILE* in, FILE* out) {
	__block struct server_runtime_ctx_s ctx	= {
		.error				= 0,
		.error_message		= NULL,
		.start				= triplestore_current_time(),
		.constructing		= 0,
		.query				= NULL,
		.result_block		= ^(query_t* query, nodeid_t* final_match){
			if (out != NULL) {
				for (int j = 1; j <= query->variables; j++) {
					nodeid_t id	= final_match[j];
					if (id > 0) {
						fprintf(out, "%s=", query->variable_names[j]);
						triplestore_print_term(t, id, out, 0);
						fprintf(out, " ");
					}
				}
				fprintf(out, "\n");
			}
		},
	};
	
	size_t allocated	= 1024;
	char* line	= malloc(allocated);
	ssize_t read;
	int output	= 0;
	while ((read = getline(&line, &allocated, in)) > 0) {
		int endpos	= read-1;
		while (endpos >= 0 && (line[endpos] == '\n' || line[endpos] == '\r')) {
			line[endpos]	= '\0';
			endpos--;
		}

		int argc_max	= 16;
		char* argv[argc_max];
		int len	= strlen(line);
		char* buffer	= malloc(1+len);
		strcpy(buffer, line);
		int argc	= 1;
		argv[0]		= buffer;
		for (int i = 0; i < len; i++) {
			if (argc >= argc_max) {
				free(buffer);
				free(line);
				return 1;
			}
			if (buffer[i] == ' ') {
				int j	= i;
				while (j < len && buffer[j] == ' ') {
					buffer[j++] = '\0';
				}
				argv[argc++]	= &(buffer[j]);
			}
		}
		
		if (triplestore_output_op(t, &ctx, argc, argv)) {
			if (s->use_http) {
				write_http_header(out, 200, "OK");
			}
			output++;
		}
		int r	= triplestore_op(t, &ctx, argc, argv);
		free(buffer);

		if (r) {
			free(line);
			write_http_error_header(&ctx, out, 400, "Bad Request");
			return 1;
		}
		
		if (ctx.constructing == 0) {
			break;
		}
	}
	free(line);

	if (!output) {
		write_http_error_header(&ctx, out, 400, "Bad Request");
		return 1;
	}
	return 0;
}
