#include "commands.h"

void help(FILE* f) {
	fprintf(f, "Commands:\n");
	fprintf(f, "  help\n");
	fprintf(f, "  (un)set print\n");
	fprintf(f, "  (un)set verbose\n");
	fprintf(f, "  (un)set limit LIMIT\n");
	fprintf(f, "  match PATTERN\n");
	fprintf(f, "  ntriples\n");
	fprintf(f, "  data\n");
	fprintf(f, "  nodes\n");
	fprintf(f, "  edges\n");
	fprintf(f, "  bgp S1 P1 O1 S2 P2 O2 ...\n");
	fprintf(f, "  triple S P O\n");
	fprintf(f, "  filter starts|ends|contains VAR STRING S1 P1 O1 S2 P2 O2 ...\n");
	fprintf(f, "  filter re VAR PATTERN FLAGS S1 P1 O1 S2 P2 O2 ...\n");
	fprintf(f, "  agg GROUPVAR COUNT VAR S1 P1 O1 S2 P2 O2 ...\n");
	fprintf(f, "\n");
}

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

int triplestore_print_ntriples(triplestore_t* t, FILE* f, int64_t limit) {
	uint32_t count	= 0;
	for (nodeid_t s = 1; s <= t->nodes_used; s++) {
		nodeid_t idx	= t->graph[s].out_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->edges[idx].p;
			nodeid_t o	= t->edges[idx].o;
			triplestore_print_triple(t, s, p, o, f);
			idx			= t->edges[idx].next_out;
			count++;
			if (limit > 0 && count == limit) goto ntriples_break;
		}
	}
ntriples_break:
	return 0;
}

int triplestore_node_dump(triplestore_t* t, int64_t limit, FILE* f) {
	fprintf(f, "# %"PRIu32" nodes\n", t->nodes_used);
	for (nodeid_t s = 1; s <= t->nodes_used; s++) {
		char* ss		= triplestore_term_to_string(t, t->graph[s]._term);
		fprintf(f, "N %07"PRIu32" %s (%"PRIu32", %"PRIu32")\n", s, ss, t->graph[s].in_degree, t->graph[s].out_degree);
		free(ss);
		if (limit > 0 && s == limit) break;
	}
	return 0;
}

int triplestore_edge_dump(triplestore_t* t, int64_t limit, FILE* f) {
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
			if (limit > 0 && count == limit) goto edge_break;
		}
	}
edge_break:
	return 0;
}

int triplestore_print_data(triplestore_t* t, FILE* f) {
	triplestore_node_dump(t, -1, f);
	triplestore_edge_dump(t, -1, f);
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

int triplestore_match_terms(triplestore_t* t, const char* pattern, int64_t limit, int(^block)(nodeid_t id)) {
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
		
		if (limit > 0 && count == limit) {
			break;
		}
		
		if (r) {
			break;
		}
	}
	pcre_free(re);     /* Release memory used for the compiled pattern */
	return 0;
}

int _triplestore_run_query(triplestore_t* t, query_t* query, struct command_ctx_s* ctx, FILE* f) {
	if (ctx->verbose) {
		fprintf(stderr, "Matching Query:\n");
		triplestore_print_query(t, query, stderr);
	}

	ctx->preamble_block(query);
	
	double start	= triplestore_current_time();
	__block int count	= 0;
	triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
		count++;
		ctx->result_block(query, final_match);
		return 0;
	});
	if (ctx->verbose) {
		double elapsed	= triplestore_elapsed_time(start);
		fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" results\n", elapsed, count);
	}
	return 0;
}

query_t* construct_bgp_query(triplestore_t* t, struct command_ctx_s* ctx, int argc, char** argv, int i) {
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
			fprintf(stderr, "No node ID found for BGP term %s\n", ts);
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

// query_t* construct_bgp_query(triplestore_t* t, struct command_ctx_s* ctx, int argc, char** argv, int i) {
// 	int triples		= (argc - i) / 3;
// 	int variables	= 3 * triples;
// // 	int next_var	= 1;
// 	query_t* query	= triplestore_new_query(t, 0);
// 	bgp_t* bgp		= triplestore_new_bgp(t, variables, triples);
// 	int j			= 0;
// 	int64_t* ids	= calloc(sizeof(int64_t), variables);
// 	while (i+1 < argc) {
// 		int index			= j++;
// 		const char* ts		= argv[++i];
// 		int64_t id			= query_node_id(t, query, ts);
// 		ids[index]	= id;
// 	}
// 	
// 	for (j = 0; j < triples; j++) {
// 		int64_t s	= ids[3*j + 0];
// 		int64_t p	= ids[3*j + 1];
// 		int64_t o	= ids[3*j + 2];
// 		triplestore_bgp_set_triple_nodes(bgp, j, s, p, o);
// 	}
// 	free(ids);
// 
// 	triplestore_query_add_op(query, QUERY_BGP, bgp);
// 	
// 	return query;
// }

#pragma mark -

int64_t query_node_id(triplestore_t* t, struct command_ctx_s* ctx, query_t* query, const char* ts) {
	int64_t id	= 0;
	if (isdigit(ts[0])) {
		id			= atoi(ts);
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
		id = triplestore_get_termid(t, term);
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
			int len	= q - p + 1;
			char* dt	= malloc(1 + len);
			snprintf(dt, len, "%s", p);
			rdf_term_t* dtterm = triplestore_new_term(t, TERM_IRI, dt, NULL, 0);
			int64_t dtid = triplestore_get_termid(t, dtterm);
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
		
		id = triplestore_get_termid(t, term);
// 		fprintf(stderr, "%"PRId64" Literal: (%d) \"%s\"\n", id, len, value);
		free(value);
	} else {
		id	= triplestore_query_get_variable_id(query, ts);
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
		ctx->set_error(-1, "Unrecognized term string");
		fprintf(stderr, "Unrecognized term string %s\n", ts);
	}
	return id;
}

int64_t triplestore_query_get_variable_id(query_t* query, const char* var) {
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

int triplestore_op(triplestore_t* t, struct command_ctx_s* ctx, int argc, char** argv) {
	if (argc == 0) {
		ctx->set_error(-1, "No arguments given");
		return 1;
	}
	
	int i	= 0;
	FILE* f	= ctx->print ? stdout : NULL;
	const char* op			= argv[i];
	if (!strcmp(op, "") || op[0] == '#') {
	} else if (!strcmp(op, "help")) {
		help(f);
	} else if (!strcmp(op, "set")) {
		const char* field	= argv[++i];
		if (!strcmp(field, "print")) {
			ctx->print	= 1;
		} else if (!strcmp(field, "verbose")) {
			ctx->verbose	= 1;
		} else if (!strcmp(field, "limit")) {
			ctx->limit	= atoll(argv[++i]);
		} else if (!strcmp(field, "language")) {
			ctx->language	= calloc(1, 1+strlen(argv[i+1]));
			strcpy(ctx->language, argv[++i]);
		}
	} else if (!strcmp(op, "unset")) {
		const char* field	= argv[++i];
		if (!strcmp(field, "print")) {
			ctx->print	= 0;
		} else if (!strcmp(field, "verbose")) {
			ctx->verbose	= 0;
		} else if (!strcmp(field, "limit")) {
			ctx->limit	= -1;
		}
	} else if (!strcmp(op, "size")) {
		uint32_t count	= triplestore_size(t);
		fprintf(f, "%"PRIu32" triples\n", count);
	} else if (!strcmp(op, "begin")) {
		ctx->constructing	= 1;
		ctx->query			= NULL;
		if (argc > i+1) {
			ctx->query	= construct_bgp_query(t, ctx, argc, argv, i);
			if (!ctx->query) {
				ctx->set_error(-1, "Failed to build query object in BEGIN");
			}
		}
	} else if (!strcmp(op, "end")) {
		query_t* query	= ctx->query;
		if (!query) {
			ctx->set_error(-1, "No query object present in END");
		}
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
			fprintf(stderr, "*** Cannot add a BGP to an existing query\n");
			ctx->constructing	= 0;
			ctx->query			= NULL;
			ctx->set_error(-1, "Cannot add a BGP to an existing query");
			return 1;
		}
		
		query_t* query	= construct_bgp_query(t, ctx, argc, argv, i);
		if (!query) {
			ctx->set_error(-1, "No query object present in BGP");
			return 1;
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
			ctx->set_error(-1, "UNIQUE can only be used during query construction");
			return 1;
		}
		query_t* query	= ctx->query;
		if (!query) {
			ctx->set_error(-1, "No query object present in UNIQUE");
			return 1;
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
			ctx->set_error(-1, "SORT can only be used during query construction");
			return 1;
		}
		query_t* query	= ctx->query;
		if (!query) {
			ctx->set_error(-1, "No query object present in SORT");
			return 1;
		}
		int svars		= argc-i-1;
// 		fprintf(stderr, "%d sort variables\n", svars);
		sort_t* sort	= triplestore_new_sort(t, query->variables, svars, 0);
		for (int j = 0; j < svars; j++) {
			const char* var	= argv[j+i+1];
			int64_t v	= triplestore_query_get_variable_id(query, var);
			if (v == 0) {
				ctx->set_error(-1, "No such term or variable in SORT");
				return 1;
			}
// 			fprintf(stderr, "setting sort variable #%d to ?%s (%"PRId64")\n", j, var, v);
			triplestore_set_sort(sort, j, v);
		}
		triplestore_query_add_op(ctx->query, QUERY_SORT, sort);
	} else if (!strcmp(op, "project")) {
		if (ctx->constructing == 0) {
			ctx->set_error(-1, "project can only be used during query construction\n");
			return 1;
		}
		query_t* query	= ctx->query;
		if (!query) {
			ctx->set_error(-1, "No query object present in PROJECT");
			return 1;
		}
		project_t* project	= triplestore_new_project(t, query->variables);
		for (int j = i+1; j < argc; j++) {
			const char* var	= argv[j];
			int64_t v	= triplestore_query_get_variable_id(query, var);
			if (v == 0) {
				ctx->set_error(-1, "No such term or variable in PROJECT");
				return 1;
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
			ctx->set_error(-1, "No query object present in FILTER");
			return 1;
		}

		int64_t var		= triplestore_query_get_variable_id(query, vs);
		if (var == 0) {
			ctx->set_error(-1, "No such term or variable in FILTER");
			return 1;
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
				ctx->set_error(-1, "Unrecognized FILTER operation");
				return 1;
			}
		} else {
			const char* pat	= argv[++i];
			int64_t patid		= query_node_id(t, ctx, query, pat);
// 			rdf_term_t* term	= triplestore_new_term(t, patid); // TODO: XXX
			if (!strncmp(op, "re", 2)) {
				filter	= triplestore_new_filter(FILTER_REGEX, var, pat, "i");
			} else {
				filter_type_t ftype;
				if (!strcmp(op, "starts")) {
					ftype	= FILTER_STRSTARTS;
				} else if (!strcmp(op, "ends")) {
					ftype	= FILTER_STRENDS;
				} else if (!strcmp(op, "contains")) {
					ftype	= FILTER_CONTAINS;
				} else {
					ctx->set_error(-1, "Unrecognized FILTER operation");
					return 1;
				}
				
				// TODO: allow parsing languages and datatypes from the command string
				if (ctx->language) {
					filter	= triplestore_new_filter(ftype, var, pat, TERM_LANG_LITERAL, ctx->language);
				} else {
					filter	= triplestore_new_filter(ftype, var, pat, TERM_XSDSTRING_LITERAL);
				}
			}
		}
		
		if (ctx->constructing) {
			triplestore_query_add_op(ctx->query, QUERY_FILTER, filter);
			return 0;
		}
		
		triplestore_query_add_op(query, QUERY_FILTER, filter);
		_triplestore_run_query(t, query, ctx, f);
		triplestore_free_query(query);
	} else if (!strcmp(op, "match")) {
		const char* pattern	= argv[++i];
		triplestore_match_terms(t, pattern, ctx->limit, ^(nodeid_t id) {
			if (f != NULL) {
				char* string		= triplestore_term_to_string(t, t->graph[id]._term);
				fprintf(f, "%-7"PRIu32" %s\n", id, string);
				free(string);
			}
			return 0;
		});
	} else if (!strcmp(op, "ntriples")) {
		triplestore_print_ntriples(t, stdout, ctx->limit);
	} else if (!strcmp(op, "load")) {
		const char* filename	= argv[++i];
		double start	= triplestore_current_time();
		triplestore_load(t, filename, ctx->verbose);
		uint32_t count	= triplestore_size(t);
		if (ctx->verbose) {
			double elapsed	= triplestore_elapsed_time(start);
			fprintf(stderr, "loaded %"PRIu32" triples in %lfs (%5.1f triples/second)\n", count, elapsed, ((double)count/elapsed));
		}
	} else if (!strcmp(op, "dump")) {
		const char* filename	= argv[++i];
		double start	= triplestore_current_time();
		triplestore_dump(t, filename);
		uint32_t count	= triplestore_size(t);
		if (ctx->verbose) {
			double elapsed	= triplestore_elapsed_time(start);
			fprintf(stderr, "dumped %"PRIu32" triples in %lfs (%5.1f triples/second)\n", count, elapsed, ((double)count/elapsed));
		}
	} else if (!strcmp(op, "import")) {
		const char* filename	= argv[++i];
		if (triplestore__load_file(t, filename, ctx->verbose)) {
			fprintf(stderr, "Failed to import file %s\n", filename);
		}
	} else if (!strcmp(op, "debug")) {
		fprintf(stdout, "Triplestore:\n");
		fprintf(stdout, "- Nodes: %"PRIu32"\n", t->nodes_used);
		for (uint32_t i = 1; i <= t->nodes_used; i++) {
			char* s	= triplestore_term_to_string(t, t->graph[i]._term);
			fprintf(stdout, "       %4d: %s (out head: %"PRIu32"; in head: %"PRIu32")\n", i, s, t->graph[i].out_edge_head, t->graph[i].in_edge_head);
			free(s);
			nodeid_t idx	= t->graph[i].out_edge_head;
			while (idx != 0) {
				nodeid_t s	= t->edges[idx].p;
				nodeid_t p	= t->edges[idx].p;
				nodeid_t o	= t->edges[idx].o;
				fprintf(stdout, "       -> %"PRIu32" %"PRIu32" %"PRIu32"\n", s, p, o);
				idx			= t->edges[idx].next_out;
			}
		}
		fprintf(stdout, "- Edges: %"PRIu32"\n", t->edges_used);
	} else if (!strcmp(op, "data")) {
		triplestore_print_data(t, stdout);
	} else if (!strcmp(op, "nodes")) {
		triplestore_node_dump(t, ctx->limit, stdout);
	} else if (!strcmp(op, "edges")) {
		triplestore_edge_dump(t, ctx->limit, stdout);
	} else if (!strcmp(op, "test")) {
		query_t* query	= construct_bgp_query(t, ctx, argc, argv, i);
		if (ctx->verbose) {
			fprintf(stderr, "Matching Query:\n");
			triplestore_print_query(t, query, stderr);
		}
		
		table_t* table	= triplestore_new_table(query->variables);
		double start	= triplestore_current_time();
		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
			triplestore_table_add_row(table, final_match);
			return 0;
		});

		int count	= table->used;
// 		triplestore_table_sort(t, table);
		if (f != NULL) {
			for (int row = 0; row < count; row++) {
				uint32_t* result	= triplestore_table_row_ptr(table, row);
				for (int j = 1; j <= query->variables; j++) {
					nodeid_t id	= result[j];
					fprintf(f, "%s=", query->variable_names[j]);
					triplestore_print_term(t, id, f, 0);
					fprintf(f, " ");
				}
				fprintf(f, "\n");
				if (ctx->limit > 0 && count == ctx->limit) {
					break;
				}
			}
		}
		triplestore_free_table(table);
		if (ctx->verbose) {
			double elapsed	= triplestore_elapsed_time(start);
			fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" results\n", elapsed, count);
		}
		triplestore_free_query(query);
	} else if (!strcmp(op, "triple")) {
		int64_t s	= atoi(argv[++i]);
		int64_t p	= atoi(argv[++i]);
		int64_t o	= atoi(argv[++i]);
		double start	= triplestore_current_time();
		nodeid_t count	= triplestore_print_match(t, s, p, o, f);
		if (ctx->verbose) {
			double elapsed	= triplestore_elapsed_time(start);
			fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" triples\n", elapsed, count);
		}
	} else if (!strcmp(op, "count")) {
		query_t* query		= ctx->query;
		if (!query) {
			ctx->set_error(-1, "No query object present in END");
			return 1;
		}
		if (ctx->verbose) {
			fprintf(stderr, "Counting Query:\n");
			triplestore_print_query(t, query, stderr);
		}
		double start	= triplestore_current_time();
		__block int count	= 0;
		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
			count++;
			return 0;
		});
		if (ctx->verbose) {
			double elapsed	= triplestore_elapsed_time(start);
			fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" results\n", elapsed, count);
		}
		return 0;
	} else if (!strcmp(op, "agg")) {
		const char* gs		= argv[++i];
		const char* op		= argv[++i];
		const char* vs		= argv[++i];
		query_t* query;
		if (ctx->constructing) {
			query		= ctx->query;
		} else {
			query		= construct_bgp_query(t, ctx, argc, argv, i);
		}
		int64_t groupvar	= triplestore_query_get_variable_id(query, gs);
		if (groupvar == 0) {
			return 1;
		}
// 		int64_t var			= strcmp(vs, "*") ? triplestore_query_get_variable_id(query, vs) : 0;
// 		int aggid			= triplestore_query_add_variable(query, ".agg");
		if (ctx->verbose) {
			fprintf(stderr, "Matching Aggregate Query: (GROUP BY %s) %s %s\n", gs, op, vs);
			triplestore_print_query(t, query, stderr);
		}
		
		double start		= triplestore_current_time();
		
		if (strcmp(op, "count")) {
			fprintf(stderr, "Unrecognized aggregate operation. Assuming count.\n");
		}
		uint32_t* counts	= calloc(sizeof(uint32_t), 1+t->nodes_used);
		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
			nodeid_t group	= 0;
			if (groupvar != 0) {
				group	= final_match[-groupvar];
// 				fprintf(stderr, "aggregating in group %"PRIu32" ", group);
// 				triplestore_print_term(t, group, stderr, 1);
			}
			counts[group]++;
			return 0;
		});
		
		__block int count	= 0;
		for (uint32_t j = 0; j <= t->nodes_used; j++) {
			count++;
			if (counts[j] > 0) {
				if (f != NULL) {
					fprintf(f, "%"PRIu32"", counts[j]);
					if (j == 0) {
						fprintf(f, "\n");
					} else {
						fprintf(f, " => ");
						triplestore_print_term(t, j, f, 1);
					}
				}
				if (ctx->limit > 0 && count >= ctx->limit) {
					break;
				}
			}
		}
		free(counts);
		if (ctx->verbose) {
			double elapsed	= triplestore_elapsed_time(start);
			fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" results\n", elapsed, count);
		}
		triplestore_free_query(query);
		if (ctx->constructing) {
			ctx->constructing	= 0;
			ctx->query			= NULL;
		}
	} else {
		fprintf(stderr, "Unrecognized operation '%s'\n", op);
		return 1;
	}
	return 0;
}

int triplestore_vop(triplestore_t* t, struct command_ctx_s* ctx, int argc, ...) {
	va_list ap;
	va_start(ap, argc);
	char* argv[argc];
	for (int i = 0; i < argc; i++) {
		argv[i]	= va_arg(ap, char*);
	}
	return triplestore_op(t, ctx, argc, argv);
}
