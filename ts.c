#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#include <pcre.h>
#include "linenoise.h"
#include "triplestore.h"

struct runtime_ctx_s {
	int verbose;
	int print;
	int error;
	int64_t limit;
	double start;
};

#pragma mark -

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
	for (nodeid_t s = 1; s < t->nodes_used; s++) {
		nodeid_t idx	= t->graph[s].out_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->edges[idx].p;
			nodeid_t o	= t->edges[idx].o;
			triplestore_print_triple(t, s, p, o, f);
			idx			= t->edges[idx].next_out;
		}
	}
	return 0;
}

int triplestore_node_dump(triplestore_t* t, int64_t limit, FILE* f) {
	fprintf(f, "# %"PRIu32" nodes\n", t->nodes_used);
	for (nodeid_t s = 1; s < t->nodes_used; s++) {
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
	for (nodeid_t s = 1; s < t->nodes_used; s++) {
		nodeid_t idx	= t->graph[s].out_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->edges[idx].p;
			nodeid_t o	= t->edges[idx].o;
			fprintf(f, "E %07"PRIu32" %07"PRIu32" %07"PRIu32"\n", s, p, o);
			idx			= t->edges[idx].next_out;
			count++;
			if (limit > 0 && count == limit) break;
		}
	}
	return 0;
}

int triplestore_dump(triplestore_t* t, FILE* f) {
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

#pragma mark -

void help(FILE* f) {
	fprintf(f, "Commands:\n");
	fprintf(f, "  help\n");
	fprintf(f, "  (un)set print\n");
	fprintf(f, "  (un)set verbose\n");
	fprintf(f, "  (un)set limit LIMIT\n");
	fprintf(f, "  match PATTERN\n");
	fprintf(f, "  ntriples\n");
	fprintf(f, "  dump\n");
	fprintf(f, "  nodes\n");
	fprintf(f, "  edges\n");
	fprintf(f, "  bgp S1 P1 O1 S2 P2 O2 ...\n");
	fprintf(f, "  triple S P O\n");
	fprintf(f, "\n");
}

query_t* construct_bgp_query(triplestore_t* t, struct runtime_ctx_s* ctx, int argc, char** argv, int i) {
	int triples		= (argc - i) / 3;
	int variables	= 3 * triples;
	int next_var	= 1;
	query_t* query	= triplestore_new_query(t, variables);
	bgp_t* bgp		= triplestore_new_bgp(t, variables, triples);
	int j	= 0;
	int64_t* ids	= calloc(sizeof(int64_t), variables);
	while (i+1 < argc) {
		int index			= j++;
		const char* ts		= argv[++i];
		int64_t id	= 0;
		if (isdigit(ts[0])) {
			id			= atoi(ts);
		} else {
			for (int j = 1; j <= variables; j++) {
				if (query->variable_names[j]) {
					if (!strcmp(query->variable_names[j]+1, ts)) {
						id	= -j;
							fprintf(stderr, "Variable ?%s already assigned ID %"PRId64"\n", ts, id);
						break;
					}
				}
			}
			if (id == 0) {
				id			= -(next_var++);
					fprintf(stderr, "Setting variable ?%s ID %"PRId64"\n", ts, id);
				triplestore_query_set_variable_name(query, -id, ts);
// 				bgp.variable_names[-id]		= calloc(1,2+strlen(ts));
// 				sprintf(bgp.variable_names[-id], "?%s", ts);
			}
		}
		ids[index]	= id;
	}
	query->variables	= next_var - 1;
	
	for (j = 0; j < triples; j++) {
		int64_t s	= ids[3*j + 0];
		int64_t p	= ids[3*j + 1];
		int64_t o	= ids[3*j + 2];
		triplestore_bgp_set_triple_nodes(bgp, j, s, p, o);
	}
	free(ids);

	triplestore_query_add_op(query, QUERY_BGP, bgp);
	
	return query;
}

int triplestore_op(triplestore_t* t, struct runtime_ctx_s* ctx, int argc, char** argv) {
	if (argc == 0) {
		return 1;
	}
	
// 	fprintf(stderr, "Interactive command:\n");
// 	for (int i = 0; i < argc; i++) {
// 		fprintf(stderr, "[%d] %s\n", i, argv[i]);
// 	}
	
	int i	= 0;
	FILE* f	= ctx->print ? stdout : NULL;
	const char* op			= argv[i];
	if (!strcmp(op, "help")) {
		help(f);
	} else if (!strcmp(op, "set")) {
		const char* field	= argv[++i];
		if (!strcmp(field, "print")) {
			ctx->print	= 1;
		} else if (!strcmp(field, "verbose")) {
			ctx->verbose	= 1;
		} else if (!strcmp(field, "limit")) {
			ctx->limit	= atoll(argv[++i]);
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
		triplestore_print_ntriples(t, stdout);
	} else if (!strcmp(op, "dump")) {
		triplestore_dump(t, stdout);
	} else if (!strcmp(op, "nodes")) {
		triplestore_node_dump(t, ctx->limit, stdout);
	} else if (!strcmp(op, "edges")) {
		triplestore_edge_dump(t, ctx->limit, stdout);
	} else if (!strcmp(op, "bgp")) {
		query_t* query	= construct_bgp_query(t, ctx, argc, argv, i);
		if (ctx->verbose) {
			fprintf(stderr, "------------------------\n");		
			fprintf(stderr, "Matching Query:\n");
			triplestore_print_query(t, query, stderr);
			fprintf(stderr, "------------------------\n");		
		}
		
		
		double start	= triplestore_current_time();
		__block int count	= 0;
		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
			count++;
			if (f != NULL) {
				for (int j = 1; j <= query->variables; j++) {
					nodeid_t id	= final_match[j];
					fprintf(f, "%s=", query->variable_names[j]);
					triplestore_print_term(t, id, f, 0);
					fprintf(f, " ");
				}
				fprintf(f, "\n");
			}
			return (ctx->limit > 0 && count == ctx->limit);
		});
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
	} else if (!strcmp(op, "test")) {
		query_t* query	= construct_bgp_query(t, ctx, argc, argv, i);
		
		int64_t var	= -2;
		query_filter_t* filter	= triplestore_new_filter(FILTER_ISIRI, var);
		triplestore_query_add_op(query, QUERY_FILTER, filter);
		
		if (ctx->verbose) {
			fprintf(stderr, "------------------------\n");		
			fprintf(stderr, "Matching Query:\n");
			triplestore_print_query(t, query, stderr);
			fprintf(stderr, "------------------------\n");		
		}
		
		
		double start	= triplestore_current_time();
		__block int count	= 0;
		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
			count++;
			if (f != NULL) {
				for (int j = 1; j <= query->variables; j++) {
					nodeid_t id	= final_match[j];
					fprintf(f, "%s=", query->variable_names[j]);
					triplestore_print_term(t, id, f, 0);
					fprintf(f, " ");
				}
				fprintf(f, "\n");
			}
			return (ctx->limit > 0 && count == ctx->limit);
		});
		if (ctx->verbose) {
			double elapsed	= triplestore_elapsed_time(start);
			fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" results\n", elapsed, count);
		}
		triplestore_free_query(query);
	} else {
		fprintf(stderr, "Unrecognized operation '%s'\n", op);
		return 1;
	}
	return 0;
}

void usage(int argc, char** argv, FILE* f) {
	fprintf(f, "Usage: %s [-v] [-p] input.nt OP\n\n", argv[0]);
}

int main (int argc, char** argv) {
	if (argc > 1 && !strcmp(argv[1], "--help")) {
		usage(argc, argv, stdout);
		help(stdout);
		return 0;
	}
	
	if (argc == 1) {
		usage(argc, argv, stderr);
		return 1;
	}

	char* linenoiseHistoryFile	= ".triplestore-history";
	linenoiseHistoryLoad(linenoiseHistoryFile);

	
	int interactive		= 0;
	int max_edges		= 268435456; // 2^28
	int max_nodes		= 268435456; // 2^28
	triplestore_t* t	= new_triplestore(max_nodes, max_edges);

	__block struct runtime_ctx_s ctx	= {
		.limit				= -1,
		.verbose			= 0,
		.error				= 0,
		.print				= 1,
		.start				= triplestore_current_time(),
	};

	__block int i	= 1;
	while (i < argc && argv[i][0] == '-') {
		const char* flag	= argv[i++];
		if (!strncmp(flag, "-v", 2)) {
			ctx.verbose++;
		} else if (!strncmp(flag, "-p", 2)) {
			ctx.print++;
		} else if (!strncmp(flag, "-i", 2)) {
			interactive	= 1;
			ctx.print = 1;
		}
	}

	const char* filename	= argv[i++];
	if (ctx.verbose) {
		fprintf(stderr, "Importing %s\n", filename);
	}
	
	triplestore__load_file(t, filename, ctx.print, ctx.verbose);
	if (ctx.error) {
		return 1;
	}

	triplestore_op(t, &ctx, argc-i, &(argv[i]));
	
	if (interactive) {
		char* line;
		while ((line = linenoise("sparql> ")) != NULL) {
			char* argv[16];
			int len	= strlen(line);
			char* buffer	= malloc(1+len);
			strcpy(buffer, line);
			int argc	= 1;
			argv[0]		= buffer;
			for (int i = 0; i < len; i++) {
				if (buffer[i] == ' ') {
					buffer[i]	= '\0';
					argv[argc++]	= &(buffer[i+1]);
				}
			}
			if (!triplestore_op(t, &ctx, argc, argv)) {
				linenoiseHistoryAdd(line);
			}
			free(buffer);
		}
		linenoiseHistorySave(linenoiseHistoryFile);
	}
	
	free_triplestore(t);
	return 0;
}

