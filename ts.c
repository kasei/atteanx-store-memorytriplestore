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
		char* string		= triplestore_term_to_string(t->graph[s]._term);
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

int triplestore_print_term(triplestore_t* t, nodeid_t s, FILE* f, int newline) {
	rdf_term_t* subject		= t->graph[s]._term;
	if (subject == NULL) assert(0);
	char* ss		= triplestore_term_to_string(subject);
	fprintf(f, "%s", ss);
	if (newline) {
		fprintf(f, "\n");
	}
	free(ss);
	return 0;
}

int triplestore_print_triple(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o, FILE* f) {
	rdf_term_t* subject		= t->graph[s]._term;
	rdf_term_t* predicate	= t->graph[p]._term;
	rdf_term_t* object		= t->graph[o]._term;

	if (subject == NULL) assert(0);
	if (predicate == NULL) assert(0);
	if (object == NULL) assert(0);

	char* ss		= triplestore_term_to_string(subject);
	char* sp		= triplestore_term_to_string(predicate);
	char* so		= triplestore_term_to_string(object);
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
			nodeid_t p	= t->out_edges[idx].p;
			nodeid_t o	= t->out_edges[idx].o;
			triplestore_print_triple(t, s, p, o, f);
			idx			= t->out_edges[idx].next;
		}
	}
	return 0;
}

int triplestore_node_dump(triplestore_t* t, int64_t limit, FILE* f) {
	fprintf(f, "# %"PRIu32" nodes\n", t->nodes_used);
	for (nodeid_t s = 1; s < t->nodes_used; s++) {
		char* ss		= triplestore_term_to_string(t->graph[s]._term);
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
			nodeid_t p	= t->out_edges[idx].p;
			nodeid_t o	= t->out_edges[idx].o;
			fprintf(f, "E %07"PRIu32" %07"PRIu32" %07"PRIu32"\n", s, p, o);
			idx			= t->out_edges[idx].next;
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

nodeid_t triplestore_print_bgp_match(triplestore_t* t, bgp_t* bgp, int64_t limit, FILE* f) {
	__block nodeid_t count	= 0;
	triplestore_bgp_match(t, bgp, limit, ^(nodeid_t* final_match){
		count++;
		if (f != NULL) {
			for (int j = 1; j <= bgp->variables; j++) {
// 				if (bgp->variable_indexes[j] >= 0) {
					nodeid_t id	= final_match[j];
					fprintf(f, "%s=", bgp->variable_names[j]);
// 					fprintf(f, "%"PRIu32"", id);
					triplestore_print_term(t, id, f, 0);
					fprintf(f, " ");
// 				}
			}
			fprintf(f, "\n");
		}
		return (limit > 0 && count == limit);
	});
	return count;
}

#pragma mark -

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
	if (!strcmp(op, "set")) {
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
				char* string		= triplestore_term_to_string(t->graph[id]._term);
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
		bgp_t bgp;
		bgp.variables	= 0;
		bgp.triples				= (argc - i) / 3;
		bgp.nodes				= calloc(sizeof(int64_t), 3 * bgp.triples);
		bgp.variable_names		= calloc(sizeof(char*), 3*bgp.triples+1);
// 		bgp.variable_indexes	= calloc(sizeof(int), 3*bgp.triples+1);
// 		for (int j = 0; j <= 3*bgp.triples; j++) {
// 			bgp.variable_indexes[j]	= -1;
// 		}
		int j	= 0;
		while (i+1 < argc) {
			int index			= j++;
			const char* ts		= argv[++i];
			int64_t id	= 0;
			if (isdigit(ts[0])) {
				id			= atoi(ts);
			} else {
				for (int j = 1; j <= bgp.variables; j++) {
					if (bgp.variable_names[j]) {
						if (!strcmp(bgp.variable_names[j]+1, ts)) {
							id	= -j;
							fprintf(stderr, "Variable ?%s already assigned ID %"PRId64"\n", ts, id);
							break;
						}
					}
				}
				if (id == 0) {
					id			= -(++bgp.variables);
					fprintf(stderr, "Setting variable ?%s ID %"PRId64"\n", ts, id);
// 					bgp.variable_indexes[-id]	= bgp.variables;
					bgp.variable_names[-id]		= calloc(1,2+strlen(ts));
					sprintf(bgp.variable_names[-id], "?%s", ts);
				}
			}
			bgp.nodes[index]	= id;
		}
		double start	= triplestore_current_time();
		nodeid_t count	= triplestore_print_bgp_match(t, &bgp, ctx->limit, f);
		if (ctx->verbose) {
			double elapsed	= triplestore_elapsed_time(start);
			fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" results\n", elapsed, count);
		}
		
		free(bgp.nodes);
		free(bgp.variable_names);
// 		free(bgp.variable_indexes);
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
	} else {
		fprintf(stderr, "Unrecognized operation '%s'\n", op);
		return 1;
	}
	return 0;
}

int main (int argc, char** argv) {
	if (argc < 3) {
		fprintf(stderr, "Usage: %s input.nt OP\n\n", argv[0]);
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
	
	triplestore_load_file(t, filename, ctx.print, ctx.verbose);
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

