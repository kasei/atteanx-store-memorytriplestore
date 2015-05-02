#include <fcntl.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include "linenoise.h"
#include "triplestore.h"

typedef uint32_t nodeid_t;

typedef struct bgp_s {
	int triples;
	int64_t* nodes;
	int* variable_indexes;
} bgp_t;

struct parser_ctx_s {
	int verbose;
	int print;
	int error;
	uint64_t count;
	uint64_t graph;
	double start;
	triplestore_t* store;
	uint64_t timestamp;
};

typedef struct {
	nodeid_t id;
	raptor_term* node;	// TODO: change to native term structure
} hx_nodemap_item;

static double current_time ( void ) {
	struct timeval t;
	gettimeofday (&t, NULL);
	double start	= t.tv_sec + (t.tv_usec / 1000000.0);
	return start;
}

static double elapsed_time ( double start ) {
	struct timeval t;
	gettimeofday (&t, NULL);
	double time	= t.tv_sec + (t.tv_usec / 1000000.0);
	double elapsed	= time - start;
	return elapsed;
}

static int _hx_node_cmp_str ( const void* a, const void* b, void* param ) {
	hx_nodemap_item* ia	= (hx_nodemap_item*) a;
	hx_nodemap_item* ib	= (hx_nodemap_item*) b;
	int c	= raptor_term_compare(ia->node, ib->node);	// TODO: change to native term structure
	return c;
}

static void _hx_free_node_item (void *avl_item, void *avl_param) {
	hx_nodemap_item* i	= (hx_nodemap_item*) avl_item;
	if (i->node != NULL) {
		raptor_free_term( i->node );	// TODO: change to native term structure
	}
	free( i );
}

#pragma mark -

triplestore_t* new_triplestore(int max_nodes, int max_edges) {
	triplestore_t* t	= (triplestore_t*) calloc(sizeof(triplestore_t), 1);
	t->world			= raptor_new_world();	// TODO: this should be removed from the triplestore when the switch to native term structures is complete
	raptor_world_open(t->world);

	t->edges_alloc	= max_edges;
	t->nodes_alloc	= max_nodes;
	t->edges_used	= 0;
	t->nodes_used	= 0;
	t->out_edges	= calloc(sizeof(index_list_element_t), max_edges);
	t->in_edges		= calloc(sizeof(index_list_element_t), max_edges);
	t->graph		= calloc(sizeof(graph_node_t), max_nodes);
	t->dictionary	= avl_create( _hx_node_cmp_str, NULL, &avl_allocator_default );
	return t;
}

int free_triplestore(triplestore_t* t) {
	avl_destroy(t->dictionary, _hx_free_node_item);
	raptor_free_world(t->world);
	free(t->out_edges);
	free(t->in_edges);
	free(t->graph);
	free(t);
	return 0;
}

#pragma mark -

int triplestore_add_triple(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o, uint64_t timestamp) {
	if (t->edges_used >= t->edges_alloc) {
		fprintf(stderr, "*** Exhausted allocated space for edges.\n");
		return 1;
	}
	
	if (s == 0 || p == 0 || o == 0) {
		return 1;
	}
	
	nodeid_t edge				= ++t->edges_used;

	t->out_edges[ edge ].s		= s;
	t->out_edges[ edge ].p		= p;
	t->out_edges[ edge ].o		= o;
	t->out_edges[ edge ].next	= t->graph[s].out_edge_head;
	t->graph[s].out_edge_head	= edge;
	t->graph[s].mtime			= timestamp;
	t->graph[s].out_degree++;
	
	t->in_edges[ edge ].s		= s;
	t->in_edges[ edge ].p		= p;
	t->in_edges[ edge ].o		= o;
	t->in_edges[ edge ].next	= t->graph[o].in_edge_head;
	t->graph[o].in_edge_head	= edge;
	t->graph[o].mtime			= timestamp;
	t->graph[o].in_degree++;
	
	return 0;
}

nodeid_t triplestore_add_term(triplestore_t* t, raptor_term* term) {	// TODO: change to native term structure
	hx_nodemap_item i;
	i.node			= term;
	i.id			= 0;
	hx_nodemap_item* item	= (hx_nodemap_item*) avl_find( t->dictionary, &i );
	if (item == NULL) {
		if (t->nodes_used >= t->nodes_alloc) {
			fprintf(stderr, "*** Exhausted allocated space for nodes.\n");
			return 1;
		}

		item	= (hx_nodemap_item*) calloc( 1, sizeof( hx_nodemap_item ) );
		item->node	= raptor_term_copy(term);
		item->id	= ++t->nodes_used;
		avl_insert( t->dictionary, item );
		
		graph_node_t node	= { .term = item->node, .mtime = 0, .out_edge_head = 0, .in_edge_head = 0 };
		t->graph[item->id]	= node;
// 		fprintf(stdout, "+ %6"PRIu32" %s\n", item->id, raptor_term_to_string(term));
	} else {
// 		fprintf(stdout, "  %6"PRIu32" %s\n", item->id, raptor_term_to_string(term));
	}
	return item->id;
}

static void parser_handle_triple (void* user_data, raptor_statement* triple) {
	struct parser_ctx_s* pctx	= (struct parser_ctx_s*) user_data;
	if (pctx->error) {
		return;
	}
	
	pctx->count++;

	// TODO: change to convert raptor terms to native term structure before adding to triplestore
	nodeid_t s	= triplestore_add_term(pctx->store, triple->subject);
	nodeid_t p	= triplestore_add_term(pctx->store, triple->predicate);
	nodeid_t o	= triplestore_add_term(pctx->store, triple->object);
	if (triplestore_add_triple(pctx->store, s, p, o, pctx->timestamp)) {
		pctx->error++;
		return;
	}
	
	if (pctx->verbose) {
		if (pctx->count % 250 == 0) {
			double elapsed	= elapsed_time(pctx->start);
			fprintf(stderr, "\r%llu triples imported (%5.1f triples/second)", (unsigned long long) pctx->count, ((double)pctx->count/elapsed));
		}
	}
	
	return;
}

static void parse_rdf_from_file ( const char* filename, struct parser_ctx_s* pctx ) {
	unsigned char* uri_string	= raptor_uri_filename_to_uri_string( filename );
	raptor_uri* uri				= raptor_new_uri(pctx->store->world, uri_string);
	raptor_parser* rdf_parser	= raptor_new_parser(pctx->store->world, "guess");
	raptor_uri *base_uri		= raptor_uri_copy(uri);
	
	raptor_parser_set_statement_handler(rdf_parser, pctx, parser_handle_triple);
	
// 	if (1) {
		int fd	= open(filename, O_RDONLY);
		fcntl(fd, F_NOCACHE, 1);
		fcntl(fd, F_RDAHEAD, 1);
		FILE* f	= fdopen(fd, "r");
		raptor_parser_parse_file_stream(rdf_parser, f, filename, base_uri);
		fclose(f);
// 	} else {
// 		raptor_parser_parse_file(rdf_parser, uri, base_uri);
// 	}
	
	if (pctx->error) {
		fprintf( stderr, "\nError encountered during parsing\n" );
	} else if (pctx->verbose) {
		double elapsed	= elapsed_time(pctx->start);
		fprintf( stderr, "\nFinished parsing %"PRIu64" triples in %lfs\n", pctx->count, elapsed );
	}
	
	free(uri_string);
	raptor_free_parser(rdf_parser);
	raptor_free_uri( base_uri );
	raptor_free_uri( uri );
}

#pragma mark -

int triplestore_print_term(triplestore_t* t, nodeid_t s, FILE* f, int newline) {
	raptor_term* subject	= t->graph[s].term;	// TODO: change to native term structure
	if (subject == NULL) assert(0);
	unsigned char* ss		= raptor_term_to_string(subject);
	fprintf(f, "%s", ss);
	if (newline) {
		fprintf(f, "\n");
	}
	free(ss);
	return 0;
}

int triplestore_print_triple(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o, FILE* f) {
	raptor_term* subject	= t->graph[s].term;	// TODO: change to native term structure
	raptor_term* predicate	= t->graph[p].term;
	raptor_term* object		= t->graph[o].term;

	if (subject == NULL) assert(0);
	if (predicate == NULL) assert(0);
	if (object == NULL) assert(0);

	unsigned char* ss		= raptor_term_to_string(subject);
	unsigned char* sp		= raptor_term_to_string(predicate);
	unsigned char* so		= raptor_term_to_string(object);
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

int triplestore_dump(triplestore_t* t, FILE* f) {
	fprintf(f, "# %"PRIu32" nodes\n", t->nodes_used);
	fprintf(f, "# %"PRIu32" edges\n", t->edges_used);
	for (nodeid_t s = 1; s < t->nodes_used; s++) {
		unsigned char* ss		= raptor_term_to_string(t->graph[s].term);	// TODO: change to native term structure
		fprintf(f, "N %07"PRIu32" %s (%"PRIu32", %"PRIu32")\n", s, ss, t->graph[s].in_degree, t->graph[s].out_degree);
		free(ss);
	}

	for (nodeid_t s = 1; s < t->nodes_used; s++) {
		nodeid_t idx	= t->graph[s].out_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->out_edges[idx].p;
			nodeid_t o	= t->out_edges[idx].o;
			fprintf(f, "E %07"PRIu32" %07"PRIu32" %07"PRIu32"\n", s, p, o);
			idx			= t->out_edges[idx].next;
		}
	}
	return 0;
}

// int _triplestore_walk_path(triplestore_t* t, uint32_t start, uint32_t pred, FILE* f, int depth, char* seen) {
// 	if (seen[start]) {
// 		return 1;
// 	}
// 	seen[start]++;
// 	uint32_t idx	= t->graph[start].out_edge_head;
// 	while (idx != 0) {
// 		uint32_t p	= t->out_edges[idx].p;
// 		if (p == pred) {
// 			uint32_t o	= t->out_edges[idx].o;
// 			fprintf(f, "(%d) ", depth);
// 			triplestore_print_term(t, o, f, 1);
// 			_triplestore_walk_path(t, o, pred, f, 1+depth, seen);
// 		}
// 		idx			= t->out_edges[idx].next;
// 	}
// 	return 0;
// }
// 
// // print out all ?end for ?start pred+ ?end
// int triplestore_walk_path(triplestore_t* t, uint32_t start, uint32_t pred, FILE* f) {
// // 	fprintf(stderr, "Allocating %"PRIu32" bytes for path walk...\n", t->nodes_used);
// 	char* seen	= calloc(t->nodes_used, 1);
// 	_triplestore_walk_path(t, start, pred, f, 1, seen);
// 	free(seen);
// 	return 0;
// }

int triplestore_match_triple(triplestore_t* t, int64_t _s, int64_t _p, int64_t _o, void(^block)(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o)) {
	if (_s > 0) {
		nodeid_t idx	= t->graph[_s].out_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->out_edges[idx].p;
			nodeid_t o	= t->out_edges[idx].o;
			if (_p <= 0 || _p == p) {
				if (_o <= 0 || _o == o) {
					block(t, _s, p, o);
				}
			}
			idx			= t->out_edges[idx].next;
		}
	} else if (_o > 0) {
		nodeid_t idx	= t->graph[_o].in_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->in_edges[idx].p;
			nodeid_t s	= t->in_edges[idx].s;
			if (_p <= 0 || _p == p) {
				if (_s <= 0 || _s == s) {
					block(t, s, p, _o);
				}
			}
			idx			= t->in_edges[idx].next;
		}
	} else {
		for (nodeid_t s = 1; s < t->nodes_used; s++) {
			if (_s <= 0 || _s == s) {
				nodeid_t idx	= t->graph[s].out_edge_head;
				while (idx != 0) {
					nodeid_t p	= t->out_edges[idx].p;
					nodeid_t o	= t->out_edges[idx].o;
					if (_p <= 0 || _p == p) {
						if (_o <= 0 || _o == o) {
							block(t, s, p, o);
						}
					}
					idx			= t->out_edges[idx].next;
				}
			}
		}
		return 0;
	}
	return 0;
}

nodeid_t triplestore_print_match(triplestore_t* t, int64_t s, int64_t p, int64_t o, FILE* f) {
	__block nodeid_t count	= 0;
	triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o) {
		count++;
		if (f != NULL) {
			triplestore_print_triple(t, s, p, o, f);
		}
	});
	return count;
}

int _triplestore_bgp_match(triplestore_t* t, bgp_t* bgp, int current_triple, nodeid_t* current_match, void(^block)(nodeid_t* final_match)) {
	if (current_triple == bgp->triples) {
		block(current_match);
		return 0;
	}
	
	int offset	= 3*current_triple;
	int64_t s	= bgp->nodes[offset];
	int64_t p	= bgp->nodes[offset+1];
	int64_t o	= bgp->nodes[offset+2];
	if (s < 0) {
		int i	= bgp->variable_indexes[-s];
		if (i < offset) {
			s	= current_match[i];
		}
	}
	if (p < 0) {
		int i	= bgp->variable_indexes[-p];
		if (i < offset) {
			p	= current_match[i];
		}
	}
	if (o < 0) {
		int i	= bgp->variable_indexes[-o];
		if (i < offset) {
			o	= current_match[i];
		}
	}
	
// 	fprintf(stderr, "BGP matching triple %d: %"PRId64" %"PRId64" %"PRId64"\n", current_triple, s, p, o);
	triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o) {
		current_match[offset]	= s;
		current_match[offset+1]	= p;
		current_match[offset+2]	= o;
		_triplestore_bgp_match(t, bgp, current_triple+1, current_match, block);
	});
	
	return 0;
}

nodeid_t triplestore_print_bgp_match(triplestore_t* t, bgp_t* bgp, FILE* f) {
	__block nodeid_t count	= 0;
	nodeid_t* current_match	= calloc(sizeof(nodeid_t), 3*bgp->triples);
	_triplestore_bgp_match(t, bgp, 0, current_match, ^(nodeid_t* final_match){
		count++;
		if (f != NULL) {
			for (int j = 0; j < 3*bgp->triples; j++) {
				if (bgp->variable_indexes[j] >= 0) {
					triplestore_print_term(t, final_match[bgp->variable_indexes[j]], f, 0);
					fprintf(f, " ");
				}
			}
			fprintf(f, "\n");
		}
	});
	free(current_match);
	return count;
}

#pragma mark -

int triplestore_op(triplestore_t* t, struct parser_ctx_s* pctx, int argc, char** argv) {
	if (argc == 0) {
		return 1;
	}
	
// 	fprintf(stderr, "Interactive command:\n");
// 	for (int i = 0; i < argc; i++) {
// 		fprintf(stderr, "[%d] %s\n", i, argv[i]);
// 	}
	
	int i	= 0;
	FILE* f	= pctx->print ? stdout : NULL;
	const char* op			= argv[i];
	if (!strcmp(op, "set")) {
		const char* field	= argv[++i];
		if (!strcmp(field, "print")) {
			pctx->print	= 1;
		} else if (!strcmp(field, "verbose")) {
			pctx->verbose	= 1;
		}
	} else if (!strcmp(op, "unset")) {
		const char* field	= argv[++i];
		if (!strcmp(field, "print")) {
			pctx->print	= 0;
		} else if (!strcmp(field, "verbose")) {
			pctx->verbose	= 0;
		}
	} else if (!strcmp(op, "ntriples")) {
		triplestore_print_ntriples(t, stdout);
	} else if (!strcmp(op, "dump")) {
		triplestore_dump(t, stdout);
	} else if (!strcmp(op, "bgp")) {
		bgp_t bgp;
		bgp.triples	= (argc - i) / 3;
		bgp.nodes	= calloc(sizeof(int64_t), 3 * bgp.triples);
		bgp.variable_indexes	= calloc(sizeof(int), 3 * bgp.triples);
		for (int j = 0; j < 3*bgp.triples; j++) {
			bgp.variable_indexes[j]	= -1;
		}
		int j	= 0;
		while (i+1 < argc) {
			int index			= j++;
			int64_t id			= atoi(argv[++i]);
			bgp.nodes[index]	= id;
			if (id < 0) {
				if (bgp.variable_indexes[-id] < 0) {
					bgp.variable_indexes[-id]	= index;
				}
			}
		}
// 			fprintf(stderr, "BGP with %d triples\n", bgp.triples);
// 			for (int j = 0; j < bgp.triples; j++) {
// 				fprintf(stderr, "- %"PRId64" %"PRId64" %"PRId64"\n", bgp.nodes[j*3], bgp.nodes[j*3+1], bgp.nodes[j*3+2]);
// 			}
// 			for (int j = 0; j < 3*bgp.triples; j++) {
// 				if (bgp.variable_indexes[j] >= 0) {
// 					fprintf(stderr, "BGP variable %d appears first at node %d\n", -j, bgp.variable_indexes[j]);
// 				}
// 			}
		double start	= current_time();
		nodeid_t count	= triplestore_print_bgp_match(t, &bgp, f);
		if (pctx->verbose) {
			double elapsed	= elapsed_time(start);
			fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" results\n", elapsed, count);
		}
	} else if (!strcmp(op, "match")) {
		int64_t s	= atoi(argv[++i]);
		int64_t p	= atoi(argv[++i]);
		int64_t o	= atoi(argv[++i]);
		double start	= current_time();
		nodeid_t count	= triplestore_print_match(t, s, p, o, f);
		if (pctx->verbose) {
			double elapsed	= elapsed_time(start);
			fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" triples\n", elapsed, count);
		}
// 		} else if (!strcmp(op, "path")) {
// 			uint32_t start	= atoi(argv[++i]);
// 			uint32_t p		= atoi(argv[++i]);
// 			triplestore_walk_path(t, start, p, f);
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

	__block struct parser_ctx_s pctx	= {
		.verbose			= 0,
		.error				= 0,
		.graph				= 0LL,
		.count				= 0,
		.print				= 0,
		.start				= current_time(),
		.store				= t,
		.timestamp			= (uint64_t) time(NULL),
	};

	__block int i	= 1;
	while (i < argc && argv[i][0] == '-') {
		const char* flag	= argv[i++];
		if (!strncmp(flag, "-v", 2)) {
			pctx.verbose++;
		} else if (!strncmp(flag, "-p", 2)) {
			pctx.print++;
		} else if (!strncmp(flag, "-i", 2)) {
			interactive	= 1;
			pctx.print = 1;
		}
	}

	const char* filename	= argv[i++];
	if (pctx.verbose) {
		fprintf(stderr, "Importing %s\n", filename);
	}
	parse_rdf_from_file(filename, &pctx);
	if (pctx.error) {
		return 1;
	}

	
	triplestore_op(t, &pctx, argc-i, &(argv[i]));
	
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
			if (!triplestore_op(t, &pctx, argc, argv)) {
				linenoiseHistoryAdd(line);
			}
			free(buffer);
		}
	}
	
	linenoiseHistorySave(linenoiseHistoryFile);
	free_triplestore(t);
	return 0;
}

