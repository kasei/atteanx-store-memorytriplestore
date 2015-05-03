#include <fcntl.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <pcre.h>
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
	int64_t limit;
	uint64_t count;
	uint64_t graph;
	double start;
	triplestore_t* store;
	uint64_t timestamp;
};

typedef struct {
	nodeid_t id;
	rdf_term_t* _term;
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

#pragma mark -

rdf_term_t* new_term( rdf_term_type_t type, char* value, char* vtype ) {
	rdf_term_t* t	= calloc(sizeof(rdf_term_t), 1);
	t->type			= type;
	t->value		= calloc(1, 1+strlen(value));
	strcpy(t->value, value);
	
	if (vtype) {
		t->value_type	= calloc(1, 1+strlen(vtype));
		strcpy(t->value_type, vtype);
	}
	
	return t;
}

void free_rdf_term(rdf_term_t* t) {
	free(t->value);
	if (t->value_type) {
		free(t->value_type);
	}
	free(t);
}

int term_compare(rdf_term_t* a, rdf_term_t* b) {
	if (a == NULL) return -1;
	if (b == NULL) return 1;
	if (a->type < b->type) {
		return -1;
	} else if (a->type > b->type) {
		return 1;
	} else {
		if (a->type == TERM_LANG_LITERAL || a->type == TERM_TYPED_LITERAL) {
			int r	= strcmp(a->value_type, b->value_type);
			if (r) {
				return r;
			}
		}
		
		return strcmp(a->value, b->value);
	}
	return 0;
}

char* term_to_string(rdf_term_t* t) {
	char* string	= NULL;
	switch (t->type) {
		case TERM_IRI:
			string	= calloc(3+strlen(t->value), 1);
			sprintf(string, "<%s>", t->value);
			break;
		case TERM_BLANK:
			string	= calloc(3+strlen(t->value), 1);
			sprintf(string, "_:%s", t->value);
			break;
		case TERM_XSDSTRING_LITERAL:
			string	= calloc(3+strlen(t->value), 1);
			sprintf(string, "\"%s\"", t->value);	// TODO: handle escaping
			break;
		case TERM_LANG_LITERAL:
			string	= calloc(4+strlen(t->value)+strlen(t->value_type), 1);
			sprintf(string, "\"%s\"@%s", t->value, t->value_type);
			break;
		case TERM_TYPED_LITERAL:
			string	= calloc(7+strlen(t->value)+strlen(t->value_type), 1);
			sprintf(string, "\"%s\"^^<%s>", t->value, t->value_type);
			break;
	}
	return string;
}

#pragma mark -

static int _hx_node_cmp_str ( const void* a, const void* b, void* param ) {
	hx_nodemap_item* ia	= (hx_nodemap_item*) a;
	hx_nodemap_item* ib	= (hx_nodemap_item*) b;
	int c				= term_compare(ia->_term, ib->_term);
	return c;
}

static void _hx_free_node_item (void *avl_item, void *avl_param) {
	hx_nodemap_item* i	= (hx_nodemap_item*) avl_item;
	if (i->_term != NULL) {
		if (i->_term) {
			free_rdf_term(i->_term);
		}
	}
	free( i );
}

#pragma mark -

triplestore_t* new_triplestore(int max_nodes, int max_edges) {
	triplestore_t* t	= (triplestore_t*) calloc(sizeof(triplestore_t), 1);
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

static rdf_term_t* term_from_raptor_term(raptor_term* t) {
	// TODO: datatype IRIs should be referenced by term ID, not an IRI string (lots of wasted space)
	char* value				= NULL;
	char* vtype				= NULL;
	switch (t->type) {
		case RAPTOR_TERM_TYPE_URI:
			value	= (char*) raptor_uri_as_string(t->value.uri);
			return new_term(TERM_IRI, value, NULL);
		case RAPTOR_TERM_TYPE_BLANK:
			value	= (char*) t->value.blank.string;
			return new_term(TERM_BLANK, value, NULL);
		case RAPTOR_TERM_TYPE_LITERAL:
			value	= (char*) t->value.literal.string;
			if (t->value.literal.language) {
				vtype	= (char*) t->value.literal.language;
				return new_term(TERM_LANG_LITERAL, value, vtype);
			} else if (t->value.literal.datatype) {
				vtype	= (char*) raptor_uri_as_string(t->value.literal.datatype);
				return new_term(TERM_TYPED_LITERAL, value, vtype);
			} else {
				return new_term(TERM_XSDSTRING_LITERAL, value, NULL);
			}
		default:
			fprintf(stderr, "*** unknown node type %d during import\n", t->type);
			return NULL;
	}
}

nodeid_t triplestore_add_term(triplestore_t* t, rdf_term_t* myterm) {
	hx_nodemap_item i;
	i._term			= myterm;
	i.id			= 0;
	hx_nodemap_item* item	= (hx_nodemap_item*) avl_find( t->dictionary, &i );
	if (item == NULL) {
		if (t->nodes_used >= t->nodes_alloc) {
			fprintf(stderr, "*** Exhausted allocated space for nodes.\n");
			return 1;
		}

		item	= (hx_nodemap_item*) calloc( 1, sizeof( hx_nodemap_item ) );
		item->_term	= myterm;
		item->id	= ++t->nodes_used;
		avl_insert( t->dictionary, item );
		
		graph_node_t node	= { ._term = item->_term, .mtime = 0, .out_edge_head = 0, .in_edge_head = 0 };
		t->graph[item->id]	= node;
// 		fprintf(stdout, "+ %6"PRIu32" %s\n", item->id, term_to_string(term));
	} else {
		free_rdf_term(myterm);
// 		fprintf(stdout, "  %6"PRIu32" %s\n", item->id, term_to_string(term));
	}
	return item->id;
}

static void parser_handle_triple (void* user_data, raptor_statement* triple) {
	struct parser_ctx_s* pctx	= (struct parser_ctx_s*) user_data;
	if (pctx->error) {
		return;
	}
	
	pctx->count++;

	nodeid_t s	= triplestore_add_term(pctx->store, term_from_raptor_term(triple->subject));
	nodeid_t p	= triplestore_add_term(pctx->store, term_from_raptor_term(triple->predicate));
	nodeid_t o	= triplestore_add_term(pctx->store, term_from_raptor_term(triple->object));
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
	raptor_world* world	= raptor_new_world();
	raptor_world_open(world);

	unsigned char* uri_string	= raptor_uri_filename_to_uri_string( filename );
	raptor_uri* uri				= raptor_new_uri(world, uri_string);
	raptor_parser* rdf_parser	= raptor_new_parser(world, "guess");
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

	raptor_free_world(world);
}

#pragma mark -

int triplestore_match_triple(triplestore_t* t, int64_t _s, int64_t _p, int64_t _o, int(^block)(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o)) {
	if (_s > 0) {
		nodeid_t idx	= t->graph[_s].out_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->out_edges[idx].p;
			nodeid_t o	= t->out_edges[idx].o;
			if (_p <= 0 || _p == p) {
				if (_o <= 0 || _o == o) {
					if (block(t, _s, p, o)) {
						return 1;
					}
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
					if (block(t, s, p, _o)) {
						return 1;
					}
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
							if (block(t, s, p, o)) {
								return 1;
							}
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

int _triplestore_bgp_match(triplestore_t* t, bgp_t* bgp, int current_triple, nodeid_t* current_match, int(^block)(nodeid_t* final_match)) {
	// TODO: Fix cardinality issues here. BGP results should always be unique w.r.t. variables (negative numbers in bgp[])
	if (current_triple == bgp->triples) {
		return block(current_match);
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
	return triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o) {
		current_match[offset]	= s;
		current_match[offset+1]	= p;
		current_match[offset+2]	= o;
		return _triplestore_bgp_match(t, bgp, current_triple+1, current_match, block);
	});
}

int triplestore_bgp_match(triplestore_t* t, bgp_t* bgp, int64_t limit, int(^block)(nodeid_t* final_match)) {
	nodeid_t* current_match	= calloc(sizeof(nodeid_t), 3*bgp->triples);
	int r	= _triplestore_bgp_match(t, bgp, 0, current_match, block);
	free(current_match);
	return r;
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
		char* string		= term_to_string(t->graph[s]._term);
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
	char* ss		= term_to_string(subject);
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

	char* ss		= term_to_string(subject);
	char* sp		= term_to_string(predicate);
	char* so		= term_to_string(object);
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
		char* ss		= term_to_string(t->graph[s]._term);
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
			for (int j = 0; j < 3*bgp->triples; j++) {
				if (bgp->variable_indexes[j] >= 0) {
					nodeid_t id	= final_match[bgp->variable_indexes[j]];
					fprintf(f, "%"PRIu32"", id);
					triplestore_print_term(t, id, f, 0);
					fprintf(f, " ");
				}
			}
			fprintf(f, "\n");
		}
		return (limit > 0 && count == limit);
	});
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
		} else if (!strcmp(field, "limit")) {
			pctx->limit	= atoll(argv[++i]);
		}
	} else if (!strcmp(op, "unset")) {
		const char* field	= argv[++i];
		if (!strcmp(field, "print")) {
			pctx->print	= 0;
		} else if (!strcmp(field, "verbose")) {
			pctx->verbose	= 0;
		} else if (!strcmp(field, "limit")) {
			pctx->limit	= -1;
		}
	} else if (!strcmp(op, "match")) {
		const char* pattern	= argv[++i];
		triplestore_match_terms(t, pattern, pctx->limit, ^(nodeid_t id) {
			if (f != NULL) {
				char* string		= term_to_string(t->graph[id]._term);
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
		triplestore_node_dump(t, pctx->limit, stdout);
	} else if (!strcmp(op, "edges")) {
		triplestore_edge_dump(t, pctx->limit, stdout);
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
		double start	= current_time();
		nodeid_t count	= triplestore_print_bgp_match(t, &bgp, pctx->limit, f);
		if (pctx->verbose) {
			double elapsed	= elapsed_time(start);
			fprintf(stderr, "%lfs elapsed during matching of %"PRIu32" results\n", elapsed, count);
		}
	} else if (!strcmp(op, "triple")) {
		int64_t s	= atoi(argv[++i]);
		int64_t p	= atoi(argv[++i]);
		int64_t o	= atoi(argv[++i]);
		double start	= current_time();
		nodeid_t count	= triplestore_print_match(t, s, p, o, f);
		if (pctx->verbose) {
			double elapsed	= elapsed_time(start);
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

	__block struct parser_ctx_s pctx	= {
		.limit				= -1,
		.verbose			= 0,
		.error				= 0,
		.graph				= 0LL,
		.count				= 0,
		.print				= 1,
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
		linenoiseHistorySave(linenoiseHistoryFile);
	}
	
	free_triplestore(t);
	return 0;
}

