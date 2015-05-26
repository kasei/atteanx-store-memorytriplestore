// TODO: change encoding of typed literals to use t.value_id slot with node id instead of t.value_type slot with IRI string.



#include <fcntl.h>
#include <time.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include "triplestore.h"

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

double triplestore_current_time ( void ) {
	struct timeval t;
	gettimeofday (&t, NULL);
	double start	= t.tv_sec + (t.tv_usec / 1000000.0);
	return start;
}

double triplestore_elapsed_time ( double start ) {
	struct timeval t;
	gettimeofday (&t, NULL);
	double time	= t.tv_sec + (t.tv_usec / 1000000.0);
	double elapsed	= time - start;
	return elapsed;
}

#pragma mark -
#pragma mark RDF Terms

rdf_term_t* triplestore_new_term( rdf_term_type_t type, char* value, char* vtype, nodeid_t vid ) {
	rdf_term_t* t	= calloc(sizeof(rdf_term_t), 1);
	t->type			= type;
	t->value		= calloc(1, 1+strlen(value));
	strcpy(t->value, value);
	
	if (vtype) {
		t->value_type	= calloc(1, 1+strlen(vtype));
		strcpy(t->value_type, vtype);
	} else {
		t->value_id	= vid;
	}
	
	return t;
}

void free_rdf_term(rdf_term_t* t) {
	free(t->value);
	if (t->type == TERM_LANG_LITERAL) {
		free(t->value_type);
	}
	free(t);
}

int triplestore_size(triplestore_t* t) {
	return t->edges_used;
}

int term_compare(rdf_term_t* a, rdf_term_t* b) {
	if (a == NULL) return -1;
	if (b == NULL) return 1;
	if (a->type < b->type) {
		return -1;
	} else if (a->type > b->type) {
		return 1;
	} else {
		if (a->type == TERM_LANG_LITERAL) {
			int r	= strcmp(a->value_type, b->value_type);
			if (r) {
				return r;
			}
		} else if (a->type == TERM_TYPED_LITERAL) {
			if (a->value_id != b->value_id) {
				return (a->value_id - b->value_id);
			}
		}
		
		return strcmp(a->value, b->value);
	}
	return 0;
}

char* triplestore_term_to_string(triplestore_t* store, rdf_term_t* t) {
	char* string	= NULL;
	char* extra		= NULL;
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
			// TODO: handle escaping
			sprintf(string, "\"%s\"", t->value);
			break;
		case TERM_LANG_LITERAL:
			// TODO: handle escaping
			string	= calloc(4+strlen(t->value)+strlen(t->value_type), 1);
			sprintf(string, "\"%s\"@%s", t->value, t->value_type);
			break;
		case TERM_TYPED_LITERAL:
			// TODO: handle escaping
			
			extra	= triplestore_term_to_string(store, store->graph[ t->value_id ]._term);
			
			string	= calloc(7+strlen(t->value)+strlen(extra), 1);
			if (!strcmp(extra, "<http://www.w3.org/2001/XMLSchema#float>")) {
				sprintf(string, "%s", t->value);
			} else if (!strcmp(extra, "<http://www.w3.org/2001/XMLSchema#integer>")) {
				sprintf(string, "%s", t->value);
			} else if (!strcmp(extra, "<http://www.w3.org/2001/XMLSchema#boolean>")) {
				sprintf(string, "%s", t->value);
			} else {
				sprintf(string, "\"%s\"^^%s", t->value, extra);
			}
			free(extra);
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
#pragma mark Triplestore

triplestore_t* new_triplestore(int max_nodes, int max_edges) {
	triplestore_t* t	= (triplestore_t*) calloc(sizeof(triplestore_t), 1);
	t->edges_alloc	= max_edges;
	t->nodes_alloc	= max_nodes;
	t->edges_used	= 0;
	t->nodes_used	= 0;
// 	fprintf(stderr, "allocating %d bytes for %"PRIu32" edges\n", max_edges * sizeof(index_list_element_t), max_edges);
	t->edges		= calloc(sizeof(index_list_element_t), max_edges);
	if (t->edges == NULL) {
		fprintf(stderr, "*** Failed to allocate memory for triplestore edges\n");
		return NULL;
	}
// 	fprintf(stderr, "allocating %d bytes for graph for %"PRIu32" nodes\n", max_nodes * sizeof(graph_node_t), max_nodes);
	t->graph		= calloc(sizeof(graph_node_t), max_nodes);
	if (t->graph == NULL) {
		free(t->edges);
		fprintf(stderr, "*** Failed to allocate memory for triplestore graph\n");
		return NULL;
	}
	t->dictionary	= avl_create( _hx_node_cmp_str, NULL, &avl_allocator_default );
	return t;
}

int free_triplestore(triplestore_t* t) {
	avl_destroy(t->dictionary, _hx_free_node_item);
	free(t->edges);
	free(t->graph);
	free(t);
	return 0;
}

int triplestore_expand_edges(triplestore_t* t) {
	int alloc	= t->edges_alloc;
	alloc		*= 2;
// 	fprintf(stderr, "Expanding triplestore to accept %d edges\n", alloc);
	index_list_element_t* edges	= realloc(t->edges, alloc * sizeof(index_list_element_t));
	if (edges) {
		t->edges		= edges;
		t->edges_alloc	= alloc;
		return 0;
	} else {
		return 1;
	}
}

int triplestore_expand_nodes(triplestore_t* t) {
	int alloc	= t->nodes_alloc;
	alloc		*= 2;
// 	fprintf(stderr, "Expanding triplestore to accept %d nodes\n", alloc);
	graph_node_t* graph	= realloc(t->graph, alloc * sizeof(graph_node_t));
	if (graph) {
		t->graph		= graph;
		t->nodes_alloc	= alloc;
		return 0;
	} else {
		return 1;
	}
}

#pragma mark -
#pragma mark Filters

query_filter_t* triplestore_new_filter(filter_type_t type, ...) {
	va_list ap;
	va_start(ap, type);
	query_filter_t* filter	= calloc(1, sizeof(query_filter_t));
	filter->type	= type;
	if (type == FILTER_ISIRI || type == FILTER_ISLITERAL || type == FILTER_ISBLANK) {
		int64_t id		= va_arg(ap, int64_t);
		filter->node1	= id;
	} else if (type == FILTER_SAMETERM) {
		filter->node1		= va_arg(ap, int64_t);
		filter->node2		= va_arg(ap, int64_t);
	
// 	FILTER_REGEX,		// REGEX(?var, "string", "flags")
// 	FILTER_LANGMATCHES,	// LANGMATCHES(STR(?var), "string")
// 	FILTER_CONTAINS,	// CONTAINS(?var, "string")
	
	} else if (type == FILTER_STRSTARTS || type == FILTER_STRENDS) {
		filter->node1		= va_arg(ap, int64_t);
		const char* pat		= va_arg(ap, char*);
		filter->string2		= calloc(1, 1+strlen(pat));
		strcpy(filter->string2, pat);
	} else {
		fprintf(stderr, "*** Unexpected filter type %d\n", type);
	}
	return filter;
}

int triplestore_free_filter(query_filter_t* filter) {
	if (filter->string2) {
		free(filter->string2);
	}
	if (filter->string3) {
		free(filter->string3);
	}
	free(filter);
	return 0;
}

int _triplestore_filter_match(triplestore_t* t, query_filter_t* filter, nodeid_t* current_match, int(^block)(nodeid_t* final_match)) {
	int64_t node1;
	int64_t node2;
	rdf_term_t* term;
	switch (filter->type) {
		case FILTER_ISIRI:
			if (t->graph[ current_match[-(filter->node1)] ]._term->type != TERM_IRI) {
				return 0;
			}
			break;
		case FILTER_ISLITERAL:
			term	= t->graph[ current_match[-(filter->node1)] ]._term;
			if (!(term->type == TERM_XSDSTRING_LITERAL || term->type == TERM_LANG_LITERAL || term->type == TERM_TYPED_LITERAL)) {
				return 0;
			}
			break;
		case FILTER_ISBLANK:
			if (t->graph[ current_match[-(filter->node1)] ]._term->type != TERM_BLANK) {
				return 0;
			}
			break;
		case FILTER_SAMETERM:
			node1	= filter->node1;
			node2	= filter->node2;
			if (node1 < 0) {
				node1	= current_match[-node1];
			}
			if (node2 < 0) {
				node2	= current_match[-node2];
			}
			if (node1 != node2) {
				return 0;
			}
			break;
		case FILTER_STRSTARTS:
			term	= t->graph[ current_match[-(filter->node1)] ]._term;
			if (strlen(term->value) >= strlen(filter->string2)) {
				if (0 == strncmp(term->value, filter->string2, strlen(filter->string2))) {
					break;
				}
			}
			return 0;
		case FILTER_STRENDS:
			term	= t->graph[ current_match[-(filter->node1)] ]._term;
			if (strlen(term->value) >= strlen(filter->string2)) {
				const char* suffix	= term->value + strlen(term->value) - strlen(filter->string2);
				if (0 == strcmp(suffix, filter->string2)) {
					break;
				}
			}
			return 0;
		default:
			fprintf(stderr, "*** Unrecognized filter type %d\n", filter->type);
	}
	
	// filter evaluated to true
	return block(current_match);
}

#pragma mark -
#pragma mark BGPs

bgp_t* triplestore_new_bgp(triplestore_t* t, int variables, int triples) {
	bgp_t* bgp		= calloc(sizeof(bgp_t), 1);
	bgp->triples	= triples;
	bgp->nodes		= calloc(sizeof(int64_t), 3 * triples);
	return bgp;
}

int triplestore_free_bgp(bgp_t* bgp) {
	free(bgp->nodes);
	free(bgp);
	return 0;
}

int triplestore_bgp_set_triple_nodes(bgp_t* bgp, int triple, int64_t s, int64_t p, int64_t o) {
	int i	= 3 * triple;
	bgp->nodes[i+0]	= s;
	bgp->nodes[i+1]	= p;
	bgp->nodes[i+2]	= o;
	return 0;
}

int _triplestore_bgp_match(triplestore_t* t, bgp_t* bgp, int current_triple, nodeid_t* current_match, int(^block)(nodeid_t* final_match)) {
	if (current_triple == bgp->triples) {
		return block(current_match);
	}
	
	int offset	= 3*current_triple;
	int64_t s	= bgp->nodes[offset];
	int64_t p	= bgp->nodes[offset+1];
	int64_t o	= bgp->nodes[offset+2];
	int64_t sv	= -s;
	int64_t pv	= -p;
	int64_t ov	= -o;
	int reset_s	= 0;
	int reset_p	= 0;
	int reset_o	= 0;
	if (s < 0) {
		if (current_match[sv] > 0) {
			s	= current_match[sv];
// 			fprintf(stderr, "- carrying over match of subject (variable %"PRId64"): %"PRId64"\n", sv, s);
			if (s == 0) {
				fprintf(stderr, "*** Got unexpected zero node for variable %"PRId64"\n", sv);
				assert(0);
			}
		} else {
			reset_s	= 1;
		}
	}
	if (p < 0) {
		if (current_match[pv] > 0) {
			p	= current_match[pv];
// 			fprintf(stderr, "- carrying over match of predicate (variable %"PRId64"): %"PRId64"\n", pv, p);
			if (p == 0) {
				fprintf(stderr, "*** Got unexpected zero node for variable %"PRId64"\n", pv);
				assert(0);
			}
		} else {
			reset_p	= 1;
		}
	}
	if (o < 0) {
		if (current_match[ov] > 0) {
			o	= current_match[ov];
// 			fprintf(stderr, "- carrying over match of object (variable %"PRId64"): %"PRId64"\n", ov, o);
			if (o == 0) {
				fprintf(stderr, "*** Got unexpected zero node for variable %"PRId64"\n", ov);
				assert(0);
			}
		} else {
			reset_o	= 1;
		}
	}
	
	if (o == 0) {
		fprintf(stderr, "Got unexpected zero node from nodes[%d]\n", offset+2);
		assert(0);
	}
// 	fprintf(stderr, "BGP matching triple %d: %"PRId64" %"PRId64" %"PRId64"\n", current_triple, s, p, o);
	int r	= triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t _s, nodeid_t _p, nodeid_t _o) {
// 		fprintf(stderr, "-> BGP triple %d match: %"PRIu32" %"PRIu32" %"PRIu32"\n", current_triple, _s, _p, _o);
		if (s < 0) {
			current_match[-s]	= _s;
		}
		if (p < 0) {
			current_match[-p]	= _p;
		}
		if (o < 0) {
			current_match[-o]	= _o;
		}
		return _triplestore_bgp_match(t, bgp, current_triple+1, current_match, block);
	});
	
	if (reset_s) current_match[sv]	= 0;
	if (reset_p) current_match[pv]	= 0;
	if (reset_o) current_match[ov]	= 0;
	
	return r;
}

// final_match is a node ID array with final_match[0] representing the number of following node IDs
int triplestore_bgp_match(triplestore_t* t, bgp_t* bgp, int variables, int(^block)(nodeid_t* final_match)) {
	nodeid_t* current_match	= calloc(sizeof(nodeid_t), 1+variables);
	current_match[0]	= variables;
	int r	= _triplestore_bgp_match(t, bgp, 0, current_match, block);
	free(current_match);
	return r;
}

#pragma mark -
#pragma mark Query

query_t* triplestore_new_query(triplestore_t* t, int variables) {
	query_t* query			= calloc(sizeof(query_t), 1);
	query->variables		= variables;
	query->variable_names	= calloc(sizeof(char*), variables+1);
	return query;
}

int triplestore_free_query_op(query_op_t* op) {
	if (op->next) {
		triplestore_free_query_op(op->next);
	}
	
	switch (op->type) {
		case QUERY_BGP:
			triplestore_free_bgp(op->ptr);
			break;
		case QUERY_FILTER:
			triplestore_free_filter(op->ptr);
			break;
		default:
			fprintf(stderr, "Unrecognized query operation %d\n", op->type);
			return 1;
	};
	free(op);
	return 0;
}

int triplestore_free_query(query_t* query) {
	for (int i = 0; i < query->variables; i++) {
		free(query->variable_names[i]);
	}
	free(query->variable_names);
	
	if (query->head) {
		triplestore_free_query_op(query->head);
	}
	
	free(query);
	return 0;
}

int triplestore_query_set_variable_name(query_t* query, int variable, const char* name) {
	query->variable_names[variable]	= calloc(1, 1+strlen(name));
	strcpy(query->variable_names[variable], name);
	return 0;
}

int triplestore_query_add_op(query_t* query, query_type_t type, void* ptr) {
	query_op_t* op	= calloc(1, sizeof(query_op_t));
	op->next	= NULL;
	op->type	= type;
	op->ptr		= ptr;
	if (query->tail) {
		query->tail->next	= op;
		query->tail			= op;
	} else {
		query->head	= op;
		query->tail	= op;
	}
	return 0;
}

int _triplestore_query_op_match(triplestore_t* t, query_t* query, query_op_t* op, nodeid_t* current_match, int(^block)(nodeid_t* final_match)) {
	if (op) {
		switch (op->type) {
			case QUERY_BGP:
				return _triplestore_bgp_match(t, op->ptr, 0, current_match, ^(nodeid_t* final_match){
					return _triplestore_query_op_match(t, query, op->next, final_match, block);
				});
			case QUERY_FILTER:
				return _triplestore_filter_match(t, op->ptr, current_match, ^(nodeid_t* final_match){
					return _triplestore_query_op_match(t, query, op->next, final_match, block);
				});
			default:
				fprintf(stderr, "Unrecognized query op in _triplestore_query_op_match: %d\n", op->type);
				return 1;
		};
	} else {
		block(current_match);
	}
	return 0;
}

int triplestore_query_match(triplestore_t* t, query_t* query, int64_t limit, int(^block)(nodeid_t* final_match)) {
	nodeid_t* current_match	= calloc(sizeof(nodeid_t), 1+query->variables);
	current_match[0]	= query->variables;
	int r	= _triplestore_query_op_match(t, query, query->head, current_match, block);
	free(current_match);
	return r;
}

#pragma mark -

int triplestore_add_triple(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o, uint64_t timestamp) {
	if (t->edges_used >= t->edges_alloc) {
		if (triplestore_expand_edges(t)) {
			fprintf(stderr, "*** Exhausted allocated space for edges.\n");
			return 1;
		}
	}
	
	if (s == 0 || p == 0 || o == 0) {
		return 1;
	}
	
	nodeid_t edge				= ++t->edges_used;

	t->edges[ edge ].s			= s;
	t->edges[ edge ].p			= p;
	t->edges[ edge ].o			= o;
	t->edges[ edge ].next_out	= t->graph[s].out_edge_head;
	t->edges[ edge ].next_in	= t->graph[o].in_edge_head;

	t->graph[s].out_edge_head	= edge;
	t->graph[s].mtime			= timestamp;
	t->graph[s].out_degree++;
	
	t->graph[o].in_edge_head	= edge;
	t->graph[o].mtime			= timestamp;
	t->graph[o].in_degree++;
	
	return 0;
}

static rdf_term_t* term_from_raptor_term(triplestore_t* store, raptor_term* t) {
	// TODO: datatype IRIs should be referenced by term ID, not an IRI string (lots of wasted space)
	char* value				= NULL;
	char* vtype				= NULL;
	switch (t->type) {
		case RAPTOR_TERM_TYPE_URI:
			value	= (char*) raptor_uri_as_string(t->value.uri);
			return triplestore_new_term(TERM_IRI, value, NULL, 0);
		case RAPTOR_TERM_TYPE_BLANK:
			value	= (char*) t->value.blank.string;
			return triplestore_new_term(TERM_BLANK, value, NULL, 0);
		case RAPTOR_TERM_TYPE_LITERAL:
			value	= (char*) t->value.literal.string;
			if (t->value.literal.language) {
				vtype	= (char*) t->value.literal.language;
				return triplestore_new_term(TERM_LANG_LITERAL, value, vtype, 0);
			} else if (t->value.literal.datatype) {
				vtype	= (char*) raptor_uri_as_string(t->value.literal.datatype);
				
				rdf_term_t* datatype	= triplestore_new_term(TERM_IRI, vtype, NULL, 0);
				nodeid_t datatypeid		= triplestore_add_term(store, datatype);
				
				return triplestore_new_term(TERM_TYPED_LITERAL, value, NULL, datatypeid);
			} else {
				return triplestore_new_term(TERM_XSDSTRING_LITERAL, value, NULL, 0);
			}
		default:
			fprintf(stderr, "*** unknown node type %d during import\n", t->type);
			return NULL;
	}
}

nodeid_t triplestore_get_term(triplestore_t* t, rdf_term_t* myterm) {
	hx_nodemap_item i;
	i._term			= myterm;
	i.id			= 0;
	hx_nodemap_item* item	= (hx_nodemap_item*) avl_find( t->dictionary, &i );
	free_rdf_term(myterm);
	if (item == NULL) {
		return 0;
	} else {
		return item->id;
	}
}

nodeid_t triplestore_add_term(triplestore_t* t, rdf_term_t* myterm) {
	hx_nodemap_item i;
	i._term			= myterm;
	i.id			= 0;
	hx_nodemap_item* item	= (hx_nodemap_item*) avl_find( t->dictionary, &i );
	if (item == NULL) {
		if (t->nodes_used >= t->nodes_alloc) {
			if (triplestore_expand_nodes(t)) {
				fprintf(stderr, "*** Exhausted allocated space for nodes.\n");
				return 1;
			}
		}

		item	= (hx_nodemap_item*) calloc( 1, sizeof( hx_nodemap_item ) );
		item->_term	= myterm;
		item->id	= ++t->nodes_used;
		avl_insert( t->dictionary, item );
		
		graph_node_t node	= { ._term = item->_term, .mtime = 0, .out_edge_head = 0, .in_edge_head = 0 };
		t->graph[item->id]	= node;
// 		fprintf(stdout, "+ %6"PRIu32" %s\n", item->id, triplestore_term_to_string(t, term));
	} else {
		free_rdf_term(myterm);
// 		fprintf(stdout, "  %6"PRIu32" %s\n", item->id, triplestore_term_to_string(t, term));
	}
	return item->id;
}

static void parser_handle_triple (void* user_data, raptor_statement* triple) {
	struct parser_ctx_s* pctx	= (struct parser_ctx_s*) user_data;
	if (pctx->error) {
		return;
	}
	
	pctx->count++;

	nodeid_t s	= triplestore_add_term(pctx->store, term_from_raptor_term(pctx->store, triple->subject));
	nodeid_t p	= triplestore_add_term(pctx->store, term_from_raptor_term(pctx->store, triple->predicate));
	nodeid_t o	= triplestore_add_term(pctx->store, term_from_raptor_term(pctx->store, triple->object));
	if (triplestore_add_triple(pctx->store, s, p, o, pctx->timestamp)) {
		pctx->error++;
		return;
	}
	
	if (pctx->verbose) {
		if (pctx->count % 250 == 0) {
			double elapsed	= triplestore_elapsed_time(pctx->start);
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
// 		fcntl(fd, F_NOCACHE, 1);
// 		fcntl(fd, F_RDAHEAD, 1);
		FILE* f	= fdopen(fd, "r");
		raptor_parser_parse_file_stream(rdf_parser, f, filename, base_uri);
		fclose(f);
// 	} else {
// 		raptor_parser_parse_file(rdf_parser, uri, base_uri);
// 	}
	
	if (pctx->error) {
		fprintf( stderr, "\nError encountered during parsing\n" );
	} else if (pctx->verbose) {
		double elapsed	= triplestore_elapsed_time(pctx->start);
		fprintf( stderr, "\nFinished parsing %"PRIu64" triples in %lfs\n", pctx->count, elapsed );
	}
	
	free(uri_string);
	raptor_free_parser(rdf_parser);
	raptor_free_uri( base_uri );
	raptor_free_uri( uri );

	raptor_free_world(world);
}

int triplestore__load_file(triplestore_t* t, const char* filename, int print, int verbose) {
	__block struct parser_ctx_s pctx	= {
		.limit				= -1,
		.error				= 0,
		.graph				= 0LL,
		.count				= 0,
		.verbose			= verbose,
		.print				= print,
		.start				= triplestore_current_time(),
		.store				= t,
		.timestamp			= (uint64_t) time(NULL),
	};
	parse_rdf_from_file(filename, &pctx);
	return pctx.error;
}

#pragma mark -

int triplestore_match_triple(triplestore_t* t, int64_t _s, int64_t _p, int64_t _o, int(^block)(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o)) {
	// TODO: check for repeated variables used to filter results
	//       e.g. (-1, 0, -1) for all triples where subject == object
	if (_s > 0) {
		nodeid_t idx	= t->graph[_s].out_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->edges[idx].p;
			nodeid_t o	= t->edges[idx].o;
			if (_p <= 0 || _p == p) {
				if (_o <= 0 || _o == o) {
					if (block(t, _s, p, o)) {
						return 1;
					}
				}
			}
			idx			= t->edges[idx].next_out;
		}
	} else if (_o > 0) {
		nodeid_t idx	= t->graph[_o].in_edge_head;
		while (idx != 0) {
			nodeid_t p	= t->edges[idx].p;
			nodeid_t s	= t->edges[idx].s;
			if (_p <= 0 || _p == p) {
				if (_s <= 0 || _s == s) {
					if (block(t, s, p, _o)) {
						return 1;
					}
				}
			}
			idx			= t->edges[idx].next_in;
		}
	} else {
		for (nodeid_t s = 1; s < t->nodes_used; s++) {
			if (_s <= 0 || _s == s) {
				nodeid_t idx	= t->graph[s].out_edge_head;
				while (idx != 0) {
					nodeid_t p	= t->edges[idx].p;
					nodeid_t o	= t->edges[idx].o;
					if (_p <= 0 || _p == p) {
						if (_o <= 0 || _o == o) {
							if (block(t, s, p, o)) {
								return 1;
							}
						}
					}
					idx			= t->edges[idx].next_out;
				}
			}
		}
		return 0;
	}
	return 0;
}

#pragma mark -
#pragma mark Print Functions

int triplestore_print_term(triplestore_t* t, nodeid_t s, FILE* f, int newline) {
	rdf_term_t* subject		= t->graph[s]._term;
	if (subject == NULL) assert(0);
	char* ss		= triplestore_term_to_string(t, subject);
	fprintf(f, "%s", ss);
	if (newline) {
		fprintf(f, "\n");
	}
	free(ss);
	return 0;
}

static void _print_term_or_variable(triplestore_t* t, int variables, char** variable_names, int64_t s, FILE* f) {
	if (s == 0) {
		fprintf(f, "[]");
	} else if (s < 0) {
		fprintf(f, "%s", variable_names[-s]);
	} else {
		triplestore_print_term(t, s, f, 0);
	}
}

void triplestore_print_filter(triplestore_t* t, query_t* query, query_filter_t* filter, FILE* f) {
	fprintf(f, "Filter: ");
	switch (filter->type) {
		case FILTER_ISIRI:
			fprintf(f, "ISIRI(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ")\n");
			break;
		case FILTER_ISLITERAL:
			fprintf(f, "FILTER_ISLITERAL(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ")\n");
			break;
		case FILTER_ISBLANK:
			fprintf(f, "ISBLANK(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ")\n");
			break;
		case FILTER_SAMETERM:
			fprintf(f, "SAMETERM(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ", ");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node2, f);
			fprintf(f, ")\n");
			break;
		case FILTER_STRSTARTS:
			fprintf(f, "STRSTARTS(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ", \"%s\")\n", filter->string2);
			break;
		case FILTER_STRENDS:
			fprintf(f, "STRENDS(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ", \"%s\")\n", filter->string2);
			break;
		default:
			fprintf(f, "***UNRECOGNIZED FILTER***\n");
	}
}

void triplestore_print_bgp(triplestore_t* t, bgp_t* bgp, int variables, char** variable_names, FILE* f) {
	fprintf(f, "- Variables: %d\n", variables);
	for (int v = 1; v <= variables; v++) {
		fprintf(f, "  - %s\n", variable_names[v]);
	}
	fprintf(f, "- Triples: %d\n", bgp->triples);
	for (int i = 0; i < bgp->triples; i++) {
		int64_t s	= bgp->nodes[3*i+0];
		int64_t p	= bgp->nodes[3*i+1];
		int64_t o	= bgp->nodes[3*i+2];
		
		fprintf(f, "  - ");
		_print_term_or_variable(t, variables, variable_names, s, f);
		fprintf(f, " ");
		_print_term_or_variable(t, variables, variable_names, p, f);
		fprintf(f, " ");
		_print_term_or_variable(t, variables, variable_names, o, f);
		fprintf(f, "\n");
	}
}

void triplestore_print_query_op(triplestore_t* t, query_t* query, query_op_t* op, FILE* f) {
	if (op->type == QUERY_BGP) {
		triplestore_print_bgp(t, op->ptr, query->variables, query->variable_names, f);
	} else if (op->type == QUERY_FILTER) {
		triplestore_print_filter(t, query, op->ptr, f);
	} else {
		fprintf(stderr, "*** Unrecognized query op %d\n", op->type);
	}
}

void triplestore_print_query(triplestore_t* t, query_t* query, FILE* f) {
	fprintf(f, "- Variables: %d\n", query->variables);
	for (int v = 1; v <= query->variables; v++) {
		fprintf(f, "  - %s\n", query->variable_names[v]);
	}
	
	query_op_t* op	= query->head;
	while (op != NULL) {
		triplestore_print_query_op(t, query, op, f);
		op	= op->next;
	}
}


