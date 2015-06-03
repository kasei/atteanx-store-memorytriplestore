#include <fcntl.h>
#include <arpa/inet.h>
#include <time.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
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

static void __indent(FILE* f, int depth) {
	for (int x = 0; x < depth; x++) {
		fprintf(f, "\t");
	}
}

#pragma mark -
#pragma mark RDF Terms

rdf_term_t* triplestore_new_term( rdf_term_type_t type, char* value, char* vtype, nodeid_t vid ) {
	rdf_term_t* t	= calloc(sizeof(rdf_term_t), 1);
	t->type			= type;
	t->value		= calloc(1, 1+strlen(value));
	strcpy(t->value, value);
	
	if (vtype) {
		t->vtype.value_type	= calloc(1, 1+strlen(vtype));
		strcpy(t->vtype.value_type, vtype);
	} else {
		t->vtype.value_id	= vid;
	}
	
	return t;
}

void free_rdf_term(rdf_term_t* t) {
	free(t->value);
	if (t->type == TERM_LANG_LITERAL) {
		free(t->vtype.value_type);
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
			int r	= strcmp(a->vtype.value_type, b->vtype.value_type);
			if (r) {
				return r;
			}
		} else if (a->type == TERM_TYPED_LITERAL) {
			if (a->vtype.value_id != b->vtype.value_id) {
				return (a->vtype.value_id - b->vtype.value_id);
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
			string	= calloc(4+strlen(t->value)+strlen(t->vtype.value_type), 1);
			sprintf(string, "\"%s\"@%s", t->value, t->vtype.value_type);
			break;
		case TERM_TYPED_LITERAL:
			// TODO: handle escaping
			
			extra	= triplestore_term_to_string(store, store->graph[ t->vtype.value_id ]._term);
			
			string	= calloc(7+strlen(t->value)+strlen(extra), 1);
			if (!strcmp(extra, "<http://www.w3.org/2001/XMLSchema#decimal>")) {
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

static int _write32(int fd, uint32_t value) {
	uint32_t l	= htonl(value);
	return write(fd, &l, sizeof(uint32_t));
}

static int _writeterm(int fd, rdf_term_t* term) {
	uint32_t type	= (uint32_t) term->type;
	uint32_t extra_int	= 0;
	if (type == TERM_LANG_LITERAL) {
		extra_int	= strlen(term->vtype.value_type);
	} else if (type == TERM_TYPED_LITERAL) {
		extra_int	= term->vtype.value_id;
	}
	
	char buffer[12];
	int vlen	= strlen(term->value);
	*((uint32_t*) &(buffer[0]))		= htonl(type);
	*((uint32_t*) &(buffer[4]))		= htonl(extra_int);
	*((uint32_t*) &(buffer[8]))		= htonl(vlen);
	write(fd, buffer, 12);
	write(fd, term->value, 1+vlen);
	if (type == TERM_LANG_LITERAL) {
		write(fd, term->vtype.value_type, 1+extra_int);
	}
	return 0;
}

static rdf_term_t* _readterm(int fd) {
	char buffer[12];
	read(fd, buffer, 12);
	rdf_term_type_t type	= (rdf_term_type_t) ntohl(*((uint32_t*) &(buffer[0])));
	uint32_t extra_int		= ntohl(*((uint32_t*) &(buffer[4])));
	uint32_t vlen			= ntohl(*((uint32_t*) &(buffer[8])));
	char* value				= calloc(1, 1+vlen);
	read(fd, value, vlen+1);
	
	char* value_type		= NULL;
	uint32_t value_id		= 0;
	if (type == TERM_LANG_LITERAL) {
		value_type	= calloc(1, 1+extra_int);
		read(fd, value_type, 1+extra_int);
	} else if (type == TERM_TYPED_LITERAL) {
		value_id	= extra_int;
	}
	return triplestore_new_term(type, value, value_type, value_id);
}

int _triplestore_dump_edge(int fd, index_list_element_t* edge) {
	char buffer[20];
	*((uint32_t*) &(buffer[0]))		= htonl(edge->s);
	*((uint32_t*) &(buffer[4]))		= htonl(edge->p);
	*((uint32_t*) &(buffer[8]))		= htonl(edge->o);
	*((uint32_t*) &(buffer[12]))	= htonl(edge->next_in);
	*((uint32_t*) &(buffer[16]))	= htonl(edge->next_out);
	write(fd, buffer, 20);
	return 0;
}

int _triplestore_dump_node(int fd, graph_node_t* node) {
	char buffer[24];
	*((uint64_t*) &(buffer[0]))		= node->mtime;
	*((uint32_t*) &(buffer[8]))		= htonl(node->out_degree);
	*((uint32_t*) &(buffer[12]))	= htonl(node->in_degree);
	*((uint32_t*) &(buffer[16]))	= htonl(node->out_edge_head);
	*((uint32_t*) &(buffer[20]))	= htonl(node->in_edge_head);
	write(fd, buffer, 24);
	_writeterm(fd, node->_term);
	return 0;
}

int _triplestore_load_node(int fd, int i, graph_node_t* node) {
	char buffer[24];
	read(fd, buffer, 24);
	node->mtime			= *((uint64_t*) &(buffer[0]));
	node->out_degree	= ntohl(*((uint32_t*) &(buffer[8])));
	node->in_degree		= ntohl(*((uint32_t*) &(buffer[12])));
	node->out_edge_head	= ntohl(*((uint32_t*) &(buffer[16])));
	node->in_edge_head	= ntohl(*((uint32_t*) &(buffer[20])));
	node->_term			= _readterm(fd);
	return 0;
}

int triplestore_dump(triplestore_t* t, const char* filename) {
	int fd	= open(filename, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP);
	if (fd == -1) {
		perror("failed to open file for dumping triplestore");
		return 1;
	}
	
	write(fd, "3STR", 4);
	_write32(fd, t->edges_alloc);
	_write32(fd, t->edges_used);
	_write32(fd, t->nodes_alloc);
	_write32(fd, t->nodes_used);
	
	for (uint32_t i = 1; i <= t->nodes_used; i++) {
		_triplestore_dump_node(fd, &(t->graph[i]));
	}
	for (uint32_t i = 1; i <= t->edges_used; i++) {
		_triplestore_dump_edge(fd, &(t->edges[i]));
	}
	return 0;
}

int triplestore_load(triplestore_t* t, const char* filename) {
	int fd	= open(filename, O_RDONLY);
	if (fd == -1) {
		perror("failed to open file for loading triplestore");
		return 1;
	}
	
	free(t->edges);
	free(t->graph);

	// LOAD replaces all the triples in the store, so drop and re-create the dictionary to clear it.
	if (t->dictionary) {
		avl_destroy(t->dictionary, _hx_free_node_item);
	}
	t->dictionary	= avl_create( _hx_node_cmp_str, NULL, &avl_allocator_default );
	
	char buffer[20];
	read(fd, buffer, 20);
	
	char* ptr	= buffer;
	if (strncmp(ptr, "3STR", 4)) {
		fprintf(stderr, "Bad cookie\n");
		return 1;
	}
	
// 	uint32_t ealloc	= ntohl(*((uint32_t*) &(buffer[4])));
	uint32_t edges	= ntohl(*((uint32_t*) &(buffer[8])));
// 	uint32_t nalloc	= ntohl(*((uint32_t*) &(buffer[12])));
	uint32_t nodes	= ntohl(*((uint32_t*) &(buffer[16])));

	t->nodes_alloc	= nodes;
	t->nodes_used	= nodes;
	t->edges_alloc	= edges;
	t->edges_used	= edges;
	fprintf(stderr, "loading triplestore with %"PRIu32" edges and %"PRIu32" nodes\n", t->edges_used, t->nodes_used);
	
	t->graph				= calloc(sizeof(graph_node_t), 1+nodes);
	for (uint32_t i = 1; i <= nodes; i++) {
		hx_nodemap_item* item	= (hx_nodemap_item*) calloc( 1, sizeof( hx_nodemap_item ) );
		_triplestore_load_node(fd, i, &(t->graph[i]));
		item->_term	= t->graph[i]._term;
		item->id	= i;
		avl_insert( t->dictionary, item );
// 		fprintf(stderr, "Loaded term (%"PRIu32") %s\n", i, triplestore_term_to_string(t, t->graph[i]._term));
	}

	t->edges		= calloc(sizeof(index_list_element_t), 1+edges);
	read(fd, &(t->edges[1]), 20 * edges);
	for (uint32_t i = 1; i <= edges; i++) {
// 		_triplestore_load_edge(fd, i, &(t->edges[i]));
		t->edges[i].s			= ntohl(t->edges[i].s);
		t->edges[i].p			= ntohl(t->edges[i].p);
		t->edges[i].o			= ntohl(t->edges[i].o);
		t->edges[i].next_in		= ntohl(t->edges[i].next_in);
		t->edges[i].next_out	= ntohl(t->edges[i].next_out);
	}


// 	if (0) {
// 		size_t used	= avl_count( t->dictionary );
// 		struct avl_traverser iter;
// 		avl_t_init( &iter, t->dictionary );
// 		hx_nodemap_item* item	= NULL;
// 		while ((item = (hx_nodemap_item*) avl_t_next( &iter )) != NULL) {
// 			char* string		= triplestore_term_to_string(t, item->_term);
// 			fprintf( stdout, "%-10"PRIu32"\t%s\n", item->id, string );
// 			free( string );
// 		}
// 	}

	return 0;
}

#pragma mark -
#pragma mark Result Tables

table_t* triplestore_new_table(int width) {
	table_t* table	= calloc(1, sizeof(table_t));
	table->width	= width;
	table->alloc	= 128;
	table->used		= 0;
	table->ptr		= calloc(table->alloc, (1+width) * sizeof(nodeid_t));
	return table;
}

int triplestore_free_table(table_t* table) {
	free(table->ptr);
	free(table);
	return 0;
}

uint32_t* triplestore_table_row_ptr(table_t* table, int row) {
	nodeid_t* p	= &( table->ptr[ row*(1+table->width) ] );
	return p;
}

int triplestore_table_add_row(table_t* table, nodeid_t* result) {
	if (table->used == table->alloc) {
		table->alloc	*= 2;
		table->ptr	= realloc(table->ptr, table->alloc * (1+table->width) * sizeof(nodeid_t));
		if (table->ptr == NULL) {
			fprintf(stderr, "failed to grow table size\n");
			return 1;
		}
	}
	int i	= table->used++;
	nodeid_t* p	= &( table->ptr[ i*(1+table->width) ] );
	memcpy(p, result, (1+table->width) * sizeof(nodeid_t));
	return 0;
}

struct _sort_s {
	triplestore_t* t;
	sort_t* sort;
};

#ifdef __APPLE__
int _table_row_cmp(void* thunk, const void* a, const void* b) {
#else
int _table_row_cmp(const void* a, const void* b, void* thunk) {
#endif
	struct _sort_s* s	= (struct _sort_s*) thunk;
	triplestore_t* t	= s->t;
	sort_t* sort		= s->sort;
	uint32_t width		= sort->size;
	uint32_t* ap		= (uint32_t*) a;
	uint32_t* bp		= (uint32_t*) b;
	for (int i = 0; i < width; i++) {
		int64_t slot	= -(sort->vars[i]);
		nodeid_t aid	= ap[slot];
		nodeid_t bid	= bp[slot];
		rdf_term_t* aterm	= t->graph[aid]._term;
		rdf_term_t* bterm	= t->graph[bid]._term;
		
		char* as	= triplestore_term_to_string(t, aterm);
		char* bs	= triplestore_term_to_string(t, bterm);
		int r		= strcmp(as, bs);
		free(as);
		free(bs);
		if (r) {
			return r;
		}
	}
	return 0;
}

int triplestore_table_sort(triplestore_t* t, table_t* table, sort_t* sort) {
	struct _sort_s s	= { .t = t, .sort = sort };
	size_t bytes	= (1+table->width) * sizeof(nodeid_t);
#ifdef __APPLE__
	qsort_r(table->ptr, table->used, bytes, &s, _table_row_cmp);
#else
	qsort_r(table->ptr, table->used, bytes, _table_row_cmp, &s);
#endif
	return 0;
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
	} else if (type == FILTER_REGEX) {
		int64_t id		= va_arg(ap, int64_t);
		const char* pat	= va_arg(ap, char*);
		const char* fl	= va_arg(ap, char*);
		filter->node1	= id;
		filter->string2	= calloc(1, 1+strlen(pat));
		filter->string3	= calloc(1, 1+strlen(fl));
		strcpy(filter->string2, pat);
		strcpy(filter->string3, fl);

		const char *error;
		int erroffset;
		int flags		= 0;
		if (strstr(fl, "i")) {
			flags		|= PCRE_CASELESS;
		}
		filter->re = pcre_compile(
			pat,			/* the pattern */
			flags,			/* default options */
			&error,			/* for error message */
			&erroffset,		/* for error offset */
			NULL			/* use default character tables */
		);
		if (filter->re == NULL) {
			printf("PCRE compilation failed at offset %d: %s\n", erroffset, error);
			free(filter->string2);
			free(filter->string3);
			free(filter);
			return NULL;
		}
		
		
// 	FILTER_LANGMATCHES,	// LANGMATCHES(STR(?var), "string")
	} else if (type == FILTER_STRSTARTS || type == FILTER_STRENDS || type == FILTER_CONTAINS) {
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
	if (filter->string2_lang) {
		free(filter->string2_lang);
	}
	if (filter->string2) {
		free(filter->string2);
	}
	if (filter->string3) {
		free(filter->string3);
	}
	if (filter->re) {
		pcre_free(filter->re);
	}
	free(filter);
	return 0;
}

int _triplestore_filter_match(triplestore_t* t, query_t* query, query_filter_t* filter, nodeid_t* current_match, int(^block)(nodeid_t* final_match)) {
	int64_t node1;
	int64_t node2;
	int rc;
	rdf_term_t* term;
	int OVECCOUNT	= 30;
	int ovector[OVECCOUNT];
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
		case FILTER_CONTAINS:
			term	= t->graph[ current_match[-(filter->node1)] ]._term;
			if (strlen(filter->string2) == 0) {
				// all strings contain the empty pattern
				break;
			}
			if (strlen(term->value) >= strlen(filter->string2)) {
				if (strstr(term->value, filter->string2) != NULL) {
					break;
				}
			}
			return 0;
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
		case FILTER_REGEX:
			term	= t->graph[ current_match[-(filter->node1)] ]._term;
			// fprintf(stderr, "matching (?%s) %s =~ %s (%p)\n", query->variable_names[-(filter->node1)], term->value, filter->string2, filter->re);
			rc		= pcre_exec(
				filter->re,					/* the compiled pattern */
				NULL,						/* no extra data - we didn't study the pattern */
				term->value,				/* the subject string */
				strlen(term->value),		/* the length of the subject */
				0,							/* start at offset 0 in the subject */
				0,							/* default options */
				ovector,					/* output vector for substring information */
				OVECCOUNT					/* number of elements in the output vector */
			);
			if (rc <= 0) {
				return 0;
			}
			break;
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
#pragma mark Projection

project_t* triplestore_new_project(triplestore_t* t, int variables) {
	project_t* project	= calloc(sizeof(project_t), 1);
	project->size	= variables;
	project->keep	= calloc(1, 1+variables);
	return project;
}

int triplestore_free_project(project_t* project) {
	free(project->keep);
	free(project);
	return 0;
}

int triplestore_set_projection(project_t* project, int64_t var) {
	project->keep[-var]	= 1;
	return 0;
}

int _triplestore_project(triplestore_t* t, query_t* query, project_t* project, nodeid_t* current_match, int(^block)(nodeid_t* final_match)) {
	for (int i = 1; i <= project->size; i++) {
		if (project->keep[i] == 0) {
			current_match[i]	= 0;
		}
	}
	return block(current_match);
}


#pragma mark -
#pragma mark Sorting

sort_t* triplestore_new_sort(triplestore_t* t, int result_width, int variables) {
	sort_t* sort	= calloc(sizeof(sort_t), 1);
	sort->size		= variables;
	sort->vars		= calloc(sizeof(int64_t), variables);
	sort->table		= triplestore_new_table(result_width);
	return sort;
}

int triplestore_free_sort(sort_t* sort) {
	triplestore_free_table(sort->table);
	free(sort->vars);
	free(sort);
	return 0;
}

int triplestore_set_sort(sort_t* sort, int rank, int64_t var) {
	sort->vars[rank]	= var;
	return 0;
}

int _triplestore_sort_fill(triplestore_t* t, query_t* query, sort_t* sort, nodeid_t* current_match) {
	return triplestore_table_add_row(sort->table, current_match);
}


#pragma mark -
#pragma mark Paths

path_t* triplestore_new_path(triplestore_t* t, path_type_t type, int64_t start, nodeid_t pred, int64_t end) {
	path_t* path	= calloc(sizeof(path_t), 1);
	path->type	= type;
	path->start	= start;
	path->end	= end;
	path->pred	= pred;
	return path;
}

int triplestore_free_path(path_t* path) {
	free(path);
	return 0;
}

int _triplestore_path_step(triplestore_t* t, nodeid_t s, nodeid_t pred, char* seen, int depth, int(^block)(nodeid_t reached)) {
// 	__indent(stderr, depth);
// 	fprintf(stderr, "matching path %"PRId64" (%"PRIu32") %"PRId64"\n", s, path->pred, path->end);
	assert(s > 0);
	
	
	
// 	__indent(stderr, depth);
// 	fprintf(stderr, "matching triple %"PRId64" %"PRIu32" 0\n", s, path->pred);
	int r	= triplestore_match_triple(t, s, pred, 0, ^(triplestore_t* t, nodeid_t _s, nodeid_t _p, nodeid_t _o) {
		if (seen[_o]) {
			return 0;
		} else {
			seen[_o]	= 1;
	// 		__indent(stderr, depth);
	// 		fprintf(stderr, "got triple --> %"PRIu32" %"PRIu32" %"PRIu32"\n", _s, _p, _o);

			if (block(_o)) {
				return 1;
			}
			
			int r	= _triplestore_path_step(t, _o, pred, seen, 1+depth, block);
			return r;
		}
	});
	return r;
}

int _triplestore_path_match(triplestore_t* t, path_t* path, nodeid_t* current_match, int(^block)(nodeid_t* final_match)) {
	if (path->type == PATH_STAR) {
		fprintf(stderr, "*** should emit graph terms for * path\n");
	}
	
	int r	= 0;
	if (path->type == PATH_STAR || path->type == PATH_PLUS) {
		char* seen	= calloc(1, t->nodes_used);
		int r;
		if (path->start <= 0) {
			char* starts	= calloc(1, t->nodes_used);
			r	= triplestore_match_triple(t, path->start, path->pred, 0, ^(triplestore_t* t, nodeid_t _s, nodeid_t _p, nodeid_t _o) {
				if (starts[_s]++) {
					return 0;
				}
				memset(seen, 0, t->nodes_used);
				current_match[-(path->start)]	= _s;
				return _triplestore_path_step(t, _s, path->pred, seen, 0, ^(nodeid_t reached) {
					if (path->end < 0) {
						current_match[-(path->end)]	= reached;
					} else if (path->end > 0) {
						if (reached != path->end) {
							return 0;
						}
					}
					return block(current_match);
				});
			});
			free(starts);
		} else {
			r	= _triplestore_path_step(t, path->start, path->pred, seen, 0, ^(nodeid_t reached) {
				if (path->end < 0) {
					current_match[-(path->end)]	= reached;
				}
				return block(current_match);
			});
		}
		free(seen);
	}
	return r;
}

int triplestore_path_match(triplestore_t* t, path_t* path, int variables, int(^block)(nodeid_t* final_match)) {
	nodeid_t* current_match	= calloc(sizeof(nodeid_t), 1+variables);
	current_match[0]	= variables;
	int r				= _triplestore_path_match(t, path, current_match, block);
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
		case QUERY_PATH:
			triplestore_free_path(op->ptr);
			break;
		case QUERY_FILTER:
			triplestore_free_filter(op->ptr);
			break;
		case QUERY_PROJECT:
			triplestore_free_project(op->ptr);
			break;
		case QUERY_SORT:
			triplestore_free_sort(op->ptr);
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
	triplestore_ensure_variable_capacity(query, variable);
	query->variable_names[variable]	= calloc(1, 1+strlen(name));
	strcpy(query->variable_names[variable], name);
	return 0;
}

int triplestore_ensure_variable_capacity(query_t* query, int var) {
	if (var > query->variables) {
		query->variables	= var;
		query->variable_names	= realloc(query->variable_names, sizeof(char*) * (1+query->variables));
		return 1;
	}
	return 0;
}

int64_t triplestore_query_add_variable(query_t* query, const char* name) {
	int64_t var	= 1 + query->variables;
	triplestore_ensure_variable_capacity(query, var);
	triplestore_query_set_variable_name(query, var, name);
	return -var;
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
				return _triplestore_filter_match(t, query, op->ptr, current_match, ^(nodeid_t* final_match){
					return _triplestore_query_op_match(t, query, op->next, final_match, block);
				});
			case QUERY_PROJECT:
				return _triplestore_project(t, query, op->ptr, current_match, ^(nodeid_t* final_match){
					return _triplestore_query_op_match(t, query, op->next, final_match, block);
				});
			case QUERY_PATH:
				return _triplestore_path_match(t, op->ptr, current_match, ^(nodeid_t* final_match){
					return _triplestore_query_op_match(t, query, op->next, final_match, block);
				});
			case QUERY_SORT:
				return _triplestore_sort_fill(t, query, op->ptr, current_match);
			default:
				fprintf(stderr, "Unrecognized query op in _triplestore_query_op_match: %d\n", op->type);
				return 1;
		};
	} else {
		return block(current_match);
	}
}

int triplestore_query_match(triplestore_t* t, query_t* query, int64_t limit, int(^block)(nodeid_t* final_match)) {
// 	triplestore_print_query(t, query, stderr);
	nodeid_t* current_match	= calloc(sizeof(nodeid_t), 1+query->variables);
	current_match[0]	= query->variables;
	query_op_t* op		= query->head;
	int r				= _triplestore_query_op_match(t, query, op, current_match, block);
	free(current_match);
	
	while (1) {
		if (!op) break;
		if (op->type == QUERY_SORT) {
			sort_t* sort	= (sort_t*) op->ptr;
			table_t* table	= sort->table;
			triplestore_table_sort(t, table, sort);
			for (uint32_t row = 0; row < table->used; row++) {
				uint32_t* result	= triplestore_table_row_ptr(table, row);
				_triplestore_query_op_match(t, query, op->next, result, block);
			}
		}
		op	= op->next;
	}
	
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
		for (nodeid_t s = 1; s <= t->nodes_used; s++) {
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
	if (s > t->nodes_used) {
		fprintf(f, "(undefined)");
		if (newline) {
			fprintf(f, "\n");
		}
		return 1;
	}
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
		fprintf(f, "?%s", variable_names[-s]);
	} else {
		triplestore_print_term(t, s, f, 0);
	}
}

void triplestore_print_path(triplestore_t* t, query_t* query, path_t* path, FILE* f) {
	fprintf(f, "Path: ");
	_print_term_or_variable(t, query->variables, query->variable_names, path->start, f);
	fprintf(f, " ");
	_print_term_or_variable(t, query->variables, query->variable_names, path->pred, f);
	fprintf(f, " ");
	_print_term_or_variable(t, query->variables, query->variable_names, path->end, f);
	fprintf(f, "\n");
}

void triplestore_print_project(triplestore_t* t, query_t* query, project_t* project, FILE* f) {
	fprintf(f, "Project:\n");
	for (int i = 0; i <= project->size; i++) {
		if (project->keep[i]) {
			fprintf(f, "  - ?%s\n", query->variable_names[i]);
		}
	}
}

void triplestore_print_sort(triplestore_t* t, query_t* query, sort_t* sort, FILE* f) {
	fprintf(f, "Sort:\n");
	for (int i = 0; i < sort->size; i++) {
		int64_t var	= sort->vars[i];
		fprintf(f, "  - ?%s\n", query->variable_names[-var]);
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
		case FILTER_CONTAINS:
			fprintf(f, "CONTAINS(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ", \"%s\"", filter->string2);
			if (filter->string2_lang) {
				fprintf(f, "@%s", filter->string2_lang);
			}
			fprintf(f, ")\n");
			break;
		case FILTER_STRSTARTS:
			fprintf(f, "STRSTARTS(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ", \"%s\"", filter->string2);
			if (filter->string2_lang) {
				fprintf(f, "@%s", filter->string2_lang);
			}
			fprintf(f, ")\n");
			break;
		case FILTER_STRENDS:
			fprintf(f, "STRENDS(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ", \"%s\"", filter->string2);
			if (filter->string2_lang) {
				fprintf(f, "@%s", filter->string2_lang);
			}
			fprintf(f, ")\n");
			break;
		case FILTER_REGEX:
			fprintf(f, "REGEX(");
			_print_term_or_variable(t, query->variables, query->variable_names, filter->node1, f);
			fprintf(f, ", \"%s\", \"%s\")\n", filter->string2, filter->string3);
			break;
		default:
			fprintf(f, "***UNRECOGNIZED FILTER***\n");
	}
}

void triplestore_print_bgp(triplestore_t* t, bgp_t* bgp, int variables, char** variable_names, FILE* f) {
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
	} else if (op->type == QUERY_PROJECT) {
		triplestore_print_project(t, query, op->ptr, f);
	} else if (op->type == QUERY_SORT) {
		triplestore_print_sort(t, query, op->ptr, f);
	} else if (op->type == QUERY_FILTER) {
		triplestore_print_filter(t, query, op->ptr, f);
	} else if (op->type == QUERY_PATH) {
		triplestore_print_path(t, query, op->ptr, f);
	} else {
		fprintf(stderr, "*** Unrecognized query op %d\n", op->type);
	}
}

void triplestore_print_query(triplestore_t* t, query_t* query, FILE* f) {
	fprintf(f, "--- QUERY ---\n");
	fprintf(f, "- Variables: %d\n", query->variables);
	for (int v = 1; v <= query->variables; v++) {
		fprintf(f, "  - %s\n", query->variable_names[v]);
	}
	
	query_op_t* op	= query->head;
	while (op != NULL) {
		triplestore_print_query_op(t, query, op, f);
		op	= op->next;
	}
	fprintf(f, "----------\n");
}


