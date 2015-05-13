#include <raptor2.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include "avl.h"

typedef uint32_t nodeid_t;

typedef enum {
	TERM_IRI					= 1,
	TERM_BLANK					= 2,
	TERM_XSDSTRING_LITERAL		= 3,
	TERM_LANG_LITERAL			= 4,
	TERM_TYPED_LITERAL			= 5
} rdf_term_type_t;

typedef struct rdf_term_s {
	rdf_term_type_t type;
	char* value;
	char* value_type;
} rdf_term_t;

typedef struct index_list_element_s {
	uint32_t s;
	uint32_t p;
	uint32_t o;
	uint32_t next;
} index_list_element_t;

typedef struct graph_node_s {
	rdf_term_t* _term;
	uint64_t mtime;
	
	uint32_t out_degree;
	uint32_t in_degree;
	
	uint32_t out_edge_head;
	uint32_t in_edge_head;
	
} graph_node_t;

typedef struct bgp_s {
	int triples;
	int64_t* nodes;
	int variables;
	char** variable_names;
} bgp_t;

typedef struct triplestore_s {
	int edges_alloc;
	int edges_used;
	
	int nodes_alloc;
	int nodes_used;
	
	index_list_element_t* out_edges;
	index_list_element_t* in_edges;
	graph_node_t* graph;
	
	struct avl_table* dictionary;
} triplestore_t;

double triplestore_current_time ( void );
double triplestore_elapsed_time ( double start );

rdf_term_t* triplestore_new_term( rdf_term_type_t type, char* value, char* vtype );
void free_rdf_term(rdf_term_t* t);

char* triplestore_term_to_string(rdf_term_t* t);
triplestore_t* new_triplestore(int max_nodes, int max_edges);
int free_triplestore(triplestore_t* t);
int triplestore_add_triple(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o, uint64_t timestamp);
nodeid_t triplestore_add_term(triplestore_t* t, rdf_term_t* myterm);
nodeid_t triplestore_get_term(triplestore_t* t, rdf_term_t* myterm);

int triplestore_load_file(triplestore_t* t, const char* filename, int print, int verbose);

int triplestore_match_triple(triplestore_t* t, int64_t _s, int64_t _p, int64_t _o, int(^block)(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o));
int triplestore_bgp_match(triplestore_t* t, bgp_t* bgp, int64_t limit, int(^block)(nodeid_t* final_match));

void triplestore_print_bgp(triplestore_t* t, bgp_t* bgp, FILE* f);
int triplestore_print_term(triplestore_t* t, nodeid_t s, FILE* f, int newline);
