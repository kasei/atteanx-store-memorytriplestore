#include <raptor2.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include "avl.h"

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
