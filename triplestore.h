#include <raptor2.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include "avl.h"

typedef struct index_list_element_s {
	uint32_t s;
	uint32_t p;
	uint32_t o;
	uint32_t next;
} index_list_element_t;

typedef struct graph_node_s {
	raptor_term* term;
	uint64_t mtime;
	
	uint32_t out_degree;
	uint32_t in_degree;
	
	uint32_t out_edge_head;
	uint32_t in_edge_head;
	
} graph_node_t;

typedef struct triplestore_s {
	raptor_world* world;
	
	int edges_alloc;
	int edges_used;
	
	int nodes_alloc;
	int nodes_used;
	
	index_list_element_t* out_edges;
	index_list_element_t* in_edges;
	graph_node_t* graph;
	
	struct avl_table* dictionary;
} triplestore_t;
