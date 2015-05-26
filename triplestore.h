#include <raptor2.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <stdarg.h>
#include "avl.h"

typedef uint32_t nodeid_t;

typedef enum {
	TERM_IRI					= 1,
	TERM_BLANK					= 2,
	TERM_XSDSTRING_LITERAL		= 3,
	TERM_LANG_LITERAL			= 4,
	TERM_TYPED_LITERAL			= 5
} rdf_term_type_t;

typedef enum {
	QUERY_BGP,
	QUERY_FILTER,
	QUERY_PATH,
} query_type_t;

typedef enum {
	FILTER_ISIRI,		// ISIRI(?var)
	FILTER_ISLITERAL,	// ISLITERAL(?var)
	FILTER_ISBLANK,		// ISBLANK(?var)
	FILTER_SAMETERM,	// SAMETERM(?var, CONST) or SAMETERM(?var, ?var)
	FILTER_REGEX,		// REGEX(?var, "string", "flags")
	FILTER_LANGMATCHES,	// LANGMATCHES(STR(?var), "string")
	FILTER_CONTAINS,	// CONTAINS(?var, "string")
	FILTER_STRSTARTS,	// STRSTARTS(?var, "string")
	FILTER_STRENDS,		// STRENDS(?var, "string")
    // Numeric logical testing (var, const)
    // Date logical testing (var, const)
} filter_type_t;

typedef struct rdf_term_s {
	rdf_term_type_t type;
	char* value;
	union {
		uint32_t value_id;
		char* value_type;
	};
} rdf_term_t;

typedef struct index_list_element_s {
	uint32_t s;
	uint32_t p;
	uint32_t o;
	uint32_t next_in;
	uint32_t next_out;
} index_list_element_t;

typedef struct graph_node_s {
	rdf_term_t* _term;
	uint64_t mtime;
	
	uint32_t out_degree;
	uint32_t in_degree;
	
	uint32_t out_edge_head;
	uint32_t in_edge_head;
	
} graph_node_t;

typedef struct query_op_s {
	struct query_op_s* next;
	query_type_t type;
	void* ptr;
} query_op_t;

typedef struct query_s {
	int variables;
	char** variable_names;
	query_op_t* head;
	query_op_t* tail;
} query_t;

typedef struct bgp_s {
	int triples;
	int64_t* nodes;
	
	// TODO: remove these, as they will be contained in the enclosing query_t:
// 	int variables;
// 	char** variable_names;
} bgp_t;

typedef struct query_filter_s {
	filter_type_t type;
	int64_t node1;	// var
	int64_t node2;	// var or constant term
	char* string2;	// REGEX pattern, LANGMATCHES language string, CONTAINS, STRENDS, STRSTARTS pattern string
	char* string3; 	// REGEX flags
} query_filter_t;

typedef struct triplestore_s {
	int edges_alloc;
	int edges_used;
	
	int nodes_alloc;
	int nodes_used;
	
	index_list_element_t* edges;
	graph_node_t* graph;
	
	struct avl_table* dictionary;
} triplestore_t;

double triplestore_current_time ( void );
double triplestore_elapsed_time ( double start );

rdf_term_t* triplestore_new_term( rdf_term_type_t type, char* value, char* vtype, nodeid_t vid );
void free_rdf_term(rdf_term_t* t);
int triplestore_size(triplestore_t* t);

char* triplestore_term_to_string(triplestore_t* store, rdf_term_t* t);
triplestore_t* new_triplestore(int max_nodes, int max_edges);
int free_triplestore(triplestore_t* t);
int triplestore_add_triple(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o, uint64_t timestamp);
nodeid_t triplestore_add_term(triplestore_t* t, rdf_term_t* myterm);
nodeid_t triplestore_get_term(triplestore_t* t, rdf_term_t* myterm);

int triplestore__load_file(triplestore_t* t, const char* filename, int print, int verbose);

int triplestore_match_triple(triplestore_t* t, int64_t _s, int64_t _p, int64_t _o, int(^block)(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o));
int triplestore_bgp_match(triplestore_t* t, bgp_t* bgp, int variables, int(^block)(nodeid_t* final_match));

void triplestore_print_bgp(triplestore_t* t, bgp_t* bgp, int variables, char** variable_names, FILE* f);
int triplestore_print_term(triplestore_t* t, nodeid_t s, FILE* f, int newline);

// Queries
query_t* triplestore_new_query(triplestore_t* t, int variables);
int triplestore_free_query(query_t* query);
int triplestore_query_set_variable_name(query_t* query, int variable, const char* name);
int triplestore_query_add_op(query_t* query, query_type_t type, void* ptr);
int triplestore_query_match(triplestore_t* t, query_t* query, int64_t limit, int(^block)(nodeid_t* final_match));

// BGPs
bgp_t* triplestore_new_bgp(triplestore_t* t, int variables, int triples);
int triplestore_free_bgp(bgp_t* bgp);
int triplestore_bgp_set_triple_nodes(bgp_t* bgp, int triple, int64_t s, int64_t p, int64_t o);



query_filter_t* triplestore_new_filter(filter_type_t type, ...);
int triplestore_free_filter(query_filter_t* filter);
void triplestore_print_query(triplestore_t* t, query_t* query, FILE* f);
