#pragma once

#include <pcre.h>
#include <raptor2.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <stdarg.h>
#include "avl.h"

typedef uint32_t nodeid_t;
typedef uint64_t binding_t;

typedef enum {
	TERM_IRI					= 1,
	TERM_BLANK					= 2,
	TERM_XSDSTRING_LITERAL		= 3,
	TERM_LANG_LITERAL			= 4,
	TERM_TYPED_LITERAL			= 5,
	TERM_VARIABLE				= 99,
} rdf_term_type_t;

typedef enum {
	QUERY_BGP					= 1,
	QUERY_FILTER				= 2,
	QUERY_PATH					= 3,
	QUERY_PROJECT				= 4,
	QUERY_SORT					= 5,
} query_type_t;

typedef enum {
	PATH_PLUS,
	PATH_STAR,
} path_type_t;

typedef enum {
	FILTER_ISIRI = 1,	// ISIRI(?var)
	FILTER_ISLITERAL,	// ISLITERAL(?var)
	FILTER_ISBLANK,		// ISBLANK(?var)
	FILTER_ISNUMERIC,	// ISNUMERIC(?var)
	FILTER_SAMETERM,	// SAMETERM(?var, CONST) or SAMETERM(?var, ?var)
	FILTER_REGEX,		// REGEX(?var, "string", "flags")
	FILTER_LANGMATCHES,	// LANGMATCHES(STR(?var), "string")
	FILTER_CONTAINS,	// CONTAINS(?var, "string")
	FILTER_STRSTARTS,	// STRSTARTS(?var, "string")
	FILTER_STRENDS,		// STRENDS(?var, "string")
    // Numeric logical testing (var, const)
    // Date logical testing (var, const)
} filter_type_t;

typedef struct table_s {
	uint32_t alloc;
	uint32_t used;
	uint32_t width;
	binding_t* ptr;
} table_t;

typedef struct rdf_term_s {
	char* value;
	union {
		struct {
			double numeric_value;
			nodeid_t value_id;
			char is_numeric;
		} value_type;
		int64_t value_lang;	// depending on the term type, instead of a value_id, we might pack a 1-7 char string plus trailing NULL into the value_lang integer (e.g. the language tag string for TERM_LANG_LITERAL)
	} vtype;
	rdf_term_type_t type;
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
	double __padding;
	int max_variables;
	char** variable_names;
	query_op_t* head;
	query_op_t* tail;
} query_t;

typedef struct bgp_s {
	int triples;
	int64_t* nodes;
} bgp_t;

typedef struct path_s {
	path_type_t type;
	int64_t start;
	int64_t end;
	nodeid_t pred;
} path_t;

typedef struct project_s {
	int size;
	char* keep;
} project_t;

typedef struct sort_s {
	int size;
	int unique;
	int64_t* vars;
	table_t* table;
} sort_t;

typedef struct query_filter_s {
	filter_type_t type;
	int64_t node1;	// var
	int64_t node2;	// var or constant term
	char* string2;	// REGEX pattern, LANGMATCHES language string, CONTAINS, STRENDS, STRSTARTS pattern string
	rdf_term_type_t string2_type;	// the type of the string argument (TERM_XSDSTRING_LITERAL or TERM_LANG_LITERAL)
	char* string2_lang;				// the language of the string argument (where string2_type == TERM_LANG_LITERAL)
	char* string3; 	// REGEX flags
	pcre* re;		// compile pcre object
} query_filter_t;

typedef struct triplestore_s {
	int read_only;
	
	uint32_t edges_alloc;
	uint32_t edges_used;
	
	uint32_t nodes_alloc;
	uint32_t nodes_used;
	
	index_list_element_t* edges;
	graph_node_t* graph;
	
	struct avl_table* dictionary;
	
	
	pcre* decimal_re;
	pcre* integer_re;
	pcre* float_re;
	pcre* date_re;
	pcre* datetime_re;
	pcre* lang_re;
	
	int verify_datatypes;
	nodeid_t bnode_prefix;
} triplestore_t;

double triplestore_current_time ( void );
double triplestore_elapsed_time ( double start );

rdf_term_t* triplestore_new_term(triplestore_t* t, rdf_term_type_t type, char* value, char* vtype, nodeid_t vid);
rdf_term_t* triplestore_new_term_n(triplestore_t* t, rdf_term_type_t type, const char* value, size_t value_len, const char* vtype, size_t vtype_len, nodeid_t vid);
rdf_term_t* triplestore_get_term(triplestore_t* t, nodeid_t id);
void free_rdf_term(rdf_term_t* t);
int triplestore_size(triplestore_t* t);

int triplestore_term_get_value(rdf_term_t* t, int(^block)(size_t, const char*));
char* triplestore_term_to_string(triplestore_t* store, rdf_term_t* t);
triplestore_t* new_triplestore(int max_nodes, int max_edges);
int free_triplestore(triplestore_t* t);
int triplestore_add_triple(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o, uint64_t timestamp);
nodeid_t triplestore_add_term(triplestore_t* t, rdf_term_t* myterm);
nodeid_t triplestore_get_termid(triplestore_t* t, rdf_term_t* myterm);
int triplestore_set_read_only(triplestore_t* t);
int triplestore_read_only(triplestore_t* t);

int triplestore_dump(triplestore_t* t, const char* filename);
int triplestore_load(triplestore_t* t, const char* filename, int verbose);


int triplestore__load_file(triplestore_t* t, const char* filename, int verbose);

int triplestore_match_triple(triplestore_t* t, int64_t _s, int64_t _p, int64_t _o, int(^block)(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o));
int triplestore_bgp_match(triplestore_t* t, bgp_t* bgp, int variables, int(^block)(binding_t* final_match));

void triplestore_print_bgp(triplestore_t* t, bgp_t* bgp, int variables, char** variable_names, FILE* f);
int triplestore_print_term(triplestore_t* t, nodeid_t s, FILE* f, int newline);

// Queries
query_t* triplestore_new_query(triplestore_t* t, int variables);
int triplestore_free_query(query_t* query);
int triplestore_query_set_variable_name(query_t* query, int variable, const char* name);
int triplestore_ensure_variable_capacity(query_t* query, int var);
int64_t triplestore_query_add_variable(query_t* query, const char* name);
int64_t triplestore_query_add_variable_n(query_t* query, const char* name, size_t name_len);
int triplestore_query_add_op(query_t* query, query_type_t type, void* ptr);
int triplestore_query_match(triplestore_t* t, query_t* query, int64_t limit, int(^block)(binding_t* final_match));
int triplestore_query_get_max_variables(query_t* query);
void triplestore_print_query(triplestore_t* t, query_t* query, FILE* f);
void triplestore_query_as_string_chunks(triplestore_t* t, query_t* query, void(^cb)(const char* line, size_t len));

// BGPs
bgp_t* triplestore_new_bgp(triplestore_t* t, int variables, int triples);
int triplestore_free_bgp(bgp_t* bgp);
int triplestore_bgp_set_triple_nodes(bgp_t* bgp, int triple, int64_t s, int64_t p, int64_t o);

// Paths
path_t* triplestore_new_path(triplestore_t* t, path_type_t type, int64_t start, nodeid_t pred, int64_t end);
int triplestore_free_path(path_t* path);
int triplestore_path_match(triplestore_t* t, path_t* path, int variables, int(^block)(binding_t* final_match));

// Filters
query_filter_t* triplestore_new_filter(filter_type_t type, ...);
int triplestore_free_filter(query_filter_t* filter);

// Sorting
sort_t* triplestore_new_sort(triplestore_t* t, int result_width, int variables, int unique);
int triplestore_free_sort(sort_t* sort);
int triplestore_set_sort(sort_t* sort, int rank, int64_t var);

// Result Tables
table_t* triplestore_new_table(int width);
int triplestore_free_table(table_t* table);
int triplestore_table_add_row(table_t* table, binding_t* result);
int triplestore_table_sort(triplestore_t* t, table_t* table, sort_t* sort);
binding_t* triplestore_table_row_ptr(table_t* table, int row);

// Projection
project_t* triplestore_new_project(triplestore_t* t, int variables);
int triplestore_free_project(project_t* project);
int triplestore_set_projection(project_t* project, int64_t var);
