#pragma once

#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#include <pcre.h>
#include "linenoise.h"
#include "triplestore.h"

struct command_ctx_s {
	int sandbox;
	int verbose;
	int print;
	int error;
	char* error_message;
	int64_t limit;
	double start;
	query_t* query;
	int constructing;
	char* language;
	void (^set_error)(int code, const char* message);
	void(^result_block)(query_t* query, binding_t* final_match);
	void(^preamble_block)(query_t* query);
};

void help(FILE* f);
int64_t query_node_id(triplestore_t* t, struct command_ctx_s* ctx, query_t* query, const char* ts);
int64_t triplestore_query_get_variable_id_n(query_t* query, const char* var, size_t len);
int64_t triplestore_query_get_variable_id(query_t* query, const char* var);
int triplestore_op(triplestore_t* t, struct command_ctx_s* ctx, int argc, char** argv);
int triplestore_vop(triplestore_t* t, struct command_ctx_s* ctx, int argc, ...);
