#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#include <pcre.h>
#include "linenoise.h"
#include "triplestore.h"
#include "commands.h"

#pragma mark -

#pragma mark -

void usage(int argc, char** argv, FILE* f) {
	fprintf(f, "Usage: %s [-v] [-p] input.nt OP\n\n", argv[0]);
}

int main (int argc, char** argv) {
	if (argc > 1 && !strcmp(argv[1], "--help")) {
		usage(argc, argv, stdout);
		help(stdout);
		return 0;
	}
	
	char* linenoiseHistoryFile	= ".triplestore-history";
	linenoiseHistoryLoad(linenoiseHistoryFile);

	int max_edges		= 65536;
	int max_nodes		= 65536;
	triplestore_t* t	= new_triplestore(max_nodes, max_edges);

	__block struct command_ctx_s ctx	= {
		.limit				= -1,
		.verbose			= 0,
		.error				= 0,
		.print				= 1,
		.start				= triplestore_current_time(),
		.constructing		= 0,
		.language			= NULL,
		.query				= NULL,
		.set_error			= ^(int code, const char* message){
			fprintf(stderr, "%s\n", message);
		},
		.result_block		= ^(query_t* query, nodeid_t* final_match){
			for (int j = 1; j <= query->variables; j++) {
				nodeid_t id	= final_match[j];
				if (id > 0) {
					fprintf(stdout, "%s=", query->variable_names[j]);
					triplestore_print_term(t, id, stdout, 0);
					fprintf(stdout, " ");
				}
			}
			fprintf(stdout, "\n");
		},
	};

	__block int i	= 1;
	while (i < argc && argv[i][0] == '-') {
		const char* flag	= argv[i++];
		if (!strncmp(flag, "-v", 2)) {
			ctx.verbose++;
		} else if (!strncmp(flag, "-p", 2)) {
			ctx.print++;
		}
	}

	if (i < argc) {
		const char* filename	= argv[i++];
		const char* suffix		= strstr(filename, ".db");
		if (suffix && !strcmp(suffix, ".db")) {
			triplestore_load(t, filename, ctx.verbose);
		} else {
			if (ctx.verbose) {
				fprintf(stderr, "Importing %s\n", filename);
			}
	
			triplestore__load_file(t, filename, ctx.verbose);
			if (ctx.error) {
				return 1;
			}
		}
	}

	triplestore_op(t, &ctx, argc-i, &(argv[i]));

	if (0) {
		fprintf(stderr, "Running test op sequence...\n");
		triplestore_vop(t, &ctx, 2, "load", "test.db");
		triplestore_vop(t, &ctx, 2, "import", "test2.ttl");
// 		triplestore_vop(t, &ctx, 1, "ntriples");
// 		triplestore_vop(t, &ctx, 1, "begin");
// 		triplestore_vop(t, &ctx, 4, "bgp", "s", "p", "o");
// 		triplestore_vop(t, &ctx, 2, "project", "p");
// 		triplestore_vop(t, &ctx, 1, "unique");
// 		triplestore_vop(t, &ctx, 1, "end");
// 		exit(0);
	}
	
	
	char* line;
	while ((line = linenoise("ts> ")) != NULL) {
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
		if (!triplestore_op(t, &ctx, argc, argv)) {
			if (strlen(line) > 0) {
				linenoiseHistoryAdd(line);
			}
		}
		free(buffer);
	}
	linenoiseHistorySave(linenoiseHistoryFile);
	
	if (ctx.language) {
		free(ctx.language);
	}
	free_triplestore(t);
	return 0;
}

