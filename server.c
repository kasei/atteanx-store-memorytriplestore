#include <arpa/inet.h>
#include <sys/socket.h>
#include "commands.h"
#include "triplestore-server.h"

#pragma mark -

void usage(int argc, char** argv, FILE* f) {
	fprintf(f, "Usage: %s PORT input.nt\n\n", argv[0]);
}

int main (int argc, char** argv) {
	if (argc > 1 && !strcmp(argv[1], "--help")) {
		usage(argc, argv, stdout);
		return 0;
	}
	
	__block int i			= 1;
	short port				= (short) atoi(argv[i++]);
	
	const int max_edges		= 65536;
	const int max_nodes		= 65536;
	triplestore_t* t		= new_triplestore(max_nodes, max_edges);

	__block struct command_ctx_s ctx	= {
		.error				= 0,
		.start				= triplestore_current_time(),
		.constructing		= 0,
		.query				= NULL,
	};

	if (i < argc) {
		const char* filename	= argv[i++];
		const char* dbsuffix	= strstr(filename, ".db");
		const char* ntsuffix	= strstr(filename, ".nt");
		const char* ttlsuffix	= strstr(filename, ".ttl");
		
		int db_file		= (dbsuffix && !strcmp(dbsuffix, ".db"));
		int nt_file		= (ntsuffix && !strcmp(ntsuffix, ".nt"));
		int ttl_file	= (ttlsuffix && !strcmp(ttlsuffix, ".ttl"));
		
		
		if (db_file) {
			fprintf(stderr, "loading file %s\n", filename);
			triplestore_load(t, filename, 0);
			fprintf(stderr, "done\n");
		} else if (nt_file || ttl_file) {
			fprintf(stderr, "loading file %s\n", filename);
			triplestore__load_file(t, filename, 0);
			fprintf(stderr, "done\n");
			if (ctx.error) {
				return 1;
			}
		} else {
			i--;
		}
	}
	
	int use_http	= 1;
	triplestore_server_t* server	= triplestore_new_server(port, use_http);
	signal(SIGPIPE, SIG_IGN);
	triplestore_run_server(server, t);
	triplestore_free_server(server);
	free_triplestore(t);
	return 0;
}
