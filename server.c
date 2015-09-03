#include <arpa/inet.h>
#include <sys/socket.h>
#include "triplestore-server.h"

#pragma mark -

void help(FILE* f) {
	fprintf(f, "Commands:\n");
	fprintf(f, "  help\n");
	fprintf(f, "  match PATTERN\n");
	fprintf(f, "  ntriples\n");
	fprintf(f, "  data\n");
	fprintf(f, "  nodes\n");
	fprintf(f, "  edges\n");
	fprintf(f, "  bgp S1 P1 O1 S2 P2 O2 ...\n");
	fprintf(f, "  triple S P O\n");
	fprintf(f, "  filter starts|ends|contains VAR STRING S1 P1 O1 S2 P2 O2 ...\n");
	fprintf(f, "  filter re VAR PATTERN FLAGS S1 P1 O1 S2 P2 O2 ...\n");
	fprintf(f, "  agg GROUPVAR COUNT VAR S1 P1 O1 S2 P2 O2 ...\n");
	fprintf(f, "\n");
}

void usage(int argc, char** argv, FILE* f) {
	fprintf(f, "Usage: %s PORT input.nt\n\n", argv[0]);
}

int main (int argc, char** argv) {
	if (argc > 1 && !strcmp(argv[1], "--help")) {
		usage(argc, argv, stdout);
		help(stdout);
		return 0;
	}
	
	__block int i			= 1;
	short port				= (short) atoi(argv[i++]);
	
	const int max_edges		= 65536;
	const int max_nodes		= 65536;
	triplestore_t* t		= new_triplestore(max_nodes, max_edges);

	__block struct server_runtime_ctx_s ctx	= {
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
		int nt_file		= (ntsuffix && !strcmp(dbsuffix, ".nt"));
		int ttl_file	= (ttlsuffix && !strcmp(dbsuffix, ".ttl"));
		
		
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
	free_triplestore(t);
	return 0;
}
