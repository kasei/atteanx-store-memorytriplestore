#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include <stdio.h>

#include "xs_object_magic.h"
#include "triplestore.h"

static SV *
S_new_instance (pTHX_ HV *klass)
{
	SV *obj, *self;

	obj = (SV *)newHV();
	self = newRV_noinc(obj);
	sv_bless(self, klass);

	return self;
}

static SV *
S_attach_struct (pTHX_ SV *obj, void *ptr)
{
	xs_object_magic_attach_struct(aTHX_ SvRV(obj), ptr);
	return obj;
}

static SV *
new_node_instance (pTHX_ SV *klass, UV n_args, ...)
{
	int count;
	va_list ap;
	SV *ret;
	dSP;

	ENTER;
	SAVETMPS;

	PUSHMARK(SP);
	EXTEND(SP, n_args + 1);
	PUSHs(klass);

	va_start(ap, n_args);
	while (n_args--) {
		PUSHs(va_arg(ap, SV *));
	}
	va_end(ap);

	PUTBACK;

	count = call_method("new", G_SCALAR);

	if (count != 1) {
		croak("Big trouble");
	}

	SPAGAIN;
	ret = POPs;
	SvREFCNT_inc(ret);

	FREETMPS;
	LEAVE;

	return ret;
}

static void
call_handler_cb (pTHX_ SV *closure, UV n_args, ...)
{
	int count;
	va_list ap;
	SV *ret;
	dSP;

	ENTER;
	SAVETMPS;

	PUSHMARK(SP);
	EXTEND(SP, n_args);

	va_start(ap, n_args);
	while (n_args--) {
// 		fprintf(stderr, "pushing argument for callback...\n");
		PUSHs(va_arg(ap, SV *));
	}
	va_end(ap);

	PUTBACK;

	count = call_sv(closure, G_DISCARD | G_VOID);

	if (count != 0) {
		croak("Big trouble");
	}

	SPAGAIN;

	FREETMPS;
	LEAVE;
}

static SV*
rdf_term_to_object(triplestore_t* t, rdf_term_t* term) {
	SV* object;
	SV* class;
	SV* string;
	char* dtvalue;
	SV* dt;
	SV* lang;
	SV* value;
	SV* value_key;
	SV* lang_key;
	SV* dt_key;
	switch (term->type) {
		case TERM_IRI:
			class	= newSVpvs("AtteanX::Store::MemoryTripleStore::IRI");
			object	= new_node_instance(aTHX_ class, 0);
			SvREFCNT_dec(class);
			xs_object_magic_attach_struct(aTHX_ SvRV(object), term);
			return object;
		case TERM_BLANK:
			class	= newSVpvs("AtteanX::Store::MemoryTripleStore::Blank");
			object	= new_node_instance(aTHX_ class, 0);
			SvREFCNT_dec(class);
			xs_object_magic_attach_struct(aTHX_ SvRV(object), term);
			return object;
		case TERM_XSDSTRING_LITERAL:
			class		= newSVpvs("Attean::IRI");
			value		= newSVpvs("http://www.w3.org/2001/XMLSchema#string");
			dt			= new_node_instance(aTHX_ class, 1, value);
			SvREFCNT_dec(value);
			SvREFCNT_dec(class);

			dt_key		= newSVpvs("datatype");
			value_key	= newSVpvs("value");
			class	= newSVpvs("Attean::Literal");
			string	= newSVpv((const char*) term->value, 0);
			object	= new_node_instance(aTHX_ class, 4, value_key, string, dt_key, dt);
			SvREFCNT_dec(string);
			SvREFCNT_dec(dt);
			SvREFCNT_dec(dt_key);
			SvREFCNT_dec(value_key);
			return object;
		case TERM_LANG_LITERAL:
			lang_key		= newSVpvs("language");
			value_key	= newSVpvs("value");
			class	= newSVpvs("Attean::Literal");
			string	= newSVpv((const char*) term->value, 0);
			lang	= newSVpv((const char*) term->vtype.value_type, 0);
			object	= new_node_instance(aTHX_ class, 4, value_key, string, lang_key, lang);
			SvREFCNT_dec(string);
			SvREFCNT_dec(lang);
			SvREFCNT_dec(lang_key);
			SvREFCNT_dec(value_key);
			return object;
		case TERM_TYPED_LITERAL:
			class		= newSVpvs("Attean::IRI");
			dtvalue		= t->graph[ term->vtype.value_id ]._term->value;
			value		= newSVpv(dtvalue, strlen(dtvalue));
			dt			= new_node_instance(aTHX_ class, 1, value);
			SvREFCNT_dec(value);
			SvREFCNT_dec(class);

			dt_key		= newSVpvs("datatype");
			value_key	= newSVpvs("value");
			class		= newSVpvs("Attean::Literal");
			string		= newSVpv((const char*) term->value, 0);
			object		= new_node_instance(aTHX_ class, 4, value_key, string, dt_key, dt);
			SvREFCNT_dec(string);
			SvREFCNT_dec(dt);
			SvREFCNT_dec(dt_key);
			SvREFCNT_dec(value_key);
			return object;
		default:
			fprintf(stderr, "*** unknown node type %d during import\n", term->type);
			return &PL_sv_undef;
	}
}

int64_t _triplestore_query_get_variable_id(query_t* query, const char* var) {
	int64_t v	= 0;
	char* p		= (char*) var;
	if (p[0] == '?') {
		p++;
	}
	for (int x = 1; x <= query->variables; x++) {
		if (!strcmp(p, query->variable_names[x])) {
			v	= -x;
			break;
		}
	}
	return v;
}


void
handle_new_triple_object (triplestore_t* t, SV* closure, rdf_term_t* subject, rdf_term_t* predicate, rdf_term_t* object) {
	SV* s	= rdf_term_to_object(t, subject);
	SV* p	= rdf_term_to_object(t, predicate);
	SV* o	= rdf_term_to_object(t, object);
	
	SV* class	= newSVpvs("Attean::Triple");
	SV* triple	= new_node_instance(aTHX_ class, 3, s, p, o);
	SvREFCNT_dec(class);
	SvREFCNT_dec(s);
	SvREFCNT_dec(p);
	SvREFCNT_dec(o);
	
// 	fprintf(stderr, "Parsed: %p %p %p\n", triple->subject, triple->predicate, triple->object);
	call_handler_cb(closure, 1, t);
	SvREFCNT_dec(triple);
	return;
}

void
handle_new_result_object (triplestore_t* t, SV* closure, int variables, char** variable_names, nodeid_t* match) {
	HV*	hash	= newHV();
	// fprintf(stderr, "constructing result from table:\n");
	for (int j = 1; j <= variables; j++) {
		nodeid_t id			= match[j];
		if (id > 0) {
			// fprintf(stderr, "[%d]: %"PRIu32"\n", j, id);
			rdf_term_t* term	= t->graph[id]._term;
			SV* object			= rdf_term_to_object(t, term);
			const char* key		= variable_names[j];
			hv_store(hash, key, strlen(key), object, 0);
		}
	}
	
	SV* hashref	= newRV_inc((SV*) hash);
	call_handler_cb(closure, 1, hashref);
	return;
}

#define new_instance(klass)	 S_new_instance(aTHX_ klass)
#define attach_struct(obj, ptr)	 S_attach_struct(aTHX_ obj, ptr)
#define EXPORT_FLAG(flag)  newCONSTSUB(stash, #flag, newSVuv(flag))

MODULE = AtteanX::Store::MemoryTripleStore	PACKAGE = AtteanX::Store::MemoryTripleStore	PREFIX = triplestore_

PROTOTYPES: DISABLE

BOOT:
{
	HV *stash = gv_stashpvs("AtteanX::Store::MemoryTripleStore", 0);
	EXPORT_FLAG(TERM_IRI);
	EXPORT_FLAG(TERM_BLANK);
	EXPORT_FLAG(TERM_XSDSTRING_LITERAL);
	EXPORT_FLAG(TERM_LANG_LITERAL);
	EXPORT_FLAG(TERM_TYPED_LITERAL);
	EXPORT_FLAG(FILTER_ISIRI);
	EXPORT_FLAG(FILTER_ISLITERAL);
	EXPORT_FLAG(FILTER_ISBLANK);
	EXPORT_FLAG(FILTER_ISNUMERIC);
	EXPORT_FLAG(FILTER_SAMETERM);
	EXPORT_FLAG(FILTER_REGEX);
	EXPORT_FLAG(FILTER_LANGMATCHES);
	EXPORT_FLAG(FILTER_CONTAINS);
	EXPORT_FLAG(FILTER_STRSTARTS);
	EXPORT_FLAG(FILTER_STRENDS);
	EXPORT_FLAG(PATH_PLUS);
	EXPORT_FLAG(PATH_STAR);
}

int
triplestore_build_struct (SV* self)
	PREINIT:
		triplestore_t *t;
	CODE:
		if (!(t = new_triplestore(65536, 65536))) {
			croak("Failed to create new triplestore");
			RETVAL = 1;
		} else {
			// triplestore__load_file(t, "/Users/greg/foaf.ttl", 1, 1);
			// fprintf(stderr, "new raptor parser: %p\n", parser);
			xs_object_magic_attach_struct(aTHX_ SvRV(self), t);
			RETVAL = 0;
		}
	OUTPUT:
		RETVAL

int
triplestore__load_file(triplestore_t *store, char* filename, int print, int verbose)

int
triplestore_load(triplestore_t *store, char* filename)

int
triplestore_size(triplestore_t *store)

int
triplestore__term_to_id1(triplestore_t *store, int type, char* value)
	PREINIT:
		rdf_term_t* term;
		nodeid_t id;
	CODE:
		term = triplestore_new_term(store, type, value, NULL, 0);
		id = triplestore_get_term(store, term);
		RETVAL = (int) id;
	OUTPUT:
		RETVAL

int
triplestore__term_to_id2(triplestore_t *store, int type, char* value, char* extra)
	PREINIT:
		rdf_term_t* term;
		nodeid_t id;
	CODE:
		term = triplestore_new_term(store, type, value, extra, 0);
		id = triplestore_get_term(store, term);
		RETVAL = (int) id;
	OUTPUT:
		RETVAL

int
triplestore__term_to_id3(triplestore_t *store, int type, char* value, int vid)
	PREINIT:
		rdf_term_t* term;
		nodeid_t id;
	CODE:
		term = triplestore_new_term(store, type, value, NULL, vid);
		id = triplestore_get_term(store, term);
		RETVAL = (int) id;
	OUTPUT:
		RETVAL

void
triplestore_DESTROY (triplestore_t *store)
	CODE:
//		 fprintf(stderr, "destroying triplestore: %p\n", store);
	  free_triplestore(store);

void
triplestore_match_path_cb(triplestore_t* t, IV path_type, IV variables, AV* ids, AV* names, SV* closure)
	INIT:
		int i;
		SV **svs, **svp, **svo;
		char* ptr;
		path_t* path;
		query_t* query;
		IV iv;
	CODE:
		query = triplestore_new_query(t, variables);
		for (i = 1; i <= variables; i++) {
			svp	= av_fetch(names, i, 0);
			ptr = SvPV_nolen(*svp);
			triplestore_query_set_variable_name(query, i, ptr);
		}
		svs			= av_fetch(ids, 0, 0);
		svp			= av_fetch(ids, 1, 0);
		svo			= av_fetch(ids, 2, 0);
		int64_t s	= (int64_t) SvIV(*svs);
		int64_t p	= (int64_t) SvIV(*svp);
		int64_t o	= (int64_t) SvIV(*svo);
		
		path		= triplestore_new_path(t, (path_type_t) path_type, s, (nodeid_t) p, o);		
		triplestore_query_add_op(query, QUERY_PATH, path);
		
		// triplestore_print_query(t, query, stderr);
		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
			handle_new_result_object(t, closure, query->variables, query->variable_names, final_match);
			return 0;
		});
		triplestore_free_query(query);

void
triplestore_get_triples_cb(triplestore_t* t, IV s, IV p, IV o, SV* closure)
	CODE:
		triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o){
			handle_new_triple_object(t, closure, t->graph[s]._term, t->graph[p]._term, t->graph[o]._term);
			return 0;
		});

int
triplestore__count_triples(triplestore_t* t, IV s, IV p, IV o)
	INIT:
		__block int count;
	CODE:
		count = 0;
		triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o){
			count++;
			return 0;
		});
		RETVAL = count;
	OUTPUT:
		RETVAL

void
triplestore_print_query(triplestore_t* t, query_t* query)
	CODE:
		triplestore_print_query(t, query, stdout);


MODULE = AtteanX::Store::MemoryTripleStore PACKAGE = AtteanX::Store::MemoryTripleStore::Query PREFIX = query_

int
query_build_struct (SV* self, triplestore_t* t)
	PREINIT:
		query_t *q;
	CODE:
		if (!(q = triplestore_new_query(t, 0))) {
			croak("Failed to create new query");
			RETVAL = 1;
		} else {
			xs_object_magic_attach_struct(aTHX_ SvRV(self), q);
			RETVAL = 0;
		}
	OUTPUT:
		RETVAL

void
query_DESTROY (query_t* query)
	CODE:
//		 fprintf(stderr, "destroying query: %p\n", query);
	  triplestore_free_query(query);

void
query__evaluate(query_t* query, triplestore_t* t, SV* closure)
	CODE:
		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
			handle_new_result_object(t, closure, query->variables, query->variable_names, final_match);
			return 0;
		});

int
query__add_filter (query_t* query, triplestore_t* t, char* var_name, char* op, char* pat, char* flags)
	INIT:
		int i, j, svars;
		SV** svp;
		char* ptr;
		int64_t var;
		query_filter_t* filter;
	CODE:
		var	= _triplestore_query_get_variable_id(query, var_name);
		if (!strcmp(op, "isiri")) {
			filter	= triplestore_new_filter(FILTER_ISIRI, var);
		} else if (!strcmp(op, "isblank")) {
			filter	= triplestore_new_filter(FILTER_ISBLANK, var);
		} else if (!strcmp(op, "isliteral")) {
			filter	= triplestore_new_filter(FILTER_ISLITERAL, var);
		} else if (!strcmp(op, "isnumeric")) {
			filter	= triplestore_new_filter(FILTER_ISNUMERIC, var);
		} else if (!strcmp(op, "starts")) {
			filter	= triplestore_new_filter(FILTER_STRSTARTS, var, pat);
		} else if (!strcmp(op, "ends")) {
			filter	= triplestore_new_filter(FILTER_STRENDS, var, pat);
		} else if (!strcmp(op, "contains")) {
			filter	= triplestore_new_filter(FILTER_CONTAINS, var, pat);
		} else if (!strncmp(op, "re", 2)) {
			filter	= triplestore_new_filter(FILTER_REGEX, var, pat, flags);
		} else {
			RETVAL = 1;
			return;
		}
		RETVAL = triplestore_query_add_op(query, QUERY_FILTER, filter);
	OUTPUT:
		RETVAL	

int
query__add_project (query_t* query, triplestore_t* t, AV* names)
	INIT:
		int i, j, svars;
		SV** svp;
		char* ptr;
	CODE:
		project_t* project	= triplestore_new_project(t, query->variables);
		svars	= 1 + av_top_index(names);
		for (int j = 0; j < svars; j++) {
			svp	= av_fetch(names, j, 0);
			ptr = SvPV_nolen(*svp);
			int64_t v	= _triplestore_query_get_variable_id(query, ptr);
			if (v == 0) {
				RETVAL = 1;
				return;
			}
			triplestore_set_projection(project, v);
		}
		RETVAL = triplestore_query_add_op(query, QUERY_PROJECT, project);
	OUTPUT:
		RETVAL	

int
query__add_sort (query_t* query, triplestore_t* t, AV* names, int unique)
	INIT:
		int i, j, svars;
		SV** svp;
		char* ptr;
	CODE:
		svars	= 1 + av_top_index(names);
		sort_t* sort	= triplestore_new_sort(t, query->variables, svars, unique);
		for (int j = 0; j < svars; j++) {
			svp	= av_fetch(names, j, 0);
			ptr = SvPV_nolen(*svp);
			int64_t v	= _triplestore_query_get_variable_id(query, ptr);
			if (v == 0) {
				RETVAL = 1;
				return;
			}
			triplestore_set_sort(sort, j, v);
		}
		RETVAL = triplestore_query_add_op(query, QUERY_SORT, sort);
	OUTPUT:
		RETVAL	

void
query__add_bgp (query_t* query, triplestore_t* t, IV triples, IV variables, AV* ids, AV* names)
	INIT:
		int i;
		SV** svp;
		char* ptr;
		bgp_t* bgp;
		IV iv;
	CODE:
		bgp = triplestore_new_bgp(t, variables, triples);
		for (i = 1; i <= variables; i++) {
			svp	= av_fetch(names, i, 0);
			ptr = SvPV_nolen(*svp);
			triplestore_ensure_variable_capacity(query, i);
			triplestore_query_set_variable_name(query, i, ptr);
		}
		for (i = 0; i < triples; i++) {
			SV **svs, **svp, **svo;
			svs			= av_fetch(ids, 3*i+0, 0);
			svp			= av_fetch(ids, 3*i+1, 0);
			svo			= av_fetch(ids, 3*i+2, 0);
			int64_t s	= (int64_t) SvIV(*svs);
			int64_t p	= (int64_t) SvIV(*svp);
			int64_t o	= (int64_t) SvIV(*svo);
			triplestore_bgp_set_triple_nodes(bgp, i, s, p, o);
		}
		triplestore_query_add_op(query, QUERY_BGP, bgp);


MODULE = AtteanX::Store::MemoryTripleStore PACKAGE = AtteanX::Store::MemoryTripleStore::IRI PREFIX = rdf_term_iri_

SV*
rdf_term_iri_value (rdf_term_t* term)
	CODE:
// 		fprintf(stderr, "rdf_term_iri_value called\n");
		RETVAL = newSVpv((const char*) term->value, 0);
	OUTPUT:
		RETVAL

MODULE = AtteanX::Store::MemoryTripleStore PACKAGE = AtteanX::Store::MemoryTripleStore::Blank PREFIX = rdf_term_blank_

SV*
rdf_term_blank_value (rdf_term_t* term)
	CODE:
// 		fprintf(stderr, "rdf_term_blank_value called\n");
		RETVAL = newSVpv((const char*) term->value, 0);
	OUTPUT:
		RETVAL

