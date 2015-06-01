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
rdf_term_to_object(rdf_term_t* t) {
	SV* object;
	SV* class;
	SV* string;
	SV* dt;
	SV* value;
	SV* value_key;
	SV* lang_key;
	SV* dt_key;
	switch (t->type) {
		case TERM_IRI:
			class	= newSVpvs("AtteanX::Store::MemoryTripleStore::IRI");
			object	= new_node_instance(aTHX_ class, 0);
			SvREFCNT_dec(class);
			xs_object_magic_attach_struct(aTHX_ SvRV(object), t);
			return object;
		case TERM_BLANK:
			class	= newSVpvs("AtteanX::Store::MemoryTripleStore::Blank");
			object	= new_node_instance(aTHX_ class, 0);
			SvREFCNT_dec(class);
			xs_object_magic_attach_struct(aTHX_ SvRV(object), t);
			return object;
		case TERM_XSDSTRING_LITERAL:
			class		= newSVpvs("Attean::IRI");
			value		= newSVpv("http://www.w3.org/2001/XMLSchema#string", 0);
			dt			= new_node_instance(aTHX_ class, 1, value);
			SvREFCNT_dec(value);
			SvREFCNT_dec(class);

			dt_key		= newSVpvs("datatype");
			value_key	= newSVpvs("value");
			class	= newSVpvs("Attean::Literal");
			string	= newSVpv((const char*) t->value, 0);
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
			string	= newSVpv((const char*) t->value, 0);
			SV* lang	= newSVpv((const char*) t->value_type, 0);
			object	= new_node_instance(aTHX_ class, 4, value_key, string, lang_key, lang);
			SvREFCNT_dec(string);
			SvREFCNT_dec(lang);
			SvREFCNT_dec(lang_key);
			SvREFCNT_dec(value_key);
			return object;
		case TERM_TYPED_LITERAL:
			class		= newSVpvs("Attean::IRI");
			value		= newSVpv((const char*) t->value_type, 0);
			dt			= new_node_instance(aTHX_ class, 1, value);
			SvREFCNT_dec(value);
			SvREFCNT_dec(class);

			dt_key		= newSVpvs("datatype");
			value_key	= newSVpvs("value");
			class	= newSVpvs("Attean::Literal");
			SV* string	= newSVpv((const char*) t->value, 0);
			object	= new_node_instance(aTHX_ class, 4, value_key, string, dt_key, dt);
			SvREFCNT_dec(string);
			SvREFCNT_dec(dt);
			SvREFCNT_dec(dt_key);
			SvREFCNT_dec(value_key);
			return object;
		default:
			fprintf(stderr, "*** unknown node type %d during import\n", t->type);
			return &PL_sv_undef;
	}
}

void
handle_new_triple_object (SV* closure, rdf_term_t* subject, rdf_term_t* predicate, rdf_term_t* object) {
	SV* s	= rdf_term_to_object(subject);
	SV* p	= rdf_term_to_object(predicate);
	SV* o	= rdf_term_to_object(object);
	
	SV* class	= newSVpvs("Attean::Triple");
	SV* t	= new_node_instance(aTHX_ class, 3, s, p, o);
	SvREFCNT_dec(class);
	SvREFCNT_dec(s);
	SvREFCNT_dec(p);
	SvREFCNT_dec(o);
	
// 	fprintf(stderr, "Parsed: %p %p %p\n", triple->subject, triple->predicate, triple->object);
	call_handler_cb(closure, 1, t);
	SvREFCNT_dec(t);
	return;
}

void
handle_new_result_object (triplestore_t* t, SV* closure, int variables, char** variable_names, nodeid_t* match) {
	HV*	hash	= newHV();
	// fprintf(stderr, "constructing result from table:\n");
	for (int j = 1; j <= variables; j++) {
		nodeid_t id			= match[j];
		// fprintf(stderr, "[%d]: %"PRIu32"\n", j, id);
		rdf_term_t* term	= t->graph[id]._term;
		SV* object			= rdf_term_to_object(term);
		const char* key		= variable_names[j];
		hv_store(hash, key, strlen(key), object, 0);
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
triplestore_size(triplestore_t *store)

int
triplestore__term_to_id1(triplestore_t *store, int type, char* value)
	PREINIT:
		rdf_term_t* term;
		nodeid_t id;
	CODE:
		term = triplestore_new_term(type, value, NULL, 0);
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
		term = triplestore_new_term(type, value, extra, 0);
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
		term = triplestore_new_term(type, value, NULL, vid);
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
triplestore_match_bgp_with_filter_cb(triplestore_t* t, IV triples, IV variables, AV* ids, AV* names, AV* filters, SV* closure)
	INIT:
		int i;
		SV** svp;
		char* ptr;
		bgp_t* bgp;
		query_t* query;
		IV iv;
	CODE:
		bgp = triplestore_new_bgp(t, variables, triples);
		query = triplestore_new_query(t, variables);
		for (i = 1; i <= variables; i++) {
			svp	= av_fetch(names, i, 0);
			ptr = SvPV_nolen(*svp);
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
		
		SSize_t len	= av_len(filters);
		if (len > 0) {
			for (i = 0; i < len; i+=2) {
				SV** key	= av_fetch(filters, i+0, 0);
				SV** val	= av_fetch(filters, i+1, 0);
				filter_type_t type	= (filter_type_t) SvIV(*key);
				if (!SvROK(*val) || SvTYPE(SvRV(*val)) != SVt_PVHV) {
					croak("Not a hash in REGEX filter?");
					continue;
				}
				HV* hash	= (HV*) SvRV(*val);
				if (type == FILTER_REGEX) {
					SV** var	= hv_fetch(hash, "variable", 8, 0);
					SV** pat	= hv_fetch(hash, "pattern", 7, 0);
					SV** fl		= hv_fetch(hash, "flags", 5, 0);
					if (!var) {
						croak("No variable in REGEX filter");
						continue;
					}
					if (!pat) {
						croak("No pattern in REGEX filter");
						continue;
					}
					if (!fl) {
						croak("No flags in REGEX filter");
						continue;
					}
					
					int64_t varid	= (int64_t) SvIV(*var);
					char* pattern	= SvPV_nolen(*pat);
					char* flags		= SvPV_nolen(*fl);
					query_filter_t* filter	= triplestore_new_filter(FILTER_REGEX, varid, pattern, flags);
					// fprintf(stderr, "Adding REGEX filter: %s =~ /%s/%s\n", query->variable_names[-varid], pattern, flags);
					triplestore_query_add_op(query, QUERY_FILTER, filter);
				} else if (type == FILTER_ISIRI || type == FILTER_ISLITERAL || type == FILTER_ISBLANK) {
					SV** var	= hv_fetch(hash, "variable", 8, 0);
					if (!var) {
						croak("No variable in ISIRI/ISLITERAL/ISBLANK filter");
						continue;
					}
					int64_t varid	= (int64_t) SvIV(*var);
					query_filter_t* filter	= triplestore_new_filter(type, varid);
					triplestore_query_add_op(query, QUERY_FILTER, filter);
				} else if (type == FILTER_STRSTARTS || type == FILTER_STRENDS) {
					SV** var	= hv_fetch(hash, "variable", 8, 0);
					SV** pat	= hv_fetch(hash, "pattern", 7, 0);
					if (!var) {
						croak("No variable in STSTARTS/STRENDS filter");
						continue;
					}
					if (!pat) {
						croak("No pattern in STSTARTS/STRENDS filter");
						continue;
					}
					int64_t varid	= (int64_t) SvIV(*var);
					char* pattern	= SvPV_nolen(*pat);
					query_filter_t* filter	= triplestore_new_filter(FILTER_STRSTARTS, varid, pattern);
					triplestore_query_add_op(query, QUERY_FILTER, filter);
				} else {
					croak("Unexpected filter type %d", type);
					return;
				}
			}
		}
		
		// triplestore_print_query(t, query, stderr);
		triplestore_query_match(t, query, -1, ^(nodeid_t* final_match){
			handle_new_result_object(t, closure, query->variables, query->variable_names, final_match);
			return 0;
		});
		triplestore_free_query(query);

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
triplestore_match_bgp_cb(triplestore_t* t, IV triples, IV variables, AV* ids, AV* names, int re_var, char* pattern, char* flags, SV* closure)
	INIT:
		int i;
		SV** svp;
		char* ptr;
		bgp_t* bgp;
		query_t* query;
		IV iv;
	CODE:
		bgp = triplestore_new_bgp(t, variables, triples);
		query = triplestore_new_query(t, variables);
		for (i = 1; i <= variables; i++) {
			svp	= av_fetch(names, i, 0);
			ptr = SvPV_nolen(*svp);
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
		
		if (re_var < 0) {
			// fprintf(stderr, "Adding REGEX filter: %s =~ /%s/%s\n", query->variable_names[-re_var], pattern, flags);
			query_filter_t* filter	= triplestore_new_filter(FILTER_REGEX, re_var, pattern, flags);
			triplestore_query_add_op(query, QUERY_FILTER, filter);
		}
		
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
			handle_new_triple_object(closure, t->graph[s]._term, t->graph[p]._term, t->graph[o]._term);
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

