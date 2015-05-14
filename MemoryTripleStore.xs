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
handle_new_result_object (triplestore_t* t, SV* closure, const bgp_t* bgp, nodeid_t* match) {
	HV*	hash	= newHV();
	for (int j = 1; j <= bgp->variables; j++) {
		nodeid_t id	= match[j];
		rdf_term_t* term	= t->graph[id]._term;
		SV* object			= rdf_term_to_object(term);
		const char* key		= bgp->variable_names[j];
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
}

void
triplestore_build_struct (SV* self)
	PREINIT:
		triplestore_t *t;
	CODE:
		if (!(t = new_triplestore(268435456, 268435456))) {
			croak("Failed to create new triplestore");
		}
		// triplestore__load_file(t, "/Users/greg/foaf.ttl", 1, 1);
		// fprintf(stderr, "new raptor parser: %p\n", parser);
		xs_object_magic_attach_struct(aTHX_ SvRV(self), t);

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
		term = triplestore_new_term(type, value, NULL);
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
		term = triplestore_new_term(type, value, extra);
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
triplestore_match_bgp_cb(triplestore_t* t, IV triples, IV variables, AV* ids, AV* names, SV* closure)
	INIT:
		int i;
		SV** svp;
		char* ptr;
		bgp_t bgp;
		IV iv;
	CODE:
		bgp.variables		= variables;
		bgp.triples			= triples;
		bgp.variable_names	= calloc(sizeof(char*), variables+1);
		bgp.nodes			= calloc(sizeof(int64_t), 3 * bgp.triples);
		for (i = 1; i <= variables; i++) {
			svp	= av_fetch(names, i, 0);
			ptr = SvPV_nolen(*svp);
			bgp.variable_names[i] = ptr;
// 			fprintf(stderr, "name[%d] = '%s'\n", i, ptr);
		}
		for (i = 0; i < 3*triples; i++) {
			svp	= av_fetch(ids, i, 0);
			iv = SvIV(*svp);
			bgp.nodes[i] = (int64_t) iv;
		}
// 		triplestore_print_bgp(t, &bgp, stderr);
		triplestore_bgp_match(t, &bgp, -1, ^(nodeid_t* final_match){
			handle_new_result_object(t, closure, &bgp, final_match);
			return 0;
		});
		free(bgp.variable_names);
		free(bgp.nodes);

void
triplestore_get_triples_cb(triplestore_t* t, IV s, IV p, IV o, SV* closure)
	CODE:
		triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o){
			handle_new_triple_object(closure, t->graph[s]._term, t->graph[p]._term, t->graph[o]._term);
			return 0;
		});


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

