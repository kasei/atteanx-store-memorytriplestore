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
call_triple_handler_cb (pTHX_ SV *closure, UV n_args, ...)
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
			class	= newSVpvs("AtteanX::Store::MemoryTriplestore::IRI");
			object	= new_node_instance(aTHX_ class, 0);
			SvREFCNT_dec(class);
			xs_object_magic_attach_struct(aTHX_ SvRV(object), t);
			return object;
		case TERM_BLANK:
			class	= newSVpvs("AtteanX::Store::MemoryTriplestore::Blank");
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
	call_triple_handler_cb(closure, 1, t);
	SvREFCNT_dec(t);
	return;
}

#define new_instance(klass)	 S_new_instance(aTHX_ klass)
#define attach_struct(obj, ptr)	 S_attach_struct(aTHX_ obj, ptr)

MODULE = AtteanX::Store::MemoryTriplestore	PACKAGE = AtteanX::Store::MemoryTriplestore	PREFIX = triplestore_

PROTOTYPES: DISABLE

void
triplestore_build_struct (SV* self)
	PREINIT:
		triplestore_t *t;
	CODE:
		if (!(t = new_triplestore(268435456, 268435456))) {
			croak("Failed to create new triplestore");
		}
		triplestore_load_file(t, "/Users/greg/foaf.ttl", 1, 1);
//		fprintf(stderr, "new raptor parser: %p\n", parser);
		xs_object_magic_attach_struct(aTHX_ SvRV(self), t);

void
triplestore_DESTROY (triplestore_t *store)
	CODE:
//		 fprintf(stderr, "destroying triplestore: %p\n", store);
	  free_triplestore(store);

void
triplestore_get_triples_cb(triplestore_t* t, IV s, IV p, IV o, SV* closure)
	CODE:
		triplestore_match_triple(t, s, p, o, ^(triplestore_t* t, nodeid_t s, nodeid_t p, nodeid_t o){
			handle_new_triple_object(closure, t->graph[s]._term, t->graph[p]._term, t->graph[o]._term);
			return 0;
		});


MODULE = AtteanX::Store::MemoryTriplestore PACKAGE = AtteanX::Store::MemoryTriplestore::IRI PREFIX = rdf_term_iri_

SV*
rdf_term_iri_value (rdf_term_t* term)
	CODE:
// 		fprintf(stderr, "rdf_term_iri_value called\n");
		RETVAL = newSVpv((const char*) term->value, 0);
	OUTPUT:
		RETVAL

MODULE = AtteanX::Store::MemoryTriplestore PACKAGE = AtteanX::Store::MemoryTriplestore::Blank PREFIX = rdf_term_blank_

SV*
rdf_term_blank_value (rdf_term_t* term)
	CODE:
// 		fprintf(stderr, "rdf_term_blank_value called\n");
		RETVAL = newSVpv((const char*) term->value, 0);
	OUTPUT:
		RETVAL

