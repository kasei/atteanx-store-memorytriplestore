use Test::Roo;
use Test::More;
use Test::Exception;
use File::Temp qw(tempfile);

use v5.14;
use warnings;
no warnings 'redefine';

use Attean;

sub create_store {
	my $self	= shift;
	my %args	= @_;
	my $triples	= $args{triples};
	my $store	= Attean->get_store('MemoryTripleStore')->new();
	my ($fh, $filename)	= tempfile(SUFFIX => '.nt');
	my $s		= Attean->get_serializer('NTriples')->new();
	my $iter	= Attean::ListIterator->new(values => $triples, item_type => 'Attean::API::Triple');
	$s->serialize_iter_to_io($fh, $iter);
	close($fh);
	$store->load_file($filename);
	unlink($filename);
	return $store;
}

with 'Test::Attean::TripleStore';


test 'match_bgp' => sub {
	my $self	= shift;
	my @triples;
	{
		my $t1		= triple(iri('http://example.org/s'), iri('http://example.org/p'), iri('http://example.org/o'));
		my $t2		= triple(iri('http://example.org/x'), iri('http://example.org/y'), iri('http://example.org/z'));
		push(@triples, $t1, $t2);
		foreach (1,10,20,50) {
			push(@triples, triple(iri('http://example.org/z'), iri('http://example.org/p'), literal($_)));
		}
	}
	my $store	= $self->create_store(triples => \@triples);

	{
		note('1-triple BGP');
		my $t1		= Attean::TriplePattern->new(variable('s'), iri('http://example.org/p'), variable('o1'));
		my $iter	= $store->match_bgp($t1);
		my @r		= $iter->elements;
		is(scalar(@r), 5, 'expected 1-triple BGP result size');
		foreach my $r (@r) {
			my $s	= $r->value('s');
			like($s->value, qr<^http://example.org/[sz]$>, 'expected BGP match IRI value');
		}
	}
	
	{
		note('2-triple BGP with IRI endpoint');
		my $t1		= Attean::TriplePattern->new(iri('http://example.org/x'), variable('p1'), variable('o'));
		my $t2		= Attean::TriplePattern->new(variable('o'), variable('p2'), variable('end'));
		my $iter	= $store->match_bgp($t1, $t2);
		my @r		= $iter->elements;
		is(scalar(@r), 4, 'expected 2-triple BGP result size');
		foreach my $r (@r) {
			my $end		= $r->value('end');
			like($end->value, qr<^(1|10|20|50)$>, 'expected BGP match literal value');
		}
	}
	
	{
		note('2-triple BGP with variable endpoints');
		my $t1		= Attean::TriplePattern->new(variable('start'), variable('p1'), variable('o'));
		my $t2		= Attean::TriplePattern->new(variable('o'), variable('p2'), variable('end'));
		my $iter	= $store->match_bgp($t1, $t2);
		my @r		= $iter->elements;
		is(scalar(@r), 4, 'expected 2-triple BGP result size');
		foreach my $r (@r) {
			my $start	= $r->value('start');
			my $end		= $r->value('end');
			is($start->value, 'http://example.org/x');
			like($end->value, qr<^(1|10|20|50)$>, 'expected BGP match literal value');
		}
	}
};

run_me; # run these Test::Attean tests

# {
# 	my $s	= Attean::Blank->new('x');
# 	my $p	= Attean::IRI->new('http://example.org/p1');
# 	my $o	= Attean::Literal->new(value => 'foo', language => 'en-US');
# 	my $g	= Attean::IRI->new('http://example.org/graph');
# 	my $q	= Attean::Quad->new($s, $p, $o, $g);
# 
# 	my @quads;
# 	push(@quads, $q);
# 	
# 	my $s2	= Attean::IRI->new('http://example.org/values');
# 	foreach my $value (1 .. 3) {
# 		my $o	= Attean::Literal->new(value => $value, datatype => 'http://www.w3.org/2001/XMLSchema#integer');
# 		my $p	= Attean::IRI->new("http://example.org/p$value");
# 		my $q	= Attean::Quad->new($s2, $p, $o, $g);
# 		push(@quads, $q);
# 	}
# 	
# 	my $store	= Attean->get_store('Simple')->new( quads => \@quads );
# 	isa_ok($store, 'AtteanX::Store::Simple');
# 
# 	is($store->size, 4);
# 	is($store->count_quads($s), 1);
# 	is($store->count_quads($s2), 3);
# 	is($store->count_quads(), 4);
# 	is($store->count_quads(undef, $p), 2);
# 	{
# 		my $iter	= $store->get_quads($s2);
# 		while (my $q = $iter->next()) {
# 			my $o	= $q->object->value;
# 			like($o, qr/^[123]$/, "Literal value: $o");
# 		}
# 	}
# 	
# 	my $iter	= $store->get_graphs;
# 	my @graphs	= $iter->elements;
# 	is(scalar(@graphs), 1);
# 	is($graphs[0]->value, 'http://example.org/graph');
# }

done_testing();


sub does_ok {
    my ($class_or_obj, $does, $message) = @_;
    $message ||= "The object does $does";
    ok(eval { $class_or_obj->does($does) }, $message);
}
