use Test::Roo;
use Test::Modern;
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

sub _store_with_data {
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
	return $store;
}

with 'Test::Attean::TripleStore';

test '1-triple' => sub {
	my $self	= shift;
	my $store	= $self->_store_with_data();
	my $t1		= Attean::TriplePattern->new(variable('s'), iri('http://example.org/p'), variable('o1'));
	subtest 'get_triples' => sub {
		my $iter	= $store->get_triples($t1->values);
		my @r		= $iter->elements;
		is(scalar(@r), 5, 'expected 1-triple result size');
		foreach my $r (@r) {
			does_ok($r, 'Attean::API::Triple');
			my $s	= $r->subject;
			like($s->value, qr<^http://example.org/[sz]$>, 'expected BGP match IRI value');
		}
	};
	
	subtest 'match_bgp' => sub {
		my $iter	= $store->match_bgp($t1);
		my @r		= $iter->elements;
		is(scalar(@r), 5, 'expected 1-triple result size');
		foreach my $r (@r) {
			does_ok($r, 'Attean::API::Result');
			my $s	= $r->value('s');
			like($s->value, qr<^http://example.org/[sz]$>, 'expected BGP match IRI value');
		}
	};
};

test '2-triple BGP with IRI endpoint' => sub {
	my $self	= shift;
	my $store	= $self->_store_with_data();
	my $t1		= Attean::TriplePattern->new(iri('http://example.org/x'), variable('p1'), variable('o'));
	my $t2		= Attean::TriplePattern->new(variable('o'), variable('p2'), variable('end'));
	my $iter	= $store->match_bgp($t1, $t2);
	my @r		= $iter->elements;
	is(scalar(@r), 4, 'expected 2-triple BGP result size');
	foreach my $r (@r) {
		my $end		= $r->value('end');
		like($end->value, qr<^(1|10|20|50)$>, 'expected BGP match literal value');
	}
};

test '2-triple BGP with variable endpoints' => sub {
	my $self	= shift;
	my $store	= $self->_store_with_data();
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
};

run_me; # run these Test::Attean tests

done_testing();
