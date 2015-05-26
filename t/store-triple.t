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

test 'store-planning for BGP filter' => sub {
	my $self	= shift;
	my $store	= $self->_store_with_data();
	my $graph	= iri('http://example.org/');
	my $model	= Attean::TripleModel->new( stores => { $graph->value => $store } );
	my $planner	= Attean::IDPQueryPlanner->new();

	my $pat		= '1';
	my $t1		= Attean::TriplePattern->new(variable('s'), iri('http://example.org/p'), variable('o'));
	my $bgp		= Attean::Algebra::BGP->new(triples => [$t1]);
	my $var		= Attean::ValueExpression->new(value => variable('o'));
	my $pattern	= Attean::ValueExpression->new(value => literal($pat));
	my $expr	= Attean::FunctionExpression->new( children => [$var, $pattern], operator => 'regex' );
	my $filter	= Attean::Algebra::Filter->new(children => [$bgp], expression => $expr);
	my $plan	= $planner->plan_for_algebra($filter, $model, [$graph]);
	isa_ok($plan, 'AtteanX::Store::MemoryTripleStore::RegexBGPPlan');
	
	my $iter	= $plan->evaluate();
	my $count	= 0;
	while (my $r = $iter->next) {
		$count++;
		like($r->value('o')->value, qr/$pat/);
	}
	is($count, 2);
};

test 'match_bgp' => sub {
	my $self	= shift;
	my $store	= $self->_store_with_data();

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

done_testing();


sub does_ok {
    my ($class_or_obj, $does, $message) = @_;
    $message ||= "The object does $does";
    ok(eval { $class_or_obj->does($does) }, $message);
}
