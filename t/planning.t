use Test::Roo;
use Test::More;
use Test::Exception;
use File::Temp qw(tempfile);

use v5.14;
use warnings;
no warnings 'redefine';

use Attean;
use Attean::RDF;
use AtteanX::RDFQueryTranslator;
use RDF::Query;

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

sub _store_with_path_data {
	my $self	= shift;
	my @triples;
	{
		my $type	= iri('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
		my $person	= iri('http://xmlns.com/foaf/0.1/Person');
		my $knows	= iri('http://xmlns.com/foaf/0.1/knows');
		my $name	= iri('http://xmlns.com/foaf/0.1/name');
		my $class	= iri('http://www.w3.org/2000/01/rdf-schema#Class');
		my $alice	= iri('http://example.org/alice');
		my $bob		= iri('http://example.org/bob');
		my $eve		= iri('http://example.org/eve');
		my $tim		= iri('http://example.org/tim');

		# eve -> alice <-> bob
		#               -> tim

		push(@triples, triple($tim, $type, $person));
		push(@triples, triple($tim, $name, literal('Timothy')));
		push(@triples, triple($alice, $type, $person));
		push(@triples, triple($alice, $name, literal('Alice')));
		push(@triples, triple($alice, $knows, $bob));
		push(@triples, triple($alice, $knows, $tim));
		push(@triples, triple($bob, $type, $person));
		push(@triples, triple($bob, $name, literal('Robert')));
		push(@triples, triple($bob, $knows, $alice));
		push(@triples, triple($eve, $type, $person));
		push(@triples, triple($eve, $name, literal('Eve')));
		push(@triples, triple($eve, $knows, $alice));
		push(@triples, triple($person, $type, $class));
	}
	my $store	= $self->create_store(triples => \@triples);
	return $store;
}

test 'store-planning for BGP REGEX filter' => sub {
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
	isa_ok($plan, 'AtteanX::Store::MemoryTripleStore::Query');
	
	my $iter	= $plan->evaluate($model);
	does_ok($iter, 'Attean::API::ResultIterator');
	my $count	= 0;
	while (my $r = $iter->next) {
		$count++;
		like($r->value('o')->value, qr/$pat/, 'Result satisfies filters');
	}
	is($count, 2, 'Expected result count');
};

test 'store-planning for BGP type+REGEX filter' => sub {
	my $self	= shift;
	my $store	= $self->_store_with_data();
	my $graph	= iri('http://example.org/');
	my $model	= Attean::TripleModel->new( stores => { $graph->value => $store } );
	my $planner	= Attean::IDPQueryPlanner->new();

	my $pat		= '1';
	my $t1		= Attean::TriplePattern->new(variable('s'), iri('http://example.org/p'), variable('o'));
	my $bgp		= Attean::Algebra::BGP->new(triples => [$t1]);
	my $var		= Attean::ValueExpression->new(value => variable('o'));
	my $expr1	= Attean::FunctionExpression->new( children => [$var], operator => 'isiri' );
	my $filter1	= Attean::Algebra::Filter->new(children => [$bgp], expression => $expr1);

	my $pattern	= Attean::ValueExpression->new(value => literal($pat));
	my $expr2	= Attean::FunctionExpression->new( children => [$var, $pattern], operator => 'regex' );
	my $filter2	= Attean::Algebra::Filter->new(children => [$filter1], expression => $expr2);
	my $plan	= $planner->plan_for_algebra($filter2, $model, [$graph]);
	isa_ok($plan, 'AtteanX::Store::MemoryTripleStore::Query');
	
	my $iter	= $plan->evaluate();
	my $count	= 0;
	while (my $r = $iter->next) {
		$count++;
		# The filters in this query shouldn't produce any results (the combination of the ISIRI type check and the /1/ REGEX does not match any data)
		fail("Unexpected result shoudn't satisfy filter: " . $r->as_string);
	}
	is($count, 0, 'Expected result count');
};

test 'store-planning for BGP string filter' => sub {
	my $self	= shift;
	my $store	= $self->_store_with_path_data();
	my $graph	= iri('http://example.org/');
	my $model	= Attean::TripleModel->new( stores => { $graph->value => $store } );
	my $planner	= Attean::IDPQueryPlanner->new();

	my $pat		= 'o';
	my $pattern	= Attean::ValueExpression->new(value => literal($pat));
	my $t1		= Attean::TriplePattern->new(variable('s'), iri('http://xmlns.com/foaf/0.1/name'), variable('name'));
	my $bgp		= Attean::Algebra::BGP->new(triples => [$t1]);
	my $var		= Attean::ValueExpression->new(value => variable('name'));
	my $expr1	= Attean::FunctionExpression->new( children => [$var, $pattern], operator => 'contains' );
	my $filter	= Attean::Algebra::Filter->new(children => [$bgp], expression => $expr1);

	my $plan	= $planner->plan_for_algebra($filter, $model, [$graph]);
	
	isa_ok($plan, 'AtteanX::Store::MemoryTripleStore::Query');
	
	my $iter	= $plan->evaluate();
	my $count	= 0;
	my %expect	= (
		'Timothy'	=> 1,
		'Robert'	=> 1,
	);
	while (my $r = $iter->next) {
		$count++;
		ok(exists $expect{$r->value('name')->value}, 'expected literal contains pattern');
	}
	is($count, 2, 'Expected result count');
};

test 'OnOrMore path with ground subject' => sub {
	my $self	= shift;
	my $store	= $self->_store_with_path_data();
	my $graph	= iri('http://example.org/');
	my $model	= Attean::TripleModel->new( stores => { $graph->value => $store } );
	my $planner	= Attean::IDPQueryPlanner->new();
	my $t		= AtteanX::RDFQueryTranslator->new();
	
	# Knows relationships
	# eve -> alice <-> bob
	#               -> tim

	my %tests	= (
		eve		=> [qw(alice bob tim)],
		alice	=> [qw(alice bob tim)],
		bob		=> [qw(alice bob tim)],
		tim		=> [],
	);
	
	foreach my $person (keys %tests) {
		my @knows	= @{ $tests{$person} };
		my %expect;
		foreach my $k (@knows) {
			$expect{"http://example.org/$k"}	= 1;
		}
		my $query	= RDF::Query->new("PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT * WHERE { <http://example.org/$person> foaf:knows+ ?o }");
		my $algebra	= $t->translate_query($query);
		my $plan	= $planner->plan_for_algebra($algebra, $model, [$graph]);
		my $iter	= $plan->evaluate();
		my $count	= 0;
		my %seen;
		while (my $r = $iter->next) {
			$count++;
			$seen{ $r->value('o')->value }++;
		}
		is_deeply(\%seen, \%expect, "expected knows paths from $person");
	}
};

test 'OnOrMore path with variable subject' => sub {
	my $self	= shift;
	my $store	= $self->_store_with_path_data();
	my $graph	= iri('http://example.org/');
	my $model	= Attean::TripleModel->new( stores => { $graph->value => $store } );
	my $planner	= Attean::IDPQueryPlanner->new();
	my $t		= AtteanX::RDFQueryTranslator->new();
	
	# Knows relationships
	# eve -> alice <-> bob
	#               -> tim

	my $query	= RDF::Query->new("PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT * WHERE { ?s foaf:knows+ ?o }");
	my $algebra	= $t->translate_query($query);
	my $plan	= $planner->plan_for_algebra($algebra, $model, [$graph]);
	my $iter	= $plan->evaluate($model);
	my $count	= 0;
	my %seen;
	while (my $r = $iter->next) {
		$count++;
		$seen{ $r->value('s')->value }{ $r->value('o')->value }++;
	}
	
	my %expect;
	$expect{'http://example.org/eve'}{'http://example.org/alice'}++;
	$expect{'http://example.org/eve'}{'http://example.org/bob'}++;
	$expect{'http://example.org/eve'}{'http://example.org/tim'}++;

	$expect{'http://example.org/alice'}{'http://example.org/alice'}++;
	$expect{'http://example.org/alice'}{'http://example.org/bob'}++;
	$expect{'http://example.org/alice'}{'http://example.org/tim'}++;

	$expect{'http://example.org/bob'}{'http://example.org/bob'}++;
	$expect{'http://example.org/bob'}{'http://example.org/alice'}++;
	$expect{'http://example.org/bob'}{'http://example.org/tim'}++;

# 	use Data::Dumper;
# 	warn Dumper(\%seen);
	is_deeply(\%seen, \%expect, "expected knows paths");
};

run_me; # run these Test::Attean tests

done_testing();


sub does_ok {
    my ($class_or_obj, $does, $message) = @_;
    $message ||= "The object does $does";
    ok(eval { $class_or_obj->does($does) }, $message);
}
