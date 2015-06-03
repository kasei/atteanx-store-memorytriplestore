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
use FindBin qw($Bin);
use File::Spec;
use Data::Dumper;

sub create_store {
	my $self	= shift;
	my $db	= File::Spec->catfile($Bin, 'data', 'trees.db');
	my $store	= Attean->get_store('MemoryTripleStore')->new(database => $db);
	return $store;
}

test 'simple query construction' => sub {
	my $self	= shift;
	my $store	= $self->create_store();
	my $graph	= iri('http://example.org/');
	my $model	= Attean::TripleModel->new( stores => { $graph->value => $store } );
	
	my $query	= AtteanX::Store::MemoryTripleStore::Query->new(store => $store);
	isa_ok($query, 'AtteanX::Store::MemoryTripleStore::Query');

	my $t1		= Attean::TriplePattern->new(variable('s'), iri('http://data.smgov.net/resource/zzzz-zzzz/commonname'), variable('tree'));
	my $bgp		= Attean::Algebra::BGP->new(triples => [$t1]);
	$query->add_bgp(@{ $bgp->triples });
	my $iter	= $query->evaluate($model);
	does_ok($iter, 'Attean::API::ResultIterator');

	my $count	= 0;
	while (my $result = $iter->next) {
		$count++;
	}
	is($count, 55);
};

test 'filter query construction' => sub {
	my $self	= shift;
	my $store	= $self->create_store();
	my $graph	= iri('http://example.org/');
	my $model	= Attean::TripleModel->new( stores => { $graph->value => $store } );
	
	my $query	= AtteanX::Store::MemoryTripleStore::Query->new(store => $store);
	isa_ok($query, 'AtteanX::Store::MemoryTripleStore::Query');

	my $t1		= Attean::TriplePattern->new(variable('s'), iri('http://data.smgov.net/resource/zzzz-zzzz/commonname'), variable('tree'));
	my $bgp		= Attean::Algebra::BGP->new(triples => [$t1]);
	$query->add_bgp(@{ $bgp->triples });
	$query->add_filter('tree', 'contains', 'PEPPER');
	my $iter	= $query->evaluate($model);
	does_ok($iter, 'Attean::API::ResultIterator');
	
	my %seen;
	while (my $result = $iter->next) {
		my $tree	= $result->value('tree');
		does_ok($tree, 'Attean::API::Literal');
		$seen{$tree->value}++;
	}
	is_deeply(\%seen, {
		'PEPPERMINT TREE'	=> 1,
		'BRAZILIAN PEPPER'	=> 4,
	});
};

test 'complex query construction' => sub {
	my $self	= shift;
	my $store	= $self->create_store();
	my $graph	= iri('http://example.org/');
	my $model	= Attean::TripleModel->new( stores => { $graph->value => $store } );
	
	my $query	= AtteanX::Store::MemoryTripleStore::Query->new(store => $store);
	isa_ok($query, 'AtteanX::Store::MemoryTripleStore::Query');

	my $t1		= Attean::TriplePattern->new(variable('s'), iri('http://data.smgov.net/resource/zzzz-zzzz/fullname'), variable('street'));
	my $t2		= Attean::TriplePattern->new(variable('s'), iri('http://data.smgov.net/resource/zzzz-zzzz/commonname'), variable('tree'));
	my $bgp		= Attean::Algebra::BGP->new(triples => [$t1, $t2]);
	$query->add_bgp(@{ $bgp->triples });
	$query->add_filter('tree', 'regex', 'palm', 'i');
	$query->add_project(qw(street tree));
	$query->add_sort(qw(street tree));
	
	my $count	= 0;
	my $seen_euclid	= 0;
	my $iter	= $query->evaluate($model);
	does_ok($iter, 'Attean::API::ResultIterator');
	while (my $result = $iter->next) {
		$count++;
		my @keys	= $result->variables;
		is_deeply([sort @keys], [qw(street tree)], 'expected bindings after projection');
		my $street	= $result->value('street');
		does_ok($street, 'Attean::API::Literal');
		
		# this tests both the expected literal value as well as ensures the ordering ('ALTA AVE' < 'EUCLID ST')
		if ($seen_euclid) {
			is($street->value, 'EUCLID ST', 'expected literal value');
		} else {
			like($street->value, qr/^(ALTA AVE|EUCLID ST)$/, 'expected literal value');
			if ($street->value =~ /EUCLID/) {
				$seen_euclid	= 1;
			}
		}
		
		my $tree	= $result->value('tree');
		does_ok($tree, 'Attean::API::Literal');
		like($tree->value, qr/PALM/i);
# 		say $result->as_string;
	}
	is($count, 12);
};

run_me; # run these Test::Attean tests

done_testing();


sub does_ok {
    my ($class_or_obj, $does, $message) = @_;
    $message ||= "The object does $does";
    ok(eval { $class_or_obj->does($does) }, $message);
}
