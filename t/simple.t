#!/usr/bin/env perl

use v5.14;
use Test::More;
use Attean;

my $class	= Attean->get_store('MemoryTripleStore');
is($class, 'AtteanX::Store::MemoryTripleStore');

{
	my $store	= $class->new();
	isa_ok($store, 'AtteanX::Store::MemoryTripleStore');

	is($store->size, 0);
}

done_testing();
