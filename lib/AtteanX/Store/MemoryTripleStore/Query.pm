use v5.14;
use warnings;

package AtteanX::Store::MemoryTripleStore::Query 0.001 {
	use AtteanX::Store::MemoryTripleStore;
	use Moo;
	use Types::Standard qw(Str);

	has store => (is => 'ro');
	
	# NOTE: Objects of this class are not meant to be constructed from perl.
	#       They should only be constructed from within the XS code that is a
	#       part of this package, allowing an underlying raptor structure to be
	#       associated with the perl-level object.
	
	sub BUILD {
		my $self	= shift;
		my $args	= shift;
		my $store	= $args->{store};
		if ($self->build_struct($store)) {
			die "Failed to construct triplestore object";
		}
	}
	
	sub debug {
		my $self	= shift;
		$self->print($self->store);
	}
	
	sub add_project {
		my $self	= shift;
		my @names	= @_;
		return $self->_add_project($self->store, \@names);
	}
	
	sub add_sort {
		my $self	= shift;
		my @names	= @_;
		return $self->_add_sort($self->store, \@names);
	}
	
	sub add_filter {
		my $self	= shift;
		my $var		= shift;
		my $op		= shift;
		my $pat		= shift;
		my $pat2	= shift // '';
		return $self->_add_filter($self->store, $var, $op, $pat, $pat2);
	}
	
	sub add_bgp {
		my $self	= shift;
		my @triples	= @{ shift->triples };
		my $bgp		= $self->store->_bgp_ids([], @triples);
		unless (ref($bgp)) {
			return;
		}
		my @results;
		$self->_add_bgp(
			$self->store,
			$bgp->{triples},
			$bgp->{variables},
			$bgp->{nodes},
			$bgp->{variable_names},
		);
		return 1;
	}
	
	sub evaluate {
		my $self	= shift;
		my $store	= $self->store;
		return $self->_evaluate($store, @_);
	}
}

1;
