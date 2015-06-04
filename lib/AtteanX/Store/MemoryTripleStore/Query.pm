use v5.14;
use warnings;

package AtteanX::Store::MemoryTripleStore::Query 0.001 {
	use strict;
	use warnings;
	use Attean::RDF;
	use AtteanX::Store::MemoryTripleStore;
	use Moo;
	use Scalar::Util qw(refaddr);
	use Types::Standard qw(Str);
# 	use namespace::clean;
	has store => (is => 'ro');
	with 'Attean::API::Plan', 'Attean::API::NullaryQueryTree';
	
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
	
	around 'BUILDARGS' => sub {
		my $orig	= shift;
		my $class	= shift;
		my $args	= $class->$orig(@_);
		
		$args->{distinct}			= 0;
		$args->{in_scope_variables}	= [];
		$args->{ordered}			= [];
		
		return $args;
	};
	
	sub _bgp_ids {
		my $self	= shift;
		my @ids;
		my $last	= 0;
		my %vars;
		my @names	= ('');
		my $triple_count	= scalar(@_);
		foreach my $triple (@_) {
			foreach my $term ($triple->values) {
				if ($term->does('Attean::API::Variable')) {
					if (exists $vars{$term->value}) {
						push(@ids, $vars{$term->value});
					} else {
						my $id	= ++$last;
						$names[$id]	= $term->value;
						$vars{$term->value}	= -$id;
						push(@ids, -$id);
					}
				} else {
					my $id		= $self->store->_id_from_term($term);
					unless ($id) {
# 						warn 'term does not exist in the store: ' . $term->as_string;
						# term does not exist in the store
						return;
					}
					push(@ids, $id);
				}
			}
		}

		my $bgp	= {
			triples			=> $triple_count,
			variables		=> $last,
			nodes			=> \@ids,
			variable_names	=> \@names,
		};

		return $bgp;
	}
	
	sub debug {
		my $self	= shift;
		$self->print($self->store);
	}
	
	sub add_project {
		my $self	= shift;
		my @names	= @_;
		@{ $self->in_scope_variables }	= @names;
		return $self->_add_project($self->store, \@names);
	}
	
	sub add_sort {
		my $self	= shift;
		my @names	= @_;
		
		my @cmps;
		foreach my $var (@names) {
			my $expr	= Attean::ValueExpression->new(value => variable($var));
			my $cmp		= Attean::Algebra::Comparator->new(ascending => 1, expression => $expr);
			push(@cmps, $cmp);
		}
		@{ $self->ordered }	= @cmps;
		
		return $self->_add_sort($self->store, \@names, 0);
	}
	
	sub add_unique {
		my $self	= shift;
		my @names	= @{ $self->in_scope_variables };
		
		my @cmps;
		foreach my $var (@names) {
			my $expr	= Attean::ValueExpression->new(value => variable($var));
			my $cmp		= Attean::Algebra::Comparator->new(ascending => 1, expression => $expr);
			push(@cmps, $cmp);
		}
		@{ $self->ordered }	= @cmps;
		
		return $self->_add_sort($self->store, \@names, 1);
	}
	
	sub add_filter {
		my $self	= shift;
		my $var		= shift;
		my $op		= shift;
		my $pat		= shift // '';
		my $pat2	= shift // '';
		$op			= 'isiri' if ($op eq 'isuri');
		return $self->_add_filter($self->store, $var, $op, $pat, AtteanX::Store::MemoryTripleStore::TERM_XSDSTRING_LITERAL, '', $pat2);
	}
	
	sub add_lang_filter {
		my $self	= shift;
		my $var		= shift;
		my $op		= shift;
		my $pat		= shift;
		my $lang	= shift;
		$op			= 'isiri' if ($op eq 'isuri');
		return $self->_add_filter($self->store, $var, $op, $pat, AtteanX::Store::MemoryTripleStore::TERM_LANG_LITERAL, $lang, '');
	}
	
	sub add_bgp {
		my $self	= shift;
		my @triples	= @_;
		my $bgp		= $self->_bgp_ids(@triples);
		unless (ref($bgp)) {
			return;
		}
		
		my @names	= @{ $bgp->{variable_names} };
		shift(@names);	# get rid of the leading empty string
		@{ $self->in_scope_variables }	= @names;
		
		$self->_add_bgp(
			$self->store,
			$bgp->{triples},
			$bgp->{variables},
			$bgp->{nodes},
			$bgp->{variable_names},
		);
		return 1;
	}
	
	sub impl {
		my $self	= shift;
		my $model	= shift;
		my $store	= $self->store;
		return sub {
			my @results;
			$self->_evaluate($store, sub {
				my $hash	= shift;
				my $result	= Attean::Result->new( bindings => $hash );
				push(@results, $result);
			});
			return Attean::ListIterator->new(values => \@results, item_type => 'Attean::API::Result');
		}
	}
}

1;
