=head1 NAME

AtteanX::Store::MemoryTripleStore - In-memory RDF triple store

=head1 VERSION

This document describes AtteanX::Store::MemoryTripleStore version 0.001

=head1 SYNOPSIS

 use AtteanX::Store::MemoryTripleStore;

=head1 DESCRIPTION

AtteanX::Store::MemoryTripleStore provides an in-memory triple-store that is
especially optimized for matching BGPs which contain at least one bound subject
or object (e.g. preferring star or path queries rather than analytical
queries).

The triple store is read-only, requiring a filename to be specified during
construction which points at an N-Triples or Turtle file containing the RDF
data to load. The triple store links with libraptor2 to allow fast parsing of
input files.

=head1 METHODS

Beyond the methods documented below, this class consumes the
L<Attean::API::TripleStore> and L<Attean::API::CostPlanner> roles.

=over 4

=item C<< new (filename => $filename) >>

Returns a new memory-backed storage object containing the RDF data parsed from
the specified N-Triples- or Turtle-encoded file.

=cut

use v5.14;
use warnings;

package AtteanX::Store::MemoryTripleStore {
	use Moo;
	use XSLoader;
	use XS::Object::Magic;
	use Attean::API;
	use Scalar::Util qw(blessed);
	
	BEGIN {
		our $VERSION	= '0.000_01';
		XSLoader::load('AtteanX::Store::MemoryTripleStore', $VERSION);
	}

	use AtteanX::Store::MemoryTripleStore::IRI;
	use AtteanX::Store::MemoryTripleStore::Blank;
	with 'Attean::API::TripleStore';
	with 'Attean::API::CostPlanner';
	
	sub BUILD {
		my $self	= shift;
		my $args	= shift;
		if ($self->build_struct()) {
			die "Failed to construct triplestore object";
		}
		if (my $filename = $args->{'filename'}) {
			if (-r $filename) {
				$self->load_file($filename);
			} else {
				warn "*** Cannot read from $filename";
			}
		}
	}
	
	sub load_file {
		my $self	= shift;
		my $filename	= shift;
		$self->_load_file($filename, 0, 0);
	}
	
=item C<< get_triples ( $subject, $predicate, $object ) >>

Returns an iterator object of all L<Attean::API::Triple> objects matching the
specified subject, predicate and objects. Any of the arguments may be undef to
match any value, an L<Attean::API::Term> object, or an ARRAY reference
containing a set of possible term values.

=cut

	sub get_triples {
		my $self	= shift;
		my @nodes	= @_;
		my $nextvar	= -1;
		my %vars;
		
		my @ids;
		foreach my $pos (0 .. 2) {
			my $n	= $nodes[ $pos ];
			if (not(blessed($n))) {
				$ids[$pos]	= 0;
			} elsif ($n->does('Attean::API::Variable')) {
				if (my $id = $vars{$n->value}) {
					$ids[$pos]	= $id;
				} else {
					$vars{$n->value}	= $nextvar--;
					$ids[$pos]	= $vars{$n->value};
				}
			} else {
				my $id		= $self->_id_from_term($n);
				unless ($id) {
					# term does not exist in the store
					return Attean::ListIterator->new(values => [], item_type => 'Attean::API::Triple');
				}
				$ids[$pos]	= $id;
			}
		}
		
		my @triples;
		$self->get_triples_cb(@ids, sub {
			my $t	= shift;
			push(@triples, $t);
		});
		return Attean::ListIterator->new(values => \@triples, item_type => 'Attean::API::Triple');
	}
	
=item C<< match_bgp ( @triples ) >>

Returns an iterator object of all L<Attean::API::Result> objects representing
variable bindings which match the specified L<Attean::API::TriplePattern>s.

=cut

	sub match_bgp {
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
					my $id		= $self->_id_from_term($term);
					unless ($id) {
						# term does not exist in the store
						return Attean::ListIterator->new(values => [], item_type => 'Attean::API::Triple');
					}
					push(@ids, $id);
				}
			}
		}
		
		my $bgp	= {
			triples			=> $triple_count,
			variables		=> $last,
			nodes			=> \@ids,
			variable_names	=> \@names
		};
		my @results;
		$self->match_bgp_cb($triple_count, $last, \@ids, \@names, sub {
			my $hash	= shift;
			my $result	= Attean::Result->new( bindings => $hash );
			push(@results, $result);
		});
		return Attean::ListIterator->new(values => \@results, item_type => 'Attean::API::Result');
	}
	
	sub _id_from_term {
		my $self	= shift;
		my $term	= shift;
		if ($term->does('Attean::API::IRI')) {
			return $self->_term_to_id1(TERM_IRI, $term->value);
		} elsif ($term->does('Attean::API::Blank')) {
			return $self->_term_to_id1(TERM_BLANK, $term->value);
		} elsif ($term->does('Attean::API::Literal')) {
			if ($term->has_language) {
				return $self->_term_to_id2(TERM_LANG_LITERAL, $term->value, $term->language);
			} else {
				my $dt	= $term->datatype;
				if ($dt->value eq 'http://www.w3.org/2001/XMLSchema#string') {
					return $self->_term_to_id1(TERM_XSDSTRING_LITERAL, $term->value);
				} else {
					my $dtid		= $self->_id_from_term($dt);
					return $self->_term_to_id3(TERM_TYPED_LITERAL, $term->value, $dtid);
				}
			}
		}
		warn "uh oh. no id found for term: " . $term->as_string;
		return 0;
	}
	
	sub _triple_cost {
		my $t		= shift;
		my @nodes	= $t->values;
		my @v		= map { $_->value } grep { $_->does('Attean::API::Variable') } @nodes;
		my $cost	= scalar(@v);
		foreach my $i (0,2) {
			if ($nodes[$i]->does('Attean::API::Variable')) {
				$cost	*= 2;
			}
		}
		return $cost;
	}
	
	sub _ordered_triples_from_bgp {
		my $self	= shift;
		my $bgp		= shift;
		my @triples	= map { [_triple_cost($_), $_] } @{ $bgp->triples };
		my @sorted	= sort { $a->[0] <=> $b->[0] } @triples;
		
		my $first	= shift(@sorted)->[1];
		my @final	= ($first);
		my %seen	= map { $_->value => 1 } $first->values_consuming_role('Attean::API::Variable');
		LOOP: while (scalar(@sorted)) {
			foreach my $i (0 .. $#sorted) {
				my $pair	= $sorted[$i];
				my $triple	= $pair->[1];
				my @tvars	= map { $_->value } $triple->values_consuming_role('Attean::API::Variable');
				foreach my $tvar (@tvars) {
					if ($seen{$tvar}) {
						push(@final, $triple);
						splice(@sorted, $i, 1);
						foreach my $v (@tvars) {
							$seen{$v}++;
						}
						next LOOP;
					}
				}
			}
			
			my $default	= shift(@sorted);
			push(@final, $default->[1]);
		}
		return @final;
	}
	
=item C<< plans_for_algebra ( $algebra ) >>

If C<$algebra> is an L<Attean::Algebra::BGP> object, returns a store-specific
L<Attean::API::Plan> object representing the evaluation of the entire BGP
against the triplestore.

Otherwise, returns an empty list.

=cut

	sub plans_for_algebra {
		my $self	= shift;
		my $algebra	= shift;
		if ($algebra->isa('Attean::Algebra::BGP')) {
			my @triples	= $self->_ordered_triples_from_bgp($algebra);
			return AtteanX::Store::MemoryTripleStore::BGPPlan->new(
				triples 	=> \@triples,
				store		=> $self,
				distinct	=> 0,
				in_scope_variables	=> [ $algebra->in_scope_variables ],
				ordered	=> [],
			);
		}
		return;
	}

=item C<< cost_for_plan ( $plan ) >>

If C<$plan> is a recognized store-specific L<Attean::API::Plan> object,
returns an estimated (relative) cost value for evaluating the represented BGP.

Otherwise returns C<undef>.

=cut

	sub cost_for_plan {
		my $self	= shift;
		my $plan	= shift;
		if ($plan->isa('AtteanX::Store::MemoryTripleStore::BGPPlan')) {
			return 1; # TODO: actually estimate cost here
		}
		return;
	}
}

package AtteanX::Store::MemoryTripleStore::BGPPlan 0.001 {
	use Moo;
	use Types::Standard qw(ConsumerOf ArrayRef InstanceOf);
	with 'Attean::API::Plan', 'Attean::API::NullaryQueryTree';
	has 'triples' => (is => 'ro',  isa => ArrayRef[ConsumerOf['Attean::API::TriplePattern']], default => sub { [] });
	has 'store' => (is => 'ro', isa => InstanceOf['AtteanX::Store::MemoryTripleStore'], required => 1);
	sub plan_as_string {
		return 'BGP(MemoryTripleStore)'
	}
	
	sub impl {
		my $self	= shift;
		my $model	= shift;
		my $store	= $self->store;
		my @triples	= @{ $self->triples };
		return sub {
			return $store->match_bgp(@triples);
		}
	}
}

1;

__END__

=back

=head1 BUGS

Please report any bugs or feature requests to through the GitHub web interface
at L<https://github.com/kasei/atteanx-store-memorytriplestore/issues>.

=head1 SEE ALSO

L<http://www.perlrdf.org/>

=head1 AUTHOR

Gregory Todd Williams  C<< <gwilliams@cpan.org> >>

=head1 COPYRIGHT

Copyright (c) 2015 Gregory Todd Williams.
This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License.

=cut
