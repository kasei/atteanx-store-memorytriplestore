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
		our $VERSION	= '0.000_03';
		XSLoader::load('AtteanX::Store::MemoryTripleStore', $VERSION);
	}

	use AtteanX::Store::MemoryTripleStore::Query;
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
		if (my $rdf = $args->{'filename'}) {
			if (-r $rdf) {
				$self->load_file($rdf);
			} else {
				warn "*** Cannot read from RDF $rdf";
			}
		} elsif (my $db = $args->{'database'}) {
			if (-r $db) {
				my $verbose	= 0;
				$self->load($db, $verbose);
			} else {
				warn "*** Cannot read from database $db";
			}
		}
	}
	
	sub load_file {
		my $self	= shift;
		my $filename	= shift;
		$self->_load_file($filename, 0);
	}
	
=item C<< get_triples ( $subject, $predicate, $object ) >>

Returns an iterator object of all L<Attean::API::Triple> objects matching the
specified subject, predicate and objects. Any of the arguments may be undef to
match any value, an L<Attean::API::Term> object, or an ARRAY reference
containing a set of possible term values.

=cut

	sub _triple_ids {
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
# 					warn 'term does not exist in the store: ' . $n->as_string;
					# term does not exist in the store
					return;
				}
				$ids[$pos]	= $id;
			}
		}
		return @ids;
	}
	
	sub get_triples {
		my $self	= shift;
		my @ids		= $self->_triple_ids(@_);
		unless (scalar(@ids) == 3) {
			return Attean::ListIterator->new(values => [], item_type => 'Attean::API::Triple');
		}
		
		my @triples;
		$self->get_triples_cb(@ids, sub {
			my $t	= shift;
			push(@triples, $t);
		});
		return Attean::ListIterator->new(values => \@triples, item_type => 'Attean::API::Triple');
	}
	
	sub count_triples {
		my $self	= shift;
		my @ids		= $self->_triple_ids(@_);
		unless (scalar(@ids) == 3) {
			return 0;
		}
		return $self->_count_triples(@ids);
	}
	
	sub match_bgp {
		my $self	= shift;
		my @triples	= @_;
		my $query	= AtteanX::Store::MemoryTripleStore::Query->new(store => $self);
		$query->add_bgp(@triples);
		my @results;
		$query->_evaluate($self, sub {
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

	sub _query_for_plannable_algebra {
		my $self	= shift;
		my $algebra	= shift;
		return unless blessed($algebra);
		if ($algebra->isa('Attean::Algebra::Join')) {
			my ($lhs, $rhs)	= @{ $algebra->children };
			if ($rhs->isa('Attean::Algebra::Path')) {
				if (my $query = $self->_query_for_plannable_algebra($lhs)) {
					my $path	= $rhs->path;
					if ($path->isa('Attean::Algebra::OneOrMorePath')) {
						my @children	= @{ $path->children };
						if (scalar(@children) == 1) {
							my ($p)	= @children;
							if ($p->isa('Attean::Algebra::PredicatePath')) {
								$query->add_path('+', $rhs->subject, $p->predicate, $rhs->object);
								return $query;
							}
						}
					}
				}
			}
		} elsif ($algebra->isa('Attean::Algebra::BGP')) {
			my @triples	= $self->_ordered_triples_from_bgp($algebra);
			my $query	= AtteanX::Store::MemoryTripleStore::Query->new(store => $self);
			$query->add_bgp(@triples);
			return $query;
		} elsif ($algebra->isa('Attean::Algebra::Path')) {
			my $path	= $algebra->path;
			if ($path->isa('Attean::Algebra::OneOrMorePath')) {
				my @children	= @{ $path->children };
				if (scalar(@children) == 1) {
					my ($p)	= @children;
					if ($p->isa('Attean::Algebra::PredicatePath')) {
						my $query	= AtteanX::Store::MemoryTripleStore::Query->new(store => $self);
						$query->add_path('+', $algebra->subject, $p->predicate, $algebra->object);
						return $query;
					}
				}
			}
			return;
		} else {
			my ($child)	= @{ $algebra->children };
			if (my $query = $self->_query_for_plannable_algebra($child)) {
				if ($algebra->isa('Attean::Algebra::Project')) {
					my $vars	= $algebra->variables;
					my @vars	= map { $_->value } @$vars;
					$query->add_project(@vars);
					return $query;
				} elsif ($algebra->isa('Attean::Algebra::Filter')) {
					my $expr	= $algebra->expression;
					my $s		= $expr->as_string;
					if ($s =~ /^REGEX\([?]\w+, "[^"]+"\)$/) {
						my $var		= $expr->children->[0]->value;
						my $pattern	= $expr->children->[1]->value;
						my $flags	= ''; # TODO: add flags support
						$query->add_filter($var->value, 'regex', $pattern->value, $flags);
						return $query;
					} elsif ($s =~ /^IS(IRI|URI|LITERAL|BLANK)\([?]\w+\)$/) {
						my $var		= $expr->children->[0]->value;
						my $op		= lc($expr->operator);
						$query->add_filter($var->value, $op);
						return $query;
					} elsif ($s =~ /^(?:CONTAINS|STRSTARTS|STRENDS)\([?]\w+, "[^"]+"\)$/) {
						my $op		= lc($expr->operator);
						my $var		= $expr->children->[0]->value;
						my $pattern	= $expr->children->[1]->value;
						my $type;
						if ($s =~ /CONTAINS/) {
							$type	= FILTER_CONTAINS;
						} elsif ($s =~ /STARTS/) {
							$type	= FILTER_STRSTARTS;
						} elsif ($s =~ /ENDS/) {
							$type	= FILTER_STRENDS;
						}
						$query->add_filter($var->value, $op, $pattern->value);
						return $query;
					}
				}
			}
		}
		return;
	}

	sub plans_for_algebra {
		my $self	= shift;
		my $algebra	= shift;
		return unless ($algebra);
		
		if (my $query = $self->_query_for_plannable_algebra($algebra)) {
			return $query;
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
		if ($plan->isa('AtteanX::Store::MemoryTripleStore::Query')) {
			return 1; # TODO: actually estimate cost here
		}
		return;
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
