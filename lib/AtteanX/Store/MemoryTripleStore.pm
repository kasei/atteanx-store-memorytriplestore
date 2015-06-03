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
				$self->load($db);
			} else {
				warn "*** Cannot read from database $db";
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
	
=item C<< match_bgp ( @triples ) >>

Returns an iterator object of all L<Attean::API::Result> objects representing
variable bindings which match the specified L<Attean::API::TriplePattern>s.

=cut

	sub _bgp_ids {
		my $self	= shift;
		my $f		= shift;
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
# 						warn 'term does not exist in the store: ' . $term->as_string;
						# term does not exist in the store
						return;
					}
					push(@ids, $id);
				}
			}
		}

		foreach my $i (0 .. $#{ $f }) {
			if (ref($f->[$i])) {
				foreach my $k (keys %{ $f->[$i] }) {
					my $v	= $f->[$i]{$k};
					if (blessed($v)) {
						if ($v->does('Attean::API::Variable')) {
							$f->[$i]{$k}	= $vars{$v->value};
						} elsif ($v->does('Attean::API::Literal')) {
							$f->[$i]{$k}	= $v->value;
						} else {
							Carp::cluck "Unexpected object in filter planning: " . Dumper($v);
						}
					}
				}
			}
		}
		
		my $bgp	= {
			triples			=> $triple_count,
			variables		=> $last,
			nodes			=> \@ids,
			variable_names	=> \@names,
			filters			=> $f,
		};

		return $bgp;
	}
	
	sub match_bgp_with_filters {
		my $self	= shift;
		my $bgp		= $self->_bgp_ids(@_);
		unless (ref($bgp)) {
			return Attean::ListIterator->new(values => [], item_type => 'Attean::API::Triple');
		}
		my @results;
		$self->match_bgp_with_filter_cb(
			$bgp->{triples},
			$bgp->{variables},
			$bgp->{nodes},
			$bgp->{variable_names},
			$bgp->{filters},
			sub {
				my $hash	= shift;
				my $result	= Attean::Result->new( bindings => $hash );
				push(@results, $result);
			}
		);
		return Attean::ListIterator->new(values => \@results, item_type => 'Attean::API::Result');
	}
	
	sub match_bgp {
		my $self	= shift;
		return $self->match_bgp_with_filters([], @_);
	}
	
	sub match_path {
		my $self	= shift;
		my @ids;
		my $last	= 0;
		my %vars;
		my @names	= ('');
		my $type	= shift;
		my $vars	= 0;
		foreach my $term (@_) {
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
# 					warn 'term does not exist in the store: ' . $term->as_string;
					return Attean::ListIterator->new(values => [], item_type => 'Attean::API::Triple');
				}
				push(@ids, $id);
			}
		}
		
		my @results;
		$self->match_path_cb($type, $last, \@ids, \@names, sub {
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

	sub _data_for_plannable_algebra {
		my $self	= shift;
		my $algebra	= shift;
		if ($algebra->isa('Attean::Algebra::BGP')) {
			my @triples	= $self->_ordered_triples_from_bgp($algebra);
			return { triples => \@triples, filters => [] };
		} else {
			my ($child)	= @{ $algebra->children };
			if (my $data = $self->_data_for_plannable_algebra($child)) {
				if ($algebra->isa('Attean::Algebra::Filter')) {
					my $expr	= $algebra->expression;
					my $s		= $expr->as_string;
					my $ok		= 0;
					if ($s =~ /^REGEX\([?]\w+, "[^"]+"\)$/) {
						$ok	= 1;
						my $var		= $expr->children->[0]->value;
						my $pattern	= $expr->children->[1]->value;
						my $flags	= ''; # TODO: add flags support
						push(@{ $data->{filters} }, FILTER_REGEX, {variable => $var, pattern => $pattern, flags => $flags});
					} elsif ($s =~ /^IS(IRI|URI|LITERAL|BLANK)\([?]\w+\)$/) {
						my $var		= $expr->children->[0]->value;
						my $type;
						if ($s =~ /IRI|URI/) {
							$type	= FILTER_ISIRI;
						} elsif ($s =~ /LITERAL/) {
							$type	= FILTER_ISLITERAL;
						} elsif ($s =~ /BLANK/) {
							$type	= FILTER_ISBLANK;
						}
						push(@{ $data->{filters} }, $type, {variable => $var});
						$ok	= 1;
					} elsif ($s =~ /^(?:CONTAINS|STRSTARTS|STRENDS)\([?]\w+, "[^"]+"\)$/) {
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
						push(@{ $data->{filters} }, $type, {variable => $var, pattern => $pattern});
						$ok	= 1;
					}
					
					if ($ok) {
# 						warn "=========\nAlgebra is plannable:\n" . $algebra->as_string . "\n-------\n";
# 						use Data::Dumper;
# 						warn Dumper($data);
						return $data;
					}
				}
			}
		}
		return;
	}

	sub plans_for_algebra {
		my $self	= shift;
		my $algebra	= shift;
		
		if ($algebra->isa('Attean::Algebra::Filter')) {
			if (my $data = $self->_data_for_plannable_algebra($algebra)) {
				return AtteanX::Store::MemoryTripleStore::FilteredBGPPlan->new(
					%$data,
					store		=> $self,
					distinct	=> 0,
					in_scope_variables	=> [ $algebra->in_scope_variables ],
					ordered	=> [],
				);
			}
		} elsif ($algebra->isa('Attean::Algebra::Path')) {
			my $s		= $algebra->subject;
			my $o		= $algebra->object;
			my $path	= $algebra->path;
			if ($path->isa('Attean::Algebra::OneOrMorePath')) {
				my @children	= @{ $path->children };
				if (scalar(@children) == 1) {
					my ($p)	= @children;
					if ($p->isa('Attean::Algebra::PredicatePath')) {
						return AtteanX::Store::MemoryTripleStore::PathPlan->new(
							subject	=> $s,
							object	=> $o,
							predicate	=> $p->predicate,
							type => PATH_PLUS,
							store		=> $self,
							distinct	=> 0,
							in_scope_variables	=> [ $algebra->in_scope_variables ],
							ordered	=> [],
						);
					}
				}
			}
			return;
		} elsif ($algebra->isa('Attean::Algebra::BGP')) {
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

package AtteanX::Store::MemoryTripleStore::PathPlan 0.001 {
	use Moo;
	use Types::Standard qw(ConsumerOf ArrayRef InstanceOf Int);
	with 'Attean::API::Plan', 'Attean::API::NullaryQueryTree';
	has 'type' => (is => 'ro', isa => Int, required => 1);
	has 'subject' => (is => 'ro',  isa => ConsumerOf['Attean::API::TermOrVariable'], required => 1);
	has 'predicate' => (is => 'ro',  isa => ConsumerOf['Attean::API::IRI'], required => 1);
	has 'object' => (is => 'ro',  isa => ConsumerOf['Attean::API::TermOrVariable'], required => 1);
	has 'store' => (is => 'ro', isa => InstanceOf['AtteanX::Store::MemoryTripleStore'], required => 1);
	sub plan_as_string {
		my $self	= shift;
		my $op		= ($self->type == AtteanX::Store::MemoryTripleStore::PATH_PLUS) ? '+' : '*';
		return sprintf('MemoryTripleStorePath { %s <%s>%s %s }', $self->subject->as_string, $self->predicate->as_string, $op, $self->object->as_string);
	}
	
	sub impl {
		my $self	= shift;
		my $model	= shift;
		my $store	= $self->store;
		my $s		= $self->subject;
		my $p		= $self->predicate;
		my $o		= $self->object;
		my $type	= $self->type;
		return sub {
			return $store->match_path($type, $s, $p, $o);
		}
	}
}

package AtteanX::Store::MemoryTripleStore::BGPPlan 0.001 {
	use Moo;
	use Types::Standard qw(ConsumerOf ArrayRef InstanceOf);
	with 'Attean::API::Plan', 'Attean::API::NullaryQueryTree';
	has 'triples' => (is => 'ro',  isa => ArrayRef[ConsumerOf['Attean::API::TriplePattern']], default => sub { [] });
	has 'store' => (is => 'ro', isa => InstanceOf['AtteanX::Store::MemoryTripleStore'], required => 1);
	sub plan_as_string {
		return 'MemoryTripleStoreBGP'
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

package AtteanX::Store::MemoryTripleStore::FilteredBGPPlan 0.001 {
	use Moo;
	use Types::Standard qw(Str ConsumerOf ArrayRef InstanceOf);
	has 'filters' => (is => 'ro', isa => ArrayRef, required => 1);
	extends 'AtteanX::Store::MemoryTripleStore::BGPPlan';
	
	sub filter_count {
		my $self	= shift;
		my $f		= $self->filters;
		my $count	= scalar(@$f) / 2;
		return $count;
	}
	
	sub plan_as_string {
		my $self	= shift;
		my $model	= shift;
		my $count	= $self->filter_count;
		
		return sprintf('MemoryTripleStoreBGP (%d filters)', $count);
	}

	sub impl {
		my $self	= shift;
		my $model	= shift;
		my $store	= $self->store;
		my $f		= $self->filters;
		my @triples	= @{ $self->triples };
		return sub {
			return $store->match_bgp_with_filters($f, @triples);
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
