use v5.14;
use warnings;

package AtteanX::Store::MemoryTriplestore 0.001 {
	use Moo;
	use XSLoader;
	use XS::Object::Magic;
	use Attean::API;
	use Scalar::Util qw(blessed);
	
	BEGIN {
		our $VERSION;
		XSLoader::load('AtteanX::Store::MemoryTriplestore', $VERSION);
	}

	use AtteanX::Store::MemoryTriplestore::IRI;
	use AtteanX::Store::MemoryTriplestore::Blank;
	with 'Attean::API::TripleStore';
	
	sub BUILD {
		my $self	= shift;
		$self->build_struct();
	}
	
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
				$ids[$pos]	= $self->_id_from_term($n);
			}
		}
		
		warn 'get_triples called';
		my @triples;
		$self->get_triples_cb(@ids, sub {
			my $t	= shift;
			push(@triples, $t);
		});
		return Attean::ListIterator->new(values => \@triples, item_type => 'Attean::API::Triple');
	}
}

1;
