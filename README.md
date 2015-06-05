[`AtteanX::Store::MemoryTripleStore`](https://github.com/kasei/atteanx-store-memorytriplestore/) - In-memory RDF triple store
===

AtteanX::Store::MemoryTripleStore provides an in-memory triple-store that is
especially optimized for matching BGPs which contain at least one bound subject
or object (e.g. preferring star or path queries).

The triple store is read-only, requiring a filename to be specified during
construction which points at an N-Triples or Turtle file containing the RDF
data to load. The triple store links with libraptor2 to allow fast parsing of
input RDF files.


Perl Example
---

In addition to being a fully-functioning [Attean](https://github.com/kasei/attean/) store, this code may be
used in a more direct fashion via the `AtteanX::Store::MemoryTripleStore::Query`
class to iteratively construct queries over the loaded RDF data:

```
% perl Makefile.PL && make install
% cat test.pl
use v5.14;
use Attean::RDF;
my $store	= Attean->get_store('MemoryTripleStore')->new(filename => 'dbpedia.nt');
my $model	= Attean::TripleModel->new( stores => { 'http://example.org/' => $store } );
my $query	= AtteanX::Store::MemoryTripleStore::Query->new(store => $store);
my $t1		= Attean::TriplePattern->new(variable('s'), iri('http://dbpedia.org/ontology/longName'), variable('name'));
my $t2		= Attean::TriplePattern->new(variable('s'), iri('http://www.w3.org/2003/01/geo/wgs84_pos#lat'), variable('lat'));
my $t3		= Attean::TriplePattern->new(variable('s'), iri('http://www.w3.org/2003/01/geo/wgs84_pos#long'), variable('long'));
$query->add_bgp($t1, $t2, $t3);
$query->add_filter('name', 'regex', 'Island', 'i');
$query->add_sort(qw(name));
$query->add_project(qw(name));
my $iter	= $query->evaluate($model);
while (my $result = $iter->next) {
	say $result->as_string;
}

% perl test.pl
{name="Ascension Island"@en}
{name="Bouvet Island"@en}
{name="Cayman Islands"@en}
{name="Commander Islands"@en}
{name="Commonwealth of the Northern Mariana Islands"@en}
{name="Cook Islands"@en}
{name="Falkland Islands"@en}
{name="Faroe Islands"@en}
...
```

Command Line Example
---

The package also contains a Makefile for building a command-line tool that may
be used to query loaded RDF data directly, without using the perl API:

```
% make -f Makefile.cli
% ./ts -v dbpedia.nt
ts> begin
ts> bgp ?s <http://dbpedia.org/ontology/longName> ?name ?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat ?s <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?long
ts> filter regex ?name island i
ts> project name lat long
ts> sort name
ts> end
name="Ascension Island"@en lat=-7.933333333333334 long=-14.416666666666666 
name="Bouvet Island"@en lat=-54.43 long=3.38 
name="Cayman Islands"@en lat=19.333333333333332 long=-81.4 
name="Commander Islands"@en lat=55.2 long=165.98333333333332 
name="Commonwealth of the Northern Mariana Islands"@en lat=15.233333333333333 long=145.75 
name="Cook Islands"@en lat=-21.2 long=-159.76666666666668 
name="Falkland Islands"@en lat=-51.7 long=-57.85 
name="Faroe Islands"@en lat=62.0 long=-6.783333333333333 
...
```
