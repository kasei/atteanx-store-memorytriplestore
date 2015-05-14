`AtteanX::Store::MemoryTripleStore` - In-memory RDF triple store
===

AtteanX::Store::MemoryTripleStore provides an in-memory triple-store that is
especially optimized for matching BGPs which contain at least one bound subject
or object (e.g. preferring star or path queries rather than analytical
queries).

The triple store is read-only, requiring a filename to be specified during
construction which points at an N-Triples or Turtle file containing the RDF
data to load. The triple store links with libraptor2 to allow fast parsing of
input files.
