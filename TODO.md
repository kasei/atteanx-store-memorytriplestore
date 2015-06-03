TODO
====

* Add support for property paths to ::Query
* Ensure arguments are argument compatible for string filter functions.
* Translate document-scoped bnode IDs to unique IDs on import
* Perform simple lexical verification/canonicalization on input for known D-types (numerics, dates)
* Implement simple filtering over BGP matching (1-variable SARGs plus simple 2-variable ops)
    * Numeric logical testing (var, const)
    * Date logical testing (var, const)
    * LANGMATCHES (LANG(var), const)
* Implement aggregates over BGP matching, with support for grouping by variable (but not complex expressions)
	* Use an AVL tree with `item = (nodeid_t* group_key, table_t* table)`, then evaluate just like the materialization used for sorting
    * SUM
    * AVG
    * MIN
    * MAX
    * COUNT
* Implement subset of property paths (does the obvious implementation correlate to the ALP algorithm?)
    * `p*`
    * `^p` (disregard; this can be avoided during planning)
    * `p/q`
    * `p|q`
    * `!p`
* Add optional text indexing (map words to list of graph node IDs; specify participating predicates)
    * Optimize matching of BGPs when there is a filter which is subsumed by keyword matching (CONTAINS filters where the pattern contains at least one whole word)
* Graph algorithm to produce schema statistics useful for query planning (identify (inverse-) functional properties, predicate cardinality, etc.)

Done
====

* Make AtteanX::Store::MemoryTripleStore::::Query consume Attean::API::Plan
* Implement count of triple matching, expose in XS (run triple matching, but avoid materializing perl objects for results)
* Generalize BGP matching to a query structure that can nest operations (BGPs, filters, aggregates, property paths)
* Implement simple filtering over BGP matching (1-variable SARGs plus simple 2-variable ops)
    * ISIRI/ISLITERAL/ISBLANK (var)
    * SAMETERM (var, const)
    * STRSTARTS (var, const)
    * STRENDS (var, const)
    * REGEX (var, const) (link with pcre; base on code in ts.c)
    * CONTAINS (var, const)
* Implement dump to/load from disk
    * Serialize term values AVL tree (simple loading code as it's guaranteed to be unique)
    * Dump edges array directly to disk (with int elements in network order)
* Implement subset of property paths (does the obvious implementation correlate to the ALP algorithm?)
    * `p+`
