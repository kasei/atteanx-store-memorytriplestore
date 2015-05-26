TODO
====

* Translate document-scoped bnode IDs to unique IDs on import
* Perform simple lexical verification/canonicalization on input for known D-types (numerics, dates)
* Implement dump to/load from disk
    * Serialize term values AVL tree (simple loading code as it's guaranteed to be unique)
    * Dump edges array directly to disk (with int elements in network order)
* Implement count of triple/BGP matching, expose in XS (run BGP matching, but avoid materializing perl objects for results)
* Implement simple filtering over BGP matching (1-variable SARGs plus simple 2-variable ops)
    * Numeric logical testing (var, const)
    * Date logical testing (var, const)
    * CONTAINS (var, const)
    * LANGMATCHES (LANG(var), const)
* Implement aggregates over BGP matching, with support for grouping by variable (but not complex expressions)
    * SUM
    * AVG
    * MIN
    * MAX
    * COUNT
* Implement subset of property paths (does the obvious implementation correlate to the ALP algorithm?)
    * `^p`
    * `p/q`
    * `p|q`
    * `p*`
    * `p+`
    * `!p`
* Add optional text indexing (map words to list of graph node IDs; specify participating predicates)
    * Optimize matching of BGPs when there is a filter which is subsumed by keyword matching (CONTAINS filters where the pattern contains at least one whole word)
* Graph algorithm to produce schema statistics useful for query planning (identify (inverse-) functional properties, predicate cardinality, etc.)

Done
====

* Generalize BGP matching to a query structure that can nest operations (BGPs, filters, aggregates, property paths)
* Implement simple filtering over BGP matching (1-variable SARGs plus simple 2-variable ops)
    * ISIRI/ISLITERAL/ISBLANK (var)
    * SAMETERM (var, const)
    * STRSTARTS (var, const)
    * STRENDS (var, const)
    * REGEX (var, const) (link with pcre; base on code in ts.c)


Notes
=====

`query_t` should represent a sequence of graph-walking operations, in the same way `bgp_t` represents a sequence of triple pattern walks. Therefore, `query_t` can represent only operations that can pipeline individual results:

* BGP
* Filter
* Property paths
* Pipelined aggregates with no grouping (`current_match` will have to be upgraded to hold not just a node_id array, but aggregate state values (e.g. `count` and `sum` for `AVG`))

Aggregation *with* grouping will need more complex setup, as there may be an unknown number of groups which each will need their own aggregation state values.
