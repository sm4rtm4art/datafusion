<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

<!--
MOVE HANDSHAKE: Conceptual orientation, inherited execution concepts, and
Further Reading arrived from ../joins.md. The migration source remains
unchanged for coordinator comparison.

LOCAL TODO OWNERS: JOIN-TODO-001, JOIN-TODO-002, JOIN-TODO-003,
JOIN-TODO-013, JOIN-TODO-015, JOIN-TODO-016, JOIN-TODO-020, JOIN-TODO-021,
JOIN-TODO-022, JOIN-TODO-023, JOIN-TODO-025, and JOIN-TODO-026.
-->
<!-- JOIN-TODO-001: Add the title-line highlighting sentence, abstract, Concepts Covered table, and conclusion after this leaf stabilizes. -->
<!-- JOIN-TODO-020: This leaf temporarily owns inherited deep execution material pending an approved extraction destination. -->
<!-- JOIN-TODO-025: Register this leaf as a doctest after Author approval. -->
# Join Concepts

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::

```{contents} Table of Contents for Join Concepts
:local:
:depth: 2
```

<!-- JOIN-TODO-002 JOIN-TODO-003: Preserved from the orientation pass. -->
<!-- JOIN-TODO-023: Broaden this unapproved opener so logical left/right inputs may derive from the same DataFrame. -->
## Relate Rows Across DataFrames

**A join crosses the frame boundary by relating rows from two logical inputs; the matching relationship determines which rows pair, and the join type determines which matches and non-matches the result preserves.**

Where earlier transformations reshape one `DataFrame`, a join introduces a left and a right input. Three choices define the result: the two inputs, the key columns or condition that relate their rows, and the [`JoinType`] that controls preservation. This makes joins useful for enriching records with related data, filtering by whether a relationship exists, and reconciling records across systems.

The matches can change both the schema and the number of rows. Most join types carry columns from both inputs into the result, while semi and anti joins use the other input only to test for a match and return columns from one side. In joins that emit matched row pairs, one-to-many and many-to-many relationships can repeat input rows. A row with no match may disappear or be preserved with `NULL` values, depending on the join type.

This row relationship distinguishes joins from [set operations]. A join correlates rows using keys or a condition and often places columns from the inputs side by side. A set operation aligns complete rows under a compatible schema to concatenate or compare them. See [Transformation Concepts] for the broader frame-boundary model and [Set Operations] when the task is whole-row combination rather than row matching.

:::{admonition} Choose the API That Makes the Join Logic Clear
:class: note

Use the DataFrame API when Rust code needs to generate the relationship conditionally or compose the joined result directly with other transformations. [`.join()`] expresses named equality keys, while [`.join_on()`] accepts expression conditions.

Use SQL when a fixed multi-table relationship or a join form expressed only in SQL is clearer to read and maintain. Both APIs produce DataFusion logical plans and use the same optimizer and execution engine. Choose between them for clarity, maintainability, and composition—not for an assumed execution-speed advantage.

:::

With the frame boundary established, the first practical decision is how to express the matching relationship: as named key columns or as expression conditions.

---

<!-- JOIN-TODO-013 JOIN-TODO-016: Trim this to plan interpretation or move it to the execution owner; verify all operator, optimizer, Arrow, SIMD, late-materialization, and benchmark claims. -->
## How Joins Execute

Under the hood, DataFusion selects from [several join algorithms] based on your data:

| Algorithm                  | When Used                                                                                      |
| :------------------------- | :--------------------------------------------------------------------------------------------- |
| [**Hash Join**]            | Default for equi-joins (`=`). Builds a hash table on the smaller side, probes with the larger. |
| [**Sort-Merge Join**]      | Pre-sorted inputs; can spill to disk for huge datasets.                                        |
| [**Symmetric Hash Join**]  | Streaming/unbounded data—both sides build hash tables, rows pruned via sliding windows.        |
| [**Nested Loop Join**]     | General non-equi conditions where hash-based algorithms don't apply.                           |
| [**Piecewise Merge Join**] | Single range filter (`<`, `>`, `<=`, `>=`)—much faster than nested loop for these cases.       |
| [**Cross Join**]           | Cartesian product—used for SQL [`CROSS JOIN`] and [`.join()`] with empty key lists.            |

The optimizer _can_ (based on configuration and statistics):

- **Swap sides** to put the smaller table on the build side
- **Choose partition mode**—broadcast small tables or hash-partition both sides
- **Push dynamic filters**—min/max bounds from the build side skip irrelevant probe data (e.g., Parquet row groups)

These behaviors are tunable via [`datafusion.optimizer`] settings.

All join algorithms leverage [Arrow]'s columnar format: instead of copying rows, DataFusion computes index arrays and uses vectorized [`take()`] operations to assemble results efficiently.

> **Why DataFusion Joins Are Fast**
>
> Unlike traditional row-based databases, DataFusion combines several modern techniques:
>
> | Technique                          | Benefit                                                                                                                                          |
> | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
> | **Columnar format (Arrow)**        | Read only the columns you need; SIMD instructions process thousands of keys in parallel\*                                                        |
> | **Vectorized execution**           | Joins process batches of rows, not one at a time—simple inner loops let CPUs parallelize at the instruction level                                |
> | **SQL = DataFrame**                | Both compile to the same `LogicalPlan`—identical optimizer benefits regardless of API choice                                                     |
> | **Statistics-driven optimization** | Table metadata (row counts, min/max) guide join order and algorithm selection—[**16x faster** on TPC-H benchmarks][datafusion join optimization] |
> | **Late materialization**           | During joins, only key columns + row indices are processed; other columns are fetched afterward                                                  |
>
> \*SIMD requires `RUSTFLAGS='-C target-cpu=native'`. See [Crate Configuration](../../../../user-guide/crate-configuration.md).
>
> The result: you describe _what_ to join, and the optimizer handles _how_—often matching or exceeding hand-tuned imperative code.

---

<!-- JOIN-TODO-015 JOIN-TODO-016: Add the missing conclusion before Further Reading; prune links and remove unsupported promotional descriptions. -->
## Further Reading

Joins are fundamental yet often misunderstood. These resources provide deeper understanding:

**DataFrame APIs** — Similar concepts in other libraries:

| Resource                 | Focus                                                        |
| :----------------------- | :----------------------------------------------------------- |
| [Spark Join Guide]       | Conceptually similar API with extensive examples             |
| [Polars Join Operations] | Rust-native DataFrame library, closest to DataFusion's model |
| [DataFusion `.join()`]   | Official Rust API documentation                              |

**Join Algorithms & Optimization** — How joins execute under the hood:

| Resource                           | Focus                                                                                    |
| :--------------------------------- | :--------------------------------------------------------------------------------------- |
| [Optimizing SQL & DataFrames Pt 1] | Andrew Lamb on DataFusion's optimizer—why SQL and DataFrames compile to the same plan    |
| [Optimizing SQL & DataFrames Pt 2] | Deep dive: predicate pushdown, projection pushdown, join ordering in DataFusion          |
| [DataFusion Join Optimization]     | How DataFusion uses table statistics to choose build/probe sides—**16x faster** on TPC-H |
| [CMU Join Algorithms]              | Andy Pavlo's database course—excellent video lectures on hash/sort-merge joins           |
| [Hash Join (Wikipedia)]            | How hash tables enable O(n+m) equi-joins                                                 |
| [Sort-Merge Join]                  | Why pre-sorted data enables efficient streaming joins                                    |
| [Join optimization strategies]     | How databases choose algorithms and what you can control                                 |

**SQL Semantics** — Conceptual foundations:

| Resource                                                                  | Focus                                                              |
| :------------------------------------------------------------------------ | :----------------------------------------------------------------- |
| [Visual JOIN guide]                                                       | Interactive visualization of all join types with animated examples |
| [Join tutorial]                                                           | Why Venn diagrams are misleading for understanding joins           |
| [Semi and Anti joins explained]                                           | First-class existence checks that SQL forgot                       |
| [PostgreSQL JOIN docs]                                                    | Authoritative reference—DataFusion follows PostgreSQL semantics    |
| [NULL handling in joins]                                                  | Why `NULL = NULL` is `UNKNOWN`, not `TRUE`                         |
| [Understanding SQL Dialects][understanding sql dialects (medium-article)] | Medium article about different SQL dialects                        |

[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[datafusion `.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`jointype`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html
[`datafusion.optimizer`]: https://docs.rs/datafusion/latest/datafusion/optimizer/index.html
[`take()`]: https://docs.rs/arrow/latest/arrow/compute/kernels/take/fn.take.html "Arrow kernel: select elements by index"
[arrow]: https://arrow.apache.org/ "Apache Arrow: columnar in-memory format"
[`cross join`]: ../../../../user-guide/sql/select.md#cross-join
[**cross join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.CrossJoinExec.html "Cartesian product of two tables"
[**hash join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.HashJoinExec.html "Equi-join using hash table on build side"
[**nested loop join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.NestedLoopJoinExec.html "General non-equi join conditions"
[**piecewise merge join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.PiecewiseMergeJoinExec.html "Optimized for single range conditions"
[**sort-merge join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SortMergeJoinExec.html "Join pre-sorted inputs with optional spilling"
[**symmetric hash join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SymmetricHashJoinExec.html "Streaming join for unbounded data"
[several join algorithms]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/index.html "DataFusion join implementations"
[cmu join algorithms]: https://www.youtube.com/watch?v=YIdIaPopfpk&list=PLSE8ODhjZXjYMAgsGH-GtY5rJYZ6zjsd5&index=12 "CMU 15-445 Lecture 11: Join Algorithms (Andy Pavlo)"
[datafusion join optimization]: https://xebia.com/blog/making-joins-faster-in-datafusion-based-on-table-statistics/ "Making Joins Faster in DataFusion Based on Table Statistics"
[hash join (wikipedia)]: https://en.wikipedia.org/wiki/Hash_join "Hash join algorithm explanation"
[join optimization strategies]: https://use-the-index-luke.com/sql/join "How databases optimize joins and what you can control"
[join tutorial]: https://blog.jooq.org/say-no-to-venn-diagrams-when-explaining-joins/ "Why Venn diagrams mislead when explaining joins"
[null handling in joins]: https://modern-sql.com/concept/null "Why NULL comparisons return UNKNOWN, not TRUE/FALSE"
[optimizing sql & dataframes pt 1]: https://www.influxdata.com/blog/optimizing-sql-dataframes-part-one/ "Optimizing SQL (and DataFrames) in DataFusion: Part 1"
[optimizing sql & dataframes pt 2]: https://www.influxdata.com/blog/optimizing-sql-dataframes-part-two/ "Optimizing SQL (and DataFrames) in DataFusion: Part 2"
[polars join operations]: https://docs.pola.rs/user-guide/transformations/joins/ "Polars DataFrame join operations"
[postgresql join docs]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN "Authoritative reference for join semantics"
[semi and anti joins explained]: https://blog.jooq.org/semi-join-and-anti-join-should-have-its-own-syntax-in-sql/ "Why Semi/Anti joins deserve first-class syntax"
[sort-merge join]: https://en.wikipedia.org/wiki/Sort-merge_join "Sort-merge join algorithm"
[spark join guide]: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html "Apache Spark SQL join syntax and examples"
[understanding sql dialects (medium-article)]: https://medium.com/@abhapratiti27/understanding-sql-dialects-a-deeper-dive-into-the-linguistic-variations-of-sql-e7e2fdb7509b
[visual join guide]: https://joins.spathon.com/ "Interactive visual guide to SQL joins"
[transformation concepts]: ../transformation-concepts.md#combining-multiple-dataframes
[set operations]: ../set-operations.md
