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

<!--TODO (Stage 2 scaffold — reworked to the agreed arc; 2026-07-06)

ARC (agreed):
1. The Transformation Phase (on-ramp) — carries a one-line OLAP/OLTP recap + link.
2. Two APIs, One Plan (slim recap of Concepts/builder-parser.md).
3. Laziness and the Point of Execution (recap Concepts/execution-lifecycle.md).
4. Expressions: The Transformation Vocabulary (recap Concepts/expressions.md).
5. Mental Models Beyond the Single Frame — Combining (joins, set ops) then
   Analytical (aggregation, window functions).
6. From Concepts to Methods — Meet the Dataset + The Method Map.
7. Conclusion.

DATASET (agreed): a shared flat family, customer_df + orders_df, built with the
dataframe! macro (the macro is flat-only; IntoArrayRef covers scalar vecs, never
List columns). The nested/array-column story is DERIVED from orders_df via
array_agg on reshaping.md (a round trip with .unnest_columns()); the
set-operation partner frames (archived / reordered-column) are owned by
set-operations.md.

DROPPED as duplicates of their owners (recorded per no-silent-deletion):
- SessionContext→LogicalPlan ASCII diagram — owner Concepts/builder-parser.md
  (also broke markdown.mdc §5.4 box style); the slim recap links there instead.
- "The Methodical Differences" comparison table — owner builder-parser.md.
- "DataFrame Strengths" / "Where SQL Shines" / "Use Case Recommendations"
  tables — owners builder-parser.md §Choosing the Right API + Concepts/index.md.
- OLAP/OLTP + row-based TableProvider tables — owner
  Concepts/architectural-dataframe.md §Architectural Fit; cut to a one-liner.

PENDING RELOCATION (move handshake, source side): the SQL-injection code example
(was under "Why DataFrames Feel Different") moves to Concepts/builder-parser.md
§Safety and Security, whose prose bullets gain the concrete example. Removed
here; recoverable from this file's git history. Add the destination arrival note
on builder-parser.md when that edit lands.

BROKEN-LINK REPAIR (cross-file): hybrid-sql.md links
"#when-row-based-tableproviders-outperform-columnar"; that section is removed
here. Retarget to
Concepts/architectural-dataframe.md#architectural-fit-olap-vs-oltp.

METHOD MAP: kept below as raw material. Rework in Stage 4: add rows for
.intersect() / .except() (SQL INTERSECT / EXCEPT) and .union_by_name_distinct();
link each row to its leaf page.

STAGE 6: title-line highlight, abstract, "Concepts covered on this page" table
(markdown-landing.mdc §2.1), and restore reference-style link definitions.
-->

# Transformations Concepts

<!-- TODO: **Title-line highlighting sentence** (bold, pyramidal) — Stage 6. -->

<!-- TODO: Abstract — Stage 6. -->

<!-- TODO: "Concepts covered on this page" table (replaces the Key Methods
table on concept pages, markdown-landing.mdc §2.1) — add when the H2 sections
below stabilize. -->

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

```{contents} Table of Content
:local:
:depth: 2
```

## The Transformation Phase

**Every transformation reshapes the _frame_ — the query plan and the schema it carries — so a handful of shared ideas make every method page that follows click into place.**

Transformations are the DataFrame's [life phase](../index.md#the-dataframe-lifecycle): once a DataFrame exists, you reshape it by chaining methods — filter, select, join, aggregate, sort, enrich. Each call returns a new `DataFrame` that extends the underlying `LogicalPlan` and derives a fresh `DFSchema`, the output contract of column names, types, and nullability. Your source data never changes; you shape the _frame_ around it, one composable step at a time, so the calls stack into a readable, programmatic pipeline.

A few ideas carry across every method that follows. Both the SQL and DataFrame APIs build that one plan, so what you learn here applies to either ([Two APIs, One Plan](#two-apis-one-plan)). The plan is lazy — it grows as you chain calls, deriving a new schema at each step; [Laziness and the Point of Execution](#laziness-and-the-point-of-execution) covers when it runs, and [Changing Schemas with DataFrame Methods](../Schema-Management/schema-dataframe-methods.md) covers how each method rewrites that schema. Every operation speaks one [expression vocabulary](#expressions-the-transformation-vocabulary), joined by a few [mental models](#mental-models-beyond-the-single-frame) once you combine or analyze more than a single frame — then [From Concepts to Methods](#from-concepts-to-methods) hands you a shared dataset and a map into the method pages.

:::{admonition} Where this runs
:class: seealso

Transformations run on DataFusion's columnar OLAP engine, built for scanning and aggregating large datasets. For point lookups or highly selective reads, a row-based source reached through a `TableProvider` can fit better — see [Architectural Fit: OLAP vs. OLTP](../Concepts/architectural-dataframe.md#architectural-fit-olap-vs-oltp).
:::

---

## Two APIs, One Plan

**Two APIs, one plan: SQL and the DataFrame API resolve to the same `LogicalPlan` — same optimizations, same execution, same performance. Only the ergonomics differ.**

DataFusion exposes one query engine through two front-ends: the SQL API parses a string into a plan, and the DataFrame API chains builder methods that construct that plan directly — no parsing step. The examples on this page use the DataFrame API, but the mental models apply to SQL just as well, and you can [mix the two at well-defined boundaries](hybrid-sql.md). For the full side-by-side — the convergence diagram, the ergonomics comparison, and the injection-safety story — see [Two Paths to the Same Plan](../Concepts/builder-parser.md).

---

## Laziness and the Point of Execution

**Chaining transformations builds a plan, not a result — nothing touches your data until an action asks for it, and that deferral is exactly what lets the optimizer rewrite the query first.**

Each transformation returns a new `DataFrame` that extends the `LogicalPlan` — no data moves. Execution begins only when you cross the **action boundary** with `.collect()`, `.show()`, or `.write_*()`, which hands the finished plan to the optimizer and then to the physical engine. Because DataFusion sees the whole pipeline before reading a single byte, it can reorder filters, push predicates into the scan, and choose efficient algorithms — the payoff of staying lazy. Call `.explain()` before an action to inspect the plan you've been building. The full lazy-to-streaming journey lives in [Execution Lifecycle](../Concepts/execution-lifecycle.md), and the action methods themselves in [Executing DataFrames](../Writing-DataFrames/executing-dataframes.md).

---

## Expressions: The Transformation Vocabulary

<!-- PLACEHOLDER (recap layer): one-paragraph recap of Expr and the col()/lit()
constructors; every transformation method consumes Expr trees. Link to
Concepts/expressions.md (owner). Note the function libraries (string, datetime,
encoding, nested/array) as the practical vocabulary — pointer to docs.rs;
coverage TODO tracked in expressions.md. -->

---

## Mental Models Beyond the Single Frame

<!-- PLACEHOLDER (section hook): single-frame verbs (select / filter / sort /
limit) are intuitive and need no separate model. The operations that DO need one
either COMBINE frames or ANALYZE across rows. Frame those two families here,
then hand each H3 down to its action page (condensed recaps only). Key
distinction the reader must carry forward: aggregation COLLAPSES rows; a window
KEEPS them. Order below: combining first, analytical second. -->

### Joins in Brief

<!-- PLACEHOLDER: join-type taxonomy (inner / left / right / full / semi / anti;
mark = internal, used for EXISTS decorrelation — verified
common/src/join_type.rs), equi vs. non-equi conditions, execution-strategy
one-liner (hash, sort-merge, nested-loop, piecewise-merge for range/inequality
predicates — verified physical-plan/src/joins/), and the LATERAL gap (SQL-only
today; DataFrame route is hybrid via ctx.sql()). Links to joins.md. -->

### Set Operations in Brief

<!-- PLACEHOLDER: position-based vs. name-based alignment; the union / intersect
/ except family; deduplication (.distinct(), .distinct_on()). Methods verified
in dataframe/mod.rs (union, union_by_name, union_by_name_distinct,
union_distinct, distinct, distinct_on, intersect, except). Links to
set-operations.md. -->

### Aggregation in Brief

<!-- PLACEHOLDER: aggregation collapses rows into per-group summaries
(GROUP BY → .aggregate(group_exprs, agg_exprs)). Sets up the contrast the window
brief builds on. Links to aggregations.md. -->

### Window Functions in Brief

<!-- PLACEHOLDER: a window function is an aggregate that keeps the rows;
partition / order / frame model; built with the ExprFunctionExt builder. Links
to window-functions.md. -->

---

## From Concepts to Methods

<!-- PLACEHOLDER (transition — the bridge from this cognitive page into the
action pages). -->

### Meet the Dataset

<!-- PLACEHOLDER: introduce the running-example family ONCE here — a flat
customer_df + orders_df built with the dataframe! macro (macro is flat-only).
Show the rendered starting tables so every action page can open on them (jump-in
mitigation, hidden boilerplate).
  customer_df: customer_id, name, region, signup_date
  orders_df:   order_id, customer_id, product, amount, quantity, order_date
This family serves single-frame verbs, joins (on customer_id), aggregation and
window functions (group / partition by customer_id), and subqueries. The
array-column / unnest story is DERIVED from orders_df via array_agg on
reshaping.md; the set-operation partner frames live on set-operations.md.
index.md carries a rendered preview only (markdown-landing.mdc §1.2). This
thread replaces the old "data-cleaning journey". -->

### The Method Map

<!-- PLACEHOLDER + RAW MATERIAL below (moved from Transformations/index.md).
Rework in Stage 4: add rows for .intersect() / .except() (SQL INTERSECT /
EXCEPT) and .union_by_name_distinct(); link each row to its leaf page. -->

**The DataFusion DataFrame-API and SQL-API share the same execution engine—but the _experience_ of writing them is fundamentally different.**

This table maps SQL operations to their DataFrame equivalents. Methods marked **Unique** have no direct SQL counterpart—these are where the DataFrame API provides capabilities beyond standard SQL.

| Category        |       SQL Operation        |             DataFrame Method             | Key Differences & Superpowers                                                               |
| --------------- | :------------------------: | :--------------------------------------: | ------------------------------------------------------------------------------------------- |
| **Filtering**   |         [`WHERE`]          |              [`.filter()`]               | Chainable predicates; programmatic filter building.                                         |
| **Selection**   |         [`SELECT`]         |   [`.select()`], [`.select_columns()`]   | [`.select()`] supports expressions; [`.select_columns()`] is a simple projection.           |
| **Selection**   |       [`AS`] (alias)       | [`.alias()`], [`.with_column_renamed()`] | [`.with_column_renamed()`] renames existing columns without expressions.                    |
| **Mutation**    |   (No direct equivalent)   |            [`.with_column()`]            | **Unique**: Add or replace a column while keeping all others.                               |
| **Aggregation** |        [`GROUP BY`]        |             [`.aggregate()`]             | Groups data and applies aggregate functions.                                                |
| **Joins**       |          [`JOIN`]          |       [`.join()`], [`.join_on()`]        | [`.join_on()`] allows arbitrary boolean expressions for join conditions.                    |
| **Sorting**     |        [`ORDER BY`]        |               [`.sort()`]                | Sort by one or multiple expressions.                                                        |
| **Limiting**    |   [`LIMIT`] / [`OFFSET`]   |               [`.limit()`]               | [`limit(skip, fetch)`][`.limit()`] handles both offset and limit.                           |
| **Set Ops**     |   [`UNION ALL`][`union`]   |               [`.union()`]               | concatenates DataFrames.                                                                    |
| **Set Ops**     |         [`UNION`]          |          [`.union_distinct()`]           | concatenates and removes duplicates.                                                        |
| **Set Ops**     |   (No direct equivalent)   |           [`.union_by_name()`]           | **Unique**: Unions based on column names, forgiving column order mismatches.                |
| **Distinct**    |   [`DISTINCT`][`select`]   |             [`.distinct()`]              | Removes duplicate rows based on all columns.                                                |
| **Distinct**    | [`DISTINCT ON`] (Postgres) |            [`.distinct_on()`]            | **Unique**: Deduplicates based on specific columns, keeping the "first" row per sort order. |

:::{note}
While you can mix SQL and DataFrames (see [Mixing SQL and DataFrames](hybrid-sql.md)), mastering these native methods unlocks the full power of programmatic data manipulation.
:::

---

## Conclusion

<!-- PLACEHOLDER: low-level recap + handoff to selection.md (the first action
page in the reading order). Written in Stage 6. -->
