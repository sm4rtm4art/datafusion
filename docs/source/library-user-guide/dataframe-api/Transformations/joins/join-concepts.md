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

# Join Concepts

**Reason about a DataFusion join as a binary logical relationship whose planned schema and row semantics govern its result.**

DataFusion join results become predictable when you separate the schema fixed during planning from the rows determined by the condition, join type, and input data. This result-oriented model explains unmatched-row preservation, duplicate-driven multiplication, one-sided outputs, and naming conflicts without descending into physical execution. The same reasoning extends to self-joins and multi-way trees and distinguishes joins from filters and set operations.

:::{admonition} New to Joins?
:class: seealso

This page assumes basic join familiarity and focuses on DataFusion's result model. For a general SQL walkthrough, start with the [PostgreSQL joins tutorial][postgresql-join-tutorial]; see [Further Reading](#further-reading) for DataFusion-specific next steps.
:::

**Concepts covered on this page**

| Concept                                                           | Reader question                                                |
| :---------------------------------------------------------------- | :------------------------------------------------------------- |
| [Result dimensions](#fusing-dataframes-what-a-join-decides)       | What must you account for to predict a join result?            |
| [Logical-plan representation](#how-joins-extend-the-logical-plan) | What does a DataFrame join call add to the lazy `LogicalPlan`? |
| [Result schema](#how-joins-shape-result-columns)                  | Which fields appear, in what order, and with what nullability? |
| [Result rows](#how-joins-shape-result-rows)                       | Which rows appear, and when can matches multiply them?         |
| [Binary composition](#binary-join-composition)                    | How do self-joins and multi-way joins reuse the same model?    |
| [Operation boundaries](#joins-and-related-operations)             | How do joins differ from filters and set operations?           |

:::{admonition} Style Note
:class: note
:collapsible: open

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution
- **Input roles:** Base DataFrame = method receiver/left input; Extension DataFrame = right argument/right input; `JoinType` controls preservation.
- **Filter terms:** `filter` = [`.join()`] argument participating in matching; [`.filter()`] = `DataFrame` method filtering rows at its pipeline position.

:::

```{contents} Table of Contents for Join Concepts
:local:
:depth: 2
```

## Fusing DataFrames: What a Join Decides

**A join combines two logical inputs into one result: the input schemas and join type fix which columns that result can carry, while matching and preservation decide which of its rows survive, vanish, or multiply.**

A join relates rows from two [`DataFrame`][dataframe] inputs through a condition.
Inner and outer joins carry fields from both inputs; semi and anti joins return
one side after testing for matches, while mark joins return one side plus a
boolean `mark` field. Its two sides are _logical_ inputs: they may be two views
of the same `DataFrame` as readily as two separate sources. The DataFrame API
expresses the horizontal direction through [`.join()`] and [`.join_on()`], which
extend a lazy logical plan rather than touching rows.

An inner join on `customer_id`, shown with one copy of the key and in
illustrative row order:

```text
customers                  orders
┌─────────────┬───────┐    ┌──────────┬─────────────┐
│ customer_id │ name  │    │ order_id │ customer_id │
├─────────────┼───────┤    ├──────────┼─────────────┤
│      1      │ Alice │    │   101    │      1      │
│      2      │ Bob   │    │   103    │      3      │
└─────────────┴───────┘    └──────────┴─────────────┘
               │                         │
               └─────────┬───────────────┘
                         │ INNER JOIN ON customer_id
                         ▼
simplified, projected result
┌─────────────┬───────┬──────────┐
│ customer_id │ name  │ order_id │
├─────────────┼───────┼──────────┤
│      1      │ Alice │   101    │
└─────────────┴───────┴──────────┘
```

- **Matched:** Alice and order `101` satisfy the condition, so one result row
  carries fields from both inputs.
- **Unmatched:** Bob has no order and order `103` has no customer, so an inner
  join drops both.
- **Not a set overlap:** the result is built from row _combinations_, which is
  why [a Venn diagram models joins poorly][joins-not-venn-diagrams].

The two methods differ in how that condition is written, not in what the join
means: [`.join()`] takes named key columns from each side plus an optional
[`Expr`][expr] filter, while [`.join_on()`] takes [`Expr`][expr] conditions
alone. [Join Conditions](join-conditions.md) covers writing the condition, and
[Join Types](join-types.md) covers choosing the variant.

:::{admonition} SQL and the DataFrame API
:class: note

SQL states the whole relationship declaratively; the DataFrame API composes the
same relationship incrementally and mixes it with application logic. Both routes
plan and execute through the same pipeline — [Parser versus
Builder][builder-parser] covers the choice, and the [SQL `JOIN`
clause][sql-join-clause] covers join syntax.

:::

:::{admonition} Four Questions for Predicting a Join Result
:class: important

Use four questions to predict the result:

- **Matching:** Which row combinations satisfy the condition?
- **Preservation:** Which matched and unmatched rows contribute?
- **Cardinality:** How many result rows can each input row produce?
- **Payload:** Which input fields can each result row carry?

:::

### How Joins Extend the Logical Plan

**Calling [`.join()`] or [`.join_on()`] appends one binary [`Join`][join-struct] node to the lazy [`LogicalPlan`], recording its inputs, matching condition, preservation rule, and derived payload before data is read.**

Joins are lazy: either call adds a binary [`Join`][join-struct] node to the
[`LogicalPlan`] rather than reading rows. Both methods produce that same node
type; their inputs populate its fields.

| Method input                                             | `Join` field                               | Meaning                                                                     |
| -------------------------------------------------------- | ------------------------------------------ | --------------------------------------------------------------------------- |
| method receiver → `left`; right-hand DataFrame → `right` | [`left`][join-left], [`right`][join-right] | local side roles that the join type interprets                              |
| join type                                                | [`join_type`][join-type-field]             | result behavior, including returned-side payload and unmatched-row handling |
| [`.join()`] named key pairs                              | [`on: Vec<(Expr, Expr)>`][join-on]         | equality key pairs                                                          |
| [`.join()`] `filter: Option<Expr>`; [`.join_on()`] exprs | [`filter`][join-filter]                    | residual or complete matching predicate                                     |
| _(derived — no method input)_                            | [`schema`][join-schema]                    | output [`DFSchema`][dfschema] from inputs + join type                       |

The method receiver — the DataFrame you call [`.join()`] or [`.join_on()`] on —
becomes [`left`][join-left]; the right-hand DataFrame passed into the method
becomes [`right`][join-right]. These local side roles are interpreted by the
join type. [`.join()`] records named key pairs in `on` and an optional
[`Expr`][expr] in `filter`; [`.join_on()`] AND-combines its expressions into
`filter` and leaves `on` empty, though a later optimizer pass may extract
equalities into `on`. [Join Conditions](join-conditions.md) covers construction
details. The [`schema`][join-schema] is derived from the inputs and join type,
while cardinality depends on the input data.

The shapes below locate those method inputs on each call:

```text
// .join() — named keys → `on`; optional Expr → `filter`
// Method receiver is left; the right-hand DataFrame is the API parameter `right`.

left.join(
    right,                 // right-hand DataFrame → right
    JoinType::Left,        // join type (Inner|Left|Right|Full ...)
    &["key_l", …],         // left_cols → on (left keys)
    &["key_r", …],         // right_cols → on (right keys)
    Some(extra_pred),      // filter: Option<Expr>; or None
)?;

// .join_on() — every Expr AND-combined → `filter`; `on` starts empty

left.join_on(
    right,                 // right-hand DataFrame → right
    JoinType::Right,       // join type — same slot; variant picks preservation
    [eq_pred, extra_pred], // on_exprs → filter (AND); optimizer may later lift equalities into `on`
)?;
```

:::{admonition} From Logical Plan to Execution
:class: caution

A `Join` node defines the logical relationship; an action later lowers the
optimized logical plan into an `ExecutionPlan`. The physical plan selects a
physical operator and execution arrangement without changing the logical
semantics. [Why the Physical Plan Matters][physical-plan] explains that lowering.

:::

[Join Conditions](join-conditions.md) covers construction, [How Joins Shape
Result Columns](#how-joins-shape-result-columns) covers payload-schema detail,
[Join Validation](join-validation.md) shows how to inspect the selected operator
with `.explain()`.

---

## How Joins Shape Result Columns

**A join's output columns are settled at plan time: the two input schemas and the join type fix which fields appear, in which order, and which of them can be null.**

For joins that return fields from both inputs, fields arrive left input first,
then right input, and each side keeps its own field order.

**A two-sided equality join** keeps both key columns. The opening diagram
projects one copy of `customer_id`, but an actual two-key join carries it twice,
once from each input. Result schema here means this immediate join-node output,
not the final query schema after later projections.

**Inner and outer joins** return fields from both inputs. Semi and anti joins return
only the fields of the side they test, matching against the other input without
carrying its columns. Mark joins add a single boolean `mark` field to that side.
[Join Types](join-types.md) covers the payload and preservation behavior of
each variant.

**Nullability is determined at plan time.** A preserved row can have no source
values for the absent side, and DataFusion fills those fields with
[`NULL`][null-handling]. A left join marks every right-side field nullable, a
right join every left-side field, and a full join both, whether or not the data
turns out to hold an unmatched row. Inner, semi, and anti joins leave input
nullability unchanged.

### When Column Names Collide

**For joins that return both inputs, duplicate names can block schema construction; distinct qualifiers allow construction, but later unqualified references can remain ambiguous.**

The join node concatenates both schemas and then checks the combined names. Two
fields sharing a [relation qualifier][table-qualifiers] and a name fail that
check, as do two unqualified fields with the same name. A qualified field and an
unqualified field that share a name also fail because the mixed schema makes
unqualified lookup ambiguous. A self-join that returns both sides needs distinct
qualifiers for its two roles, typically with [`.alias()`]. Semi and anti joins
return only one side and do not create this collision.

Distinct qualifiers let the join schema be constructed. A later unqualified
reference remains ambiguous. Qualify the reference or rename the field before
joining — [Join Workflows](join-workflows.md) owns that schema shaping.

:::{admonition} Name Conflicts Fail the Plan, Not the Result
:class: warning

Construction-time failures include duplicate qualified names, duplicate
unqualified names, and mixed qualified/unqualified names that make unqualified
lookup ambiguous. With distinct qualifiers, a later unqualified lookup can still
raise `Ambiguous reference to unqualified field`. All occur while the plan is
built, before any data is read.

:::

---

## How Joins Shape Result Rows

**Input row counts do not predict a join's row count: matching, preservation, and multiplicity together decide whether an input row yields no row, one row, or many.**

Unlike the immediate schema, rows depend on input values the plan has not seen:
the condition fixes matching, the join type fixes preservation, and the data
drives cardinality.

### Candidate Row Pairs and Matching

**A pair matches only when the condition evaluates to `TRUE`: ordinary equality does not match `NULL`, though null-safe `.join_on()` conditions can.**

A **candidate row pair** consists of one row from each input. Pair-emitting joins
produce a result row only when the condition is `TRUE`. Under ordinary equality,
three-valued logic makes `NULL = NULL` yield `NULL`, so two rows whose keys are
both missing do not pair up. A null-safe
`.join_on()` condition can instead match them. [Null Handling in
`.join()`][null-join] covers that behavior and the null-safe alternative;
detailed condition construction belongs in [Join Conditions](join-conditions.md).

### Unmatched-Row Preservation

**Preservation decides whose unmatched rows still appear; it never creates a match, so a preserved row arrives with `NULL`s where the other input would have been.**

An **unmatched row** is an input row with zero matches. The join type determines
whether that row contributes once or is omitted:

| Two-sided join | Unmatched left row | Unmatched right row |
| :------------- | :----------------: | :-----------------: |
| Inner          |      Omitted       |       Omitted       |
| Left           |     Preserved      |       Omitted       |
| Right          |      Omitted       |      Preserved      |
| Full           |     Preserved      |      Preserved      |

A preserved unmatched row contributes one result row; an omitted row contributes
none. [How Joins Shape Result Columns](#how-joins-shape-result-columns) explains
how the result represents fields from the absent input. [Join
Types](join-types.md) covers each variant's full preservation behavior.
An outer join can preserve an unmatched row whose join key is `NULL`.

Preservation sets a floor, not a target. A left join returns at least as many
rows as its left input, because every left row appears at least once — but it can
return more, which is what the next section explains.

### Match Cardinality

**Each matching row repeats once per match, so duplicate keys multiply rows and a left join can return more rows than its left input.**

For a pair-emitting join, the number of matches controls an input row's
contribution:

- **Zero matches:** the row contributes no result unless its side is preserved,
  in which case it contributes one unmatched result row.
- **One match:** the row participates in one matched result row.
- **Multiple matches:** the row participates in one result row per satisfying
  pair.

Duplicate keys therefore multiply rows. Two left rows and three right rows
sharing the same equality key produce 2 × 3 = 6 matched pairs when no additional
condition excludes any of them. Checking a result against that expectation
belongs to [Join Validation](join-validation.md).

### Exceptions to the Pair Rule

**Semi and anti joins and cross joins are opposite exceptions to pair-emitting joins: the former reduce matching to an existence test, while the latter remove the matching restriction.**

Pair-emitting joins produce one result row for each matching pair. This section
contrasts the two exceptions: semi and anti joins use matching only to decide
whether a row is returned, whereas a cross join returns every candidate pair.

**Semi and anti joins use matching as an existence test and emit at most one row
from the returned side:**

- a semi join tests whether at least one match exists
- an anti join tests whether none exists.

Duplicate keys on the other side therefore cannot multiply the result.
[Join Types](join-types.md) describes their returned fields and full behavior.

**A cross join removes the matching restriction:** every left-right candidate
pair contributes one result row, so inputs of 1,000 and 5,000 rows produce five
million.
The DataFrame API has no cross-join [`JoinType`][jointype] variant; an `Inner`
join with empty key lists and no filter produces the same result, as does SQL
[`CROSS JOIN`][sql-cross-join]. An accidentally condition-less join is still a
legitimate plan, so DataFusion does not warn about it — [Join
Validation](join-validation.md) covers recognizing one before running it.

---

## Binary Join Composition

**Every join stays binary: a self-join and a three-way tree are compositions of two-input nodes, so the four result questions are answered once per node, not once per query.**

Each `Join` node takes two inputs (see [How Joins Extend the Logical
Plan](#how-joins-extend-the-logical-plan)), and its output becomes the next node's
input. Row multiplication carries forward — a leg that turns one row into three
hands three rows to the next leg, which can multiply them again — and so does
width, because each node carries the payload selected by its join type. Building
and shaping such chains is owned by [Join Workflows](join-workflows.md).

### Self-Joins

**Nothing about a self-join is special except naming: one source fills both roles, and the result follows the same rules once each role can be referenced unambiguously.**

Hierarchies and comparisons against the same relation — an employee matched to a
manager, a customer matched to the customer who referred them — use one input in
two roles: the same source fills the left and right positions. Identical field
names create the [name collision](#when-column-names-collide) described above;
giving one role its own qualifier with [`.alias()`] is a schema-shaping task
owned by [Join Workflows](join-workflows.md).

### Multi-Way Join Trees

**A three-way join is two binary joins, and the grouping is not cosmetic: re-associating outer joins can change which rows survive.**

Customers, their orders, and the payments against those orders form a
three-frame question. Either input may be the result of an earlier join, so the
three inputs form a tree of two binary joins:

```text
                    ┌─────────────┐
                    │   Join 2    │
                    └──────┬──────┘
                      ┌────┴────┐
                      ▼         ▼
               ┌─────────────┐ ┌───┐
               │   Join 1    │ │ C │
               └──────┬──────┘ └───┘
                  ┌───┴───┐
                  ▼       ▼
                ┌───┐   ┌───┐
                │ A │   │ B │
                └───┘   └───┘
```

- **Left and right stay local:** `A` and `B` fill those roles for `Join 1`,
  while the output of `Join 1` and `C` fill them for `Join 2`.
- **Tree shape follows where the subtree sits:** chaining `.join()` on an
  already-joined `DataFrame` builds a left-deep tree. A right-deep tree passes a
  prejoined subtree as the right input; a bushy tree uses prejoined subtrees on
  both sides.
- **The intermediate result is a plan subtree:** not a promise that DataFusion
  materializes it before evaluating the surrounding plan.
- **Grouping carries meaning:** a row preserved by `Join 1` enters `Join 2`
  `NULL`-filled on the absent side. Ordinary equality does not match `NULL`,
  though a null-safe condition can, so whether it survives depends on `Join 2`'s
  matching and preservation rules.

---

## Joins and Related Operations

**A join answers a relationship question between inputs; filters decide which existing rows remain, while set operations combine or compare whole rows.**

These operations can all change which rows appear, but their relational questions
distinguish them: matching and preservation, post-join retention, unary retention,
match existence or non-existence, and whole-row combination or comparison.

Unary filters use one input, while joins and set operations cross the frame
boundary by consuming another logical input. [Transformation Concepts] places
that difference in the broader transformation map.

### Join Conditions and Post-Join Filters

**A join condition decides which candidate pairs match; a post-join filter decides which join-result rows remain, so outer-join preservation can differ.**

In a left join of customers to orders, putting `orders.amount > 100` in the join
condition leaves customers without a qualifying order as preserved, `NULL`-extended
rows. Applying the predicate after the join instead evaluates the join-result rows,
so a `NULL` order-side amount does not pass the post-join predicate.

:::{admonition} Preserved Rows Can Still Be Filtered Out
:class: caution

On an unmatched side, preservation produces `NULL` values. A predicate on those
values after the join may reject the result rows.

:::

[Join Conditions](join-conditions.md) owns constructing join conditions, and
[Filtering Rows](../filtering.md) owns filter predicates and recipes. This
distinction concerns logical semantics, not whether an optimizer can move a
particular predicate.

### Existence Joins and Unary Filters

**A unary filter decides whether one row qualifies; a semi or anti join decides whether that row has—or lacks—a match in another input.**

A unary customer predicate depends only on the customer row; a semi join asks
which customers have matching orders, while an anti join asks which do not. In
SQL, [`EXISTS` / `IN` subqueries][sql-exists] are a common spelling of the same
existence question.

Replacing a semi or anti join with an inner join followed by a projection is not
generally equivalent. An inner join can multiply a row when several matches exist,
whereas a semi join asks only whether at least one match exists. [Join
Types](join-types.md) owns full semi and anti join behavior.

### Joins and Set Operations

**A join uses matching and preservation to relate input rows; a set operation stacks or compares whole rows under a shared layout.**

Joining customers to orders relates them through `customer_id`; unioning monthly
order frames instead appends complete order rows under a shared layout. The
methods make that difference concrete: [`.union()`] and [`.union_by_name()`]
stack complete rows; [`.intersect()`] keeps whole rows present in both inputs,
and [`.except()`] keeps whole rows from the left that are absent from the right.
The corresponding distinct variants remove duplicate result rows. [Set
Operations] owns alignment, duplicate-policy detail, and variant selection.

---

## Conclusion

At each binary `Join` node, derive the output fields from the input schemas and join type, then account for result rows from the condition, preservation rule, and match multiplicities. Carry that node-level reasoning through larger trees, then use [Join Conditions](join-conditions.md), [Join Types](join-types.md), [Join Workflows](join-workflows.md), and [Join Validation](join-validation.md) to construct and check each relationship.

### Further Reading

- [PostgreSQL joins tutorial][postgresql-join-tutorial] — a general beginner
  walkthrough of matching, outer preservation, aliases, and self-joins; its
  examples use PostgreSQL SQL, but the relational model transfers.
- [Join Conditions](join-conditions.md) and [Join Types](join-types.md#jointype-catalogue) —
  express relationships and choose preservation and payload behavior.
- [Join Workflows](join-workflows.md) and [Join Validation](join-validation.md)
  — compose joins and check plans and results.
- DataFusion [`.join()`] and [`.join_on()`] API docs — exact signatures and
  parameters.
- [SQL `JOIN` clause][sql-join-clause] — the SQL counterpart.
- [Joins as row combinations rather than Venn diagrams][joins-not-venn-diagrams]
  — explains row combinations and why Venn diagrams fit set operations better.
- [Set Operations] — the whole-row counterpart, including alignment and
  duplicate semantics.

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`.except()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[`jointype`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[builder-parser]: ../../Concepts/builder-parser.md#choosing-the-right-api-for-the-task
[dataframe]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[dfschema]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[expr]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[join-filter]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Join.html#structfield.filter
[join-left]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Join.html#structfield.left
[join-on]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Join.html#structfield.on
[join-right]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Join.html#structfield.right
[join-schema]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Join.html#structfield.schema
[join-struct]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Join.html
[join-type-field]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Join.html#structfield.join_type
[joins-not-venn-diagrams]: https://blog.jooq.org/say-no-to-venn-diagrams-when-explaining-joins/
[null-handling]: ../../Concepts/null-handling.md
[null-join]: ../../Concepts/null-handling.md#null-handling-in-join
[physical-plan]: ../../Concepts/execution-lifecycle.md#why-the-physical-plan-matters
[postgresql-join-tutorial]: https://www.postgresql.org/docs/current/tutorial-join.html
[set operations]: ../set-operations.md
[sql-exists]: ../../../../user-guide/sql/subqueries.md#-not--exists
[sql-cross-join]: ../../../../user-guide/sql/select.md#cross-join
[sql-join-clause]: ../../../../user-guide/sql/select.md#join-clause
[table-qualifiers]: ../../Schema-Management/schema-anatomy.md#table-qualifiers
[transformation concepts]: ../transformation-concepts.md#combining-multiple-dataframes
