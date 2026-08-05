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
DRAFT TRANSFER (Author-approved 2026-08-04): Body promoted from
WIP-join-concepts.md. Physical operator catalog removed with that source.

PLAN REWORK (Author-approved 2026-08-05): first H2 renamed and tightened to an
on-ramp that keeps the logical-plan H3; Result Schema Shape merged into its H2;
self-join trimmed to the role claim; closing checklist replaced by a Conclusion.
Deviation on record: the first H2 deliberately exceeds the markdown.mdc 7.4
budget because it owns the on-ramp and the plan-time mechanism together.
JOIN-TODO-025 closed — all five leaves are registered in datafusion/core/src/lib.rs.
See joins/index.md Plan freeze and WIP record.

FIRST-H2 DRAFT (Author-approved 2026-08-05): three duplications cut (SQL lead
vs admonition, transition vs H3 highlight, H3 closing vs H3 highlight); diagram
moved above the method material with a bulleted reading; on/filter corrected to
include .join()'s filter argument; caution extended with the planner's on-slot
branch, without operator names or speed claims. JOIN-TODO-023 resolved here.

LOCAL TODO OWNERS: JOIN-TODO-001, JOIN-TODO-013, JOIN-TODO-015,
JOIN-TODO-016, JOIN-TODO-022, JOIN-TODO-026.
-->
<!-- JOIN-TODO-001: Title-line highlight, abstract, Concepts Covered table, and conclusion are provisional until Polish. -->
<!-- JOIN-TODO-013 JOIN-TODO-022 JOIN-TODO-026: Deep execution material remains excluded; preserve these identifiers without inferring resolution. -->

# Join Concepts

<!-- TODO: Abstract is written last !
**A DataFusion join relates two logical inputs, and its matching, preservation, cardinality, and payload rules together determine the result.**

DataFrame transformations build a lazy logical plan, so a join first exists as a
binary relationship between two logical inputs. Reasoning about that relationship
requires more than naming a join type: readers must account for which row pairs
match, which unmatched rows remain, how matches affect the row count, and which
columns each output row carries. This page develops that result-oriented model and
then applies it to semi and anti joins, self-joins, multi-way join trees, filters,
and set operations.

-->

**Concepts covered on this page**

| Concept                                                                                                    | Reader question                                                      |
| :--------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------- |
| [Fusing DataFrames: what a join decides](#fusing-dataframes-what-a-join-decides)                           | Which dimensions of the result does a join decide?                   |
| [Joins and the logical plan](#joins-and-the-logical-plan)                                                  | How does DataFusion represent a join before execution?               |
| [How joins shape result columns](#how-joins-shape-result-columns)                                          | Which fields can each result row carry, and how are they referenced? |
| [Result rows: matching, preservation, and cardinality](#result-rows-matching-preservation-and-cardinality) | Which matched and unmatched rows appear, and how many?               |
| [Binary join composition](#binary-join-composition)                                                        | How do self-joins and multi-way joins reuse the same model?          |
| [Joins and related operations](#joins-and-related-operations)                                              | How do joins differ from filters and set operations?                 |

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (for example, `.join()` and `.filter()`)
- **Types and variants:** `TypeName` and `TypeName::Variant` (for example,
  `JoinType::Left`)
- **Logical-plan types:** `LogicalPlan`
- **Logical roles:** left input and right input
- **Result dimensions:** matching, preservation, cardinality, and payload

:::

```{contents} Table of Contents for Join Concepts
:local:
:depth: 2
```

## Fusing DataFrames: What a Join Decides

**A join combines two logical inputs into one result: the input schemas and join type fix which columns that result can carry, while matching and preservation decide which of its rows survive, vanish, or multiply.**

A join fuses two [`DataFrame`][dataframe]s horizontally: it relates their rows
through a condition and, for most join types, carries columns from both sides
into one result — semi and anti joins are the exception, matching against the
other input without returning any of its fields. Set operations fuse in the other
direction, stacking or comparing whole rows. Both cross the [frame
boundary][transformation concepts], but a join's two sides are _logical_ inputs:
they may be two views of the same `DataFrame` as readily as two separate sources.
The DataFrame API expresses the horizontal direction through [`.join()`] and
[`.join_on()`], which extend a lazy logical plan rather than touching rows.

An inner join on `customer_id`, shown with one copy of the key and in
illustrative row order:

```text
customers                           orders
┌─────────────┬───────┐             ┌──────────┬─────────────┐
│ customer_id │ name  │             │ order_id │ customer_id │
├─────────────┼───────┤             ├──────────┼─────────────┤
│      1      │ Alice │             │   101    │      1      │
│      2      │ Bob   │             │   103    │      3      │
└─────────────┴───────┘             └──────────┴─────────────┘
               │                                  │
               └──────────────────┬───────────────┘
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
  why a Venn diagram models joins poorly.

The two methods differ in how that condition is written, not in what the join
means: [`.join()`] takes named key columns from each side plus an optional
[`Expr`][expr] filter, while [`.join_on()`] takes [`Expr`][expr] conditions
alone. The condition decides which row combinations match, and the [`JoinType`]
decides which matched and unmatched rows reach the result. [Join
Conditions](join-conditions.md) covers writing the condition, and [Join
Types](join-types.md) covers choosing the variant.

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

Because both routes converge on the same plan node, that node is where the four
answers are actually settled.

### Joins and the Logical Plan

**The join node settles the result's schema before any row is read, and records its condition in two separate slots that later decide how the join can be executed.**

A `DataFrame` carries a lazy, unoptimized [`LogicalPlan`] that each
transformation extends until an action runs it (see [Execution
Lifecycle][execution-lifecycle]). A join extends that plan with a binary node
holding a left input, a right input, and the rule relating them — so what the
engine can do with the join is already determined by what that node records,
before a single row is read.

The node keeps that rule in two slots: `on: Vec<(Expr, Expr)>` for paired
equality expressions, and `filter: Option<Expr>` for everything else. Which slot
a method fills is not fixed at construction. [`.join()`] puts its named key pairs
in `on` and its optional filter argument in `filter`; [`.join_on()`] combines
every expression it receives with `AND` into `filter` and leaves `on` empty. A
later optimizer pass extracts equality predicates back out of `filter` and into
`on`. The node also records the [`JoinType`] and an output schema that
[`Join::try_new()`][join-node] derives from both input schemas and that type. How
such a schema is assembled belongs to [Anatomy of a Schema][schema-anatomy]; what
this one contains is the subject of [How Joins Shape Result
Columns](#how-joins-shape-result-columns).

:::{admonition} Logical Sides Are Not Physical Roles
:class: caution

Left and right are structural labels local to one binary logical join. They
govern side-sensitive semantics, such as unmatched-row preservation and which
input supplies a one-sided result. They do not prescribe physical build or probe
roles, buffering, processing order, or execution order.

Those roles are chosen when the planner lowers the logical node into a physical
operator, and it reads the `on` slot first. Equality pairs in `on` let the join
be executed by matching keys; without them the planner falls back on strategies
that evaluate the condition across candidate pairs, or on a plain cross product
when there is no condition at all. That is what the equality-extraction pass is
for — it keeps a [`.join_on()`] equality from being stranded in `filter`. See
[Why the Physical Plan Matters][physical-plan] for the lowering stage itself.

:::

<!-- JOIN-TODO-013 JOIN-TODO-016: Physical operator names, partition modes, build/probe detail, and performance claims stay off this page; these TODOs remain open pending an approved ownership decision. -->

---

## How Joins Shape Result Columns

**The immediate schema of a logical join describes what its result rows can contain before execution determines which rows exist.**

As described in the preceding logical-plan discussion, that schema already
belongs to the join node during planning. Here, result schema means this immediate
join-node output, not the final query schema or the schema of a final
[`RecordBatch`][recordbatch] after later projections or operations that alias,
rename, drop, reorder, or coalesce fields.

In the four-question model, payload means the fields an immediate result row can
carry. Many joins make fields from both inputs available, while semi and anti
joins are common examples that return fields from only one side. These are
examples rather than an exhaustive classification: a particular join variant may
also introduce a result field.

When a join preserves a row with no match from the other input, fields from that
absent side have no source values in the result row. DataFusion represents those
missing values as [`NULL`][null-handling], so fields representing the absent side
may become nullable in the immediate result schema. For the complete payload and
preservation behavior of each join variant, see [Join Types](join-types.md).

### Column Names and Qualification

Result shape and missing values are separate from name resolution. If multiple
fields have the same unqualified name, a reference to that name can be ambiguous,
and [relation qualifiers][table-qualifiers] participate in resolving which field
the reference denotes. Relation qualifiers support logical name resolution; they
should not be treated as durable lineage. For constructing equality-key and
expression-based join conditions, see [Join Conditions](join-conditions.md).

The immediate schema therefore describes what result rows can contain; it does
not determine which candidate pairs match, which unmatched rows remain, or how
duplicate matches multiply rows. Those data-dependent questions determine the
concrete row population discussed next.

---

## Result Rows: Matching, Preservation, and Cardinality

**The output schema describes what each result row can carry, but it does not determine which rows exist.**

Concrete result rows depend on matching candidate pairs, preserving selected
unmatched rows, and accounting for match multiplicity.

### Candidate Row Pairs and Matching

A **candidate row pair** consists of one row from the left input and one row from
the right input. The join condition determines whether that pair matches. This is
a semantic model for reasoning about a join, not a claim that DataFusion
physically enumerates every possible pair.

Pair-emitting joins produce one result row for each satisfying pair. Detailed
condition construction belongs in [Join Conditions](join-conditions.md).

### Unmatched-Row Preservation

An **unmatched row** is an input row with zero matches. The join type determines
whether that row contributes once or is omitted:

| Two-sided join | Unmatched left row | Unmatched right row |
| :------------- | :----------------: | :-----------------: |
| Inner          |      Omitted       |       Omitted       |
| Left           |     Preserved      |       Omitted       |
| Right          |      Omitted       |      Preserved      |
| Full           |     Preserved      |      Preserved      |

A preserved unmatched row contributes one result row; an omitted row contributes
none. Preservation does not create a match. [How Joins Shape Result
Columns](#how-joins-shape-result-columns) explains how the result represents
fields from the absent input.

### Match Cardinality

For a pair-emitting join, the number of matches controls an input row's
contribution:

- **Zero matches:** the row contributes no result unless its side is preserved,
  in which case it contributes one unmatched result row.
- **One match:** the row participates in one matched result row.
- **Multiple matches:** the row participates in one result row per satisfying
  pair.

Duplicate keys can therefore multiply rows. If two left rows and three right rows
share the same equality key, they produce \(2 \times 3 = 6\) matched pairs when
no additional condition excludes any pair. Input row counts alone cannot predict
the result size; key uniqueness, duplicate values, the complete condition, and
preservation all matter.

Semi and anti joins do not emit one row per matched pair. Each row from the
returned side contributes at most once: a semi join tests whether at least one
match exists, while an anti join tests whether none exists. [Join
Types](join-types.md) describes their returned fields and full behavior.

### Cross Joins

A cross join has no matching restriction, so every possible left-right candidate
pair contributes one result row. Inputs with \(L\) and \(R\) rows therefore
produce \(L \times R\) result rows.

---

## Binary Join Composition

**Self-joins and multi-way joins do not require new join semantics: they reuse binary join nodes with locally defined left and right roles.**

### Self-Joins

A self-join uses one underlying relation in two logical roles — an employee role
matched to a manager role, for example. Nothing about the join node changes: the
two roles are still a left input and a right input, even though their data
originates from the same `DataFrame`. Keeping those roles and their column
references distinguishable — with [`.alias()`], for example — is a schema-shaping
task owned by [Join Workflows](join-workflows.md).

The ordinary result questions still apply independently: which role pairs match,
which role's unmatched rows survive, how many matches each row has, and which
role's columns the result carries.

### Multi-Way Join Trees

A join node has exactly two logical inputs, but either input may be the logical
result of an earlier join. Three inputs therefore form a tree of two binary joins:

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

Left and right are local to each node: `A` and `B` fill those roles for
`Join 1`, while the output of `Join 1` and `C` fill them for `Join 2`. The
intermediate output is a logical plan subtree, not a promise that DataFusion
materializes it before evaluating the surrounding plan.

The grouping is part of the query's meaning. In particular, changing the grouping
of outer joins can change which unmatched rows and `NULL`-extended payloads reach
the next join.

---

## Joins and Related Operations

**Joins, filters, and set operations may all change result rows, but they answer different relational questions.**

### Join Conditions and Post-Join Filters

A join condition decides whether a left-right candidate pair matches before the
join applies its preservation rule. A later [`.filter()`] evaluates the rows
already produced by the join and keeps only rows for which its predicate is true.

:::{admonition} Preserved Rows Can Still Be Filtered Out
:class: caution

A post-join filter can remove rows that an outer join preserved. In particular, a
predicate on columns from an unmatched side encounters the `NULL` values introduced
by preservation and may reject those result rows.

:::

This distinction concerns logical semantics, not whether an optimizer can move a
particular predicate.

### Existence Joins and Unary Filters

A unary filter tests each row of one logical input using expressions available in
that input. A semi or anti join instead tests whether a relationship with a second
logical input exists. The resulting payload may look like a filtered version of
one side, but the decision depends on matching against the other side. In SQL,
[`EXISTS` / `IN` subqueries][sql-exists] are a common alternative spelling of the
same existence question; DataFusion may rewrite correlated forms toward joins.

Replacing a semi or anti join with an inner join followed by a projection is not
generally equivalent. An inner join can multiply a row when several matches exist,
whereas a semi join asks only whether at least one match exists.

### Joins and Set Operations

A join relates rows from two inputs through a matching relationship, and the inputs
do not need the same schema. A set operation combines or compares complete rows
under compatible-schema rules. The common intuition that joins combine data
"horizontally" while set operations combine it "vertically" can be useful, but it
is not a definition: semi and anti joins, for example, return columns from only one
side.

See [Transformation Concepts] for the broader transformation model and
[Set Operations] when the task concerns whole-row combination rather than
relationships between rows.

---

<!-- JOIN-TODO-015 JOIN-TODO-016: This conclusion and pruned Further Reading list are provisional until section-wise and final review. -->
<!-- JOIN-TODO-001: The four-dimension model is also stated in the abstract, the first-H2 highlighting sentence, and the Four Questions admonition. When Polish writes the abstract and this conclusion, differentiate the four statements instead of adding a fifth; the admonition wording is the approved anchor. -->

## Conclusion

A join is predictable once its four dimensions are answered explicitly: which row
combinations match, whose unmatched rows are preserved, how many result rows each
input row can produce, and which fields the result carries. Those answers are
independent of the join's eventual physical execution, and they scale unchanged
from a single binary node to an entire join tree — which is why the same model
carries into expressing a condition in [Join Conditions](join-conditions.md),
choosing a variant in [Join Types](join-types.md), composing larger relationships
in [Join Workflows](join-workflows.md), and checking an outcome in [Join
Validation](join-validation.md).

### Further Reading

- [DataFusion `DataFrame` join methods][`.join()`]
- [DataFusion join types][`jointype`]
- [SQL `JOIN` clause][sql-join-clause]
- [SQL subqueries (`EXISTS` / `IN`)][sql-exists]
- [Building Logical Plans]
- [Set Operations]
- [Joins as row combinations rather than Venn diagrams][joins-not-venn-diagrams]

<!-- JOIN-TODO-028: Deeper SQL type/LATERAL/config links belong on sibling leaves; keep concepts to join-clause + existence only unless Plan expands. -->

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[`jointype`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[building logical plans]: https://datafusion.apache.org/library-user-guide/building-logical-plans.html
[builder-parser]: ../../Concepts/builder-parser.md#choosing-the-right-api-for-the-task
[dataframe]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[execution-lifecycle]: ../../Concepts/execution-lifecycle.md
[expr]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[join-node]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Join.html#method.try_new
[joins-not-venn-diagrams]: https://blog.jooq.org/say-no-to-venn-diagrams-when-explaining-joins/
[null-handling]: ../../Concepts/null-handling.md
[physical-plan]: ../../Concepts/execution-lifecycle.md#why-the-physical-plan-matters
[recordbatch]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[schema-anatomy]: ../../Schema-Management/schema-anatomy.md
[set operations]: ../set-operations.md
[sql-exists]: ../../../../user-guide/sql/subqueries.md#-not--exists
[sql-join-clause]: ../../../../user-guide/sql/select.md#join-clause
[table-qualifiers]: ../../Schema-Management/schema-anatomy.md#table-qualifiers
[transformation concepts]: ../transformation-concepts.md#combining-multiple-dataframes
