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

<!--TODO (structure LOCKED 2026-07-08 — three reflection rounds; supersedes the
2026-07-07 arc)

SCOPE: recap-and-link for concepts owned elsewhere; depth only for what
transformations ARE and CHANGE. Anything busting its budget moves DOWN to its
owner leaf page — it does not grow this file.

LOCKED ARC (invariant → variation → the two families that need a model → bridge):
1. The Transformation Phase — MERGED 2026-07-09 into The Transformation Contract
   (item 2). Standalone on-ramp H2 removed: its advertisement/roadmap is abstract
   material (parked above the concepts table); its on-ramp sentence duplicated the
   Contract opening. No first-H2 "Introduction" (markdown.mdc §7.1/§7.4).
2. The Transformation Contract — now the FIRST H2 (on-ramp + contract, merged
   2026-07-09). Orienting highlight lands the reader in the lifecycle, then the
   one invariant as a compact model + a misconceptions caution retitled "Three
   clarifications: mutation, timing, and schema". Conscious altitude deviation
   accepted: a first H2 may carry this simple invariant; deep mechanism stays in
   the H3s/owner pages (markdown.mdc §7.1). Frames three facet H3s. DONE.
   2a. Two APIs, One Plan (H3) — two INTERFACES (not "front-ends") build the same
       LogicalPlan → same engine path; choose on ergonomics/safety, never speed
       (markdown.mdc §1). Carries the OLAP/TableProvider seealso relocated from
       Conclusion. DONE 2026-07-09.
   2b. Expressions: The Transformation Vocabulary (H3) — recap-and-link to
       Concepts/expressions.md (owner). Function libraries now pointed to from
       expressions.md Further Reading (not this page). DONE 2026-07-09.
   2c. Laziness and the Point of Execution (H3) — whole-plan optimization is the
       payoff; Rule of Four completed with a failure-mode caution (no action → no
       work; .collect() OOM on unbounded data). Crossref via "Execution", off the
       method name. DONE 2026-07-09.
3. What Transformations Can Change — own H2 (was buried in the Contract).
   Reordered 2026-07-09 (Rule of Four): what/why → five-dimension table → grain
   in an `important` admonition BENEATH the table → trimmed fork. Grain owned
   here; Kimball dimensional-modeling ref PENDING in Further Reading (§6). DONE.
4. Across Rows: Aggregation and Windows — H2 for the grain dimension (one frame,
   many rows). H2 framing DONE; aggregate-expression claim corrected 2026-07-09
   (windows use window expressions, not aggregate expressions); grain linked back
   to item 3. ### Aggregation / ### Window Functions in Brief written as concept
   recaps (highlight + grain contrast + link to aggregations.md /
   window-functions.md). DONE 2026-07-09.
5. Combining Multiple DataFrames — H2 for the frame-boundary dimension (more than
   one input frame; renamed + anchor retargeted #combining-multiple-dataframes
   2026-07-09). H2 framing DONE; where-it-breaks caution added; H2 body wording
   softened off "widens the result" to frame-boundary language 2026-07-10.
   ### Joins in Brief WRITTEN 2026-07-10 (frame-boundary-first recap: matching +
   schema widening + semi/anti existence-test exception + result-shape danger kept
   in prose, no new admonition; tiny inline customer_id nod; links joins.md;
   detailed table-style ASCII diagram Frame A/Frame B → matched output; self-join
   single-frame note).
   ### Set Operations in Brief WRITTEN 2026-07-10 (leaner highlight; align +
   stack/compare rows, change cardinality, contrast with joins = no schema
   widening; detailed table-style ASCII diagram Frame A over Frame B → stacked
   output; orders_df domain nod; single-frame dedup .distinct()/.distinct_on()
   noted; links set-operations.md; no new admonition — H2 caution owns
   compatibility).
   DIAGRAMS (updated 2026-07-10): H2 carries a compact general-concept contrast
   (horizontal widen vs. vertical stack); each H3 carries a detailed table-style
   diagram of the concrete transformation. General at H2, specific at H3 — no
   duplication.
6. From Concepts to Methods — bridge WRITTEN 2026-07-10 (highlight + one-paragraph
   vocabulary recap; routes to the Transformations index for the reading path).
   REFRAMED 2026-07-10 as concept application (concept-page-as-router repair,
   markdown-landing.mdc §2): highlight + body recentered on the worked pipeline;
   one routing sentence to index.md retained; concepts-table row reworded off
   "reading path".
   WORKED PIPELINE (was ### Concepts in Action, MERGED UP into this H2 2026-07-11 —
   the H3 added nothing; anchor #concepts-in-action retired, index.md deep link
   retargeted to #from-concepts-to-methods). REWORKED 2026-07-10 from a bare "Meet
   the Dataset" data block (it taught nothing, and — doctests being hermetic,
   rust-docs.mdc §1 — could never be a shared code definition anyway). A single
   pipeline on the running dataset (customer_df + orders_df via dataframe!, aliased
   to disambiguate the shared customer_id, Carol no orders + order 104 orphan,
   string ISO dates): join → aggregate → sort, collected once, asserted (live
   doctest). Highlight/list REVISED 2026-07-11 to claim ALL FIVE dimensions (the
   chain moves schema+cardinality+grain+ordering+frame boundary, not "three"); the
   post-example list annotates each step's dimensions. Rendered starting-dataset
   TABLES live on index.md (move handshake — index.md TODO 4).
   ### Method Families MOVED 2026-07-10 to Transformations/index.md (move
   handshake): the slim five-dimension reading-path table is routing, which is the
   landing page's job (markdown-landing.mdc §1.1); the bridge prose now points
   there. Per-method SQL equivalents still to be distributed to leaf pages (see
   METHOD MAP below).
7. Conclusion — WRITTEN 2026-07-10, REVISED 2026-07-11. Renamed H2 from "Conclusion
   & Further Reading" to "## Conclusion"; Further Reading is now its own ###
   subsection (markdown.mdc §7.1/§7.4 updated to allow the H3). Recap highlight
   kept; added a seealso "Related concepts" (execution-lifecycle, expressions,
   architectural-fit). Further Reading REWORKED: dropped DDIA + Date (book-ad /
   loose fit); now Andy Grove "How Query Engines Work" (free, query-engine
   internals), DataFusion "Building Logical Plans" (first-party), Kimball
   "Declaring the Grain" (grain); optional 4th (relational algebra) open. OLAP fit
   pointer stays in Two APIs, One Plan.
   REFERENCES 2026-07-11: page-wide reference-style pass DONE — all cross-page
   links + DataFrame method mentions resolve to grouped defs at file bottom
   (types / methods / internal pages / external). Same-page #anchors kept inline.

DATASET (agreed): a shared flat family, customer_df + orders_df, built with the
dataframe! macro (flat-only). Nested/array story DERIVED via array_agg on
reshaping.md; set-op partner frames owned by set-operations.md.

OLAP/OLTP NOTE (relocated again 2026-07-09): the one-line seealso pointer to
Concepts/architectural-dataframe.md#architectural-fit-olap-vs-oltp now lives in
Two APIs, One Plan (framed as "another engine / different contract fits") and was
removed from Conclusion to avoid duplication. Recoverable from git history.

PENDING RELOCATION (move handshake, source side): the SQL-injection code example
moves to Concepts/builder-parser.md §Safety and Security. Recoverable from git.

BROKEN-LINK REPAIR (cross-file): hybrid-sql.md links
"#when-row-based-tableproviders-outperform-columnar" (removed here). Retarget to
Concepts/architectural-dataframe.md#architectural-fit-olap-vs-oltp.

METHOD MAP (decision LOCKED 2026-07-08; shrink EXECUTED 2026-07-10): a full
per-method SQL↔DataFrame reference does NOT belong on a concept page
(markdown-landing.mdc §2.1) and is PROHIBITED on index.md (§1.2). The rendered
raw-material table under Method Families was replaced with a slim reading-path
table keyed by the five dimensions → method family → leaf page. The old table
and its now-unused reference-style link defs (per-method docs.rs URLs, verified
SQL-keyword anchors) were removed the same day — recover exact raw material from
git history. MOVED 2026-07-10: the slim reading-path table itself relocated to
Transformations/index.md (routing is the landing page's job, markdown-landing.mdc
§1.1); From Concepts to Methods now routes there in prose. STILL PENDING:
distribute the per-method SQL equivalents DOWN to the
leaf action pages (markdown.mdc §7.4) — selection / filtering / sorting-limiting /
aggregations / window-functions / joins / set-operations. No silent deletion.

STAGE 6 (remaining, abstract-last per §7.2): title-line highlight + abstract
(now owns the full roadmap). Concepts-covered table updated 2026-07-08.
Reference-style link definitions at bottom.
-->

# Transformations Concepts

<!-- TODO: **Title-line highlighting sentence** (bold, pyramidal) — Stage 6. -->

<!-- TODO: Abstract — Stage 6 (written last; owns the storyline roadmap). -->

<!-- OLD roadmap, needs revision! Is covered by the concepts table below. 
This description-not-mutation idea is the thread through the whole page. Every transformation, however different its effect, follows the same [contract](#the-transformation-contract); the ways they differ reduce to a few [dimensions of change](#what-transformations-can-change); and only two families — [analyzing across rows](#across-rows-aggregation-and-windows) and [combining multiple DataFrames](#combining-multiple-dataframes) — bend that model far enough to need a closer look before you reach the [method pages](#from-concepts-to-methods).
-->

**Concepts covered on this page:**

| Concept                                                                     | What it covers                                                                                     |
| :-------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------ |
| [The Transformation Contract](#the-transformation-contract)                 | Where transformations sit in the lifecycle, the one invariant every method follows, and its three facets: two APIs, expressions, laziness |
| [What Transformations Can Change](#what-transformations-can-change)          | The five dimensions of change — schema, cardinality, ordering, grain, frame boundary — and `grain` |
| [Across Rows: Aggregation and Windows](#across-rows-aggregation-and-windows) | One DataFrame, many rows analyzed together — aggregation changes grain, windows preserve it         |
| [Combining Multiple DataFrames](#combining-multiple-dataframes)             | The transformations that cross the frame boundary — joins and set operations                        |
| [From Concepts to Methods](#from-concepts-to-methods)                       | The contract and dimensions applied — one worked pipeline on the shared dataset                     |

```{contents} Table of Contents for Transformations Concepts
:local:
:depth: 2
```

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

## The Transformation Contract

**Transformations are the DataFrame's life phase — the methods that reshape your data — and beneath their variety they share one contract, for predictable and reliable data transformations.**

Once you [create][creating] a [`DataFrame`], you enter the transformation phase of its [lifecycle][lifecycle]: the stage where you filter, select, join, aggregate, sort, and enrich. The DataFrame API exposes dozens of methods for this, yet they are variations on a single move.

That single move is the contract. A transformation takes an existing [`DataFrame`], describes a logical change to the query plan, and returns a new [`DataFrame`] whose [`LogicalPlan`] has been updated and whose [`DFSchema`] has been re-derived. The source data is never mutated, and no work runs until an [action](#laziness-and-the-point-of-execution) asks for results. That shape never varies — which is what makes the whole surface predictable, and what lets the optimizer rewrite the accumulated plan before a single byte is read.

:::{admonition} Three clarifications: mutation, timing, and schema
:class: caution

The contract trips readers who bring habits from in-memory collections:

1. A transformation does **not** mutate the DataFrame. It returns a new one and consumes the old, which is why you [`.clone()`][clone-concept] a handle to keep using the original.
2. Chaining methods does **not** do the work step by step. Each call only extends the plan; execution waits for an action.
3. The derived schema does **not** capture everything a step changed. A transformation can reshape more than its columns — the subject of [What Transformations Can Change](#what-transformations-can-change).
:::

Three facets of this contract each repay a closer look: the plan is the same whichever API builds it, its operations are written in one expression vocabulary, and the deferral of work is what makes whole-plan optimization possible.

### Two APIs, One Plan

**SQL and the DataFrame API are two ways to build the same `LogicalPlan`: equivalent plans enter the same optimizer and execution engine, so the choice is one of ergonomics and safety, not speed.**

DataFusion exposes one query engine through two APIs. The DataFrame API builds a [`LogicalPlan`] directly by chaining methods; the SQL API parses a query string into that same plan. Equivalent plans follow one optimization and execution path — there is no separate DataFrame engine and SQL engine. For transformations, this means the model on this page is a property of the plan, not the API that built it: it holds whichever you write in, and you can [mix the two at well-defined boundaries][hybrid-sql].

Building a plan from typed method calls rather than concatenated strings also removes a hazard by construction: with no query text for user input to break out of, an entire class of SQL-injection risks cannot arise. For the full side-by-side, see [Two Paths to the Same Plan][builder-parser].

:::{admonition} When another engine fits better
:class: seealso

Both APIs target DataFusion's columnar OLAP engine. Point lookups, heavy row-level updates, and pushdown-friendly sources often fit a different contract — an external [`TableProvider`] or an OLTP store — better than the transformations described here. See [Architectural Fit: OLAP vs. OLTP][architectural-fit].
:::

### Expressions: The Transformation Vocabulary

**Where a transformation accepts [`Expr`]s, those expression trees are its shared vocabulary — DataFusion resolves each against the current schema and folds it into the plan.**

When a transformation needs row-level logic — which rows to keep, which columns to compute, how to group — you pass it as an [`Expr`] tree rather than an immediate value. `.filter()`, `.select()`, `.with_column()`, `.aggregate()`, and the window builders all accept the same vocabulary: `col()` and `lit()`, comparison and logical operators, scalar functions, and aggregate or window expressions. DataFusion resolves each tree against the current [`DFSchema`] and folds it into the [`LogicalPlan`], which is why a bad column name surfaces at plan time, not when you build the expression. For the full model — how `Expr` trees are built, optimized, and validated, plus the built-in function libraries — see [Expressions][expressions].

### Laziness and the Point of Execution

**Transformations are lazy: each call extends the `LogicalPlan` and returns at once, and only an action runs it — so DataFusion optimizes the whole pipeline before any data moves, instead of one step at a time.**

Each transformation adds to the plan and returns immediately; the data stays put. Work begins only when you cross the **action boundary** — a call to `.collect()`, `.show()`, or `.write_*()`. Because the whole chain is visible before that moment, the optimizer can reorder filters, push predicates down into the scan, and choose join and aggregation strategies that a step-by-step evaluator could never see. You can inspect the accumulated plan without running it: `df.explain(false, false)?.show().await?`.

:::{admonition} Laziness has a flip side
:class: caution

A chain with no action does no work and returns no results — a frequent first surprise. The action is also where cost is paid: `.collect()` on an unbounded or very large result pulls every row into memory and can exhaust it. When the result size is unknown, prefer streaming or bound it with `.limit()` first.
:::

The full path from a lazy plan to streaming results lives in [Execution Lifecycle][execution-lifecycle].

---

## What Transformations Can Change

**A transformation can change more than its schema — also its cardinality, ordering, grain, and the frames that feed it — so the output schema alone never tells you what really moved.**

Every transformation returns a new DataFrame, but the ways its output can differ from the input reach past the columns. The output schema — the columns and their types — shows what was projected, not whether rows were dropped, reordered, regrouped, or drawn from more than one frame. A transformation's full effect falls along five dimensions:

| Dimension          | What it changes                          | Example methods                            |
| :----------------- | :--------------------------------------- | :----------------------------------------- |
| **schema**         | which columns exist, and their types     | [`.select()`][select-method], [`.with_column()`][with-column-method] |
| **cardinality**    | how many rows come out                    | [`.filter()`][filter-method], [`.limit()`][limit-method] |
| **ordering**       | the order rows arrive in                  | [`.sort()`][sort-method]                   |
| **grain**          | what a single row represents              | [`.aggregate()`][aggregate-method] changes it; windows keep it |
| **frame boundary** | how many DataFrames feed the operation    | [`.join()`][join-method], [`.union()`][union-method] |

:::{admonition} Grain: what one row represents
:class: important

**Grain** is what a single row stands for — one order, one customer, one customer-month. Borrowed from dimensional modeling, it is distinct from both the schema and the row count: grain can change while the columns and even the row count look unchanged. Group an orders frame by customer and each row shifts from meaning _one order_ to meaning _one customer_.
:::

Three of these — schema, cardinality, and ordering — are the everyday reshaping of a single frame, handled by the selection, filtering, and sorting methods you meet first. The other two earn their own sections: changing **grain** by [analyzing across rows](#across-rows-aggregation-and-windows), and crossing the **frame boundary** by [combining multiple DataFrames](#combining-multiple-dataframes). Whichever dimension moves, the output schema alone never describes everything a transformation changed.

---

## Across Rows: Aggregation and Windows

**Aggregation and window functions reason across many rows of one DataFrame, but land on opposite sides of grain: aggregation collapses rows into groups; a window keeps every row.**

The single-frame methods so far treat rows independently — a filter tests one row, a projection maps one row. Aggregation and window functions break that assumption: each output value is computed from a _set_ of related rows, not a single one. Both stay within one DataFrame, yet they land on opposite sides of the [grain](#what-transformations-can-change) dimension from the previous section. That contrast is the one idea to carry into their method pages.

### Aggregation in Brief

**Aggregation collapses each group of rows into one summary row — the grain shifts from the input row up to the group.**

You supply grouping columns and aggregate expressions; DataFusion partitions rows by the group keys and reduces each partition to one value per expression. The result is compression — many detail rows become one summary row, turning per-order records into per-customer or per-region totals. Because rows merge, the **grain becomes coarser**: the output holds one row per group, and any column that is neither grouped nor aggregated drops away. See [Aggregation Patterns][aggregations] for the functions and grouping patterns.

### Window Functions in Brief

**A window function reads a set of related rows but keeps every one — it adds analytical columns without changing the grain.**

Each row defines a window of peers — a partition, an ordering, and an optional frame — and the function computes a value from that window, such as a rank, a running total, or a moving average. Like aggregation it reasons across many rows, but nothing is merged: every input row survives and simply gains new columns, so the **grain is preserved**. That is the dividing line — aggregation yields one row per group, a window yields one value per row, in context. See [Window Functions][window-functions] for the builder and frame model.

---

## Combining Multiple DataFrames

**Joins and set operations cross the frame boundary — each takes more than one DataFrame — and combine them differently: a join correlates rows by a condition; a set operation stacks or compares whole rows.**

Every transformation so far reshaped a single DataFrame. Joins and set operations are different in kind: they need a second input, and the output depends on how the two relate. A join relates rows across the two frames by a matching condition, and most join types carry columns from both into the result; a set operation treats its inputs as comparable row sets and combines them by alignment. Both are introduced here and detailed on their own pages.

```text
join = horizontal (widen)     set op = vertical (stack)
┌─────┬─────┐                 ┌───────┐
│  A  │  B  │                 │   A   │
└─────┴─────┘                 ├───────┤
                              │   B   │
                              └───────┘
```

:::{admonition} Combining inputs has a matching cost
:class: caution

Set operations need compatible inputs, but the exact rule depends on the variant: a positional [`union`][union-method] requires the same number of columns and reconciles their types loosely; [`union_by_name`][union-by-name-method] instead aligns by column name and fills missing columns with `NULL`; [`intersect`][intersect-method] and [`except`][except-method] also require the same number of columns. A join called with no keys and no filter becomes a cross join (every row paired with every row), multiplying rows instead of matching them.
:::

### Joins in Brief

**A join relates rows from two DataFrames by a matching condition, crossing the frame boundary — the join type governs both which rows survive and which columns come with them.**

Where the single-frame methods reshape one DataFrame, a join crosses the [frame boundary](#what-transformations-can-change): it pairs each row of one frame with rows of the other that satisfy a condition — for example, matching `customer_id` across `customer_df` and `orders_df`. Most join types carry the columns of both inputs into the result, **widening the schema**; the important exception is semi and anti joins, which use the second frame only as an existence test and add none of its columns.

```text
JOIN (inner): relate rows across frames

Frame A               Frame B
┌──────────────┐     ┌──────────────┐
│ id │ value_a │     │ id │ value_b │
├────┼─────────┤     ├────┼─────────┤
│ 1  │   A1    │◄───►│ 1  │   B1    │
│ 2  │   A2    │     │ 3  │   B3    │
└──────────────┘     └──────────────┘
        │ matching condition
        ▼
┌────────────────────────┐
│ id │ value_a │ value_b │
├────┼─────────┼─────────┤
│ 1  │   A1    │   B1    │
└────────────────────────┘
```

Because the result is assembled from matches, its shape follows the matching: non-matches may disappear, preserved rows may receive nulls on the empty side, and a key that matches many rows may multiply them — so a join can **change cardinality** in either direction. The same mechanism can relate a frame to itself: a self-join joins a DataFrame with an aliased copy of itself — still two logical inputs. See [Join Patterns][joins] for [`.join()`][join-method] / [`.join_on()`][join-on-method], the join-type taxonomy, key and condition options, and execution detail.

### Set Operations in Brief

**Set operations combine two like-shaped DataFrames by stacking or comparing whole rows — they cross the frame boundary and change cardinality, without widening the schema the way a join does.**

Like a join, a set operation crosses the [frame boundary](#what-transformations-can-change), but it combines the inputs the other way round: instead of matching rows and placing columns side by side, it lines up whole rows under a shared column layout. Positional `union`, `intersect`, and `except` align their inputs by column position; the by-name variants such as `union_by_name` align by column name. Either way, the inputs line up under a shared column layout rather than contributing new columns; how strictly they must match depends on the variant (see the caution above for the exact rules).

```text
SET OPERATION (union): stack like-shaped frames

Frame A
┌──────────────┐
│ id │ value   │
├────┼─────────┤
│ 1  │   A1    │
│ 2  │   A2    │
└──────────────┘
        │ same columns
        ▼
Frame B
┌──────────────┐
│ id │ value   │
├────┼─────────┤
│ 3  │   B3    │
│ 4  │   B4    │
└──────────────┘
        │
        ▼
┌──────────────┐
│ id │ value   │
├────┼─────────┤
│ 1  │   A1    │
│ 2  │   A2    │
│ 3  │   B3    │
│ 4  │   B4    │
└──────────────┘
```

What a set operation changes is **cardinality**: `union` concatenates the two row sets (the `_distinct` variants drop duplicates), `intersect` keeps the rows found in both, and `except` keeps rows in the first but not the second — stacking one month's `orders_df` onto another's, for example. The same row comparison also works within a single frame: deduplication ([`.distinct()`][distinct-method], [`.distinct_on()`][distinct-on-method]) removes repeats without a second input, changing cardinality without crossing the frame boundary. See [Set Operations][set-operations] for the variants and alignment rules.

---

## From Concepts to Methods

**The concepts on this page are working vocabulary, not theory: one runnable pipeline puts the contract to work and moves all five dimensions on the shared dataset.**

Every transformation from here follows the [contract](#the-transformation-contract), moves one or more of the five [dimensions](#what-transformations-can-change), and — for the two families that bend the model — either [analyzes across rows](#across-rows-aggregation-and-windows) or [combines multiple frames](#combining-multiple-dataframes). For the reading path — which family lives on which page — see the [Transformations index][index].

The two frames below — `customer_df` (one row per customer) and `orders_df` (one row per order, linked by `customer_id`) — are the running example the action pages reuse; a [rendered preview][index] opens the section. One question ties the concepts together: _total revenue per region, largest first._

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Alias each frame so the two customer_id columns remain distinguishable.
    let customer_df = dataframe!(
        "customer_id" => [1, 2, 3, 4],
        "name" => ["Alice", "Bob", "Carol", "Dave"],
        "region" => ["West", "East", "West", "East"],
        "signup_date" => ["2023-01-15", "2023-03-22", "2023-06-10", "2023-09-01"]
    )?
    .alias("customers")?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1],
        "order_date" => ["2024-01-05", "2024-02-11", "2024-01-20", "2024-03-02"]
    )?
    .alias("orders")?;

    // One lazy plan: total revenue per region, largest first.
    let revenue_by_region = customer_df
        .join(
            orders_df,
            JoinType::Inner,
            &["customer_id"],
            &["customer_id"],
            None,
        )? // frame boundary
        .aggregate(
            vec![col("region")],
            vec![sum(col("amount")).alias("revenue")],
        )? // grain
        .sort(vec![col("revenue").sort(false, true)])?; // ordering

    // Nothing above executed the plan; collect() runs the complete plan.
    let batches = revenue_by_region
        .clone()
        .collect()
        .await
        .expect("revenue query should succeed");

    assert_batches_sorted_eq!(
        &[
            "+--------+---------+",
            "| region | revenue |",
            "+--------+---------+",
            "| West   | 300     |",
            "| East   | 150     |",
            "+--------+---------+",
        ],
        &batches
    );

    Ok(())
}
```

Read the chain against the concepts on this page — three steps move all five dimensions:

- [`.join()`][join-method] crosses the **frame boundary**, and in doing so also reshapes the **schema** (customer rows gain order columns) and the **cardinality** (only matching customer–order pairs survive).
- [`.aggregate()`][aggregate-method] shifts the **grain** to one row per region, again reshaping the **schema** (down to `region` and `revenue`) and the **cardinality** (fewer rows out).
- [`.sort()`][sort-method] sets the **ordering** of the final result.

Every call returns a new [`DataFrame`] over a lazy [`LogicalPlan`], so the plan is built in full before `.collect()` executes it once. The inner join also quietly drops Carol, Dave, and the orphan order 104 — pairing only matching customer and order rows, so just West and East survive and the [survivorship][joins] cost of matching is visible rather than surprising.

---

## Conclusion

**One contract, five dimensions, and two families that bend the model — that is the whole conceptual map of DataFusion's DataFrame transformations.**

Every transformation returns a new [`DataFrame`] over a lazy [`LogicalPlan`] and leaves the source untouched until an action runs. What separates one method from the next is the dimension it moves — schema, cardinality, ordering, grain, or the frame boundary — and only two families need a closer look: aggregation and windows, which reason across rows, and joins and set operations, which cross the frame boundary. With that map in hand, start with [Selection][selection], the first action page, and use the [Transformations index][index] for the reading path through the rest.

:::{admonition} Related concepts
:class: seealso

- [Execution Lifecycle][execution-lifecycle] — how a lazy plan becomes streaming results across the action boundary.
- [Expressions][expressions] — the `Expr` vocabulary every transformation shares.
- [Architectural Fit: OLAP vs. OLTP][architectural-fit] — when another engine or `TableProvider` suits the workload better.
:::

### Further Reading

- [How Query Engines Work][how-query-engines-work] (Andy Grove, DataFusion's original author): a free, ground-up walk through logical plans, joins, and aggregation — the machinery beneath this page.
- [Building Logical Plans][datafusion-logical-plans] (DataFusion documentation): how transformations accumulate into a `LogicalPlan` before execution.
- [Declaring the Grain][kimball-grain] (Kimball Group): the dimensional-modeling origin of _grain_ as "what one row represents."

<!-- Reference-style link definitions (markdown.mdc §6), grouped: DataFusion
types, DataFrame methods (docs.rs, unpinned /latest), internal pages, external
Further Reading. The former per-method SQL↔DataFrame map and its SQL-keyword
anchors were REMOVED 2026-07-10 (recoverable from git history); when those SQL
equivalents are distributed to leaf pages, the verified SQL-keyword anchors
(../../../user-guide/sql/select.md — three levels up, NOT ../../) belong there. -->

<!-- DataFusion types -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`Expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

<!-- DataFrame methods (docs.rs) -->

[select-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[with-column-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[filter-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[limit-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit
[sort-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[aggregate-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[join-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[join-on-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[union-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[union-by-name-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[intersect-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[except-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[distinct-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[distinct-on-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct_on

<!-- Internal pages -->

[index]: index.md
[selection]: selection.md
[aggregations]: aggregations.md
[window-functions]: window-functions.md
[joins]: joins.md
[set-operations]: set-operations.md
[hybrid-sql]: hybrid-sql.md
[creating]: ../Creating-DataFrames/index.md
[lifecycle]: ../index.md#the-dataframe-lifecycle
[execution-lifecycle]: ../Concepts/execution-lifecycle.md
[clone-concept]: ../Concepts/execution-lifecycle.md#ownership-vs-execution-why-you-see-clone-everywhere
[builder-parser]: ../Concepts/builder-parser.md
[expressions]: ../Concepts/expressions.md
[architectural-fit]: ../Concepts/architectural-dataframe.md#architectural-fit-olap-vs-oltp

<!-- Further Reading (external, non-API) — markdown.mdc §6. -->

[how-query-engines-work]: https://howqueryengineswork.com/
[datafusion-logical-plans]: https://datafusion.apache.org/library-user-guide/building-logical-plans.html
[kimball-grain]: https://www.kimballgroup.com/2003/03/declaring-the-grain/
