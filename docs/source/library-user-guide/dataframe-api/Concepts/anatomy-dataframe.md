<!--
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

# Anatomy of a Dataframe: LogicalPlan + SessionState

**A DataFrame is a lightweight handle pairing an immutable query plan (LogicalPlan) with a frozen execution environment (SessionState) — everything needed for reproducible execution.**

Every DataFrame you create captures two things: a LogicalPlan describing what to compute, and a SessionState snapshot describing how to compute it. Each LogicalPlan node carries a DFSchema that validates column names, types, and provenance at build time — long before any data flows. This separation is what makes DataFrames lightweight, cheaply cloneable, and safe to use across async boundaries.

```{contents} Table of Contents for Anatomy of a Dataframe
:local:
:depth: 2
```

## Introduction to Anatomy of a Dataframe

**Two components, one contract: the `LogicalPlan` says _what_ to compute, the `SessionState` says _how_ — together they guarantee reproducible execution.**

Understanding what a `DataFrame` actually _contains_ explains why queries are reproducible and why certain patterns (like registering UDFs before creating DataFrames) matter. The following structure shows exactly what lives inside every DataFrame:

```text
DataFrame
├── LogicalPlan     (what to compute)
│   ├── DFSchema    (column names, types, ...)
│   └── Plan nodes  (Filter, Join, Aggregate, ...)
└── SessionState    (how to compute it)
    ├── Config      (batch_size, partitions, timezone)
    ├── Catalog     (registered tables)
    └── Functions   (UDFs, UDAFs, UDWFs)
```

The **left branch** — the [`LogicalPlan`] — is the query recipe. Each node in the plan tree represents a relational operation (filter, join, aggregate) and carries a [`DFSchema`] that validates column names, types, and provenance at plan-build time (see [DFSchema: The Schema Layer](#dfschema-the-schema-layer) below). The **right branch** — the [`SessionState`] — is a frozen snapshot of the execution environment: configuration, registered tables, UDFs, and runtime resources.

The [`SessionContext`] is mutable and evolves over your session, but each `DataFrame` captures an **immutable** `SessionState` snapshot at creation time. Transformations like `.filter()` or `.select()` return new DataFrames with updated plans but the same snapshot; actions like `.collect()` execute using that frozen state. This snapshot guarantees reproducibility:

| What's captured                           | Why it matters                                            |
| ----------------------------------------- | --------------------------------------------------------- |
| Config (batch size, partitions, timezone) | Same performance even if global settings change           |
| UDFs and registered tables                | Queries don't fail if dependencies are deregistered later |
| Query start timestamp                     | Functions like [`.now()`] return consistent values        |

The rest of this page explores each component in detail: first the inner workings with a step-by-step walkthrough, then the schema layer that validates every transformation, and finally the escape hatch to `LogicalPlanBuilder` for advanced use.

---

## Inside a DataFrame: Step by Step

**Think of query execution like cooking—the recipe alone isn't enough; you need the kitchen too.**

The concepts might come clearer with an everyday analogy of a kitchen.

| Concept            | Cooking Analogy    | What it holds                                               |
| ------------------ | ------------------ | ----------------------------------------------------------- |
| [`SessionContext`] | Kitchen (mutable)  | Tools, ingredients, configuration—_changes over time_       |
| [`LogicalPlan`]    | Recipe (immutable) | Step-by-step instructions—_what to compute_                 |
| [`SessionState`]   | Kitchen State      | Given setup of the kitchen at recipe start—_frozen in time_ |
| [`DataFrame`]      | Recipe + snapshot  | Everything needed to cook the dish reproducibly             |

The [`SessionState`] defines the environment the data are processed in: if you add new tools to the kitchen after starting a dish, the dish-in-progress still uses the original setup. This prevents surprises ("where did my UDF go?") and ensures reproducibility.

Here's how this flows through the system in a nutshell:

```text
[ STEP 1: THE KITCHEN ]
┌───────────────────────────────────────────────────────────────────┐
│                                SessionContext                     │
│                    (Mutable kitchen: tools & ingredients)         │
├───────────────────────────────────────────────────────────────────┤
│  • Config:  target_partitions=8, batch_size=8192                  │
│  • UDFs:    "my_custom_func"                                      │
│  • Catalog: table "sales"                                         │
└──────────────────────────────────────┬────────────────────────────┘
                                       │
                                       │ .read_table("sales")
                                       ▼
[ STEP 2: START COOKING ]
┌───────────────────────────────────────────────────────────────────┐
│                                  DataFrame                        │
│              (Workstation setup: what's available when you start) │
├──────────────────────────────────────┬────────────────────────────┤
│           SessionState               │        LogicalPlan         │
│    (frozen tools & ingredients)      │     (first instruction)    │
├──────────────────────────────────────┼────────────────────────────┤
│ • Config/UDFs at creation time       │                            │
│ • Catalog state when cooking began   │ TableScan("sales")         │
└──────────────────────────────────────┬────────────────────────────┘
                                       │
                                       │ .filter(amount > 100)
                                       ▼
[ STEP 3: ADD INSTRUCTIONS ]
┌──────────────────────────────────────────────────────────────────────┐
│                                New DataFrame                         │
│                  (Same workstation, extended recipe)                 │
├──────────────────────────────────────┬───────────────────────────────┤
│           SessionState               │             LogicalPlan       │
│         (unchanged snapshot)         │           (more steps added)  │
├──────────────────────────────────────┼───────────────────────────────┤
│ • Still the original setup           │      Filter(amount > 100)     │
│ • (New kitchen tools don't appear)   │        └─ TableScan("sales")  │
└──────────────────────────────────────┬───────────────────────────────┘
                                       │
                                       │ .collect() / .show()
                                       ▼
[ STEP 4: SERVE THE DISH ]
┌──────────────────────────────────────────────────────────────────────┐
│                                ExecutionPlan                         │
│                     (Cooking happens with given setup)               │
├──────────────────────────────────────────────────────────────────────┤
│  • Recipe optimized using snapshot's rules                           │
│  • Physical operators created (ParquetExec, FilterExec, etc.)        │
│  • Output: RecordBatches (the meal!)                                 │
└──────────────────────────────────────────────────────────────────────┘
```

Unlike a single plate, the meal arrives in **RecordBatches**—sliced like a Sunday roast 🍖, one portion at a time. This streaming approach lets DataFusion handle datasets much larger than memory.

<!-- NOTICE TO PCM/CONTRIBUTORS: Tempted to add alphabet soup image here for the "meal" metaphor!

Inspiration:

https://media.istockphoto.com/id/1210366546/de/foto/tomatensuppe-mit-buchstabennudeln-auf-l%C3%B6ffel.jpg?s=2048x2048&w=is&k=20&c=SaZ0yj4WLabqvd41RKJZlS7dRgZw_A-jVtuGrfGgvZo=

Of cause without copyright etc. !
-->

:::{admonition} Best practice
:class: tip
Register UDFs and tables **before** creating DataFrames that depend on them. If you need a new UDF or table mid-processing, register it on the `SessionContext`, then create a **new DataFrame** — existing DataFrames keep their original snapshots.
:::

:::{admonition} Learn more
:class: seealso
See [SessionContext and SessionState relationship][sessioncontext and sessionstate] for implementation details.
:::

---

## DFSchema: The Schema Layer

**Every `LogicalPlan` node knows exactly what columns it produces — before any data is touched.**

[`DFSchema`] is DataFusion's schema wrapper around Arrow's `Schema`. While Arrow's `Schema` describes columnar data at rest (column name + data type + nullable), `DFSchema` adds the metadata the query planner needs to validate and optimize queries:

| What `DFSchema` tracks                      | Why it matters                                                     |
| :------------------------------------------ | :----------------------------------------------------------------- |
| **Column names + data types + nullability** | Same as Arrow `Schema` — the basics                                |
| **Table qualifier** (e.g., `orders.amount`) | Disambiguates columns after joins involving same-named columns     |
| **Functional dependencies**                 | Tracks which columns uniquely determine others (used by optimizer) |

Every node in a `LogicalPlan` tree carries its own `DFSchema`. When you chain transformations, DataFusion validates the schema at each step — this is what enables the fail-fast behavior:

```text
Aggregate(group=[region], agg=[sum(amount)])   ← DFSchema: {region: Utf8, sum(amount): Float64}
  └─ Filter(amount > 100)                      ← DFSchema: {region: Utf8, amount: Int64, ...}
       └─ TableScan("sales")                   ← DFSchema: {id: Int64, region: Utf8, amount: Int64}
```

If you reference a column that doesn't exist, the `.filter()` or `.select()` call fails immediately with a clear error — long before any data is scanned. Table qualifiers (`a.id` vs `b.id`) prevent ambiguity in joins, and the optimizer leverages schema metadata (functional dependencies, nullability) to apply more aggressive rewrites.

:::{admonition} Deep dive
:class: seealso
For the full schema API — creating schemas, coercion rules, inspection methods, and schema-aware DataFrame operations — see the [Schema Management](../Schema-Management/index.md) section.
:::

---

## Under the Hood: DataFrame and LogicalPlanBuilder

**`DataFrame` methods are convenience wrappers around `LogicalPlanBuilder` — for most users the DataFrame API is sufficient, but you can drop to the builder level when you need fine-grained control.**

For standard queries and transformations, the `DataFrame` API handles everything: it manages the `SessionState`, chains transformations, and triggers execution. Under the hood, each `DataFrame` method maps directly to a `LogicalPlanBuilder` method — they produce identical plans:

| DataFrame method           | LogicalPlanBuilder equivalent       |
| -------------------------- | ----------------------------------- |
| [`DataFrame::select()`]    | [`LogicalPlanBuilder::project()`]   |
| [`DataFrame::filter()`]    | [`LogicalPlanBuilder::filter()`]    |
| [`DataFrame::aggregate()`] | [`LogicalPlanBuilder::aggregate()`] |
| [`DataFrame::join()`]      | [`LogicalPlanBuilder::join()`]      |

Sometimes you need direct `LogicalPlan` access — custom optimizer rules, query rewriting systems, or programmatic plan inspection. DataFusion lets you move freely between the two levels using [`.into_parts()`], which consumes the DataFrame and returns `(SessionState, LogicalPlan)` as separate values:

| Use DataFrame API for...             | Use LogicalPlanBuilder for...  |
| ------------------------------------ | ------------------------------ |
| Standard queries and transformations | Custom optimizer rules         |
| Automatic SessionState management    | Fine-grained plan manipulation |
| Rapid prototyping                    | Query rewriting systems        |

The following example demonstrates the full round-trip: decompose a DataFrame, modify the plan with `LogicalPlanBuilder`, and wrap it back for execution:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!("a" => [1, 2, 3], "b" => [4, 5, 6])?;

    // Decompose: DataFrame → (SessionState, LogicalPlan)
    let (state, plan) = df.into_parts();

    // Use LogicalPlanBuilder for fine-grained control:
    // 1. Filter rows where a > 1  (keeps a=2,3)
    // 2. Project only column b    (drops column a)
    let modified = LogicalPlanBuilder::from(plan)
        .filter(col("a").gt(lit(1)))?
        .project(vec![col("b")])?
        .build()?;

    // Recompose: (SessionState, LogicalPlan) → DataFrame
    let new_df = DataFrame::new(state, modified);

    new_df.show().await?;
    // +---+
    // | b |
    // +---+
    // | 5 |
    // | 6 |
    // +---+
    Ok(())
}
```

**Key API paths:**

```text
DataFrame ↔ into_parts() ↔ (SessionState, LogicalPlan)
DataFrame → into_optimized_plan() → Optimized LogicalPlan
DataFrame → create_physical_plan() → ExecutionPlan
```

:::{admonition} Further reading
:class: seealso

- [Building Logical Plans](../building-logical-plans.md) — advanced [`LogicalPlanBuilder`] usage
- [`LogicalPlanBuilder` API docs][logicalplanbuilder] — full method reference
  :::

---

With the anatomy clear — `LogicalPlan` for the recipe, `SessionState` for the frozen environment, `DFSchema` for validation at every step — the next question is: what happens when you call `.collect()`? See [Execution Lifecycle](execution-lifecycle.md) for the full journey from lazy plan to parallel execution.
