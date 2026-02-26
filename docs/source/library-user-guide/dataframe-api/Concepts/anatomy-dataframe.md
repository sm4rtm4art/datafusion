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

<!--
TODO(Docs): Add a section on "Expressions (`Expr`) and Data Flow"

1. EXPRESSIONS (`Expr`)
   - WHAT TO ADD: Introduce the `Expr` concept. If `DataFrame` is the container and `LogicalPlan` contains the relational operators (Filter, Join), then `Expr` represents the row-level logic inside those operators.
   - HOW TO EXPLAIN IT:
     - Show a quick example: `col("a").gt(lit(10))` creates an `Expr` tree.
     - Explain that methods like `.filter()` and `.select()` take `Expr` as arguments.
     - Mention that `Expr`s are dialect-agnostic and evaluated at runtime against the schema.
     - (Optional but helpful: Link to the `Expr` API docs or a dedicated expressions guide).

2. RECORD BATCHES & PARTITIONS
   - WHAT TO ADD: A brief conceptual bridge between the execution plan and the actual data.
   - HOW TO EXPLAIN IT:
     - Clarify that a DataFrame doesn't hold data as one giant table in memory.
     - Explain that under the hood, data is divided into **partitions** (enabling parallel processing via Tokio).
     - Execution streams these partitions as **RecordBatches** (Arrow's chunked, columnar format) rather than row-by-row.
     - Tie this back to why `.execute_stream()` is memory-efficient compared to `.collect()`.

Placement suggestion: Put the `Expr` explanation right after "DataFrame Structure: LogicalPlan + SessionState" (since it naturally flows from explaining the LogicalPlan), and put the Partitions/RecordBatches explanation right after the "Execution Model / Tokio" section.
-->

## Anatomy of a Dataframe: LogicalPlan + SessionState

```{contents} Table of Contents for Anatomy of a Dataframe
:local:
:depth: 2
```

**DataFusion—the out-of-the-box query engine—provides the DataFrame with both a recipe (the query plan) and a fully-equipped kitchen (the execution environment) for reproducible results.**

Understanding what a `DataFrame` actually _contains_ explains why queries are reproducible and why certain patterns (like registering UDFs before creating DataFrames) matter.

Every [`DataFrame`] pairs two components:

- **[`LogicalPlan`]** — the query recipe (_what_ to compute)
- **[`SessionState`]** — a frozen snapshot of the execution environment (_how_ to compute it)

The [`SessionContext`] is mutable and evolves over your session, but each `DataFrame` captures an **immutable snapshot** the [`SessionState`] at creation time. Transformations return new DataFrames with updated plans but the same snapshot; actions execute using that frozen state.

---

### Inside a DataFrame: Step by Step

**Think of query execution like cooking—the recipe alone isn't enough; you need the kitchen too.**

The concepts might come clearer with an everyday analogy of a kitchen.

| Concept            | Cooking Analogy    | What it holds                                               |
| ------------------ | ------------------ | ----------------------------------------------------------- |
| [`SessionContext`] | Kitchen (mutable)  | Tools, ingredients, configuration—_changes over time_       |
| [`LogicalPlan`]    | Recipe (immutable) | Step-by-step instructions—_what to compute_                 |
| [`SessionState`]   | Kitchen State      | Given setup of the kitchen at recipe start—_frozen in time_ |
| [`DataFrame`]      | Recipe + snapshot  | Everything needed to cook the dish reproducibly             |

The [`SessionState`] defines the enviroment the data are processed in: if you add new tools to the kitchen after starting a dish, the dish-in-progress still uses the original setup. This prevents surprises ("where did my UDF go?") and ensures reproducibility.

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

**The snapshot guarantees reproducibility:**

| What's given                              | Why it matters                                            |
| ----------------------------------------- | --------------------------------------------------------- |
| Config (batch size, partitions, timezone) | Same performance even if global settings change           |
| UDFs and registered tables                | Queries don't fail if dependencies are deregistered later |
| Query start timestamp                     | Functions like [`.now()`] return consistent values        |

> **Best practice:** <br>
> Register UDFs and tables **before** creating DataFrames that depend on them. <br> > **Mid-processing?** <br>
> If you need a new UDF or table, register it on the `SessionContext`, then create a **new DataFrame**—existing DataFrames keep their original snapshots.

**Key API paths for advanced use:**

```text
DataFrame ↔ into_parts() ↔ (SessionState, LogicalPlan)
DataFrame → into_optimized_plan() → Optimized LogicalPlan
DataFrame → create_physical_plan() → ExecutionPlan
```

> **Learn more:** See [SessionContext and SessionState relationship][sessioncontext and sessionstate] for implementation details.

---

### DataFrame vs. LogicalPlanBuilder

[`DataFrame`] methods are thin wrappers around [`LogicalPlanBuilder`]—they produce identical plans:

| DataFrame method           | LogicalPlanBuilder equivalent       |
| -------------------------- | ----------------------------------- |
| [`DataFrame::select()`]    | [`LogicalPlanBuilder::project()`]   |
| [`DataFrame::filter()`]    | [`LogicalPlanBuilder::filter()`]    |
| [`DataFrame::aggregate()`] | [`LogicalPlanBuilder::aggregate()`] |
| [`DataFrame::join()`]      | [`LogicalPlanBuilder::join()`]      |

This means you can mix approaches—use DataFrame for convenience, drop to LogicalPlanBuilder when you need fine-grained control, then wrap back in a DataFrame for execution:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    // Start with: a=[1,2,3], b=[4,5,6]
    let df = dataframe!("a" => [1, 2, 3], "b" => [4, 5, 6])?;

    // Decompose into parts
    let (state, plan) = df.into_parts();

    // Use LogicalPlanBuilder for fine-grained control:
    // 1. Filter rows where a > 1  (keeps a=2,3)
    // 2. Project only column b    (drops column a)
    let modified = LogicalPlanBuilder::from(plan)
        .filter(col("a").gt(lit(1)))?
        .project(vec![col("b")])?
        .build()?;

    // Wrap back into DataFrame for execution
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

> **Further reading:** <br>
> See [Building Logical Plans](../building-logical-plans.md) for advanced [`LogicalPlanBuilder`] usage.

---

### Advanced: Converting Between `DataFrame` and `LogicalPlan`

**For most users, the DataFrame API is sufficient. This section is for advanced use cases.**

Sometimes you need direct [`LogicalPlan`] access—custom optimizer rules, query rewriting systems, or programmatic plan inspection. DataFusion lets you move freely between the two:

| Use DataFrame API for...             | Use LogicalPlan directly for... |
| ------------------------------------ | ------------------------------- |
| Standard queries and transformations | Custom optimizer rules          |
| Automatic SessionState management    | Fine-grained plan manipulation  |
| Rapid prototyping                    | Query rewriting systems         |

**Extract and modify plans using [`.into_parts()`]:**

[`.into_parts()`] consumes the DataFrame and returns `(SessionState, LogicalPlan)`—the frozen environment and the query recipe as separate values. You can then modify the plan and wrap it back into a DataFrame:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "a" => [1, 5, 10, 15],
        "b" => [2, 6, 11, 16]
    )?;

    // Decompose: DataFrame → (SessionState, LogicalPlan)
    let (state, plan) = df.into_parts();
    // state: frozen config, catalog, UDFs
    // plan:  TableScan("datafusion.public.?table?")

    // Modify the plan using LogicalPlanBuilder
    let modified_plan = LogicalPlanBuilder::from(plan)
        .filter(col("a").gt(lit(5)))?
        .build()?;
    // plan now: Filter(a > 5) → TableScan(...)

    // Recompose: (SessionState, LogicalPlan) → DataFrame
    let new_df = DataFrame::new(state, modified_plan);

    new_df.show().await?;
    Ok(())
}
```

> **Further reading:**
>
> - [Building Logical Plans](../building-logical-plans.md) — advanced [`LogicalPlanBuilder`] usage
> - [`LogicalPlanBuilder` API docs][logicalplanbuilder] — full method reference

---
