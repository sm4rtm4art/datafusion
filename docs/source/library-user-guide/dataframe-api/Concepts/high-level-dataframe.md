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

# What is a DataFrame?

```{contents} Table of Contents for What is a DataFrame?
:local:
:depth: 2
```

In DataFusion, a Data**Frame** is not your data—it's the _frame_ around your data. Think of it literally: a framework defining where data lives, how it flows through the query engine, and the environment in which transformations execute.

DataFusion's DataFrames are lazy—not in a bad way, but in an efficient way. When you call [`.filter()`] or [`.join()`], nothing happens yet. You're constructing a [`LogicalPlan`]—the recipe describing _what_ to compute. The DataFrame **wraps** this plan together with a [`SessionState`] snapshot that freezes _how_ to compute it. This pairing ensures reproducibility: re-executing a DataFrame uses the same configuration, catalogs, and query start timestamp, even if the [`SessionContext`] has since changed. (For the technical details, see [Relationship between `LogicalPlan`s and `DataFrame`s](#relationship-between-logicalplans-and-dataframes).)

**But what happens when you finally call `.collect()` and execute the plan?** <br>
The diagram below traces the journey—from lazy plan to concrete results—and shows why deferring execution lets the optimizer reorder operations, push predicates to data sources, and select efficient algorithms.

---

## How Queries Flow Through DataFusion

```text
┌───────────────────────────────────────────────────────────────┐
│                        SessionContext                         │
│   (Primary Entry Point: Catalog, Function Registry, Config)   │
└─────────────┬───────────────────────────────┬─────────────────┘
              │                               │
  ┌───────────▼───────────┐       ┌───────────▼───────────────┐
  │        SQL API        │       │       DataFrame API       │
  │     (Declarative)     │       │       (Programmatic)      │
  ├───────────────────────┤       ├───────────────────────────┤
  │  "SELECT a, b FROM t  │       │ ctx.table("t")            │
  │   WHERE a > 10"       │       │   .filter(col("a").gt(10))│
  └───────────┬───────────┘       └───────────┬───────────────┘
              │                               │
              │ parse / plan                  │ build
              └───────────────┬───────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │                DataFrame                 │
        │        (Immutable Query Handle)          │
        ├──────────────────────────────────────────┤
        │  1. LogicalPlan (Abstract Query)         │
        │  2. SessionState (Snapshot of Context)   │
        └─────────────────────┬────────────────────┘
                              │
══════════════════════════════╪═══════════════════ ACTION
                              │ (.collect / .show / .write)
                              ▼
        ┌──────────────────────────────────────────┐
        │            Logical Optimizer             │
        │ (Predicate Pushdown, Projection Pruning) │
        └─────────────────────┬────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │             Physical Planner             │
        │  (Map Logical nodes to Physical Exec)    │
        └─────────────────────┬────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │            Physical Optimizer            │
        │ (Coalesce Batches, Pipeline Parallelism) │
        └─────────────────────┬────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │            Execution (Tokio)             │
        │   (Pull-based Stream of RecordBatches)   │
        └──────────────────────────────────────────┘
```

**Reading the diagram:**

- **SessionContext (top)**: <br>
  The primary entry point holding your catalog, function registry, and configuration.
- **SQL API / DataFrame API → DataFrame**: <br>
  Both paths converge to the same `DataFrame` structure—an immutable handle wrapping a `LogicalPlan` (what to compute) and a `SessionState` snapshot (frozen context for reproducibility).
- **ACTION boundary**: <br>
  Nothing executes until you call `.collect()`, `.show()`, or `.write_*()`. Above the line is lazy; below is eager.
- **Logical Optimizer → Physical Planner → Physical Optimizer**: <br>
  The logical optimizer rewrites the plan (predicate pushdown, projection pruning), the physical planner chooses algorithms (hash join vs. sort-merge), and the physical optimizer adds parallelism and batching.
- **Execution (Tokio)**: <br> Pull-based streaming via `poll_next()`—data flows as `RecordBatch` chunks through operators in parallel.

> **Glossary snapshot**
>
> - **[`SessionContext`]**: Entry point for creating DataFrames, configuring execution, and registering tables/functions.
> - **[`SessionState`]**: Captured snapshot of context configuration and catalog state used when executing a DataFrame.
> - **[`DataFrame`]**: Lazy wrapper pairing a `LogicalPlan` with a `SessionState` snapshot; transformations build plans, actions execute them.
> - **[`LogicalPlan`]**: Tree describing _what_ to compute (projection, filter, join, etc.).
> - **[`ExecutionPlan`]**: Physical operator tree describing _how_ to compute (hash aggregate, parquet scan, shuffle, etc.).
> - **[`RecordBatch`]**: Arrow data structure representing a chunk of rows in columnar form; execution produces streams of batches.
>
> For deeper architectural details—thread scheduling, memory management, crate organization—see the [Architecture section] in the API documentation.

---
