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

# Summary

**You now have the conceptual foundation — from architecture through execution — to build with DataFusion confidently.**

The [previous sections](bigger-picture.md) walked through DataFusion's architecture, the two APIs, DataFrame internals, expressions, the execution lifecycle, and the project's broader ecosystem role. This page ties those concepts together and points you toward the next steps in the DataFrame lifecycle.

```{contents} Table of Contents for Summary
:local:
:depth: 2
```

## Concepts at a Glance

**The core principles that run through every section of this documentation.**

| Concept             | Key Insight                                                                                | Covered In                                           |
| :------------------ | :----------------------------------------------------------------------------------------- | :--------------------------------------------------- |
| Architecture        | Embeddable OLAP engine with pluggable `TableProvider`s                                     | [Architectural Overview](architectural-dataframe.md) |
| SessionContext      | Mutable hub; each DataFrame receives a structural `SessionState` clone                     | [SessionContext](sessioncontext.md)                  |
| Two APIs            | SQL (parser) and DataFrame (builder) produce identical `LogicalPlan`s                      | [Builder vs. Parser](builder-parser.md)              |
| DataFrame Anatomy   | `LogicalPlan` + `SessionState` + `DFSchema` — everything needed for reproducible execution | [Anatomy](anatomy-dataframe.md)                      |
| Expressions         | `Expr` trees are the row-level logic inside plan nodes                                     | [Expressions](expressions.md)                        |
| Execution Lifecycle | Lazy transformations → action → optimizer → physical plan → streaming results              | [Execution Lifecycle](execution-lifecycle.md)        |
| Bigger Picture      | Vectorized Volcano model; DataFusion as reusable infrastructure ("LLVM for data")          | [The Bigger Picture](bigger-picture.md)              |

Together these properties let you write declarative SQL for clarity, drop to Rust for control, and still get one optimized execution pipeline.

---

## Where to Go Next

**With concepts understood, the next phase in the DataFrame lifecycle is Birth — creating DataFrames from files, SQL, or in-memory data.**

| Lifecycle Phase | Document                                               | What Happens                                                |
| :-------------- | :----------------------------------------------------- | :---------------------------------------------------------- |
| **Birth**       | [Creating DataFrames](../Creating-DataFrames/index.md) | Load Parquet, CSV, JSON, or in-memory data into a DataFrame |
| **Life**        | [Transformations](../Transformations/index.md)         | Select, filter, aggregate, join — build the lazy plan       |
| **Death**       | [Writing & Executing](../Writing-DataFrames/index.md)  | Collect, stream, or persist results to storage              |

---

## Advanced Reference: API Cheat-Sheet

Know what you want? Find the method here:

| Goal                         | Primary API(s)                              | Keeps SessionState? | Typical Follow-up                                        |
| ---------------------------- | ------------------------------------------- | :-----------------: | -------------------------------------------------------- |
| **Re-use plan later**        | [`.into_parts()`]                           |         ✅          | mutate plan → [`.create_physical_plan()`] → execute      |
| **Inspect optimizer output** | [`.explain()`], [`.into_optimized_plan()`]  |         ⚠️          | check pushdown/pruning, join choice                      |
| **Inspect unoptimized plan** | [`.into_unoptimized_plan()`]                |         ⚠️          | verify pre-optimization structure                        |
| **Multi-language queries**   | [`.into_view()`] + [`.sql()`]               |         ✅          | clean with DataFrame → query with SQL (window fns, CTEs) |
| **Stream large result**      | [`.execute_stream()`], [`.write_parquet()`] |         ✅          | pipe to Parquet/CSV, Kafka, etc.                         |
| **Quick interactive result** | [`.collect()`], [`.show()`]                 |         ✅          | debug, notebooks, CLI                                    |

> **SessionState matters**: <br>
> Methods marked ⚠️ drop the snapshot. They're great for inspection, but to execute later use [`.into_parts()`] to preserve deterministic semantics (timestamps, timezone, config, UDF catalog). See "Re-use plan later" in the cheat-sheet for the safest way to extract and modify a plan.

---

## Further Reading

These references supplement the Concepts section. Each sub-page also links to its most relevant resources inline.

### Internal Guides

| Resource                                                     | Description                                                           |
| ------------------------------------------------------------ | --------------------------------------------------------------------- |
| [Using the DataFrame API](../using-the-dataframe-api.md)     | Overview + how this guide is structured                               |
| [Creating DataFrames](../Creating-DataFrames/index.md)       | Read data and build an initial `DataFrame`                            |
| [Transformations](../Transformations/index.md)               | Add filters, projections, joins, and aggregates (build the lazy plan) |
| [Writing DataFrames](../Writing-DataFrames/index.md)         | Execute (`.collect()`, `.execute_stream()`) and write results         |
| [Best Practices](../best-practices.md)                       | Performance tuning and correctness tips                               |
| [Building Logical Plans](../building-logical-plans.md)       | Work directly with `LogicalPlan` / `LogicalPlanBuilder`               |
| [Arrow Introduction](../../user-guide/arrow-introduction.md) | Arrow basics: `RecordBatch`, schemas, and columnar memory             |
| [SQL Data Types](../../user-guide/sql/data_types.md)         | DataFusion’s SQL type system                                          |
| [Scalar Functions](../../user-guide/sql/scalar_functions.md) | Built-in functions (used from both SQL and DataFrames)                |

---

### API Docs (docs.rs)

| Type / Trait                                                                                                                                         | Description                                                               |
| ---------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| [`SessionContext` (datafusion)](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html)                           | Entry point: register data sources, create DataFrames, run SQL            |
| [`SessionState` (datafusion)](https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html)                         | Structural clone of config/catalog/runtime used during planning/execution |
| [`DataFrame` (datafusion)](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html)                                             | Lazy plan builder; actions trigger execution                              |
| [`LogicalPlan` (datafusion-expr)](https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html)                         | Logical representation produced by SQL and DataFrames                     |
| [`LogicalPlanBuilder` (datafusion-expr)](https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html) | Lower-level builder for `LogicalPlan`                                     |
| [`ExecutionPlan` (datafusion)](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)                                  | Physical plan trait executed by the runtime                               |
| [`TableProvider` (datafusion)](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html)                                     | Data source abstraction used by `SessionContext`                          |

---

### External

| Resource                                                                                                                                    | Description                              |
| ------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| [Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine](https://dl.acm.org/doi/10.1145/3626246.3653368)                | SIGMOD 2024 paper                        |
| [How to Avoid Consecutive Repartitions](https://datafusion.apache.org/blog/2025/12/15/avoid-consecutive-repartitions/)                      | Volcano model and parallel execution     |
| [Using Rust async for Query Execution](https://datafusion.apache.org/blog/2025/06/30/cancellation/)                                         | Async execution and query cancellation   |
| [Using Rustlang's Async Tokio Runtime for CPU-Bound Tasks](https://thenewstack.io/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/) | Why async works for compute              |
| [How Parquet Pruning Works](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/)                                                 | File/row-group skipping                  |
| [Filter Pushdown in Parquet](https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/)                                               | Filter pushdown and late materialization |
| [How Query Engines Work — DataFrames](https://howqueryengineswork.com/06-dataframe.html)                                                    | Conceptual background                    |
