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
TODO(Docs): Final Restructuring (Executing, Writing, and Streaming)

1. SPLIT INTO THREE FOCUSED DOCUMENTS:

   A. `executing-dataframes.md` (In-Memory Actions)
      - Keep `.collect()`, `.show()`, `.cache()`.
      - Focus on getting data into RAM and checking results.

   B. `writing-dataframes.md` (Persistent Storage)
      - Keep Parquet, CSV, JSON, and Table writing all in this ONE file.
      - Add a warning about the `.with_single_file_output(true)` performance bottleneck.
      - Add a brief note/link about AWS S3 / GCS authentication (e.g., `AmazonS3Builder`).

   C. `streaming.md` (Advanced / Incremental Processing)
      - Move `.execute_stream()` and `.execute_stream_partitioned()` here.
      - Combine this with your empty "Creating DataFrames from Streams" document.
      - Focus: Unbounded data, backpressure, and avoiding OOM (Out Of Memory) errors.
-->

# Writing and Executing of DataFusion DataFrames

**The final phase of the DataFrame lifecycle: from lazy plan to materialized results.**

This guide covers how to **execute** DataFrames to obtain results and **persist** them to files or tables. In the [DataFrame lifecycle metaphor](./index.md), this is the "death" phase—where the lazy query plan is consumed and transformed into concrete output, whether in-memory `RecordBatch`es, pretty-printed tables, or persistent storage.

DataFusion uses **lazy evaluation**: all transformations ([`.filter()`], [`.select()`], [`.aggregate()`]) build a [`LogicalPlan`] without processing data. Execution only happens when you call an **action method**—and because action methods take ownership, the DataFrame is consumed (use [`df.clone()`][`.clone()`] when you need multiple actions).

:::{admonition} Style Note
:class: note

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)
- static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::

```{toctree}
:maxdepth: 1
:caption: Table of Contents for Writing and Executing of  DataFrames
:numbered:
executing-dataframes
writing-dataframes
streaming-execution
```

## Introduction

Unlike SQL clients where every query implicitly executes and displays results, DataFusion's Rust API gives you **explicit control** over the final phase. This design enables memory-conscious patterns: collect small results entirely, stream large datasets batch-by-batch, cache expensive computations for reuse, or write directly to storage without intermediate buffering.

The following diagram illustrates where execution fits in the DataFrame architecture:

```text
           +------------------+
           |    DataFrame     |
           |   (lazy plan)    |
           +------------------+
                    |
                    | action method called
                    v
           +------------------+
           |   LogicalPlan    |  ← Rewrites plan (pushdowns, pruning)
           +------------------+
                    |
                    v
           +------------------+
           |  ExecutionPlan   |  ← Physical operators (parallel)
           +------------------+
                    |
          +---------+---------+
          |                   |
          v                   v
    +------------+       +--------------+
    |  In-Memory |       |  Persistent  |
    |  Results   |       |    Storage   |
    +------------+       +--------------+
     .collect()           .write_parquet()
     .show()              .write_csv()
     .cache()             .write_table()
                          .write_json()
```

DataFusion uses **vectorized execution**: operators process data in columnar batches (`RecordBatch`), not tuple-at-a-time like the classic Volcano model. This design enables SIMD optimizations and cache-friendly memory access.

> **Note:** SIMD requires compiling with CPU-specific flags (`RUSTFLAGS='-C target-cpu=native'`). See [Crate Configuration](../../user-guide/crate-configuration.md#generate-code-with-cpu-specific-instructions) for details.

In this guide, you will learn how to:

- **Materialize results** with [`.collect()`] and inspect them with [`.show_limit()`] / [`.show()`] / [`.to_string()`] (best for small results)
- **Stream large results** with [`.execute_stream()`] (streaming output, backpressure-aware; avoids buffering the full result set in your application)
- **Reuse computed results** with [`.cache()`]
- **Persist results** with [`.write_parquet()`], [`.write_csv()`], [`.write_json()`], and [`.write_table()`]

> **Ownership Note:** <br>
> All action methods in this guide take **ownership** of the DataFrame (`self`, not `&self`). After any action—whether [`.collect()`], [`.show()`], or [`.write_parquet()`]—the `DataFrame` value has been **moved** and the same variable cannot be reused. This does **not** delete your data or invalidate the query plan; it’s simply Rust’s move semantics. Use [`.clone()`] before an action if you need to run multiple actions (or retry after an error).

### For deeper coverage, see:

- [Concepts § Execution Model](concepts.md#execution-model-actions-vs-transformations) — DataFrame lifecycle and async runtime
- [Architecture Guide] — Official documentation on planner → logical → physical flow
- [SIGMOD 2024 Paper]— Academic paper on DataFusion's design
- [How Query Engines Work] — Beginner-friendly book by DataFusion's creator

## Actions vs Transformations: A Quick Recap

Before diving into execution methods, it's essential to understand the distinction between **transformations** and **actions**—the two categories of DataFrame operations.

| Category            | Returns      | Executes? | Examples                                               |
| ------------------- | ------------ | --------- | ------------------------------------------------------ |
| **Transformations** | `DataFrame`  | No (lazy) | [`.filter()`], [`.select()`], [`.join()`], [`.sort()`] |
| **Actions**         | Results/Side | Yes       | [`.collect()`], [`.show()`], [`.write_parquet()`]      |

**This document focuses entirely on actions**—the methods that materialize your query into memory, streams, or persistent storage.

> **Key insight**:<br> The optimizer sees the **complete pipeline** before execution. This means filters, projections, and limits from your entire chain (including SQL operations) are optimized together—pushdowns, pruning, and reordering happen automatically.

**Ownership and cloning**: <br>

> Actions take ownership of the `DataFrame`, consuming it. If you need to perform multiple actions on the same plan, call [`.clone()`] first:

```rust
# use datafusion::prelude::*;
# use datafusion::error::Result;
# #[tokio::main]
# async fn main() -> Result<()> {
# let ctx = SessionContext::new();
# let df = ctx.sql("SELECT 1 as id").await?;
// ✗ Won't compile: df is consumed by first action
// df.show().await?;
// df.collect().await?;

// ✓ Clone for multiple actions
df.clone().show().await?;    // Preview
df.collect().await?;          // Collect for processing
# Ok(())
# }
```

> **SQL contrast**: <br>
> In SQL clients, every query implicitly executes—there's no distinction between building a query and running it. The DataFrame API's explicit action methods give you control over _when_ and _how_ execution happens.

For deeper conceptual coverage, see [Concepts § Execution Model](concepts.md#execution-model-actions-vs-transformations).

---

```

```
