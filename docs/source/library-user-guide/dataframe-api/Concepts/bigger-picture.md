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

# The Bigger Picture: The LLVM of Data—Origins and Outlook

**DataFusion stands on the shoulders of giants—and is actively shaping the future of data systems.**

Understanding where DataFusion comes from—and where it's going—helps you make informed architectural decisions. The [previous section](execution-lifecycle.md) covered _how_ DataFusion executes queries — from lazy plans through optimization to streaming results. This section zooms out: where does this execution model come from, and where is the project heading?

```{contents} Table of Contents for The Bigger Picture
:local:
:depth: 2
```

## Execution Model: Vectorized Volcano

**DataFusion combines the classic pull-based Volcano iterator model with modern vectorized batch processing and Rust's async concurrency.**

DataFusion implements a **vectorized Volcano model**, combining the classic iterator-based execution with modern batch processing. Like other high-performance engines (ClickHouse, DuckDB), each operation is an operator in a DAG, and execution proceeds by calling `poll_next()` to pull batches through the pipeline (see [DataFusion blog on repartitioning][volcano-blog]).

**The evolution (historical background):**

| Era   | Model                      | Data Flow  | Characteristics                                             |
| ----- | -------------------------- | ---------- | ----------------------------------------------------------- |
| 1990s | Volcano (Graefe)           | Pull       | Parent calls `next()` on child, single-tuple iteration      |
| 2010s | Vectorized                 | Pull       | Batch processing (1000+ rows), SIMD, cache efficiency       |
| Today | Vectorized Volcano + Async | Async-Pull | Parent polls child; child yields if not ready (cooperative) |

DataFusion's hybrid approach provides (see [SIGMOD paper Section 5.5][sigmod-paper] for benchmarks):

- **Composability**: Operators form a DAG; each calls `poll_next()` on children
- **Vectorized efficiency**: Processing batches enables SIMD and cache locality
- **Async concurrency**: Tokio's work-stealing scheduler parallelizes across partitions

This is why all DataFrame actions are `async fn`—they participate in cooperative scheduling rather than blocking threads. For a deep dive into how this enables query cancellation, see [Using Rust async for Query Execution][async-blog].

:::{admonition} Performance note: SIMD requires explicit opt-in
:class: note
By default, the Rust compiler produces code for a wide range of CPUs, which may _not_ use advanced SIMD instructions (AVX2, AVX512) available on your hardware. To enable CPU-specific optimizations:

```bash
RUSTFLAGS='-C target-cpu=native' cargo build --release
```

This can significantly improve performance for filtering, aggregation, and joins. See [Crate Configuration: Generate Code with CPU Specific Instructions](../../../user-guide/crate-configuration.md#generate-code-with-cpu-specific-instructions) for more options including LTO and PGO.
:::

---

## The LLVM Parallel: Ecosystem Role

**DataFusion is to data systems what LLVM is to compilers — reusable, modular infrastructure that lets builders focus on domain-specific features.**

The [SIGMOD 2024 paper][sigmod-paper] draws a parallel between DataFusion and LLVM—not in internal architecture, but in **ecosystem role**. From Section 4.1:

"Just as LLVM's modular design catalyzed the development of system programming languages, DataFusion catalyzes the development of data systems."

**The transformation (Compiler vs Data Systems Worlds):**

| Aspect      | Compiler World                                           | Data Systems World                                                                   |
| ----------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| **Before**  | Monolithic compilers (IBM, Solaris, AIX)                 | Monolithic databases (Oracle, SQL Server, DB2)                                       |
| **After**   | Modular compilers sharing LLVM (Rust, Swift, Zig, Julia) | Modular data systems sharing DataFusion (InfluxDB 3.0, GreptimeDB, Coralogix, Comet) |
| **Benefit** | Language authors focus on language features              | Data system authors focus on domain-specific features                                |

**What this enables:** <br>
Query engine developers can focus on value-added, domain-specific features while DataFusion provides SQL parsing, plan representations, optimizations, storage format support, and standard relational operators.

**What this is NOT:** <br>
DataFusion does not use LLVM IR or JIT compilation internally. The parallel is about the role DataFusion plays as reusable infrastructure—like LLVM is for compilers, DataFusion is for query engines.

---

## Future Roadmap

<!--Risky implementations, needs potentially updates!-->

DataFusion is actively evolving. Key initiatives include:

- **[Epic #12723: Reliable Foundation][epic-12723]** <br>
  Separating Frontend (SQL, DataFrame API) from Core (dialect-agnostic IR, optimizers) from Execution (physical planning). This layering makes DataFusion more reusable as infrastructure.

- **[Epic #12644: Extension Types][epic-12644]** <br>
  User-defined types that flow through the entire query lifecycle, enabling domain-specific type systems.

- **[Logical/Physical Type Decoupling][epic-12622]** _(under discussion)_ <br>
  Separating logical types (what the query describes) from physical types (how data is stored), enabling runtime-adaptive execution.

For the complete roadmap and quarterly planning discussions, see the [Contributor Guide: Roadmap][roadmap].

Epic #12723 draws a line between two terms that are easy to conflate. **Frontends** are the query-authoring layer — DataFusion SQL and the DataFrame API — that turn a query into a `LogicalPlan`. **Interfaces** are the surrounding integration surfaces — Python bindings, Arrow Flight, Substrait, and custom embeddings — that drive the engine or exchange plans rather than acting as SQL dialects of their own. Both eventually meet at the same logical layer.

**Putting it all together:**<br>
The following diagram shows how these concepts connect—frontends and interfaces both feed into a common logical layer, which executes via the vectorized Volcano engine:

```text
┌────────────────────────────────────────────────────────────────────┐
│                         THE DATAFUSION PLATFORM                    │
│                (The "LLVM" of Data: Modular & Composable)          │
└───────────────────────────────┬────────────────────────────────────┘
                                │
                                ▼
       [ FRONTENDS ]             [ ──── INTERFACES ──── ]
  ┌──────────────────┐    ┌────────────┐    ┌──────────────────┐
  │  SQL / DataFrame │    │  Python /  │    │   Substrait /    │
  │       API        │    │   Flight   │    │      Custom      │
  └─────────┬────────┘    └─────┬──────┘    └────────┬─────────┘
            │                   │                    │
            └───────────────────┼────────────────────┘
                                ▼
┌────────────────────────────────────────────────────────────────────┐
│                    COMMON LOGICAL INTERMEDIATE LAYER               │
│         (Dialect-Agnostic IR, Logical Plans & Global Optimizers)   │
├────────────────────────────────────────────────────────────────────┤
│  FUTURE: Separated Frontend/Core (Epic #12723)                     │
└───────────────────────────────┬────────────────────────────────────┘
                                ▼
┌────────────────────────────────────────────────────────────────────┐
│                    VECTORIZED VOLCANO EXECUTION                    │
│       (Async Task Runner + Physical Planning + Extension Points)   │
├───────────────────────────────┬────────────────────────────────────┤
│    [ EXTENSION TYPES ]        │        [ CUSTOM OPTIMIZERS ]       │
│      (Epic #12644)            │         (Domain-Specific)          │
└───────────────────────────────┼────────────────────────────────────┘
                                │
                                ▼  poll_next() ──▶ [RecordBatch]
                                ▼  poll_next() ──▶ [RecordBatch]

                  ┌───────────────────────────────────────────┐
                  │        STREAMING ARROW DATA OUTPUT        │
                  │       (High-Performance, Data-Driven)      │
                  └───────────────────────────────────────────┘
```

For a recap of all core concepts and pointers to the next documentation sections, continue to [Summary](summary.md).

---

[async-blog]: https://datafusion.apache.org/blog/2025/06/30/cancellation/
[epic-12644]: https://github.com/apache/datafusion/issues/12644
[epic-12723]: https://github.com/apache/datafusion/issues/12723
[roadmap]: https://datafusion.apache.org/contributor-guide/roadmap.html
[sigmod-paper]: https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf
[volcano-blog]: https://datafusion.apache.org/blog/2025/12/15/avoid-consecutive-repartitions/#parallel-execution-in-datafusion
[epic-12622]: https://github.com/apache/datafusion/issues/12622
