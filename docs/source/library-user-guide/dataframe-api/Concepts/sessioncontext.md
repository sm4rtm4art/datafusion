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

# SessionContext: The Entry Point for DataFrames

**SessionContext — the single entry point for both APIs, making data handling reproducible by managing configuration, catalogs, and execution state.**

The `SessionContext` is where everything begins in DataFusion. Before you can read a file, run a SQL query, or build a DataFrame, you need a context that knows about your data sources, configuration, and registered functions. The `SessionContext` itself is **mutable** — it evolves over the lifetime of your session as you register tables, add UDFs, and change settings. Every DataFrame you create receives a **structural clone** of the `SessionState`: config and function registries are independently copied, while the catalog and runtime remain shared via `Arc`. For the precise clone semantics, see [The SessionState Clone](../Creating-DataFrames/creating-concepts.md#the-sessionstate-clone).

```{contents} Table of Contents for SessionContext
:local:
:depth: 2
```

---

## Introduction to SessionContext

**The `SessionContext` holds everything your queries need — configuration, catalogs, functions, and runtime resources.**

You'll use the `SessionContext` to load data, run SQL, register tables, and configure execution behavior. Internally, it is organized into four key components:

```text
SessionContext (mutable, evolves over session lifetime)
├── ConfigOptions       ← batch_size, target_partitions, timezone, SQL options
├── RuntimeEnv          ← memory pool, disk manager, object stores
├── Catalog             ← registered tables, schemas, databases
└── Function Registry   ← UDFs, UDAFs, UDWFs
        │
        ↓ snapshot at DataFrame creation
SessionState (immutable) ← frozen environment captured by DataFrame
```

This separation provides partial isolation: config and function registry changes to the `SessionContext` after DataFrame creation don't affect existing DataFrames — those are independently cloned. However, the catalog and runtime are shared via `Arc`, so new table registrations _are_ visible to previously created DataFrames.

---

## The SessionContext API Surface

**`SessionContext` collects different data inputs into a catalog, adding metadata for efficient and predictable execution.**

Like `DataFrame`, the `SessionContext` exposes a large API surface that becomes easier to navigate once you understand the main categories:

| Category               | Purpose                                | Examples                                                                  |
| ---------------------- | -------------------------------------- | ------------------------------------------------------------------------- |
| **DataFrame Creation** | Create DataFrames from various sources | [`.read_parquet()`], [`.read_csv()`], [`.sql()`], [`.table()`]            |
| **Table Management**   | Register and manage tables by name     | [`.register_table()`], [`.register_parquet()`], [`.deregister_table()`]   |
| **Configuration**      | Control execution behavior             | [`.with_config()`], [`.state()`]                                          |
| **Catalog Operations** | Manage schemas and databases           | [`.catalog()`], [`.catalog_names()`]                                      |
| **Extensions**         | Add custom functionality               | [`.register_udf()`], [`.register_udaf()`], [`.register_table_provider()`] |

Each category builds on the same principle: you configure the context _before_ creating DataFrames, and those DataFrames inherit a frozen snapshot of everything you've set up. This is why the order matters — register your tables and UDFs first, then create DataFrames that depend on them.

:::{admonition} Learn more
:class: seealso
For complete examples of each pattern, see [Creating DataFrames](creating-dataframes.md). For all available configuration options, see [Configuration Settings](../../user-guide/configs.md).
:::

---

## Creating and Configuring SessionContext

**Most users start with `SessionContext::new()` — customize only when your workload demands it.**

Before using any of the methods above, you need a `SessionContext`. In most cases, the defaults are all you need. For performance-critical workloads, you can tune execution parameters via [`SessionConfig`]:

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;

fn main() {
    // Default context (good for getting started)
    let ctx = SessionContext::new();

    // Customized context for performance tuning
    let config = SessionConfig::new()
        .with_batch_size(8192)               // Rows per batch
        .with_target_partitions(8);          // Parallelism (typically num_cpus::get())
    let ctx = SessionContext::new_with_config(config);
}
```

Once you have a `SessionContext`, you register data sources, configure execution, and create DataFrames. When a DataFrame is created, it receives a structural clone of the `SessionState` — config and functions are independently copied, while the catalog and runtime remain shared via `Arc`. The `LogicalPlan` is what's truly frozen at creation time: it captures the relational operations as they existed at that moment.

---

## References

- [`SessionContext`] documentation (API reference)
- [`SessionState`] documentation (clone semantics)
- [Configuration Settings](../../user-guide/configs.md) (all configuration options)
- [Creating DataFrames](creating-dataframes.md) (practical examples)

---

With the execution environment in place, the next question is _how_ you build queries. DataFusion offers two paths — declarative SQL and the programmatic DataFrame builder — both producing the same optimized plan. See [Builder vs. Parser](builder-parser.md) for the detailed comparison.

---
