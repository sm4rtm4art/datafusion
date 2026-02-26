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

```{contents} Table of Contents for SessionContext
:local:
:depth: 2
```

**The [`SessionContext`] is your reproducible gateway to DataFusion**<br>

The `SessionContext` itself is **mutable**—designed to hold session information: [`ConfigOptions`] (batch size, parallelism, timezone), registered tables and catalogs, user-defined functions, and runtime resources (memory limits, object stores). But every DataFrame you create captures an **immutable** [`SessionState`] snapshot of the context at that moment. This ensures queries execute with consistent settings even if you modify the context later.

```{contents} Table of Contents for SessionContext
:local:
:depth: 2
```

You'll use the `SessionContext` to load data, run SQL, register tables, and configure execution behavior.

As an illustration the following schema should give an overview.

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

This separation ensures reproducibility: changes to the `SessionContext` after DataFrame creation don't affect existing DataFrames—each continues to execute with the `SessionState` snapshot it captured. Only newly created DataFrames will see the updated configuration, tables, or functions.

---

### Common ways to create a DataFrame using the SessionContext

Like DataFrame, SessionContext exposes a large API surface that becomes easier to navigate once you understand the main categories:

| Category               | Purpose                                | Examples                                                                  |
| ---------------------- | -------------------------------------- | ------------------------------------------------------------------------- |
| **DataFrame Creation** | Create DataFrames from various sources | [`.read_parquet()`], [`.read_csv()`], [`.sql()`], [`.table()`]            |
| **Table Management**   | Register and manage tables by name     | [`.register_table()`], [`.register_parquet()`], [`.deregister_table()`]   |
| **Configuration**      | Control execution behavior             | [`.with_config()`], [`.state()`]                                          |
| **Catalog Operations** | Manage schemas and databases           | [`.catalog()`], [`.catalog_names()`]                                      |
| **Extensions**         | Add custom functionality               | [`.register_udf()`], [`.register_udaf()`], [`.register_table_provider()`] |

> **Learn more:** <br>
> For complete examples of each pattern, see [Creating DataFrames](creating-dataframes.md). For all available configuration options, see [Configuration Settings](../../user-guide/configs.md).

---

### Creating and Configuring SessionContext

Before using any of the methods above, you need a `SessionContext`. In most cases, `SessionContext::new()` with defaults is all you need. For performance-critical workloads, you can tune execution parameters via [`SessionConfig`]:

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

Once you have a `SessionContext`, you can create DataFrames, register tables, and execute queries—the context maintains all state that DataFrames need during execution.

For more detailed explanation and examples of the [`SessionContext`] see:

- [`SessionContext`] documentation (API reference)
- [`SessionState`] documentation (snapshot semantics)
- [Configuration Settings](../../user-guide/configs.md) (all configuration options)
- [Creating DataFrames](creating-dataframes.md) (practical examples)

---
