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

# Arrow IPC — Zero-Copy Native

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents}
:local:
:depth: 2
```

**Arrow's native serialization format (Feather v2): very low deserialization overhead, fast startup, perfect for inter-process communication.**

Arrow IPC preserves Arrow's in-memory layout on disk. Deserialization is minimal and often zero-copy (depending on alignment and platform). Ideal for passing data between processes or caching intermediate results.

```rust
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::*;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let path = "data.arrow";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("datafusion/datasource-arrow/tests/data/example.arrow")
    #     .to_string_lossy().to_string();

    let df = ctx.read_arrow(&path, ArrowReadOptions::default()).await?;

    df.show().await?;
    Ok(())
}
```

**Practical considerations:**

- **Fastest startup** <br>
  Arrow IPC preserves Arrow's in-memory layout on disk, so DataFusion avoids expensive deserialization.
- **No predicate pushdown** <br>
  Arrow IPC does not provide Parquet-style statistics for pruning; DataFusion must read and decode batches to apply filters.
- **File extension matters** <br>
  DataFusion selects Arrow IPC files by extension (default: `.arrow`). If your files use a different extension (e.g., `.feather`), set the `file_extension` field in `ArrowReadOptions`.

#### When to Use Arrow IPC

| Use Case                     | Arrow IPC Fits?                          |
| ---------------------------- | ---------------------------------------- |
| Inter-process communication  | ✅ Excellent                             |
| Caching intermediate results | ✅ Excellent                             |
| Same-machine data sharing    | ✅ Excellent                             |
| Long-term storage            | ⚠️ Consider Parquet (better compression) |
| Cross-language exchange      | ✅ Arrow is language-agnostic            |

#### Trade-offs

| Advantage                     | Disadvantage              |
| ----------------------------- | ------------------------- |
| Zero deserialization cost     | No predicate pushdown     |
| Fastest startup time          | Larger files than Parquet |
| Preserves Arrow types exactly | Less ecosystem tooling    |
| Columnar layout               | No statistics for pruning |

#### ArrowReadOptions

[`ArrowReadOptions`] provides builder methods for customization.

| Builder Method                                                                 | Default | Usage                                                                                     |
| :----------------------------------------------------------------------------- | :------ | :---------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`arrowreadoptions::schema()`]**                         | `None`  | Explicit schema. Normalize schema across multiple files or override/standardize metadata. |
| **[`.table_partition_cols(Vec)`][`arrowreadoptions::table_partition_cols()`]** | `[]`    | Maps Hive-style directory paths to columns (e.g., `year=2024/month=01/`).                 |

> **Note:** [`ArrowReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

If you need to scan a directory that contains mixed file types, Arrow IPC files are selected by extension (default: `.arrow`). `ArrowReadOptions` does not expose a builder for this—set the field directly using struct update syntax:

`ArrowReadOptions { file_extension: ".feather", ..Default::default() }`

#### Arrow IPC Production Tips

- **Ideal for inter-process data passing** — Use when sharing Arrow data between processes on the same machine
- **Great for caching intermediate results** — Store DataFusion outputs for later reuse without re-computation
- **For long-term storage, prefer Parquet** — Arrow IPC has minimal compression; Parquet offers better storage efficiency
- **File extension flexibility** — Arrow IPC files may use `.arrow`, `.feather`, or `.ipc`; set `file_extension` accordingly
