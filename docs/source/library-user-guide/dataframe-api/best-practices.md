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

# Best Practices and Performance

This guide covers performance tuning, debugging techniques, common pitfalls, and optimization strategies for DataFusion DataFrames.

```{contents}
:local:
:depth: 2
```

## Performance and Best Practices

Understanding how DataFusion optimizes and executes queries is key to building efficient data applications. For details on the optimizer framework, see the [Query Optimizer guide](query-optimizer.md).

### Automatic Optimizations

DataFusion applies many optimizations automatically:

- **Projection pushdown**: Only reads columns that are actually used
- **Predicate pushdown**: Filters data as early as possible
- **Partition pruning**: Skips reading irrelevant partitions
- **Join reordering**: Optimizes join order for performance

These happen transparently due to lazy evaluation.

### Physical Optimizer Controls

Configure the execution environment for optimal performance:

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;

let config = SessionConfig::new()
    // Number of rows per batch (default: 8192)
    .with_batch_size(8192)
    // Number of parallel partitions (default: num_cpus)
    .with_target_partitions(8)
    // Enable repartitioning of file scans
    .with_repartition_file_scans(true)
    // Minimum file size to trigger repartitioning (64MB)
    .with_repartition_file_min_size(64 * 1024 * 1024);

let ctx = SessionContext::with_config(config);
```

**When to tune these settings:**

- **Increase `batch_size`** for better throughput with large datasets (but uses more memory)
- **Increase `target_partitions`** to utilize more CPU cores for parallel processing
- **Enable `repartition_file_scans`** when reading large files to parallelize I/O

### Common Pitfalls

**Schema mismatches in unions:**

```rust
// This will fail - column names must match exactly
let df1 = dataframe!("id" => [1, 2])?;
let df2 = dataframe!("ID" => [3, 4])?; // Different case!
// df1.union(df2)?; // ERROR

// Use union_by_name for flexibility
let df = df1.union_by_name(df2)?; // Works if types match
```

**Memory issues with large collects:**

```rust
// DON'T: Collect millions of rows into memory
// let all_data = huge_df.collect().await?;

// DO: Stream the results
let mut stream = huge_df.execute_stream().await?;
while let Some(batch) = stream.next().await {
    // Process batch by batch
}
```

**Avoiding Cartesian joins:**

```rust
// This creates a Cartesian product (dangerous with large tables!)
let df = left.join(right, JoinType::Inner, &[], &[], None)?;

// Always specify join conditions
let df = left.join(right, JoinType::Inner, &["id"], &["user_id"], None)?;
```

### Debugging Techniques

Inspect query plans to understand what DataFusion will execute:

```rust
use datafusion::prelude::*;
use datafusion::physical_plan::displayable;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?
        .filter(col("a").gt(lit(5)))?
        .select(vec![col("a"), col("b")])?;

    // View the logical plan
    println!("Logical Plan:\n{}", df.logical_plan().display_indent());

    // View the optimized physical plan
    let physical_plan = df.create_physical_plan().await?;
    println!("\nPhysical Plan:\n{}",
        displayable(physical_plan.as_ref()).indent(true));

    Ok(())
}
```

Use `EXPLAIN` to analyze queries:

```rust
let explain_df = df.explain(false, false)?; // (verbose, analyze)
explain_df.show().await?;
```

### Feature Flags

DataFusion's DataFrame API supports optional features. Enable them in `Cargo.toml`:

```toml
[dependencies]
datafusion = { version = "38", features = ["json", "avro", "compression"] }
```

Common features:

- `json`: Enables `read_json()` and `write_json()`
- `avro`: Enables `read_avro()`
- `parquet`: Parquet support (enabled by default)
- `compression`: Compression support for various formats

---

> **Prerequisites**:
>
> - [Concepts](concepts.md) - Understand how DataFrames work
> - [Creating DataFrames](creating-dataframes.md) - Know execution methods
> - [Transformations](transformations.md) - Understand query operations
>
> **Related**:
>
> - [Query Optimizer Guide](../query-optimizer.md) - Deep dive into optimization
> - [Index](index.md) - Return to overview
