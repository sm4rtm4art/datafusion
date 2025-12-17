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

This guide covers performance tuning, debugging techniques, common pitfalls, and optimization strategies for DataFusion DataFrames. For extending DataFusion with custom functions, data sources, and integrations, see [Advanced Topics](dataframes-advance.md).

```{contents}
:local:
:depth: 2
```

---

## Performance Quick Checklist

Use this checklist when troubleshooting slow queries or optimizing performance:

| Check                        | How to Verify                                       | Fix                                       |
| ---------------------------- | --------------------------------------------------- | ----------------------------------------- |
| Filter pushdown working      | `df.explain(false, false)?` shows filters near scan | Ensure predicates use indexed columns     |
| Partition count appropriate  | Check `target_partitions` in config                 | Match to available CPU cores              |
| Not collecting too much data | Review `.collect()` usage                           | Use `.execute_stream()` for large results |
| Join keys specified          | No `CrossJoin` in explain output                    | Always provide join columns               |
| Intermediate results cached  | Reused DataFrames call `.cache()`                   | Add `.cache().await?` before reuse        |
| Projection pushdown working  | Only needed columns in scan                         | Select columns early in pipeline          |

**Quick diagnostic commands:**

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "value" => [100, 200, 300]
    )?;

    // Check your query plan for issues
    df.clone().explain(false, false)?.show().await?;

    // Analyze actual execution (runs the query)
    df.clone().explain(false, true)?.show().await?;

    // See logical plan structure
    println!("{}", df.logical_plan().display_indent());
    Ok(())
}
```

---

## Configuration Reference

DataFusion applies many optimizations automatically (projection pushdown, predicate pushdown, partition pruning, join reordering). These happen transparently due to lazy evaluation. For details on the optimizer framework, see the [Query Optimizer guide](../query-optimizer.md).

### Session Configuration

Configure execution behavior via `SessionConfig`:

| Setting                     | Default    | When to Tune                                                      |
| --------------------------- | ---------- | ----------------------------------------------------------------- |
| `batch_size`                | 8192       | Increase for throughput (more memory), decrease for lower latency |
| `target_partitions`         | `num_cpus` | Match available cores; increase for I/O-bound workloads           |
| `repartition_file_scans`    | `true`     | Disable if files are already well-partitioned                     |
| `repartition_file_min_size` | 10MB       | Increase for very large files (e.g., 64MB+)                       |
| `repartition_joins`         | `true`     | Disable if data is pre-partitioned on join keys                   |
| `repartition_aggregations`  | `true`     | Disable for pre-partitioned data                                  |
| `collect_statistics`        | `false`    | Enable for better join ordering with cost-based optimizer         |

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;

fn main() {
    let config = SessionConfig::new()
        .with_batch_size(16384)              // Larger batches for throughput
        .with_target_partitions(16)          // More parallelism
        .with_repartition_file_scans(true)   // Parallelize large file reads
        .with_repartition_file_min_size(64 * 1024 * 1024)  // 64MB threshold
        .with_collect_statistics(true);      // Enable statistics collection

    let ctx = SessionContext::new_with_config(config);
}
```

### Memory Configuration

Configure memory limits to prevent out-of-memory errors:

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::error::Result;
use std::sync::Arc;

fn main() -> Result<()> {
    // Create a memory pool with 2GB limit that spills to disk when full
    let memory_pool = Arc::new(FairSpillPool::new(2 * 1024 * 1024 * 1024));
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .build_arc()?;

    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new(),
        runtime_env
    );
    Ok(())
}
```

| Memory Pool           | Behavior                          | Use Case                       |
| --------------------- | --------------------------------- | ------------------------------ |
| `UnboundedMemoryPool` | No limits                         | Development, trusted workloads |
| `GreedyMemoryPool`    | Hard limit, fails on exceed       | Strict memory control          |
| `FairSpillPool`       | Spills to disk when limit reached | Production with large queries  |

For custom memory allocators and advanced memory management, see [Advanced Topics](dataframes-advance.md#memory-management-overview).

---

## Debugging Techniques

### Inspecting Query Plans

Understanding query plans is essential for performance tuning:

```rust
use datafusion::prelude::*;
use datafusion::physical_plan::displayable;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create sample data
    let df = dataframe!(
        "id" => [1, 2, 3, 4, 5],
        "amount" => [500, 1500, 800, 2000, 1200]
    )?
    .filter(col("amount").gt(lit(1000)))?
    .select(vec![col("id"), col("amount")])?;

    // Logical plan (before optimization)
    println!("Logical Plan:\n{}", df.logical_plan().display_indent());

    // Physical plan (after optimization, shows actual execution)
    let physical_plan = df.create_physical_plan().await?;
    println!("\nPhysical Plan:\n{}",
        displayable(physical_plan.as_ref()).indent(true));

    Ok(())
}
```

### Using EXPLAIN

The `explain()` method provides query plan analysis:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!("id" => [1, 2, 3], "value" => [10, 20, 30])?;

    // Basic explain - shows optimized plan
    df.clone().explain(false, false)?.show().await?;

    // Verbose explain - shows more detail
    df.clone().explain(true, false)?.show().await?;

    // Analyze explain - actually runs query, shows real metrics
    df.explain(false, true)?.show().await?;
    Ok(())
}
```

### Verifying Pushdown

Look for these indicators in EXPLAIN output:

| What to Look For    | Good Sign                                         | Bad Sign                    |
| ------------------- | ------------------------------------------------- | --------------------------- |
| Filter pushdown     | `FilterExec` appears near `ParquetExec`/`CsvExec` | `FilterExec` at top of plan |
| Projection pushdown | `projection=[col1, col2]` in scan                 | All columns listed in scan  |
| Partition pruning   | `pruning_predicate` in scan                       | Full table scan             |
| Join optimization   | `HashJoinExec` or `SortMergeJoinExec`             | `CrossJoinExec` (Cartesian) |

**Example: Checking filter pushdown**

```text
Good (filter pushed down):
  ProjectionExec: expr=[id, amount]
    FilterExec: amount > 1000
      ParquetExec: file=data.parquet, projection=[id, amount], predicate=amount > 1000

Bad (filter not pushed):
  FilterExec: amount > 1000
    ProjectionExec: expr=[id, amount, name, date, ...]
      ParquetExec: file=data.parquet (full scan)
```

### Common Execution Issues

| Symptom                  | Likely Cause            | Solution                                             |
| ------------------------ | ----------------------- | ---------------------------------------------------- |
| Query hangs              | Cartesian join          | Add join keys: `.join(..., &["key"], &["key"], ...)` |
| Out of memory            | Large `.collect()`      | Use `.execute_stream()` instead                      |
| Slow despite filters     | Filter not pushed down  | Check column types match, use supported predicates   |
| High CPU, low throughput | Too many small batches  | Increase `batch_size`                                |
| Single-core execution    | `target_partitions = 1` | Increase to match CPU cores                          |
| Slow file reads          | Single large file       | Enable `repartition_file_scans`                      |

---

## Common Pitfalls

### Schema Mismatches in Unions

Column names must match exactly (case-sensitive):

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // FAILS - column names differ in case
    let df1 = dataframe!("id" => [1, 2])?;
    let df2 = dataframe!("ID" => [3, 4])?;
    // df1.clone().union(df2.clone())?; // ERROR: Schema mismatch

    // SOLUTION: Use union_by_name for flexibility
    let df = df1.union_by_name(df2)?; // Matches by name, handles case differences
    df.show().await?;
    Ok(())
}
```

### Memory Issues with Large Collects

Never collect large result sets into memory:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;

// Example batch processor
fn process_batch(batch: &RecordBatch) {
    println!("Processing {} rows", batch.num_rows());
}

#[tokio::main]
async fn main() -> Result<()> {
    // DON'T: Collect millions of rows
    // let all_data = huge_df.collect().await?;  // May OOM

    // DO: Stream results batch by batch
    let huge_df = dataframe!(
        "id" => [1, 2, 3, 4, 5],
        "value" => [10, 20, 30, 40, 50]
    )?;

    let mut stream = huge_df.execute_stream().await?;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        // Process each batch (typically 8192 rows)
        process_batch(&batch);
    }
    Ok(())
}
```

### Cartesian Joins

Empty join keys create dangerous Cartesian products:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let left = dataframe!("id" => [1, 2], "name" => ["Alice", "Bob"])?;
    let right = dataframe!("user_id" => [1, 2], "order" => [100, 200])?;

    // DANGEROUS: Creates N × M rows (Cartesian product)
    // let df = left.clone().join(right.clone(), JoinType::Inner, &[], &[], None)?;

    // SAFE: Always specify join conditions
    let df = left.join(right, JoinType::Inner, &["id"], &["user_id"], None)?;
    df.show().await?;
    Ok(())
}
```

### Type Coercion Surprises

Implicit type coercion can cause unexpected results:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Integer division truncates
    let df = dataframe!("a" => [5, 7])?
        .with_column("result", col("a") / lit(2))?;  // Returns 2, 3 (not 2.5, 3.5)
    df.show().await?;

    // SOLUTION: Use floating point literals
    let df = dataframe!("a" => [5.0, 7.0])?
        .with_column("result", col("a") / lit(2.0))?;  // Returns 2.5, 3.5
    df.show().await?;
    Ok(())
}
```

### NULL Handling in Filters

NULLs behave differently than you might expect:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let source = dataframe!(
        "id" => [1, 2, 3],
        "status" => [Some("active"), Some("inactive"), None::<&str>]
    )?;

    // This does NOT return rows where status is NULL
    let df = source.clone().filter(col("status").not_eq(lit("active")))?;
    df.show().await?;  // Only shows "inactive", not NULL

    // To include NULLs, be explicit
    let df = source.filter(
        col("status").not_eq(lit("active"))
            .or(col("status").is_null())
    )?;
    df.show().await?;  // Shows both "inactive" and NULL
    Ok(())
}
```

### String Literal Types

Use `&str` or `String` correctly in expressions:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let source = dataframe!("name" => ["Alice", "Bob", "Carol"])?;
    let name = String::from("Alice");

    // Both work, but be consistent
    let df1 = source.clone().filter(col("name").eq(lit("Alice")))?;      // &str
    let df2 = source.filter(col("name").eq(lit(name.clone())))?; // String variable

    df1.show().await?;
    df2.show().await?;
    Ok(())
}
```

### When SQL is More Ergonomic

The DataFrame API and SQL compile to the same logical plan, so performance is identical. However, SQL can be more readable for certain patterns:

| Pattern          | SQL Advantage                  | DataFrame Equivalent                    |
| ---------------- | ------------------------------ | --------------------------------------- |
| Complex CTEs     | Named, readable subqueries     | Nested DataFrame variables              |
| Window functions | Familiar `OVER()` syntax       | `.window()` with builder pattern        |
| CASE expressions | Readable conditional logic     | Nested `when().then().otherwise()`      |
| Set operations   | `UNION`, `INTERSECT`, `EXCEPT` | `.union()`, `.intersect()`, `.except()` |

**Example: Complex window function**

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Create sample sales data
    ctx.sql("CREATE TABLE sales (region TEXT, date DATE, amount INT) AS VALUES
        ('East', '2024-01-01', 100),
        ('East', '2024-01-02', 150),
        ('West', '2024-01-01', 200)
    ").await?;

    // SQL - often more readable for complex windows
    let df = ctx.sql("
        SELECT *,
               SUM(amount) OVER (PARTITION BY region ORDER BY date
                                 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as rolling_sum
        FROM sales
    ").await?;

    df.show().await?;

    // DataFrame API equivalent is more verbose for complex window functions
    // See transformations.md for window function examples
    Ok(())
}
```

> **Tip**: Mix SQL and DataFrame APIs freely. Use `ctx.sql()` for complex expressions, then continue with DataFrame methods.

---

## Memory Guidelines

### When to Use Streaming vs Collect

| Scenario                                   | Recommended Approach                                    |
| ------------------------------------------ | ------------------------------------------------------- |
| Result fits in memory (<1M rows typically) | `.collect().await?`                                     |
| Large results, batch processing            | `.execute_stream().await?`                              |
| Writing to files                           | `.write_parquet()`, `.write_csv()` (streams internally) |
| Aggregations reducing data significantly   | `.collect()` after aggregation                          |
| Displaying samples                         | `.show().await?` or `.show_limit(n).await?`             |

### Memory Pool Sizing

Rules of thumb for `FairSpillPool` or `GreedyMemoryPool` sizing:

| Workload              | Memory Pool Size                               |
| --------------------- | ---------------------------------------------- |
| Single-user analytics | 50-70% of available RAM                        |
| Multi-tenant service  | Per-query limit (e.g., 512MB-2GB)              |
| ETL pipelines         | Match largest intermediate result + 20% buffer |

### Signs Your Query Needs More Memory

- `Resources exhausted` errors
- Excessive spilling (check metrics if available)
- Query slower than expected with `FairSpillPool`
- System swapping during execution

> **Extension Point**: Detailed patterns for memory profiling, custom allocators, and NUMA-aware execution are welcome contributions. See [Contributing](#contributing-to-this-guide).

---

## Robust Execution

**Production-ready DataFrame execution requires careful attention to errors, observability, and resource limits.** This section covers patterns for building reliable, observable, and maintainable DataFrame pipelines.

### Error Handling Patterns

DataFusion uses Rust's `Result` type for error handling, which provides compile-time guarantees that errors are handled. The [`DataFusionError`] enum represents all possible error conditions.

#### Understanding DataFusionError

The [`DataFusionError`] enum has several variants representing different failure modes:

| Variant              | When It Occurs                       | Typical Response                       |
| -------------------- | ------------------------------------ | -------------------------------------- |
| `Plan`               | Invalid query plan construction      | Fix query logic (development error)    |
| `Execution`          | Runtime execution failure            | Log and retry or fail gracefully       |
| `ArrowError`         | Arrow data processing error          | Usually fatal (data corruption)        |
| `External`           | External system error (I/O, network) | Retry with backoff                     |
| `ResourcesExhausted` | Memory or other resource limit hit   | Switch to streaming or increase limits |
| `NotImplemented`     | Feature not yet supported            | Use alternative approach               |

#### The Question Mark Operator

The `?` operator propagates errors up the call stack—the standard Rust pattern used throughout DataFusion:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Each operation returns Result<T, DataFusionError>
    // The ? operator propagates errors automatically
    let df = ctx.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, val)").await?;
    let batches = df.collect().await?;

    println!("Collected {} batches", batches.len());
    Ok(())
}
```

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines**: Type-safe `Result<>` forces explicit error handling at compile time
> - **SQL shines**: In interactive environments, exceptions can be more convenient than explicit error propagation

#### Recoverable vs Fatal Errors

Some errors should trigger retries or graceful degradation, while others indicate bugs or unrecoverable failures:

```rust
use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;

async fn execute_with_fallback(df: DataFrame) -> Result<Vec<String>> {
    // Try to collect results into memory
    let collect_result = df.clone().collect().await;

    match collect_result {
        Ok(batches) => {
            // Success - process batches
            Ok(batches.iter()
                .map(|b| format!("{} rows", b.num_rows()))
                .collect())
        }
        Err(DataFusionError::ResourcesExhausted(_)) => {
            // Memory limit hit - fall back to streaming
            eprintln!("Memory limit reached, switching to streaming");

            let mut results = Vec::new();
            let mut stream = df.execute_stream().await?;

            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                results.push(format!("{} rows", batch.num_rows()));
            }

            Ok(results)
        }
        Err(e) => {
            // Other errors are fatal
            Err(e)
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)").await?;

    let summary = execute_with_fallback(df).await?;
    println!("Results: {:?}", summary);

    Ok(())
}
```

#### Context-Aware Error Messages

Provide context when propagating errors to aid debugging:

```rust
use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};

async fn load_and_process(ctx: &SessionContext, table_name: &str) -> Result<usize> {
    // Add context to errors
    let df = ctx.table(table_name).await
        .map_err(|e| DataFusionError::Plan(
            format!("Failed to load table '{}': {}", table_name, e)
        ))?;

    let batches = df.collect().await
        .map_err(|e| DataFusionError::Execution(
            format!("Failed to execute query on '{}': {}", table_name, e)
        ))?;

    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register a table
    ctx.sql("CREATE TABLE users (id INT, name TEXT) AS VALUES (1, 'Alice'), (2, 'Bob')").await?;

    match load_and_process(&ctx, "users").await {
        Ok(count) => println!("Processed {} rows", count),
        Err(e) => eprintln!("Error: {}", e),
    }

    Ok(())
}
```

### Observability with Tracing

The [`tracing`] crate provides structured, composable logging that integrates well with DataFusion's async execution model.

#### Basic Query Logging

Add `tracing` to your `Cargo.toml`:

```toml
[dependencies]
datafusion = "43"
tracing = "0.1"
```

Log query execution with structured events:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use tracing::{info, error, warn};

async fn execute_query(ctx: &SessionContext, query: &str) -> Result<usize> {
    info!(query = %query, "Executing query");

    let df = match ctx.sql(query).await {
        Ok(df) => df,
        Err(e) => {
            error!(query = %query, error = %e, "Query parsing failed");
            return Err(e);
        }
    };

    let batches = match df.collect().await {
        Ok(batches) => batches,
        Err(e) => {
            error!(query = %query, error = %e, "Query execution failed");
            return Err(e);
        }
    };

    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    info!(query = %query, rows = row_count, "Query completed successfully");

    Ok(row_count)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing (in production, use a proper subscriber)
    tracing_subscriber::fmt::init();

    let ctx = SessionContext::new();
    let query = "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)";

    match execute_query(&ctx, query).await {
        Ok(rows) => println!("Success: {} rows", rows),
        Err(e) => eprintln!("Failed: {}", e),
    }

    Ok(())
}
```

#### Instrumented Functions with Spans

Use the `#[instrument]` macro to automatically create spans with timing:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use tracing::{info, instrument};

#[instrument(skip(df), fields(query_id = %query_id))]
async fn execute_dataframe(df: DataFrame, query_id: &str) -> Result<usize> {
    info!("Starting execution");

    let batches = df.collect().await?;
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    info!(rows = row_count, "Execution complete");
    Ok(row_count)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)").await?;

    let rows = execute_dataframe(df, "query-123").await?;
    println!("Processed {} rows", rows);

    Ok(())
}
```

> **Tip**: For production deployments, configure tracing subscribers to send structured logs to your observability platform (Jaeger, Datadog, etc.). See the [tracing documentation] for integration guides.

### Resource Management

Production deployments must manage memory limits and prevent runaway queries.

#### Memory Management

Configure memory limits to prevent out-of-memory errors:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::memory_pool::FairSpillPool;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure memory limits to prevent OOM (1GB limit)
    let runtime = RuntimeEnvBuilder::default()
        .with_memory_pool(Arc::new(FairSpillPool::new(1024 * 1024 * 1024)))
        .build_arc()?;

    // Create session with memory-limited runtime
    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    // Queries will now respect the memory limit
    // If they exceed it, FairSpillPool will spill to disk
    let df = ctx.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, val)").await?;
    df.show().await?;

    Ok(())
}
```

**Memory pool comparison:**

| Memory Pool           | Behavior                          | Use Case                       |
| --------------------- | --------------------------------- | ------------------------------ |
| `UnboundedMemoryPool` | No limits                         | Development, trusted workloads |
| `GreedyMemoryPool`    | Hard limit, fails on exceed       | Strict memory control          |
| `FairSpillPool`       | Spills to disk when limit reached | Production with large queries  |

See [Memory Guidelines](#memory-guidelines) for sizing recommendations.

#### Cancellation and Timeouts

Wrap long-running queries with timeouts to prevent resource exhaustion:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use tokio::time::{timeout, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)").await?;

    // Wrap execution with timeout (30 seconds)
    let result = timeout(
        Duration::from_secs(30),
        df.collect()
    ).await;

    match result {
        Ok(Ok(batches)) => {
            println!("Success: {} batches", batches.len());
        }
        Ok(Err(e)) => {
            eprintln!("Query error: {}", e);
        }
        Err(_) => {
            eprintln!("Query timed out after 30s");
            // In production: log, clean up resources, notify monitoring
        }
    }

    Ok(())
}
```

**Production patterns for timeout handling:**

```rust
use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};
use tokio::time::{timeout, Duration};
use tracing::{error, warn};

async fn execute_with_timeout(
    df: DataFrame,
    timeout_secs: u64
) -> Result<Vec<RecordBatch>> {
    let result = timeout(
        Duration::from_secs(timeout_secs),
        df.collect()
    ).await;

    match result {
        Ok(Ok(batches)) => Ok(batches),
        Ok(Err(e)) => {
            error!(error = %e, "Query execution failed");
            Err(e)
        }
        Err(_) => {
            warn!(timeout_secs, "Query exceeded timeout");
            Err(DataFusionError::Execution(
                format!("Query timed out after {}s", timeout_secs)
            ))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1), (2)) AS t(id)").await?;

    let batches = execute_with_timeout(df, 10).await?;
    println!("Collected {} batches", batches.len());

    Ok(())
}
```

For cancellation support in interactive applications, use `tokio::select!` to handle user interrupts:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)").await?;

    tokio::select! {
        result = df.collect() => {
            match result {
                Ok(batches) => println!("Completed: {} batches", batches.len()),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        _ = signal::ctrl_c() => {
            println!("Cancelled by user");
        }
    }

    Ok(())
}
```

---

## Observability

> **Extension Point**: Production observability patterns are an area for community contribution. Useful additions include:
>
> - Prometheus/OpenTelemetry metrics integration
> - Distributed tracing for query execution
> - Query logging and audit trails
> - Performance dashboards
>
> See [Contributing](#contributing-to-this-guide) to add examples.

For basic query logging, inspect the logical plan:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

fn log_query(df: &DataFrame, user: &str) {
    let plan_str = df.logical_plan()
        .display_indent()
        .to_string();
    eprintln!("[AUDIT] User '{}' query plan:\n{}", user, plan_str);
}

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!("id" => [1, 2, 3], "value" => [10, 20, 30])?;

    // Log the query before execution
    log_query(&df, "alice");

    df.show().await?;
    Ok(())
}
```

---

## Feature Flags

DataFusion's DataFrame API supports optional features. Enable them in `Cargo.toml`:

```toml
[dependencies]
datafusion = { version = "47", features = ["parquet", "json", "avro"] }
```

| Feature       | Enables                               | Default |
| ------------- | ------------------------------------- | ------- |
| `parquet`     | Parquet read/write                    | Yes     |
| `json`        | JSON/NDJSON read/write                | No      |
| `avro`        | Avro read                             | No      |
| `compression` | Compression codecs (gzip, zstd, etc.) | No      |

---

## Contributing to This Guide

This guide is designed for community expansion. Areas where contributions are especially welcome:

- **Observability patterns**: Metrics, tracing, logging integrations
- **Memory profiling**: Tools and techniques for memory analysis
- **Platform-specific tuning**: ARM, NUMA, container environments
- **Real-world case studies**: Performance improvements from production use

**How to contribute:**

1. Choose an area that matches your expertise
2. Add working, tested examples with clear explanations
3. Submit a PR — see [CONTRIBUTING.md](../../../../CONTRIBUTING.md)

---

> **Prerequisites**:
>
> - [Concepts](concepts.md) — Understand how DataFrames work
> - [Creating DataFrames](creating-dataframes.md) — Know execution methods
> - [Transformations](transformations.md) — Understand query operations
>
> **Related**:
>
> - [Advanced Topics](dataframes-advance.md) — Custom UDFs, TableProviders, multi-tenant isolation
> - [Query Optimizer Guide](../query-optimizer.md) — Deep dive into optimization
> - [Index](index.md) — Return to overview

<!-- Link references -->

[`DataFusionError`]: https://docs.rs/datafusion/latest/datafusion/error/enum.DataFusionError.html
[`tracing`]: https://docs.rs/tracing/latest/tracing/
[tracing documentation]: https://docs.rs/tracing/latest/tracing/
