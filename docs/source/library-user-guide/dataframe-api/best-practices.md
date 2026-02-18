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

<!-- ==========================================================================
General TODOS:

This document should purposly be brief to motivate discussions on what to add!

Organize the Best practises from General to Specific.

The Specifics are deidcated to the life cycle

* General => concepts.md => Dataframes and where they live, general best practices
* Birth => creating-dataframes.md => crating dataframes with loging and fall back/error handling
* Health => schema-management.md => schema management and validation (starting at 1382 can be shortened and be more briefly)
* Life => transformations.md => transformations and analysis
* Death => writing-dataframes.md => writing dataframes to storage
* Other => valuble best practises which are inbetween  sections and should be mentioned.

     ========================================================================== -->

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

### Debugging Schema Mismatches

**Schema conflicts manifest as one of four technical problems:**

1. **Count Mismatch:** DataFrames have different number of columns
2. **Order Mismatch:** Columns are in different sequence
3. **Name Mismatch:** Column has different name or capitalization (`Region` vs `region`)
4. **Type Mismatch:** Column has different data type (`Int32` vs `Int64`)

#### Diagnosis

When an error fires like `Schema error: Union schemas have different number of fields`, compare schemas side-by-side:

```rust
use datafusion::prelude::*;

// Print and compare schemas
println!("=== Schema 1 ===");
for field in df1.schema().fields() {
    println!("{:20} {:?} nullable={}", field.name(), field.data_type(), field.is_nullable());
}

println!("\n=== Schema 2 ===");
for field in df2.schema().fields() {
    println!("{:20} {:?} nullable={}", field.name(), field.data_type(), field.is_nullable());
}
```

#### Resolution Strategies

| Problem   | Symptom                              | Solution                      |
| :-------- | :----------------------------------- | :---------------------------- |
| **Shape** | "Different number of fields"         | Use `.union_by_name()`        |
| **Order** | Columns swapped                      | Use `.union_by_name()`        |
| **Type**  | "Incompatible types Int32 and Int64" | Use `.cast_to()` (cast up)    |
| **Name**  | "Field not found"                    | Use `.alias()` in `.select()` |

**Strategy A: Resilient Fix (Count & Order)**

```rust
// union_by_name matches by column name and fills missing with NULL
let unified = df1.union_by_name(df2)?;
```

**Strategy B: Type Alignment**

```rust
use datafusion::arrow::datatypes::DataType;

// Cast narrower type to wider type
let df1_aligned = df1.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, df1.schema())?
)?;
let result = df1_aligned.union(df2)?;
```

**Strategy C: Name Normalization**

```rust
// Rename columns to match target schema
let df_fixed = df_incoming.select(vec![
    col("Region").alias("region"),  // Fix capitalization
    col("amount"),
])?;
```

> **Performance Note:** `.select()` and `.union_by_name()` are metadata-only operations (free). `.cast_to()` requires rewriting data at execution time.

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

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Compile-time schema validation, reusable schema definitions, IDE autocompletion for field names
> - **SQL shines:** Quick ad-hoc exploration where schema flexibility is preferred over strictness

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

## Schema and Data Validation

**Validation protects your pipeline at two levels: schema validation ensures structure is correct, data validation ensures values are correct.**

In production, data arrives from untrusted sources: third-party APIs change field types without warning, CSV uploads contain malformed values, and even "stable" partners send out-of-range data. DataFusion trusts what you give it—it won't automatically validate schemas or data quality.

> **Important distinction**: Schema validation checks metadata only (microseconds, no data scan). Data validation scans actual row values (performance depends on data size).

### Schema Validation (Structure & Types)

**Schema validation ensures your data has the right structure—correct field names, compatible types, expected nullability—before you process it.**

DataFusion's [`DFSchema`] provides methods for comparing schemas, looking up fields, and checking type compatibility.

#### Comparing Schemas for Compatibility

Use [`DFSchema::logically_equivalent_names_and_types()`] to verify two schemas are compatible before combining DataFrames:

```rust
use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};

async fn merge_regional_sales(us_sales: DataFrame, eu_sales: DataFrame) -> Result<DataFrame> {
    let us_schema = us_sales.schema();
    let eu_schema = eu_sales.schema();

    if !us_schema.logically_equivalent_names_and_types(&eu_schema) {
        return Err(DataFusionError::Plan(format!(
            "Regional schemas incompatible. US: {:?}, EU: {:?}",
            us_schema.field_names(),
            eu_schema.field_names()
        )));
    }

    us_sales.union(eu_sales)
}

#[tokio::main]
async fn main() -> Result<()> {
    let us = dataframe!("id" => [1], "amount" => [100])?;
    let eu = dataframe!("id" => [2], "amount" => [200])?;
    let combined = merge_regional_sales(us, eu).await?;
    combined.show().await?;
    Ok(())
}
```

#### Validating Type Cast Safety

Use [`arrow::compute::can_cast_types()`] to check if one type can be safely cast to another without data loss:

```rust
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::DataType;

fn main() {
    // Safe widening conversions
    assert!(can_cast_types(&DataType::Int8, &DataType::Int64));
    assert!(can_cast_types(&DataType::Float32, &DataType::Float64));
}
```

#### Looking Up Required Fields

Use [`DFSchema::field_with_name()`] to validate that required fields exist:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!("customer_id" => [1_i64, 2_i64])?;

    let field = df.schema()
        .field_with_name("customer_id")
        .map_err(|_| DataFusionError::Plan("Missing required field: 'customer_id'".into()))?;

    if field.data_type() != &DataType::Int64 {
        return Err(DataFusionError::Plan(format!(
            "Field 'customer_id' must be Int64, got {:?}",
            field.data_type()
        )));
    }

    println!("Validation passed: customer_id is Int64");
    Ok(())
}
```

### Data Validation (Actual Values)

**After schema validation confirms structure, data validation ensures values meet business rules—no negative amounts, valid emails, unique IDs.**

Unlike schema checks (which operate on metadata in microseconds), data validation scans actual row values—performance depends on data size.

| Pattern             | Technique                          | Use Case                     |
| ------------------- | ---------------------------------- | ---------------------------- |
| **Filter-based**    | `.filter(col("price").gt(lit(0)))` | Reject invalid rows          |
| **Flag-based**      | `.with_column("is_valid", ...)`    | Mark issues, keep all rows   |
| **Aggregate-based** | `.aggregate(...)` with `case/when` | Quality reports, CI/CD gates |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "price" => [100, -5, 50],
        "quantity" => [Some(10), None, Some(5)]
    )?;

    // Reject rows with invalid prices
    let clean_data = df
        .filter(col("price").gt(lit(0)))?
        .filter(col("quantity").is_not_null())?;

    clean_data.show().await?;
    Ok(())
}
```

#### Checking for Duplicates

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::count;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "order_id" => [1, 1, 2, 3, 3, 3],
        "amount" => [100, 100, 200, 300, 300, 300]
    )?;

    let duplicates = df.clone()
        .aggregate(
            vec![col("order_id")],
            vec![count(col("order_id")).alias("cnt")]
        )?
        .filter(col("cnt").gt(lit(1)))?;

    let has_duplicates = duplicates.clone().count().await? > 0;
    if has_duplicates {
        println!("Duplicate order IDs found:");
        duplicates.show().await?;
    }

    Ok(())
}
```

#### Range and Statistical Validation

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::{min, max, avg, count};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "transaction_id" => [1, 2, 3, 4, 5],
        "amount" => [100.0, 200.0, 150.0, 300.0, 250.0]
    )?;

    let stats = df.aggregate(
        vec![],
        vec![
            count(col("transaction_id")).alias("total"),
            min(col("amount")).alias("min_amount"),
            max(col("amount")).alias("max_amount"),
            avg(col("amount")).alias("avg_amount"),
        ]
    )?;

    stats.show().await?;
    // Compare statistics against thresholds to detect anomalies
    Ok(())
}
```

> **See also:** [Schema Management](schema-management.md) for defining schemas and type coercion rules.

---

## Schema Performance

**Schema decisions cascade through your entire pipeline—get them right early to avoid expensive corrections later.**

### The Performance Hierarchy

#### Zero-Cost Operations (Metadata Only)

These operations modify the logical plan without touching data:

```rust
// Column renaming: Updates metadata mapping, no data movement
let renamed = df.select(vec![
    col("customer_id").alias("client_id"),  // Zero cost
    col("amount"),
])?;

// Union planning: Builds a plan to combine DataFrames later
let union_plan = df1.union_by_name(df2)?;  // Free now, executed when you call collect()
```

#### Low-Cost Operations (Optimized Data Access)

```rust
// Predicate pushdown: Uses column statistics to skip row groups
let filtered = df.filter(col("year").eq(lit(2024)))?;

// Projection pushdown: Only deserializes requested columns
let projected = df.select(vec![col("id"), col("name")])?;
```

#### High-Cost Operations (Full Data Transformations)

```rust
// Type casting: Allocates new arrays and converts every value
let casted = df.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, df.schema())?
)?;

// String transformations: Process every character
let normalized = df.with_column(
    "email",
    lower(trim(col("email")))
)?;
```

### Schema Performance Best Practices

| Operation                            |      Cost       | Alternative                      |
| :----------------------------------- | :-------------: | :------------------------------- |
| **`.select()` with aliases**         |      Free       | N/A - always safe                |
| **`.union_by_name()`**               | Free (planning) | `.union()` if exact schema match |
| **`.filter()` on partition columns** |    Very Low     | N/A - always do this first       |
| **`.cast_to()` on single column**    |     Medium      | Define correct type at read time |
| **Multiple `.with_column()` calls**  |      High       | Batch into single `.select()`    |
| **Schema inference**                 |      High       | Always provide explicit schema   |

**Key principles:**

1. **Front-load schema decisions**: Fix schemas at read time, not during processing
2. **Operation order matters**: Filter → Project → Transform → Join/Aggregate
3. **Batch transformations**: One pass with multiple operations beats multiple passes

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

<!--
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
X
X    BEGINN THIGHTENING!  THIS PART IS FROM SCHEMA MANAGEMENT
X
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
-->

<!-- ==========================================================================
     TODO: CONSOLIDATE SCHEMA EVOLUTION SECTIONS

     Current structure (verbose, overlapping):
       - ## Schema Reuse and Versioning
         - ### The `TableProvider` Schema Contract
         - ### Type Control with Macros and Literals
       - ## Schema Evolution Patterns
         - ### Common Evolution Scenarios (table)
         - ### Pattern 1: Forward-Compatible Schema Design
         - ### Pattern 2: Schema Adapter Layer
         - ### Pattern 3: Schema Migration Testing
         - ### Pattern 4: Handling Breaking Changes
         - ### Execution Strategy & Common Pitfalls
         - ### References

     Proposed consolidated structure:
       - ## Schema Evolution & Versioning
         - ### Centralized Schema Patterns (from Schema Reuse intro)
         - ### Common Evolution Scenarios (KEEP - the table)
         - ### Evolution Best Practices (MERGE Pattern 1 + Pattern 3)
         - ### Migration Patterns (MERGE Pattern 2 + Pattern 4)
         - ### Reference
           - TableProvider Schema Contract
           - Type Control with Macros and Literals
           - Further Reading links

     Key changes:
     1. Rename "## Schema Reuse and Versioning" to "## Schema Evolution & Versioning"
     2. Keep centralized schemas code example
     3. Keep the "Common Evolution Scenarios" table (it's excellent)
     4. Merge Pattern 1 (Forward-Compatible) + Pattern 3 (Migration Testing)
        -> "Evolution Best Practices" - focus on additive changes + testing
     5. Merge Pattern 2 (Adapter Layer) + Pattern 4 (Breaking Changes)
        -> "Migration Patterns" - focus on handling divergent/breaking schemas
     6. Move "TableProvider Schema Contract" and "Type Control" to Reference subsection
     7. Merge "Further Reading" section into Reference
     ========================================================================== -->

## Schema Reuse and Versioning

**Centralized schemas prevent drift; explicit versioning tracks evolution.**

Scattered schema definitions—inlined in readers, duplicated in tests—inevitably diverge. By centralizing schemas in a dedicated module and versioning them explicitly (v1, v2, v3), you create a single source of truth that all components reference. This makes schema changes visible in code review, enables compatibility testing between versions, and documents exactly which contract each pipeline component expects.

```rust,ignore
// schemas.rs - Single source of truth for all schemas
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

// v1 schema (baseline, backward‑compatible contract):
//   id:        Int64, required
//   name:      Utf8,  nullable
//   email:     Utf8,  nullable
pub fn customer_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
    ]))
}

// v2 schema (adds created_at; remains backward compatible):
//   id:         Int64, required
//   name:       Utf8,  nullable
//   email:      Utf8,  nullable
//   created_at: Timestamp(Microsecond, "UTC"), required  <-- New field
pub fn customer_schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}

fn main() {
    // Usage: centralized schemas for reuse
    let v1 = customer_schema_v1();
    let v2 = customer_schema_v2();
    println!("v1 has {} fields, v2 has {} fields", v1.fields().len(), v2.fields().len());

    // Test validates against v1 compatibility
    assert!(customer_schema_v2().field_with_name("id").is_ok());
}
```

In practice, you would use these schemas with readers:

```rust,ignore
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Demonstrate schema versioning concept
    // In practice:
    // let df_v1 = ctx.read_csv("customers_old.csv",
    //     CsvReadOptions::new().schema(&customer_schema_v1())
    // ).await?;
    // let df_v2 = ctx.read_csv("customers_new.csv",
    //     CsvReadOptions::new().schema(&customer_schema_v2())
    // ).await?;

    // When fusing v1 and v2 data:
    // - Add missing columns with NULLs
    // - Align types via cast_to
    // - Use union_by_name for name-based alignment

    Ok(())
}
```

Version new schemas when fields change. Store version in metadata (`schema.metadata.insert("version", "2")`). Test that DataFrames match expected versions (see **Schema Validation (Structure & Types)**). Document breaking changes in a migration guide.

<!-- TODO: MOVE to "### Reference" subsection within consolidated "## Schema Evolution & Versioning" -->

### The `TableProvider` Schema Contract

When you implement a custom [`TableProvider`], its [`schema()`][tableprovider::schema] method is a strict contract. The optimizer, join planner, and union logic all rely on it being stable and consistent across every call. Violating this contract can lead to query failures or silent data corruption.

| ✅ Best Practice                                                                       | ❌ Anti-Pattern                                                      |
| :------------------------------------------------------------------------------------- | :------------------------------------------------------------------- |
| Return the exact same [`SchemaRef`] on every call (cloning an `Arc<Schema>` is cheap). | Never change field order, types, or nullability between scans.       |
| Define the schema once when the provider is created and store it.                      | Derive the schema dynamically from the underlying data on each call. |

Example (standalone): capture one `SchemaRef` at construction and return clones on every call. In a real [`TableProvider`], [`.schema()`] would delegate to the stored `SchemaRef`.

```rust,ignore
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

// Minimal, self-contained example showing a stable schema contract
struct MySource {
    schema: SchemaRef,
}

impl MySource {
    fn new() -> Self {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false
            ),
        ]));
        Self { schema }
    }

    // Stable across calls: always clone the stored SchemaRef
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn main() {
    let src = MySource::new();
    let s1 = src.schema();
    let s2 = src.schema();
    assert_eq!(s1.as_ref(), s2.as_ref()); // same logical schema every time
}
```

<!-- TODO: MOVE to "### Reference" subsection within consolidated "## Schema Evolution & Versioning" -->

### Type Control with Macros and Literals

The [`dataframe!`] macro infers types from Rust literals—integers default to `Int32`, not `Int64`—which breaks [`.union()`] and [`.join()`] when types don't match exactly. (See also: [DataFrame macro](./creating-dataframes.md#5-from-inline-data-using-the-dataframe-macro)) Either cast after creation or use typed arrays from the start:

```rust,ignore
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::array::Int64Array;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Problem: inferred as Int32
    let df = dataframe!("id" => [1, 2])?;

    // Solution 1: Cast to match schema
    let df = df.clone().with_column("id",
        col("id").cast_to(&DataType::Int64, df.schema())?
    )?;

    // Solution 2: Use typed arrays
    let _df = dataframe!(
        "id" => Int64Array::from(vec![1_i64, 2_i64])
    )?;

    df.show().await?;
    Ok(())
}
```

---

## Schema Evolution Patterns

**Schema evolution is the ongoing change of field names, types, and presence as data and systems grow.**
In DataFusion, some changes are absorbed automatically (name‑aligned unions, file‑level schema merging) while others require explicit normalization. This section shows what typically changes, how DataFusion handles it, and why you should still normalize to a canonical schema to keep pipelines stable and predictable.

### Common Evolution Scenarios

| Change Type                 | Risk Level |             Example             | Migration Strategy                 |
| :-------------------------- | :--------: | :-----------------------------: | :--------------------------------- |
| **Add nullable column**     |    Low     |  New `customer_segment` field   | Automatic via [`.union_by_name()`] |
| **Add non-nullable column** |   Medium   | Required `created_at` timestamp | Backfill or schema adapter         |
| **Widen type**              |    Low     |        `Int32` → `Int64`        | Automatic cast in readers          |
| **Narrow type**             |    High    |        `Int64` → `Int32`        | Validate then explicit cast        |
| **Rename column**           |    High    |    `custId` → `customer_id`     | Adapter layer with aliases         |
| **Remove column**           |   Medium   |      Drop deprecated field      | [`.select()`] to exclude           |
| **Change semantics**        |    High    |       `amount` USD → EUR        | Migration script required          |

Guidance:

- **Iterative ingestion**: align types with [`.cast_to()`] and merge shape with [`.union_by_name()`].
- **Self‑describing formats**: rely on automatic merge, then normalize via [`.select()`] and `.cast_to()`.
- **Renames/semantic shifts**: add an adapter layer until upstream and downstream agree.

Rules of thumb:

- **Prefer additive and widening changes**; add new fields as nullable.
- **Avoid in‑place renames**; publish aliases during transition.
- **Keep a canonical schema** and validate against it (see [Schema Reuse and Versioning](#schema-reuse-and-versioning), **Schema Validation (Structure & Types)**).

See also: [Schema Management](./schema-management.md), [Automatic Schema Merging](#automatic-schema-merging), [Performance Considerations](#performance-considerations).

<!-- TODO: MERGE with Pattern 3 (Migration Testing) into "### Evolution Best Practices" -->

### Pattern 1: Forward-Compatible Schema Design

Design schemas that can evolve without breaking existing readers or writers. By adding new fields as nullable and widening types (e.g., `Int32 → Int64` ), you preserve backward compatibility—old data remains valid and old queries continue to work, while new code can leverage the enhanced schema. This approach keeps pipelines stable as requirements grow, avoiding the cost and risk of rewriting historical data.

```rust,ignore
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

// V1: Initial schema
pub fn orders_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(19, 2), false),
    ]))
}

// V2: Add optional columns (backward compatible)
pub fn orders_schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(19, 2), false),
        Field::new("region", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
    ]))
}

// V3: Widen precision (backward compatible)
pub fn orders_schema_v3() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(38, 9), false),
        Field::new("region", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
    ]))
}

fn main() {
    let v1 = orders_schema_v1();
    let v2 = orders_schema_v2();
    let v3 = orders_schema_v3();
    println!("v1: {} fields, v2: {} fields, v3: {} fields",
        v1.fields().len(), v2.fields().len(), v3.fields().len());
}
```

<!-- TODO: MERGE with Pattern 4 (Breaking Changes) into "### Migration Patterns" -->

### Pattern 2: Schema Adapter Layer

When source schemas diverge—legacy systems use `custId` vs. `customer_id`, or decimal precision drifts from `Decimal128(19,2)` to `Decimal128(38,9)`—a schema adapter normalizes variants before they reach your core logic. By inspecting the incoming schema and applying targeted renames ([`.alias()`]) and type casts ([`.cast_to()`]), you isolate schema churn at the pipeline's edge. Upstream systems evolve at different paces while your queries work against a single, stable contract.

```rust,ignore
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;

/// Adapter that normalizes various legacy schemas to current canonical schema
async fn normalize_orders(df: DataFrame) -> Result<DataFrame> {
    let schema = df.schema();

    // Detect schema version and adapt accordingly
    let normalized = if schema.field_with_name("custId").is_ok() {
        // Legacy schema: rename and cast
        df.select(vec![
            col("orderId").alias("order_id"),
            col("custId").cast_to(&DataType::Int64, schema)?.alias("customer_id"),
            col("amt").cast_to(&DataType::Decimal128(38, 9), schema)?.alias("amount"),
        ])?

    } else if schema.field_with_name("customer_id").is_ok() {
        // Modern schema: just ensure types are correct
        df.select(vec![
            col("order_id"),
            col("customer_id").cast_to(&DataType::Int64, schema)?,
            col("amount").cast_to(&DataType::Decimal128(38, 9), schema)?,
        ])?
    } else {
        return Err(datafusion::error::DataFusionError::Plan(
            "Unrecognized orders schema".to_string()
        ));
    };

    Ok(normalized)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Modern schema example
    let df = dataframe!(
        "order_id" => [1001_i64],
        "customer_id" => [42_i64],
        "amount" => [9.99]
    )?;

    let normalized = normalize_orders(df).await?;
    normalized.show().await?;
    Ok(())
}
```

<!-- TODO: MERGE with Pattern 1 (Forward-Compatible) into "### Evolution Best Practices" -->

### Pattern 3: Schema Migration Testing

Seemingly harmless schema edits—dropping a field, narrowing a type, tightening nullability—can break pipelines or corrupt data. Guard against this with backward‑compatibility tests. For each new version, verify:

1. **Consistancy** every v1 field still exists in v2
2. **Types** are identical or widened (e.g., `Int32 → Int64`, `Decimal(19,2) → Decimal(38,9)`)
3. **Bullability** does not tighten (nullable → required is forbidden; required → nullable is safe).

These checks encode additive/widening evolution and catch regressions early (see also [avro-evolution], [kleppmann]).

```rust,ignore
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

fn orders_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
    ]))
}

fn orders_schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),  // New nullable field
    ]))
}

fn is_widening(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    matches!(
        (from, to),
        (Int8, Int16 | Int32 | Int64) |
        (Int16, Int32 | Int64) |
        (Int32, Int64) |
        (Float32, Float64)
    )
}

fn main() {
    let v1 = orders_schema_v1();
    let v2 = orders_schema_v2();

    // Verify backward compatibility: all v1 fields exist in v2
    for v1_field in v1.fields() {
        let v2_field = v2.field_with_name(v1_field.name())
            .expect(&format!("Field '{}' missing in v2", v1_field.name()));

        assert!(
            v2_field.data_type() == v1_field.data_type() ||
            is_widening(v1_field.data_type(), v2_field.data_type()),
            "Type changed for '{}'", v1_field.name()
        );
    }
    println!("Schema v2 is backward compatible with v1");
}
```

<!-- TODO: MERGE with Pattern 2 (Adapter Layer) into "### Migration Patterns" -->

### Pattern 4: Handling Breaking Changes

**Use a multi-phase migration to safely roll out incompatible schema changes.**

Some changes are inherently breaking—renaming core fields, dropping columns, or narrowing types. A “big bang” cutover is risky and hard to roll back. A staged migration protects downstream consumers with a no‑downtime path, clear observability, and a deterministic rollback plan (see also [avro-evolution], [kleppmann]).

**Phase 1: Dual Writing**

```rust,ignore
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::dataframe::DataFrameWriteOptions;

// Write data in both old and new formats during the transition
async fn write_dual_format(df: DataFrame) -> Result<()> {
    // Write v1 format for old consumers (minimal, stable contract)
    let v1_df = df.clone().select(vec![
        col("order_id"),
        col("customer_id"),
        col("amount"),
    ])?;
    // v1_df.write_parquet("data/v1/orders", DataFrameWriteOptions::default()).await?;

    // Write v2 format for new consumers
    // df.write_parquet("data/v2/orders", DataFrameWriteOptions::default()).await?;

    // Demonstrate the pattern compiles
    let _ = v1_df;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "order_id" => [1_i64],
        "customer_id" => [42_i64],
        "amount" => [9.99],
        "region" => ["WEST"]
    )?;
    write_dual_format(df).await?;
    Ok(())
}
```

**Phase 2: Migration and Validation**

- Deploy new code that reads from v2.
- Keep v1 available as a fallback.
- Monitor v1/v2 read volumes, error rates, and data parity.

**Phase 3: Cleanup**

- Remove v1 read paths and decommission dual‑writing.
- Archive or delete v1 data.

### Execution Strategy & Common Pitfalls

| ✅ Best Practices                                                   | ❌ Common Pitfalls                                                                                                 |
| :------------------------------------------------------------------ | :----------------------------------------------------------------------------------------------------------------- |
| Use feature flags to canary the new schema, then broaden rollout.   | “Big bang” cutovers without a dual‑writing phase and a tested rollback plan.                                       |
| Backfill v2 so consumers see a consistent historical view.          | Relying on silent positional unions in SQL; prefer name‑aligned [`.union_by_name()`] with explicit casts/defaults. |
| Define success metrics (error rates, data parity) before you begin. | Failing to coordinate timelines and impact with downstream teams.                                                  |

<!--
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
X
X    END THIGHTENING!
X
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
-->

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

### Schema Recovery Patterns

Production pipelines encounter schema surprises. Because plans are lazily executed, schema errors typically surface only when you call `.collect()`, `.show()`, or `.write_*()`.

**Typical failure scenarios:**

- **Missing or extra columns**: A publisher changes their format
- **Type mismatches**: An `Int32` column becomes `Int64`
- **Inconsistent partitions**: Schema drift across files

#### Robust Read with Schema Normalization

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

/// Reshape any DataFrame to match a target schema
fn normalize_to_schema(df: DataFrame, target: &Schema) -> Result<DataFrame> {
    let actual_schema = df.schema();
    let mut select_exprs = Vec::new();

    for target_field in target.fields() {
        match actual_schema.field_with_name(target_field.name()) {
            Ok(actual_field) => {
                if actual_field.data_type() == target_field.data_type() {
                    select_exprs.push(col(target_field.name()));
                } else {
                    select_exprs.push(
                        col(target_field.name())
                            .cast_to(target_field.data_type(), actual_schema)?
                            .alias(target_field.name())
                    );
                }
            },
            Err(_) => {
                if target_field.is_nullable() {
                    select_exprs.push(lit(ScalarValue::Null).alias(target_field.name()));
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Cannot normalize: missing required field '{}'",
                        target_field.name()
                    )));
                }
            }
        }
    }
    df.select(select_exprs)
}

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!("id" => [1_i32, 2_i32], "name" => ["Alice", "Bob"])?;

    // Target schema expects Int64 id (source has Int32)
    let target = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let normalized = normalize_to_schema(df, &target)?;
    normalized.show().await?;
    Ok(())
}
```

#### Choosing a Recovery Strategy

| Strategy                | Best for                                           | Trade-offs                                 |
| :---------------------- | :------------------------------------------------- | :----------------------------------------- |
| **Fail fast & strict**  | Curated, high-trust datasets                       | Stops the whole job; requires intervention |
| **Tolerant & per-file** | Data lakes, multi-tenant ingestion, external feeds | Requires robust logging for skipped files  |

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
3. Submit a PR — see [CONTRIBUTING.md](https://github.com/apache/datafusion/blob/main/CONTRIBUTING.md)

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

[`datafusionerror`]: https://docs.rs/datafusion/latest/datafusion/error/enum.DataFusionError.html
[`tracing`]: https://docs.rs/tracing/latest/tracing/
[tracing documentation]: https://docs.rs/tracing/latest/tracing/
[`dfschema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`dfschema::logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`dfschema::field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`arrow::compute::can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html

<!-- ============================================================
     MOVE TO best-practices.md: START

     The following 4 sections (~1470 lines) should be transferred to best-practices.md:
     1. Schema and Data Validation
     2. Error Recovery
     3. Performance Considerations
     4. Debugging and Resolving Schema Mismatches

   Legacy from schema Management
     ============================================================ -->

## Schema and Data Validation

**Validation protects your pipeline at two levels: schema validation ensures structure is correct, data validation ensures values are correct.**

In production, data arrives from untrusted sources: third-party APIs change field types without warning, CSV uploads contain malformed values, and even "stable" partners send out-of-range data. DataFusion trusts what you give it—it won't automatically validate schemas or data quality.

This section covers:

- **Schema validation** (metadata checks: field names, types, nullability)
- **Data validation** (value checks: ranges, uniqueness, patterns)
- **When to use external libraries** for complex validation rules

> **Important distinction**: Schema validation checks metadata only (microseconds, no data scan). Data validation scans actual row values (performance depends on data size).

### Schema Validation (Structure & Types)

**Schema validation ensures your data has the right structure—correct field names, compatible types, expected nullability—before you process it.**

DataFusion's [`DFSchema`] provides methods for comparing schemas, looking up fields, and checking type compatibility. Below are three commonly used patterns—for additional methods, see the [`DFSchema` documentation](https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html).

#### Comparing Schemas for Compatibility

Use [`DFSchema::logically_equivalent_names_and_types()`] to verify two schemas are compatible before combining DataFrames. This checks for logical equivalence: same field names, compatible types, and matching qualifiers. Essential for validating unions, joins, or ensuring schema contracts haven't changed between versions.

```rust
use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};

async fn merge_regional_sales(us_sales: DataFrame, eu_sales: DataFrame) -> Result<DataFrame> {
    // Before combining, verify schemas match
    let us_schema = us_sales.schema();
    let eu_schema = eu_sales.schema();

    if !us_schema.logically_equivalent_names_and_types(&eu_schema) {
        return Err(DataFusionError::Plan(format!(
            "Regional schemas incompatible. US: {:?}, EU: {:?}",
            us_schema.field_names(),
            eu_schema.field_names()
        )));
    }

    // Schemas match; safe to union
    let combined = us_sales.union(eu_sales)?;
    Ok(combined)
}

#[tokio::main]
async fn main() -> Result<()> {
    let us = dataframe!("id" => [1], "amount" => [100])?;
    let eu = dataframe!("id" => [2], "amount" => [200])?;
    let combined = merge_regional_sales(us, eu).await?;
    combined.show().await?;
    Ok(())
}
```

#### Validating Type Cast Safety

Use [`arrow::compute::can_cast_types()`] to check if one type can be safely cast to another without data loss. This follows Arrow's type coercion rules: widening conversions (`Int32`→ `Int64`, `Decimal(19,2)` → `Decimal(38,9)`) are safe; narrowing conversions risk truncation and require explicit validation. Critical for schema normalization and preventing silent data corruption.

```rust
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::DataType;

fn main() {
    let from_type = DataType::Int32;
    let to_type = DataType::Int64;

    if can_cast_types(&from_type, &to_type) {
        println!("Safe: Int32 fits in Int64 without truncation");
    }

    // Examples:
    // ✅ Int8 → Int64          (widening, safe)
    assert!(can_cast_types(&DataType::Int8, &DataType::Int64));

    // ✅ Float32 → Float64     (widening, safe)
    assert!(can_cast_types(&DataType::Float32, &DataType::Float64));

    // ❌ Utf8 → Int64          (incompatible without parsing)
    // Requires explicit parsing, not simple casting
}
```

#### Looking Up Required Fields

Use [`DFSchema::field_with_name()`] to look up a field by name and retrieve its metadata (type, nullability). Essential for validating that required fields exist before processing and for checking field types match expectations. Returns an error if the field is missing, enabling fail-fast validation.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!("customer_id" => [1_i64, 2_i64])?;

    // Fail fast if field missing or wrong type
    let field = df.schema()
        .field_with_name("customer_id")
        .map_err(|_| DataFusionError::Plan("Missing required field: 'customer_id'".into()))?;

    if field.data_type() != &DataType::Int64 {
        return Err(DataFusionError::Plan(format!(
            "Field 'customer_id' must be Int64, got {:?}",
            field.data_type()
        )));
    }

    println!("Validation passed: customer_id is Int64");
    Ok(())
}
```

> **Performance**: These checks operate on schema metadata (typically <1KB), completing in microseconds regardless of DataFrame size.

### Production Pattern: Validate-Then-Normalize

The most robust production pattern validates critical fields, then normalizes compatible types. This catches breaking changes (missing fields, incompatible types) while tolerating minor drift ( `Int32` where you expect `Int64`, slightly different decimal precision).

#### Flexible Schema Enforcement (pattern)

Pattern, not an API: validate that required fields exist, and where types differ but are castable, align them with [`.cast_to()`] before union/join; otherwise fail fast. This keeps schema normalization explicit and cheap (metadata checks up front, casts executed at runtime).

Example (inline): validate presence and normalize a single field before union/join.

```rust
use datafusion::prelude::*;
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};

async fn pipeline_step(mut df: DataFrame) -> Result<DataFrame> {
    // 1) Validate presence
    let field = df.schema()
        .field_with_name("order_id")
        .map_err(|_| DataFusionError::Plan("Missing required field: 'order_id'".into()))?;

    // 2) If types differ, check cast safety and normalize
    if field.data_type() != &DataType::Int64 {
        if can_cast_types(field.data_type(), &DataType::Int64) {
            df = df.with_column(
                "order_id",
                col("order_id").cast_to(&DataType::Int64, df.schema())?
            )?;
        } else {
            return Err(DataFusionError::Plan(format!(
                "Cannot cast 'order_id' from {:?} to Int64",
                field.data_type()
            )));
        }
    }

    Ok(df)
}

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!("order_id" => [1_i32, 2_i32])?;
    let normalized = pipeline_step(df).await?;
    normalized.show().await?;
    Ok(())
}
```

When to use:

- Partner integrations with minor drift (e.g., `Int32` vs `Int64`)
- Multi-tenant inputs that vary slightly
- Data lakes with independently evolving producers

> Note:<br>
> A full “helper” implementation belongs in recipes/advanced docs. See the [advanced section](dataframes-advance.md) for a complete version.

### Strict Validation for Compliance Workloads

For regulatory/financial data, every field must match exactly—same name, type, nullability, and order. Any deviation signals data corruption or upstream contract violation.

#### Strict Schema Equality (fail-fast pattern)

This pattern performs a strict equality check: field count, field order, field names, data types, and nullability must all match exactly. It uses Arrow's `Field::PartialEq` implementation for precise comparison and fails fast on the first mismatch.
An example of this strategy is shown in the following:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

fn expected_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!("id" => [1_i64], "name" => ["Alice"])?;

    // Strict equality check (order-sensitive)
    let actual = df.schema().as_arrow();
    let expected = expected_schema();

    // 1) Verify field count
    assert_eq!(
        actual.fields().len(),
        expected.fields().len(),
        "Field count mismatch"
    );

    // 2) Validate each field exactly (name, type, nullability)
    for (i, expected_field) in expected.fields().iter().enumerate() {
        let actual_field = &actual.fields()[i];
        assert!(
            actual_field == expected_field,
            "Field[{}] mismatch", i
        );
    }
    println!("Schema validation passed");
    Ok(())
}
```

**What it does:**

- Checks field count matches exactly
- Iterates fields in order, comparing each with `==` (uses `Field::PartialEq`)
- Returns error immediately on first mismatch
- Validates: name, type, nullability, metadata

**When to use:**

- Regulatory compliance (SOX, GDPR, HIPAA) where schema drift indicates audit failure
- Financial transactions where field order/type changes could cause accounting errors
- API contracts with strict SLAs where schema changes require versioned rollout
- Critical ML inference pipelines where type mismatches corrupt predictions

> **Performance note**: Schema validation checks metadata (typically <1KB), not data. Completes in microseconds even for billion-row DataFrames.

### Data Validation (Actual Values)

**After schema validation confirms structure, data validation ensures values meet business rules—no negative amounts, valid emails, unique IDs.**

Unlike schema checks (which operate on metadata in microseconds), data validation scans actual row values—performance depends on data size.

**Common validation patterns:**

| Pattern             | Technique                          | Use Case                     |
| ------------------- | ---------------------------------- | ---------------------------- |
| **Filter-based**    | `.filter(col("price").gt(lit(0)))` | Reject invalid rows          |
| **Flag-based**      | `.with_column("is_valid", ...)`    | Mark issues, keep all rows   |
| **Aggregate-based** | `.aggregate(...)` with `case/when` | Quality reports, CI/CD gates |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "price" => [100, -5, 50],
        "quantity" => [Some(10), None, Some(5)]
    )?;

    // Reject rows with invalid prices
    let clean_data = df
        .filter(col("price").gt(lit(0)))?
        .filter(col("quantity").is_not_null())?;

    clean_data.show().await?;
    Ok(())
}
```

> **See also:** <br>
> For complete patterns with error handling, thresholds, and CI/CD integration, see [Transformations](transformations.md).

#### Validating Value Constraints

Value constraints enforce the domain rules that a schema alone cannot express: positive monetary amounts, realistic percentages, mandatory identifiers, and so on. Use them to short-circuit bad data before it propagates into joins, aggregates, or downstream systems.

**Three-step workflow**

1. **Express violations:** as a boolean expression with [`col()`], comparisons, and logical connectors.
2. **Use [`.filter()`]:** to isolate the violating rows so you can inspect or count them cheaply.
3. **Decide how to respond:** fail fast, write the rows to quarantine, or continue with the negated filter (for example `violation_filter.clone().not()` shown below).

Typical predicates:

| Purpose              |                        Example expression                         |
| -------------------- | :---------------------------------------------------------------: |
| Range checks         |       `col("amount").between(lit(0.01), lit(1_000_000.0))`        |
| Completeness checks  |               `col("required_field").is_not_null()`               |
| Format checks        |                 `col("email").like(lit("%@%.%"))`                 |
| Composite conditions | chain `.and()`, `.or()`, `.not()` for multi-column business rules |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "order_id" => [Some(1), Some(2), None],
        "amount" => [100.0, -5.0, 50.0],
        "customer_email" => [Some("a@b.com"), Some("invalid"), None]
    )?;

    // Build a filter for all violations
    let violation_filter =
        col("amount").lt_eq(lit(0.0))                        // Negative or zero amounts
            .or(col("amount").gt(lit(1_000_000.0)))          // Suspiciously large amounts
            .or(col("order_id").is_null())                   // Missing order ID
            .or(col("customer_email").is_null())             // Missing email
            .or(col("customer_email").not_like(lit("%@%"))); // Invalid email format

    // Materialize violations
    let violations = df.clone().filter(violation_filter.clone())?;
    let violation_count = violations.clone().count().await?;

    println!("{violation_count} invalid orders detected");
    violations.show().await?;

    // Option: get clean data
    let clean = df.filter(violation_filter.not())?;
    println!("Clean records:");
    clean.show().await?;

    Ok(())
}
```

**Run these checks** at ingestion boundaries, after transformations that change business meaning (currency conversion, enrichment joins), and right before high-impact sinks such as financial exports or ML feature generation.

#### Checking for Duplicates

Duplicate detection protects unique keys (orders, invoices, device IDs) whose duplication leads to double-counted revenue, ambiguous joins, or inflated metrics. The DataFrame API gives you a compact pattern for detecting these issues and then deciding whether to fail, remediate automatically, or route them for human review.

**Workflow:**

1. **Group by:** the field that should be unique and compute a [`count()`] aggregate per key.
2. **Filter for `count > 1`:** to materialize only the duplicates.
3. **Inspect the keys and/or join:** back to the original DataFrame to fetch full offending records.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::count;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "order_id" => [1, 1, 2, 3, 3, 3],
        "amount" => [100, 100, 200, 300, 300, 300]
    )?;

    // 1. Aggregate by the key that should be unique and count occurrences
    let duplicates = df.clone()
        .aggregate(
            vec![col("order_id")],
            vec![count(col("order_id")).alias("cnt")]
        )?
        .filter(col("cnt").gt(lit(1)))?;

    // 2. Check if any duplicates exist at all
    let has_duplicates = duplicates.clone().count().await? > 0;

    if has_duplicates {
        println!("Duplicate order IDs found:");
        duplicates.clone()
            .sort(vec![col("cnt").sort(false, true)])?
            .show().await?;
    }

    Ok(())
}
```

For multi-terabyte tables, restrict the scan to recent partitions or call the DataFrame method [`.distinct()`] (for example on a projected key column) to shrink the working set. Feed the duplicate list into remediation automation (fix-and-retry jobs, support tickets) rather than leaving the insight in console logs.

#### Range and Statistical Validation

While row-level checks catch individual bad values, statistical validation reveals the hidden problems that kill production systems: a pricing algorithm stuck at zero, sensor readings that gradually drift out of calibration, or that batch job that silently stopped processing half your data last Tuesday. These issues slip through traditional validation because each record looks fine in isolation—it's only when you aggregate that the pattern emerges.

Statistical validation compares your data's shape against known good baselines. When metrics deviate beyond expected ranges, you catch problems before they compound into disasters.

**Key metrics to monitor:**

- **Bounds:** [`min()`] / [`max()`] ensure values stay within physical or business limits
- **Central tendency:** [`avg()`] or [`median()`] detect systematic shifts in your data
- **Spread:** [`stddev()`] catches when your normally stable metrics start going haywire
- **Completeness:** [`count()`] or [`count_distinct()`][`count()`] verifies you're receiving expected volumes

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::{min, max, avg, count};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "transaction_id" => [1, 2, 3, 4, 5],
        "amount" => [100.0, 200.0, 150.0, 300.0, 250.0]
    )?;

    // Compute key statistics in a single pass
    let stats = df.aggregate(
        vec![],
        vec![
            count(col("transaction_id")).alias("total"),
            min(col("amount")).alias("min_amount"),
            max(col("amount")).alias("max_amount"),
            avg(col("amount")).alias("avg_amount"),
        ]
    )?;

    stats.show().await?;
    // +-------+------------+------------+------------+
    // | total | min_amount | max_amount | avg_amount |
    // +-------+------------+------------+------------+
    // | 5     | 100.0      | 300.0      | 200.0      |
    // +-------+------------+------------+------------+

    // In production, compare these statistics against thresholds
    // to detect anomalies (volume drops, out-of-range values, etc.)

    Ok(())
}
```

**Pro tip:** Instead of hardcoding thresholds, compute rolling baselines from recent history:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::avg;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Example: calculate dynamic thresholds from historical data
    let historical = dataframe!(
        "day" => [1, 2, 3, 4, 5, 6, 7],
        "daily_avg" => [100.0, 105.0, 98.0, 102.0, 110.0, 95.0, 108.0]
    )?;

    let baseline = historical
        .aggregate(vec![], vec![
            avg(col("daily_avg")).alias("expected_avg"),
        ])?;

    baseline.show().await?;
    // Today's average should be within reasonable range of the weekly pattern
    Ok(())
}
```

This approach adapts to natural patterns (weekend dips, month-end spikes) while still catching true anomalies.

### When to Use External Validation Libraries

DataFusion excels at data-level validation—checking ranges, detecting duplicates, computing statistics. But sometimes you need specialized validation that goes beyond what SQL expressions can handle. External validators complement DataFusion by handling complex formats, business rules, and compliance requirements that would be painful to implement in query logic.

**Use external validators when you encounter:**

1. **Complex format validation** that requires parsing algorithms:
   - Email addresses, phone numbers, URLs
   - Financial identifiers (IBAN, BIC, credit card numbers)
   - Geographic data (postal codes, coordinates)
   - Industry standards (ISBN, VIN, MAC addresses)

2. **Business rule engines** that maintain state across records:
   - Order workflow state machines
   - Approval chain validation
   - Complex discount eligibility rules
   - Cross-entity referential integrity

3. **Compliance and schema standards:**
   - JSON Schema, Protobuf, or Avro schema validation
   - Industry standards (HL7, EDI, OpenAPI)
   - Regulatory formats (tax forms, medical records)

**Integration pattern:** Validate during ingestion, process with DataFusion:

```rust
use validator::Validate;
use datafusion::prelude::*;

// 1. Define validation rules using external validator
#[derive(Validate)]
struct CustomerRecord {
    #[validate(email)]
    email: String,
    #[validate(phone)]
    phone: String,
    #[validate(credit_card)]
    payment_method: String,
    #[validate(range(min = 18, max = 150))]
    age: u8,
}

// 2. Pre-validate during data ingestion
async fn ingest_with_validation(raw_data: Vec<RawRecord>) -> Result<DataFrame> {
    let mut valid_records = Vec::new();
    let mut validation_errors = Vec::new();

    for record in raw_data {
        let customer = CustomerRecord::from(record);
        match customer.validate() {
            Ok(_) => valid_records.push(customer),
            Err(e) => validation_errors.push((record.id, e.to_string())),
        }
    }

    // Log validation errors for monitoring/remediation
    if !validation_errors.is_empty() {
        println!("Rejected {} records with validation errors", validation_errors.len());
        // Write to dead letter queue, alert ops team, etc.
    }

    // 3. Convert valid records to DataFrame for bulk processing
    let ctx = SessionContext::new();
    ctx.read_json(
        valid_records.to_json_bytes(),
        NdJsonReadOptions::default()
    ).await
}

// 4. Now use DataFusion for large-scale analytics on clean data
let clean_df = ingest_with_validation(raw_records).await?;
let insights = clean_df
    .filter(col("age").gt(lit(25)))?
    .aggregate(
        vec![col("payment_method")],
        vec![avg(col("purchase_amount"))]
    )?;
```

**Key insight:** External validators act as quality gates at ingestion boundaries. They reject malformed records before they enter your analytical pipeline, preventing garbage data from polluting aggregations and causing mysterious failures hours later in production reports.

### Building Your Validation Strategy

Data validation isn't about catching every possible error—it's about catching the errors that matter before they cause damage. Build your validation in layers, starting with cheap checks and progressively adding sophistication where the risk justifies the complexity:

**Layer 1: Schema validation** (milliseconds, catches 30% of issues)

- Verify column presence, types, and nullability
- Runs on metadata alone—no data scanning required
- Catches structural breaks immediately after schema changes

**Layer 2: Value validation** (seconds, catches 50% of issues)

- Apply [`.filter()`] expressions for business rules
- Check formats, ranges, and relationships
- Identifies bad records while processing the good ones

**Layer 3: Statistical validation** (seconds to minutes, catches 15% of issues)

- Compute aggregates and compare to baselines
- Detect systemic problems invisible at the row level
- Critical for catching gradual degradation

**Layer 4: External validators** (variable, catches the remaining 5%)

- Complex domain formats and compliance rules
- Only where DataFrame operations fall short
- Apply at ingestion boundaries, not in batch processing

**The 80/20 rule of validation:** Most production issues come from simple problems—missing columns, null values where they shouldn't be, numbers outside reasonable ranges. Start there. Add complexity only after you've proven you need it through actual failures in production.

See also:

- [Schema Evolution Patterns](#schema-evolution-patterns)
- [Error Recovery](#error-recovery)

---

## Error Recovery

Production pipelines eventually encounter schema surprises. While the SQL and DataFrame APIs build the same logical plan inside [`SessionContext`], the DataFrame API makes it easier to express reusable recovery logic, add observability, and integrate with Rust's control flow. Because plans are lazily executed, schema errors typically surface only when you call [`.collect()`], [`.show()`], or `.write_*()`. Early detection and recovery logic keep long-running jobs from failing late in the process. In Rust, these problems usually surface as [`DataFusionError::SchemaError`] or [`DataFusionError::Plan`], both variants of the central [`DataFusionError`] type.

**Typical failure scenarios:**

- **Missing or extra columns**: A publisher changes their format or optional fields appear.
- **Type mismatches**: An `Int32` column becomes `Int64` or `Decimal`, or a nullable field becomes required.
- **Inconsistent partitions**: Schema drift occurs across different files or days within the same dataset.
- **Unreadable data**: Corrupt files or truncated row groups prevent deserialization.

### Choosing a Recovery Strategy

Select a strategy based on the business impact of incomplete data. Start strict to ensure correctness, then add tolerant pathways where partial data is acceptable.

| Strategy                | Best for                                                | Pros                                                       | Watch outs                                                        |
| :---------------------- | :------------------------------------------------------ | :--------------------------------------------------------- | :---------------------------------------------------------------- |
| **Fail fast & strict**  | Curated, high-trust datasets or OLTP-style workloads    | Catches issues before expensive work; easy to reason about | Stops the whole job; requires intervention to fix data            |
| **Tolerant & per-file** | Data lakes, multi-tenant ingestion, external data feeds | Keeps healthy data flowing; allows incremental cleanup     | Requires robust logging to ensure skipped/bad files are addressed |

### Robust Read with Schema Normalization

When files drift slightly from the contract—for example, a column type changes or a new field appears—a strict read will fail. This pattern shows how to build resilience into your reads by attempting the happy path first, then falling back to normalization when needed.

**The problem:** You have a known schema contract, but upstream files occasionally drift (new columns appear, types change from Int32 to Int64, etc.). You want your pipeline to keep working rather than failing immediately.

**The solution:** Create a wrapper function that tries to read with your expected schema first. If that fails, read with inference and normalize the result to match your contract.

Below is a complete example showing two helper functions you can copy into your own crate:

1. `robust_read_with_schema_recovery` - the main wrapper that handles the try/fallback logic
2. `normalize_to_schema` - reshapes any DataFrame to match a target schema

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

// Example helper: wraps read_parquet() with schema recovery logic.
async fn robust_read_with_schema_recovery(
    ctx: &SessionContext,
    path: &str,
    expected_schema: Arc<Schema>
) -> Result<DataFrame> {
    // Attempt 1: read with the contract schema — ideal for strict pipelines
    match ctx.read_parquet(path, ParquetReadOptions::default().schema(&expected_schema)).await {
        Ok(df) => Ok(df),
        Err(e) => {
            eprintln!("[WARN] Failed to read with expected schema: {e}");
            // Attempt 2: let DataFusion infer, then normalize column-by-column
            let df = ctx.read_parquet(path, ParquetReadOptions::default()).await?;
            normalize_to_schema(df, &expected_schema)
        }
    }
}

// Helper to reshape any DataFrame to match a target schema
fn normalize_to_schema(df: DataFrame, target: &Schema) -> Result<DataFrame> {
    let actual_schema = df.schema();
    let mut select_exprs = Vec::new();

    for target_field in target.fields() {
        match actual_schema.field_with_name(target_field.name()) {
            Ok(actual_field) => {
                if actual_field.data_type() == target_field.data_type() {
                    select_exprs.push(col(target_field.name()));
                } else {
                    select_exprs.push(
                        col(target_field.name())
                            .cast_to(target_field.data_type(), actual_schema)?
                            .alias(target_field.name())
                    );
                }
            },
            Err(_) => {
                if target_field.is_nullable() {
                    select_exprs.push(lit(ScalarValue::Null).alias(target_field.name()));
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Cannot normalize: missing required field '{}'",
                        target_field.name()
                    )));
                }
            }
        }
    }
    df.select(select_exprs)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Demonstrate the normalize_to_schema helper
    let df = dataframe!("id" => [1_i32, 2_i32], "name" => ["Alice", "Bob"])?;

    // Target schema expects Int64 id (source has Int32)
    let target = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let normalized = normalize_to_schema(df, &target)?;
    normalized.show().await?;

    // In practice, use robust_read_with_schema_recovery for file reads:
    // let df = robust_read_with_schema_recovery(&ctx, "data.parquet", expected_schema).await?;

    Ok(())
}
```

**How to use these helpers in your code:**

```rust
// Instead of this (which fails on schema drift):
let df = ctx.read_parquet("data/orders.parquet", ParquetReadOptions::default()).await?;

// Use this (which recovers from minor schema changes):
let expected_schema = Arc::new(Schema::new(vec![
    Field::new("order_id", DataType::Int64, false),
    Field::new("amount", DataType::Decimal128(19, 2), false),
]));
let df = robust_read_with_schema_recovery(&ctx, "data/orders.parquet", expected_schema).await?;
```

**When to use:** Use this pattern when you own the "golden" schema and want to be resilient to minor upstream drift without modifying the core pipeline logic. The helpers are built from DataFusion's public APIs: [`SessionContext`], [`.read_parquet()`], [`.schema()`], [`.select()`], [`.cast_to()`], and [`DataFusionError::Plan`].

### Fail-Fast Schema Diagnostics

For strict pipelines, you often want to abort before starting a long compute phase if the data violates the contract. Instead of waiting for a runtime error deep in an aggregation, this function compares the actual Arrow schema against expectations and returns a descriptive [`DataFusionError::Plan`] listing all differences.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::error::Result;
use std::sync::Arc;

/// Validate schemas before building expensive logical plans
fn compare_schemas(actual: &Schema, expected: &Schema) -> Result<()> {
    // Collect human-readable differences between the two schemas
    let mut errors = Vec::new();

    // First pass: ensure every expected field exists with compatible type and nullability
    for expected_field in expected.fields() {
        match actual.field_with_name(expected_field.name()) {
            Ok(actual_field) => {
                // Same name, but type changed
                if actual_field.data_type() != expected_field.data_type() {
                    errors.push(format!(
                        "  • '{}': type mismatch - expected {:?}, got {:?}",
                        expected_field.name(),
                        expected_field.data_type(),
                        actual_field.data_type()
                    ));
                }
                // Same name and type, but nullability changed
                if actual_field.is_nullable() != expected_field.is_nullable() {
                    errors.push(format!(
                        "  • '{}': nullability mismatch - expected nullable={}, got nullable={}",
                        expected_field.name(),
                        expected_field.is_nullable(),
                        actual_field.is_nullable()
                    ));
                }
            },
            // Field missing entirely from the actual schema
            Err(_) => errors.push(format!(
                "  • '{}': MISSING in actual schema",
                expected_field.name()
            )),
        }
    }

    // Second pass: detect any extra fields that are not part of the expected contract
    for actual_field in actual.fields() {
        if expected.field_with_name(actual_field.name()).is_err() {
            errors.push(format!(
                "  • '{}': UNEXPECTED field in actual schema",
                actual_field.name()
            ));
        }
    }

    // Either succeed silently or return a single Plan error summarizing all differences
    if errors.is_empty() {
        Ok(())
    } else {
        Err(datafusion::error::DataFusionError::Plan(format!(
            "Schema validation failed with {} error(s):\n{}",
            errors.len(),
            errors.join("\n")
        )))
    }
}
```

Call the here defined example function `compare_schemas` on the first `RecordBatch` or during table registration to fail immediately with a readable diff.

### Partial Failure Tolerance for Multi-File Reads

In large directories, a single corrupt file or bad partition shouldn't necessarily halt the entire job. This pattern wraps the reader to isolate failures, collecting successful DataFrames while logging errors for later remediation. The helper function `read_files_with_schema_tolerance` shown below builds on `robust_read_with_schema_recovery` to implement this pattern; like the previous helpers, it is intended as a copy‑and‑adapt example rather than a function provided by the DataFusion crate.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

// Example helper that builds on `robust_read_with_schema_recovery` for multi-file directories.
async fn read_files_with_schema_tolerance(
    ctx: &SessionContext,
    paths: Vec<&str>,
    expected_schema: Arc<Schema>
) -> Result<Vec<DataFrame>> {
    // DataFrames that we were able to read and normalize successfully
    let mut successful = Vec::new();
    // (path, error) pairs for files that failed schema validation or recovery
    let mut failed = Vec::new();

    // Attempt to read each file independently using the robust schema recovery helper
    for path in paths {
        match robust_read_with_schema_recovery(ctx, path, expected_schema.clone()).await {
            Ok(df) => {
                println!("[OK] Successfully processed: {path}");
                successful.push(df);
            },
            Err(e) => {
                eprintln!("[ERROR] Failed to process {path}: {e}");
                failed.push((path, e.to_string()));
            }
        }
    }

    // If nothing succeeded, treat this as a hard failure for the whole job
    if successful.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            format!("All {} file(s) failed to process", failed.len())
        ));
    }

    // If some files failed, log a summary but still return the good DataFrames
    if !failed.is_empty() {
        eprintln!(
            "[WARN] Proceeding with {} of {} files ({} failures logged)",
            successful.len(),
            paths.len(),
            failed.len()
        );
    }

    Ok(successful)
}
```

You can then combine the successful DataFrames using [`.union_by_name()`] before continuing with your analysis.

### Safe Type Coercion Guardrails

Blindly casting columns (e.g., `Int32` to `Int64`) can fail at runtime if the types are incompatible. This helper checks whether Arrow permits the cast before attempting it. If the cast is unsafe, you can choose to handle it gracefully—for example, by filling with `NULL` or dropping the column—rather than crashing the query.

```rust
use datafusion::prelude::*;
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};

fn safe_cast_column(
    df: DataFrame,
    column: &str,
    target_type: DataType
) -> Result<DataFrame> {
    // Look up the column's current type in the DataFrame schema
    let schema = df.schema();
    let field = schema.field_with_name(column)?;

    // Fast path: already the right type, nothing to do
    if field.data_type() == &target_type {
        return Ok(df);
    }

    // Preferred path: Arrow says this cast is safe (typically widening casts)
    if can_cast_types(field.data_type(), &target_type) {
        df.with_column(
            column,
            col(column)
                .cast_to(&target_type, schema)?
                .alias(column)
        )
    // Fallback for nullable columns: log and fill with NULLs instead of failing hard
    } else if field.is_nullable() {
        eprintln!(
            "[WARN] Cannot cast {} from {:?} to {:?}, filling NULLs instead",
            column,
            field.data_type(),
            target_type
        );
        df.with_column(column, lit(ScalarValue::Null).alias(column))
    // Non-nullable + unsafe cast: treat as a hard schema violation
    } else {
        Err(DataFusionError::Plan(format!(
            "Cannot coerce non-nullable column '{}' from {:?} to {:?}",
            column,
            field.data_type(),
            target_type
        )))
    }
}
```

### Observability and Logging

Even tolerant pipelines only work if failures are visible. Structured logging gives you a breadcrumb trail without complicating your core logic: record which file, which strategy you used (strict vs tolerant), and why recovery failed so you can revisit bad inputs later.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use tracing::warn;

async fn log_schema_result(
    path: &str,
    strategy: &str,
    result: Result<DataFrame>
) -> Result<DataFrame> {
    // Attach file path and strategy to every schema recovery attempt
    match result {
        // Happy path: just return the DataFrame unchanged
        Ok(df) => Ok(df),
        // Failure path: emit a structured warning, then propagate the error
        Err(e) => {
            warn!(file = path, strategy, error = %e, "schema recovery failed");
            Err(e)
        }
    }
}
```

You can wrap calls to a helper like `robust_read_with_schema_recovery` with `log_schema_result` so every schema issue is tagged with the file path and strategy in your logs. For comprehensive monitoring patterns—including metrics, sampling, and alerting—consult [Advanced DataFrame Topics](./dataframes-advance.md), which expands on these examples.

**See also:**

- [Creating DataFrames](./creating-dataframes.md) — I/O and inference failure patterns.
- [Schema Management](./schema-management.md) — Details on casting and coercion.
- [Advanced DataFrame Topics](./dataframes-advance.md) — Deep dives into productionizing DataFusion.

---

## Performance Considerations

**Schema decisions cascade through your entire pipeline—get them right early to avoid expensive corrections later.**

In DataFusion's columnar execution model, schema operations range from free metadata manipulations to expensive full-table scans. This section reveals the performance implications of schema choices, helping you build pipelines that are both correct and fast. We'll explore why some operations that look similar have vastly different costs, and how to structure your schema management for optimal performance.

### Understanding DataFusion's Execution Model

DataFusion separates **logical planning** (what to do) from **physical execution** (how to do it). Schema operations primarily happen during logical planning, which means:

- **Planning is cheap**: Building transformation plans (metadata operations) costs microseconds
- **Execution is expensive**: Actually transforming data (type casts, scans) costs proportional to data size
- **Lazy evaluation wins**: Operations accumulate until [`.collect()`], [`.show()`], or `.write_*()` triggers execution

This distinction explains why chaining multiple `.select()` calls is essentially free, but executing them processes every row. For more see {[DataFrame Concepts](./concepts.md#data-model--schema)}

### The Performance Hierarchy

#### Zero-Cost Operations (Metadata Only)

These operations modify the logical plan without touching data. They complete instantly regardless of dataset size:

```rust
// Column renaming: Updates metadata mapping, no data movement
let renamed = df.select(vec![
    col("customer_id").alias("client_id"),  // Zero cost: just rewrites the plan
    col("amount"),
])?;

// Union planning: Builds a plan to combine DataFrames later
let union_plan = df1.union_by_name(df2)?;  // Free now, executed when you call collect()

// Schema extension: Adds a logical NULL column to the plan
let extended = df.with_column(
    "processing_date",
    lit(ScalarValue::Null)  // No array allocation until execution
)?;

// Column reordering: Just rearranges the projection list
let reordered = df.select(vec![col("email"), col("name"), col("id")])?;
```

**Why they're free**: These operations only modify the query plan's metadata. The actual column data remains untouched until execution. You can chain hundreds of these operations with negligible overhead.

#### Low-Cost Operations (Optimized Data Access)

These operations read data but leverage DataFusion's columnar optimizations to minimize work:

```rust
// Predicate pushdown: For Parquet/ORC, uses column statistics to skip row groups
let filtered = df.filter(col("year").eq(lit(2024)))?;
// Can eliminate 95%+ of data before it's even loaded into memory

// Projection pushdown: Only deserializes requested columns from storage
let projected = df.select(vec![col("id"), col("name")])?;
// If your Parquet file has 50 columns but you need 2, reads only 4% of the data

// Partition pruning: Skips entire files/directories based on partition columns
let pruned = partitioned_df.filter(col("date").gt(lit("2024-01-01")))?;
// With daily partitions, reads only recent directories, not historical data

// Early filtering in joins: Reduces data before expensive join operations
let optimized_join = large_df
    .filter(col("active").eq(lit(true)))?  // Filter first
    .join(lookup_df, JoinType::Inner, &["id"], &["id"], None)?;
```

**Why they're fast**:

- **Columnar storage**: Operations work on compressed column chunks, not row-by-row
- **Predicate pushdown**: Filters apply at the storage layer, preventing unnecessary I/O
- **Statistics pruning**: Min/max statistics in Parquet eliminate entire row groups without reading them
- **Lazy materialization**: Columns are only decompressed when actually needed

#### High-Cost Operations (Full Data Transformations)

These operations require reading, transforming, and often materializing entire datasets:

```rust
// Type casting: Allocates new arrays and converts every value
let casted = df.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, df.schema())?
)?;
// Cost: O(n) memory allocation + conversion compute for every row

// String transformations: Process every character of every value
let normalized = df.with_column(
    "email",
    lower(trim(col("email")))  // Two passes over all string data
)?;

// Schema-changing aggregations: Must scan entire dataset
let grouped = df.aggregate(
    vec![col("category")],
    vec![count(col("sales")), sum(col("amount"))]
)?;
// Must read all rows to compute aggregates, even with partition pruning

// Deduplication: Requires sorting or hashing all rows
let distinct = df.select(vec![col("customer_id")])?.distinct()?;
// Memory cost: hash table with every unique value

// Wide type conversions: Exponentially expensive for precision changes
let wide_decimal = df.with_column(
    "amount",
    col("amount").cast_to(&DataType::Decimal256(76, 38), df.schema())?
)?;
// Allocates 4x the memory of Decimal128
```

**Why they're expensive**:

- **Memory allocation**: Creating new arrays for transformed data doubles memory usage
- **CPU intensity**: Type conversions, string operations, and decimal arithmetic are compute-heavy
- **No pushdown**: These operations can't be delegated to the storage layer
- **Materialization barriers**: Operations like `distinct()` must see all data before producing any output
- **Cache effects**: Processing entire columns invalidates CPU caches repeatedly

### Schema-Specific Performance Patterns

Understanding how schema operations interact reveals optimization opportunities:

#### Pattern: Type Harmonization During Joins

```rust
// ❌ Expensive: Cast happens during join execution
let result = int32_df.join(int64_df, JoinType::Inner, &["id"], &["id"], None)?;
// Runtime error: Schema mismatch!

// ✅ Better: Align types before the join
let aligned = int32_df.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, int32_df.schema())?
)?;
let result = aligned.join(int64_df, JoinType::Inner, &["id"], &["id"], None)?;
// Cast happens once, join proceeds efficiently
```

#### Pattern: Nullability and Memory Overhead

```rust
// Nullable columns require additional bitmap allocation
let nullable_schema = Schema::new(vec![
    Field::new("id", DataType::Int64, true),      // +12.5% memory for null bitmap
    Field::new("amount", DataType::Float64, true), // Another bitmap
]);

// Non-nullable columns are more memory-efficient
let strict_schema = Schema::new(vec![
    Field::new("id", DataType::Int64, false),      // No null bitmap needed
    Field::new("amount", DataType::Float64, false), // Pure columnar data
]);
```

**Impact**: For a billion-row dataset, nullable columns add ~125MB per column just for null tracking.

#### Pattern: Schema Inference vs. Explicit Schemas

```rust
// ❌ Expensive: Infers schema by scanning data, then reads again
let inferred = ctx.read_csv("large_file.csv", CsvReadOptions::new()).await?;
// Cost: 2 full scans (inference + actual read)

// ✅ Optimal: Single scan with predetermined schema
let explicit = ctx.read_csv(
    "large_file.csv",
    CsvReadOptions::new().schema(&schema)
).await?;
// Cost: 1 scan with direct type parsing
```

**Measurement**: For a 10GB CSV, explicit schemas save 8-10 seconds of inference time.

### Performance Best Practices

These patterns optimize schema operations for production workloads:

#### 1. Front-Load Schema Decisions

**Principle**: Define schemas at data ingestion, not during processing.

```rust
// ✅ OPTIMAL: Schema defined at read time
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("amount", DataType::Decimal128(19, 2), false),
    Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
]));

let df = ctx.read_csv("sales.csv", CsvReadOptions::new().schema(&schema)).await?;
// Single pass: Parse directly into target types

// ❌ WASTEFUL: Infer, then fix schema issues
let df = ctx.read_csv("sales.csv", CsvReadOptions::new()).await?;
let df = df.with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?;
let df = df.with_column("amount", col("amount").cast_to(&DataType::Decimal128(19, 2), df.schema())?)?;
// Three passes: Inference scan + read + two cast operations
```

**Performance impact**: For a 1GB CSV with 10 columns, explicit schemas save:

- Inference scan: ~2 seconds
- Per-column cast: ~0.5 seconds each
- Total savings: 7+ seconds per file

#### 2. Order Operations by Selectivity

**Principle**: Apply selective operations first to reduce data volume for expensive operations.

```rust
// ✅ OPTIMAL: Filter → Project → Cast
let optimized = df
    .filter(col("status").eq(lit("active")))?     // Reduces to 10% of data
    .select(vec![col("id"), col("amount")])?      // Drops unneeded columns
    .with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?;
// Cast operates on 10% of original data

// ❌ WASTEFUL: Cast → Project → Filter
let wasteful = df
    .with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?  // Processes 100%
    .select(vec![col("id"), col("amount")])?
    .filter(col("amount").gt(lit(100)))?;         // Discards 90% of processed data
```

**Rule of thumb**: Order operations by their selectivity ratio:

1. Filters (can eliminate 90%+ of data)
2. Projections (reduce width)
3. Type casts (transform remaining data)
4. Joins/Aggregations (most expensive)

#### 3. Consolidate Schema Transformations

**Principle**: Batch related schema changes into a single operation to minimize data passes.

```rust
// ✅ OPTIMAL: Single-pass transformation
let normalized = df.select(vec![
    col("user_id").cast_to(&DataType::Int64, df.schema())?.alias("id"),
    col("purchase_amount").cast_to(&DataType::Decimal128(19, 2), df.schema())?.alias("amount"),
    lower(trim(col("email"))).alias("email"),  // Multiple string ops in one pass
    when(col("region").is_null(), lit("UNKNOWN"))
        .otherwise(upper(col("region")))?
        .alias("region"),
])?;
// Result: One execution pass handles all transformations

// ❌ WASTEFUL: Sequential transformations
let df = df.with_column("id", col("user_id").cast_to(&DataType::Int64, df.schema())?)?;
let df = df.with_column("amount", col("purchase_amount").cast_to(&DataType::Decimal128(19, 2), df.schema())?)?;
let df = df.with_column("email", lower(trim(col("email"))))?;
let df = df.with_column("region", upper(col("region")))?;
// Result: Four separate execution passes over the data
```

**Performance gain**: Consolidation reduces memory bandwidth usage by 75% and improves cache locality.

#### 4. Design for Partition Pruning

**Principle**: Structure data layout to enable physical file skipping.

```rust
// Schema design that enables efficient pruning
let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    .with_table_partition_cols(vec![
        ("year".into(), DataType::Int16),     // High cardinality partition
        ("month".into(), DataType::Int8),      // Medium cardinality
        ("region".into(), DataType::Utf8),     // Low cardinality
    ]);

// ✅ OPTIMAL: Filter on partition columns first
let efficient = partitioned_table
    .filter(col("year").eq(lit(2024)))?           // Prunes 90% of directories
    .filter(col("month").in_list(vec![lit(1), lit(2), lit(3)], false))?  // Prunes 75% more
    .filter(col("amount").gt(lit(100)))?;         // Operates on 2.5% of data

// ❌ WASTEFUL: Filter on non-partition columns
let inefficient = partitioned_table
    .filter(col("amount").gt(lit(100)))?          // Must open every file
    .filter(col("year").eq(lit(2024)))?;          // Pruning happens too late
```

**Impact**: With 5 years of daily data (1,825 files), partition pruning reduces I/O from 1,825 file opens to just 365.

### Measuring and Profiling Schema Performance

#### Using EXPLAIN Plans

```rust
// Analyze the query plan to identify bottlenecks
let plan = df.explain(true, true)?;  // verbose=true, analyze=true
plan.show().await?;

// Key indicators to examine:
// 1. "ParquetExec: pruned_files=2/100" → Good partition pruning
// 2. "FilterExec" before "ProjectionExec" → Good operation order
// 3. "CAST(id AS Int64)" appearing multiple times → Consolidation needed
// 4. "SortExec" or "CoalescePartitionsExec" → Memory-intensive operations
```

#### Schema Operation Profiling

```rust
use std::time::Instant;

// Profile schema operations in isolation
async fn profile_schema_operation<F>(name: &str, mut op: F) -> Result<DataFrame>
where
    F: FnMut() -> Result<DataFrame>,
{
    let start = Instant::now();
    let df = op()?;

    // Force planning (but not execution)
    let _ = df.logical_plan();
    let planning_time = start.elapsed();

    // Execute and measure
    let exec_start = Instant::now();
    let batches = df.collect().await?;
    let exec_time = exec_start.elapsed();

    println!("{name}:");
    println!("  Planning: {:?}", planning_time);
    println!("  Execution: {:?}", exec_time);
    println!("  Rows: {}", batches.iter().map(|b| b.num_rows()).sum::<usize>());

    ctx.read_batches(batches)
}

// Usage
let df = profile_schema_operation("Type casting", || {
    source_df.with_column("id", col("id").cast_to(&DataType::Int64, source_df.schema())?)
}).await?;
```

### Common Schema Performance Pitfalls

1. **Cascading type mismatches**: One wrong type forces casts throughout the pipeline
2. **Nullable explosion**: Unnecessary nullability adds 12.5% memory overhead per column
3. **Schema inference in loops**: Re-inferring schemas for each file in a directory
4. **Ignored statistics**: Not leveraging Parquet min/max statistics for pruning
5. **Premature materialization**: Calling [`.collect()`] before applying filters

**Remember**: The best schema optimization is avoiding the need for it. Invest time in schema design upfront to minimize runtime corrections.

### Quick Reference: Schema Performance Cheat Sheet

| Operation                              |      Cost       |      When to Use       | Alternative                         |
| :------------------------------------- | :-------------: | :--------------------: | :---------------------------------- |
| **[`.select()`] with aliases**         |      Free       |    Renaming columns    | N/A - always safe                   |
| **[`.union_by_name()`]**               | Free (planning) |  Combining DataFrames  | [`.union()`] if exact schema match  |
| **[`.filter()`] on partition columns** |    Very Low     |  Early data reduction  | N/A - always do this first          |
| **[`.filter()`] on regular columns**   |       Low       |  Selective filtering   | Push to storage if possible         |
| **[`.cast_to()`] on single column**    |     Medium      |     Type alignment     | Define correct type at read time    |
| **Multiple [`.with_column()`] calls**  |      High       |         Never          | Batch into single [`.select()`]     |
| **[`.distinct()`] on full DataFrame**  |    Very High    | Deduplication required | [`.distinct()`] on key columns only |
| **Schema inference**                   |      High       |  Never in production   | Always provide explicit schema      |

### Key Takeaways

1. **Schema operations are not created equal**: Metadata changes are free, data transformations are expensive
2. **Front-load schema decisions**: Fix schemas at read time, not during processing
3. **Operation order matters**: Filter → Project → Transform → Join/Aggregate
4. **Batch transformations**: One pass with multiple operations beats multiple passes
5. **Measure, don't guess**: Use [`.explain()`] to verify optimization assumptions (For more details, see [Reading Explain Plans](../../user-guide/explain-usage.md)
   )

By understanding these performance characteristics, you can build schema management strategies that scale from gigabytes to terabytes while maintaining sub-second query response times.

---

## Debugging and Resolving Schema Mismatches

**Diagnose schema conflicts by classifying them into four types (Count, Order, Name, Type), resolve them using resilient union strategies or explicit casting, and implement robust error handling.**

In a perfect world, schemas never change. In the real world, **schemas drift constantly.** As an example the following shoul be mentioned:

- **Source Drift:** A database migration widens `user_id` from `Int32` to `Int64`.
- **Evolution:** A nightly export gains a new `customer_segment` column.
- **Inconsistency:** One CSV uses `Region` (capitalized) while another uses `region`.

When these shifts happen, DataFusion surfaces schema errors to protect data integrity. This section is your troubleshooting playbook.

### The Four Types of Mismatch

No matter the real-world cause—whether it's a migration, a typo, or a new feature—the conflict always manifests as one of **four technical problems**. Diagnosing which one you have is the first step to fixing it:

1.  **Count Mismatch:** DataFrames have a different number of columns (e.g., a new feature added a field).
2.  **Order Mismatch:** Columns are in a different sequence.
3.  **Name Mismatch:** A column has a different name or capitalization (`Region` vs `region`).
4.  **Type Mismatch:** A column has a different data type (`Int32` vs `Int64`).

The rest of this guide maps these four problems to specific solutions using [`.union_by_name()`] (for shape/name issues) and explicit casting (for type issues).

### Phase 1: Diagnosis

When an error fires like:

```
Schema error: Union schemas have different number of fields: 3 vs 4`
```

The [`.schema()`] method is your primary diagnostic tool. Compare both schemas side-by-side to spot the difference:

```rust
use datafusion::prelude::*;

// Assume df1 and df2 are failing to union.
// Print and compare their schemas side-by-side.
println!("=== Schema 1 ===");
for field in df1.schema().fields() {
    println!("{:20} {:?} nullable={}", field.name(), field.data_type(), field.is_nullable());
}

println!("\n=== Schema 2 ===");
for field in df2.schema().fields() {
    println!("{:20} {:?} nullable={}", field.name(), field.data_type(), field.is_nullable());
}
```

**Example Output:**

```text
=== Schema 1 ===
customer_id          Int32 nullable=false
amount               Float64 nullable=false
region               Utf8  nullable=true

=== Schema 2 ===
customer_id          Int64 nullable=false    ← Type Mismatch (Int32 vs Int64)
amount               Float64 nullable=false
Region               Utf8  nullable=true     ← Name Mismatch (region vs Region)
category             Utf8  nullable=true     ← Count Mismatch (Extra column)
```

### Phase 2: Resolution Strategies

Once diagnosed, choose the strategy that fits your mismatch type.

#### Strategy A: The "Resilient" Fix (Count & Order Mismatches)

**Best for:** Handling added/removed columns or shuffled column order. **This strategy solves ~90% of real-world schema mismatches.**

Use [`.union_by_name()`]. Unlike standard SQL unions which match by position, this method matches by column name and fills missing columns with `NULL`. For more information see [Schema Management](./schema-management.md).

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Schema: [a, b]
    let df1 = ctx.read_csv("tests/data/example.csv", CsvReadOptions::default()).await?
        .select_columns(&["a", "b"])?;

    // Schema: [b, a, c] (Different order, extra column)
    let df2 = ctx.read_csv("tests/data/example.csv", CsvReadOptions::default()).await?
        .select_columns(&["b", "a", "c"])?;

    // Fails: df1.union(df2) -> Schema Error
    // Succeeds: Aligns by name, fills missing 'c' in df1 with NULL
    let unified = df1.union_by_name(df2)?;

    Ok(())
}
```

#### Strategy B: The "Alignment" Fix (Type Mismatches)

**Best for:** Unifying numeric types (e.g., Int32/Int64) or compatible formats.

Use [`.cast_to()`] inside a [`.with_column()`] transformation.

> **The Golden Rule of Casting:**
> Always cast the **narrower** type up to the **widest** common type (e.g., `Int32` → `Int64`). This is safe and prevents data loss. Never cast down unless you are certain values won't be truncated. For a detailed hierarchy of safe conversions, see [Schema Management](./schema-management.md#type-coercion-auto-alignment-vs-explicit-casting).

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // df1: id is Int32
    let df1 = ctx.sql("SELECT 1::int as id").await?;

    // df2: id is Int64
    let df2 = ctx.sql("SELECT 2::bigint as id").await?;

    // Fix: Explicitly cast df1's 'id' to Int64 to match df2
    let df1_aligned = df1.with_column(
        "id",
        col("id").cast_to(&DataType::Int64, df1.schema())?
    )?;

    // Now they can be safely combined
    let result = df1_aligned.union(df2)?;
    Ok(())
}
```

#### Strategy C: The "Schema Adapter" (Name Mismatches & Complex Logic)

**Best for:** Renaming columns, fixing capitalization, or enforcing a strict target schema.

Use [`.select()`] with [`.alias()`] to manually map the problematic schema to your target shape. This acts as a "schema adapter": it picks exactly the columns you want and **normalizes** them (e.g. renaming `Region` to `region` to enforce snake_case).

> **Golden Rule of Normalization:**
> Enforce your schema boundaries explicitly. Don't just hope upstream data stays clean—use `.select()` + `.alias()` to build an explicit "Adapter Layer" that renames and filters columns before they enter your core logic.

```rust
// Problem: Incoming data has "Region" (capped), target expects "region" (lowercase)
let df_fixed = df_incoming.select(vec![
    col("Region").alias("region"), // 1. Rename "Region" -> "region"
    col("amount"),                 // 2. Keep "amount" as-is
    col("customer_id"),            // 3. Keep "customer_id"
    // Note: Any other columns in df_incoming are dropped here, enforcing the schema.
])?;
```

### Automatic Schema Merging

DataFusion automatically attempts to merge schemas when reading multiple files (e.g., `ctx.read_parquet(...)`). As detailed in [Schema Management](./schema-management.md#applying-schemas-and-modeling-data), this process promotes types (widening) and handles missing columns (nullability).

**If automatic merging fails**, it is usually due to a strict incompatibility (e.g., `Int64` vs `String`). In these cases, you must fall back to **manual alignment**: read the files as separate DataFrames, apply **Strategy B (Casting)**, and then union them.

### Summary Checklist

| Problem   | Symptom                              | Solution                          |
| :-------- | :----------------------------------- | :-------------------------------- |
| **Shape** | "Different number of fields"         | Use [`.union_by_name()`]          |
| **Order** | Columns swapped                      | Use [`.union_by_name()`]          |
| **Type**  | "Incompatible types Int32 and Int64" | Use [`.cast_to()`] (cast up)      |
| **Name**  | "Field not found"                    | Use [`.alias()`] in [`.select()`] |

> **Performance Note:**
>
> [`.select()`] and [`.union_by_name()`] are logical transformations (metadata only) and are essentially free. [`.cast_to()`] requires rewriting data at execution time and has a computational cost. Always prefer fixing schemas at the source (write time) over casting at read time.

### Debugging Resources

- [Reading Explain Plans](../../user-guide/explain-usage.md) - How to debug query plans when schemas fail.
- Medium: [Schema Mismatch Error: Understanding and Resolving][schema mismatch medium]
- Stackademic: [Apache Spark Basics: Schema Enforcement vs Inference](https://blog.stackademic.com/apache-spark-basics-101-schema-enforcement-vs-schema-inference-78b6f35cec10) - Concepts of schema enforcement that apply universally to data engines.

<!-- ============================================================
     MOVE TO best-practices.md: END
     ============================================================ -->
