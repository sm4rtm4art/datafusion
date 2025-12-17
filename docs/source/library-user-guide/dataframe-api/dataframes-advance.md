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

# Advanced DataFrame Topics

This guide covers **extending DataFusion** with custom functions, data sources, and integrations. It is intended for library developers, platform engineers, and contributors who need to go beyond the standard DataFrame API.

> **Who this guide is for:**
>
> - **Library developers**: Building custom UDFs, TableProviders, or CatalogProviders
> - **Platform engineers**: Integrating DataFusion with Spark (Comet), Iceberg, or distributed systems
> - **Contributors**: Understanding DataFusion internals for optimization or feature development

**Prerequisites**: This guide assumes familiarity with basic DataFrame operations. For fundamentals, see [Creating DataFrames](creating-dataframes.md), [Transformations](transformations.md), and [Concepts](concepts.md).

```{contents}
:local:
:depth: 2
```

---

## DataFusion Internals

Understanding DataFusion's execution model enables you to build efficient custom operators and troubleshoot performance issues.

### Physical Plan Customization

The physical plan determines how DataFusion executes your query. You can inspect and customize it for advanced optimizations.

```rust
use datafusion::prelude::*;
use datafusion::physical_plan::displayable;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create sample data
    let df = dataframe!(
        "id" => [1, 2, 3, 4],
        "amount" => [500, 1500, 2000, 800]
    )?
    .filter(col("amount").gt(lit(1000)))?;

    // Create and inspect the physical plan
    let physical_plan = df.create_physical_plan().await?;
    println!("Physical Plan:\n{}", displayable(physical_plan.as_ref()).indent(true));

    Ok(())
}
```

Custom physical operators implement the [`ExecutionPlan`] trait:

- `schema()`: Return output schema
- `execute()`: Return `RecordBatch` stream
- `children()`: Return child operators
- `with_new_children()`: Support plan rewriting

> **Complete Example**: See [`custom_datasource`] for a full implementation.

### Custom Optimizer Rules

DataFusion's optimizer is extensible. Add custom optimization passes for specialized workloads:

```rust,no_run
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;
use datafusion::optimizer::OptimizerRule;
use std::sync::Arc;

// Custom optimizer rules can:
// - Rewrite logical plans for domain-specific optimizations
// - Push down predicates to custom data sources
// - Implement cost-based join reordering
// - Optimize window function evaluation order

// Example: Register a custom optimizer rule (see building-logical-plans.md for full implementation)
// let config = SessionConfig::new()
//     .with_optimizer_rule(Arc::new(MyCustomRule::new()));
// let ctx = SessionContext::with_config(config);

fn main() {
    // Basic usage without custom rules
    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config(config);
}
```

### Memory Management Overview

DataFusion manages memory through configurable memory pools and spilling strategies. For day-to-day memory configuration (pool sizing, choosing between pool types), see [Best Practices § Memory Guidelines](best-practices.md#memory-guidelines). This section covers advanced customization.

```rust
use datafusion::prelude::*;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeConfig};
use datafusion::error::Result;
use std::sync::Arc;

fn main() -> Result<()> {
    // Configure memory limits (1GB) with spilling behavior
    let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
    let runtime_config = RuntimeConfig::new().with_memory_pool(memory_pool);
    let runtime_env = Arc::new(RuntimeEnv::new(runtime_config)?);

    // Create session with memory-limited runtime
    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config_rt(config, runtime_env);
    Ok(())
}
```

> **Extension Point**: Patterns needed for custom memory allocators, NUMA-aware execution, and GPU memory management. Contributions welcome!

---

## Custom Functions

Extend DataFusion with domain-specific logic through User-Defined Functions.

### User-Defined Functions (UDFs)

Create custom scalar functions for row-by-row transformations:

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::{create_udf, Volatility, ColumnarValue};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_string_array;
use std::sync::Arc;

fn create_length_squared_udf() -> ScalarUDF {
    let func = Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let strings = as_string_array(&args[0])?;
        let result: Int64Array = strings
            .iter()
            .map(|s| s.map(|s| (s.len() as i64).pow(2)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    });

    create_udf(
        "length_squared",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        func,
    )
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_udf(create_length_squared_udf());

    // Create sample data with names
    ctx.sql("CREATE TABLE users (name TEXT) AS VALUES ('Alice'), ('Bob'), ('Carol')").await?;

    let df = ctx.sql("SELECT name, length_squared(name) as score FROM users").await?;
    df.show().await?;
    Ok(())
}
```

### User-Defined Aggregate Functions (UDAFs)

Implement custom aggregations by implementing the [`Accumulator`] trait:

```rust,no_run
use datafusion::logical_expr::Accumulator;
use datafusion::arrow::array::ArrayRef;
use datafusion::common::{ScalarValue, cast::as_float64_array};

// Example: Geometric Mean Accumulator implementation
// This shows the structure - see simple_udaf.rs for a complete working example

#[derive(Debug)]
struct GeometricMeanAccumulator {
    product: f64,
    count: u64,
}

impl Accumulator for GeometricMeanAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        let array = as_float64_array(&values[0])?;
        for value in array.iter().flatten() {
            self.product *= value;
            self.count += 1;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        if self.count == 0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(self.product.powf(1.0 / self.count as f64))))
        }
    }

    fn size(&self) -> usize { std::mem::size_of_val(self) }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.product)),
            ScalarValue::UInt64(Some(self.count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        let products = as_float64_array(&states[0])?;
        let counts = states[1].as_primitive::<arrow::datatypes::UInt64Type>();
        for (prod, cnt) in products.iter().zip(counts.iter()) {
            if let (Some(p), Some(c)) = (prod, cnt) {
                self.product *= p;
                self.count += c;
            }
        }
        Ok(())
    }
}
```

> **Complete Examples**:
>
> - [`simple_udf.rs`] - Basic scalar UDF
> - [`advanced_udf.rs`] - Multi-argument UDFs
> - [`async_udf.rs`] - Async UDFs for I/O operations

> **Extension Point**: Examples needed for Window UDFs, stateful functions, and ML feature engineering functions.

---

## Custom Data Sources

Build custom data sources to read from APIs, databases, or specialized storage systems.

### TableProvider

The [`TableProvider`] trait is the core abstraction for custom data sources:

```rust,no_run
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::execution::context::SessionState;
use datafusion::prelude::Expr;
use std::sync::Arc;
use std::any::Any;

// Example: Custom TableProvider skeleton
// See custom_datasource example for a complete implementation

#[derive(Debug)]
struct MyCustomTable {
    schema: SchemaRef,
    // Your custom state: connection pool, file list, etc.
}

#[async_trait::async_trait]
impl TableProvider for MyCustomTable {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // 1. Apply projection to read only needed columns
        // 2. Push down filters to minimize data read
        // 3. Return ExecutionPlan that produces RecordBatches
        todo!("Implement scan logic")
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact])
    }
}
```

**Registration and usage:**

```rust,no_run
use datafusion::prelude::*;
use std::sync::Arc;

// Assuming MyCustomTable is defined and implements TableProvider
// let ctx = SessionContext::new();
// ctx.register_table("my_data", Arc::new(MyCustomTable::new()))?;

// let df = ctx.table("my_data").await?
//     .filter(col("status").eq(lit("active")))?;
// df.show().await?;
```

> **Complete Examples**: See [`custom_datasource`] and [`custom_file_format.rs`].

### CatalogProvider

For organizing tables into databases and schemas, implement [`CatalogProvider`]:

```rust,no_run
use datafusion::prelude::*;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create catalog hierarchy: catalog -> schema -> tables
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();
    catalog.register_schema("analytics", Arc::new(schema))?;
    ctx.register_catalog("warehouse", Arc::new(catalog));

    // Register tables with full path (requires actual parquet file)
    // ctx.register_parquet(
    //     "warehouse.analytics.sales",
    //     "sales.parquet",
    //     ParquetReadOptions::default()
    // ).await?;

    // Query with full path
    // let df = ctx.table("warehouse.analytics.sales").await?;
    Ok(())
}
```

The catalog hierarchy consists of:

| Trait                   | Purpose                          | Example                      |
| ----------------------- | -------------------------------- | ---------------------------- |
| [`CatalogProviderList`] | Collection of catalogs           | Default session catalog list |
| [`CatalogProvider`]     | Collection of schemas (database) | `MemoryCatalogProvider`      |
| [`SchemaProvider`]      | Collection of tables (schema)    | `MemorySchemaProvider`       |
| [`TableProvider`]       | Individual table                 | Custom implementations       |

**Remote Catalogs**: For catalogs stored remotely (e.g., Hive Metastore, Unity Catalog), provide an in-memory snapshot of metadata. The planning APIs are synchronous, so network calls must happen before planning. See the [remote_catalog example] for patterns.

> **Extension Point**: Implementations needed for Hive Metastore, AWS Glue, and Unity Catalog integration.

### Best Practices for Custom Data Sources

- **Push down aggressively**: Minimize I/O by filtering and projecting at the source
- **Provide accurate statistics**: Enable better query planning via `statistics()` method
- **Stream, don't buffer**: Return streaming `ExecutionPlan` to avoid memory issues
- **Use EXPLAIN**: Verify pushdown with `df.explain(false, false)?`
- **Consider alternatives**: Use `MemTable` for small data, `ListingTable` for files

---

## Ecosystem Integrations

DataFusion integrates with the broader data ecosystem through several projects.

### Apache DataFusion Comet

[Apache DataFusion Comet](https://datafusion.apache.org/comet/) is a high-performance accelerator for **Apache Spark**, built on DataFusion's query engine. It provides significant speedups for Spark workloads without code changes.

**Key features:**

- **2x+ speedup** on TPC-H benchmarks
- **Drop-in replacement**: No Spark code changes required
- **Iceberg acceleration**: Faster Parquet scans from Spark
- **Commodity hardware**: No GPUs or specialized hardware needed

```xml
<!-- Add to Spark project -->
<dependency>
    <groupId>org.apache.datafusion</groupId>
    <artifactId>comet-spark</artifactId>
    <version>${comet.version}</version>
</dependency>
```

See the [Comet documentation](https://datafusion.apache.org/comet/) for installation and benchmarking guides.

### Apache Iceberg

DataFusion can read Apache Iceberg tables through the [datafusion-iceberg](https://github.com/apache/iceberg-rust) project, enabling:

- Time travel queries
- Schema evolution
- Partition pruning
- Snapshot isolation

```rust,no_run
// Example: Reading Iceberg tables (requires datafusion-iceberg crate)
// use datafusion_iceberg::IcebergTableProvider;
//
// let table = IcebergTableProvider::try_new("s3://bucket/iceberg/table").await?;
// ctx.register_table("iceberg_table", Arc::new(table))?;
//
// let df = ctx.sql("SELECT * FROM iceberg_table").await?;
```

> **Extension Point**: Documentation needed for Delta Lake and Hudi integration patterns.

### Arrow Flight

Serve DataFrames over the network using [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html):

```rust,no_run
// Arrow Flight server example (requires arrow-flight crate)
// use arrow_flight::flight_service_server::FlightServiceServer;

// Create Arrow Flight server to serve DataFusion queries
// - Clients connect over gRPC
// - Efficient binary protocol for Arrow data
// - Supports streaming large results

// For client-side reading, see creating-dataframes.md
```

Arrow Flight enables:

- **High-performance data transfer**: Binary Arrow format, no serialization overhead
- **Streaming**: Handle results larger than memory
- **Authentication**: Integrate with existing auth systems

### Ballista: Distributed Execution

[Ballista](https://github.com/apache/datafusion-ballista) extends DataFusion to distributed execution across multiple machines:

```rust,no_run
// Ballista distributed execution (requires ballista crate)
// use ballista::prelude::*;

// #[tokio::main]
// async fn main() -> ballista::error::Result<()> {
//     // Connect to Ballista cluster
//     let ctx = BallistaContext::remote("localhost", 50050).await?;
//
//     // Same DataFrame API, distributed execution
//     let df = ctx.read_parquet("s3://data/", ParquetReadOptions::default()).await?;
//
//     let result = df
//         .filter(col("amount").gt(lit(1000)))?
//         .aggregate(vec![col("region")], vec![sum(col("amount"))])?
//         .collect().await?;
//
//     Ok(())
// }
```

> **Extension Point**: Patterns needed for Kubernetes deployment, resource management, and fault tolerance.

---

## Production Deployment

Patterns for deploying DataFusion in multi-tenant and resource-constrained environments.

### Multi-Tenant Isolation

Isolate data and resources between tenants using catalog-level separation:

```rust
use datafusion::prelude::*;
use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
use std::sync::Arc;

fn create_tenant_context(tenant_id: &str, data_root: &str) -> SessionContext {
    let ctx = SessionContext::new();

    // Each tenant gets own catalog
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();
    catalog.register_schema("public", Arc::new(schema)).unwrap();
    ctx.register_catalog(tenant_id, Arc::new(catalog));

    // Tenant can only access their data path
    let _tenant_path = format!("{}/{}", data_root, tenant_id);
    ctx
}

fn main() {
    // Usage: isolated contexts per tenant
    let tenant1_ctx = create_tenant_context("tenant_1", "/data");
    let tenant2_ctx = create_tenant_context("tenant_2", "/data");
}
```

For row-level security, implement a plan rewriter that adds tenant filters to all queries.

### Resource Governance

Control memory and execution time per query:

```rust
use datafusion::prelude::*;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeConfig};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use std::sync::Arc;
use std::time::Duration;

async fn run_query_with_limits(df: DataFrame) -> datafusion::error::Result<()> {
    // Per-query memory limit (512MB)
    let memory_pool = Arc::new(GreedyMemoryPool::new(512 * 1024 * 1024));
    let runtime = RuntimeEnv::new(
        RuntimeConfig::new().with_memory_pool(memory_pool)
    )?;

    let _ctx = SessionContext::new_with_config_rt(
        SessionConfig::default(),
        Arc::new(runtime)
    );

    // Query timeout (5 minutes)
    let _result = tokio::time::timeout(
        Duration::from_secs(300),
        df.collect()
    ).await??;

    Ok(())
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!("id" => [1, 2, 3])?;
    run_query_with_limits(df).await
}
```

### Audit and Lineage

Track query execution for compliance:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

struct QueryLogger {
    user: String,
}

impl QueryLogger {
    fn log_query(&self, df: &DataFrame) {
        let plan = df.logical_plan();
        // Extract table references, log to audit system
        eprintln!("User: {} accessed tables: {:?}", self.user, plan.display());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!("id" => [1, 2, 3], "amount" => [100, 200, 300])?;

    let logger = QueryLogger { user: "user123".into() };
    logger.log_query(&df);
    Ok(())
}
```

> **Extension Point**: Patterns needed for security (encryption at rest/in transit), integration with HashiCorp Vault/AWS KMS, and distributed transaction support.

---

## Data Quality & Bias Inspection

Inspired by research on [Blue Elephants Inspecting Pandas], you can track data distribution changes throughout your pipeline to detect technical biases or data quality issues. This is particularly valuable for ML pipelines where filtering operations can inadvertently introduce demographic bias.

### Tracking Tuple Identity with row_number()

Add stable identifiers to track individual rows through transformations:

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "customer" => ["Alice", "Bob", "Carol", "Dave"],
        "age" => [25, 35, 45, 55],
        "income" => [50000, 75000, 60000, 90000]
    )?;

    // Add a stable tuple identifier to track rows through transformations
    let with_tid = df.window(vec![
        row_number()
            .order_by(vec![col("customer").sort(true, true)])
            .build()?
            .alias("tid")
    ])?;

    with_tid.show().await?;
    // +----------+-----+--------+-----+
    // | customer | age | income | tid |
    // +----------+-----+--------+-----+
    // | Alice    | 25  | 50000  | 1   |
    // | Bob      | 35  | 75000  | 2   |
    // | Carol    | 45  | 60000  | 3   |
    // | Dave     | 55  | 90000  | 4   |
    // +----------+-----+--------+-----+

    Ok(())
}
```

### Computing Distribution Frequencies

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;
use datafusion::arrow::datatypes::DataType;

/// Compute distribution of values in a sensitive column
async fn compute_distribution(
    df: DataFrame,
    sensitive_col: &str,
) -> datafusion::error::Result<DataFrame> {
    // Count occurrences per group
    let grouped = df.clone().aggregate(
        vec![col(sensitive_col)],
        vec![count(lit(1)).alias("count")]
    )?;

    // Total count for ratio calculation
    let total_count = df.clone().count().await? as f64;

    // Add ratio column
    let with_ratio = grouped.with_column(
        "ratio",
        col("count").cast_to(&DataType::Float64, &grouped.schema())? / lit(total_count)
    )?;

    with_ratio.clone().sort(vec![col("ratio").sort(false, true)])?.show().await?;

    Ok(with_ratio)
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "category" => ["A", "A", "B", "B", "B", "C"],
        "value" => [1, 2, 3, 4, 5, 6]
    )?;

    compute_distribution(df, "category").await?;
    Ok(())
}
```

### Detecting Bias: Before and After Comparison

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Original data with sensitive column
    let original = dataframe!(
        "tid" => [1, 2, 3, 4, 5, 6],
        "age_group" => ["young", "young", "middle", "middle", "senior", "senior"],
        "income" => [30000, 35000, 60000, 65000, 45000, 50000]
    )?;

    // Step 1: Register original for inspection
    ctx.register_table("step_0_original", original.clone().into_view())?;

    // Step 2: Apply transformation (e.g., filter high income)
    let filtered = original
        .clone()
        .filter(col("income").gt(lit(40000)))?;

    ctx.register_table("step_1_filtered", filtered.clone().into_view())?;

    // Step 3: Inspect distribution change using SQL
    let inspection = ctx.sql("
        SELECT
            s0.age_group,
            COUNT(s0.tid) as original_count,
            COUNT(s1.tid) as filtered_count,
            CAST(COUNT(s1.tid) AS DOUBLE) / CAST(COUNT(s0.tid) AS DOUBLE) as retention_ratio
        FROM step_0_original s0
        LEFT JOIN step_1_filtered s1 ON s0.tid = s1.tid
        GROUP BY s0.age_group
        ORDER BY retention_ratio
    ").await?;

    println!("Distribution change after filtering:");
    inspection.show().await?;
    // Shows which age groups were disproportionately filtered out

    Ok(())
}
```

**Output:**

```
+-----------+----------------+----------------+-----------------+
| age_group | original_count | filtered_count | retention_ratio |
+-----------+----------------+----------------+-----------------+
| young     | 2              | 0              | 0.0             | ← Bias detected!
| middle    | 2              | 2              | 1.0             |
| senior    | 2              | 2              | 1.0             |
+-----------+----------------+----------------+-----------------+
```

### Reusable Distribution Tracking Function

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

/// Track distribution of a sensitive column through pipeline steps
async fn track_distribution(
    ctx: &SessionContext,
    step_name: &str,
    df: DataFrame,
    sensitive_col: &str,
) -> datafusion::error::Result<()> {
    // Register this step as a view
    ctx.register_table(step_name, df.clone().into_view())?;

    // Compute distribution
    let dist = df.aggregate(
        vec![col(sensitive_col)],
        vec![
            count(lit(1)).alias("count"),
        ],
    )?;

    println!("\nDistribution at {}:", step_name);
    dist.show().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let data = dataframe!(
        "gender" => ["F", "F", "M", "M", "M"],
        "score" => [85, 92, 78, 95, 88]
    )?;

    // Track at each step
    track_distribution(&ctx, "step_0_original", data.clone(), "gender").await?;

    let filtered = data.filter(col("score").gt(lit(90)))?;
    track_distribution(&ctx, "step_1_filtered", filtered.clone(), "gender").await?;
    // Check if filtering introduced gender imbalance

    Ok(())
}
```

**Key Insights:**

1. **SQL CTEs mirror DataFrame steps**: Each DataFrame transformation can be registered as a view/CTE for SQL-based inspection
2. **Tuple identifiers enable lineage**: Adding a `tid` column lets you track individual rows through transformations
3. **Distribution changes reveal bias**: Comparing group counts before/after operations detects disproportionate filtering
4. **Hybrid approach is powerful**: Use DataFrames for pipeline construction, SQL for auditing and inspection

> **See also:**
>
> - [Blue Elephants Inspecting Pandas] - Research on ML pipeline inspection in SQL
> - [mlinspect] - Python framework for ML pipeline inspection

[Blue Elephants Inspecting Pandas]: https://arxiv.org/abs/2309.07564 "Research paper on inspecting ML pipelines"
[mlinspect]: https://github.com/stefan-grafberger/mlinspect "Python ML pipeline inspection framework"

---

## Related Documentation

- **[Concepts](concepts.md)** — Core architecture and lazy evaluation
- **[Creating DataFrames](creating-dataframes.md)** — File reading, streaming sources, Arrow Flight client
- **[Schema Management](schema-management.md)** — Schema evolution, complex types, validation
- **[Transformations](transformations.md)** — Filter, join, aggregate, window functions
- **[Writing DataFrames](writing-dataframes.md)** — Output formats, partitioning, table writes
- **[Best Practices](best-practices.md)** — Performance tuning, debugging, configuration (using DataFusion well)

---

## Contributing to This Guide

This guide is designed for community expansion. Each section has **Extension Points** marking areas where contributions are welcome.

**What makes a good advanced topic:**

- Extends DataFusion (custom functions, providers, operators)
- Integrates with external systems (databases, streaming, cloud services)
- Addresses production concerns (security, multi-tenancy, observability)
- Includes working, tested code examples

**How to contribute:**

1. Choose an Extension Point that matches your expertise
2. Add working examples with clear explanations
3. Submit a PR — see [CONTRIBUTING.md](../../../../CONTRIBUTING.md)

---

**Prerequisites**: [Concepts](concepts.md), [Creating DataFrames](creating-dataframes.md)

**Next Steps**: Explore [datafusion-examples](https://github.com/apache/datafusion/tree/main/datafusion-examples) for complete working code.

<!-- Link references -->

[`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`CatalogProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html
[`CatalogProviderList`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProviderList.html
[`SchemaProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html
[`Accumulator`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.Accumulator.html
[`custom_datasource`]: https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/custom_datasource
[`custom_file_format.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_file_format.rs
[`simple_udf.rs`]: https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/simple_udf.rs
[`advanced_udf.rs`]: https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/advanced_udf.rs
[`async_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/async_udf.rs
[remote_catalog example]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html#implementing-remote-catalogs
