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

<!-- TODO Topic Execution : sampling, iterative filtering, and chunked processing  --->

[`simple_udf.rs`]: https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/udf
[`advanced_udf.rs`]: https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/udf
[`async_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/async_udf.rs

<!-- TODO  For complete large-dataset workflows. Topic itterative guidance through a data set, to prevent the Memory overflow etc.  like wokring with schma, then schow limit, then show onset limit, and apply it on expensive, batched process-->

# Advanced DataFrame Topics

This guide covers advanced DataFrame patterns, optimizations, and production-ready techniques for building high-performance data applications with DataFusion. Each section provides production-tested patterns with clear extension points for community contributions.

```{contents}
:local:
:depth: 3
```

## Why the DataFrame API for Advanced Use Cases?

While SQL excels at declarative queries, the DataFrame API unlocks unique capabilities for advanced use cases:

- **Type safety**: Catch errors at compile time with Rust's type system
- **Composability**: Build reusable transformation functions and components
- **Programmatic control**: Generate queries dynamically based on runtime conditions
- **Custom operators**: Implement domain-specific operations not expressible in SQL
- **Integration**: Seamlessly integrate with the Rust ecosystem and custom systems

This guide assumes familiarity with basic DataFrame operations. For fundamentals, see [Creating DataFrames](creating-dataframes.md) and [Transformations](transformations.md).

---

## 1. Advanced Concepts & Internals

Understanding DataFusion's execution model enables you to build efficient custom operators, optimize complex queries, and troubleshoot performance issues.

### Physical Plan Customization

The physical plan determines how DataFusion executes your query. Understanding and customizing it is key for advanced optimizations.

#### Accessing and Inspecting Physical Plans

```rust
use datafusion::prelude::*;
use datafusion::physical_plan::displayable;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?
        .filter(col("amount").gt(lit(1000)))?;

    // Create the physical plan
    let physical_plan = df.create_physical_plan().await?;

    // Inspect the plan structure
    println!("Physical Plan:\n{}", displayable(physical_plan.as_ref()).indent(true));

    Ok(())
}
```

#### Custom Physical Operators

Build custom operators for specialized processing needs:

> **Extension Point**: Custom operators enable domain-specific optimizations (SIMD operations, GPU acceleration, specialized algorithms). Contributions welcome for ML operators, graph algorithms, and time-series processing.

```rust
// Example structure for custom physical operator
// Full implementation: see datafusion-examples/examples/custom_datasource.rs

use datafusion::physical_plan::ExecutionPlan;
use datafusion::arrow::datatypes::SchemaRef;
use std::sync::Arc;

// Custom operators implement ExecutionPlan trait
// - schema(): Return output schema
// - execute(): Return RecordBatch stream
// - children(): Return child operators
// - with_new_children(): Support plan rewriting
```

### Custom Optimizer Rules

DataFusion's optimizer is extensible. Add custom optimization passes for specialized workloads:

```rust
use datafusion::execution::config::SessionConfig;
use datafusion::optimizer::OptimizerRule;

// Custom optimizer rules can:
// - Rewrite logical plans for domain-specific optimizations
// - Push down predicates to custom data sources
// - Implement cost-based join reordering
// - Optimize window function evaluation order

// Example: Register custom optimizer rule
let config = SessionConfig::new()
    .with_optimizer_rule(Arc::new(MyCustomRule::new()));
let ctx = SessionContext::with_config(config);
```

> **Extension Point**: Custom optimizer rules enable workload-specific optimizations. Examples needed: bitmap index pushdown, approximate query processing, incremental view maintenance.

### Memory Management Deep Dive

DataFusion manages memory through careful buffer allocation and spilling strategies.

#### Understanding Memory Pools

```rust
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeConfig};
use std::sync::Arc;

// Configure memory limits and spilling behavior
let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024)); // 1GB
let runtime_config = RuntimeConfig::new().with_memory_pool(memory_pool);
let runtime_env = RuntimeEnv::new(runtime_config)?;
```

#### Spilling to Disk

For queries that exceed memory limits, DataFusion spills intermediate results to disk:

```rust
use datafusion::execution::config::SessionConfig;

let config = SessionConfig::new()
    .with_batch_size(8192)
    .with_target_partitions(8)
    // Enable disk-based execution for large aggregations
    .set_str("datafusion.execution.spill_path", "/tmp/datafusion-spill");
```

### Execution Model Details

#### Partitioning Strategies

DataFusion uses partitioning to parallelize execution:

- **Hash partitioning**: Distributes rows by hash of key columns (for joins, aggregations)
- **Range partitioning**: Distributes rows by value ranges (for sorted operations)
- **Round-robin**: Distributes rows evenly (for load balancing)

```rust
use datafusion::prelude::*;
use datafusion::physical_plan::Partitioning;
use datafusion::functions_aggregate::expr_fn::sum;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("large_file.csv", CsvReadOptions::new()).await?;

    // Repartition for parallel processing
    let df_repartitioned = df.repartition(Partitioning::Hash(
        vec![col("user_id")],
        16  // 16 partitions
    ))?;

    // Subsequent operations execute in parallel per partition
    let result = df_repartitioned
        .aggregate(vec![col("user_id")], vec![sum(col("amount"))])?
        .collect().await?;

    Ok(())
}
```

#### Parallelism Control

```rust
use datafusion::execution::config::SessionConfig;

let config = SessionConfig::new()
    // Number of concurrent tasks
    .with_target_partitions(num_cpus::get())
    // Enable automatic repartitioning of large files
    .with_repartition_file_scans(true)
    // Minimum file size for repartitioning (64MB)
    .with_repartition_file_min_size(64 * 1024 * 1024);
```

### State Management for Long-Running Operations

For stateful operations like window functions and incremental aggregations:

> **Extension Point**: Patterns needed for checkpointing, incremental state updates, and distributed state management. See [Streaming Execution EPIC](https://synnada.notion.site/EPIC-Long-running-stateful-execution-support-for-unbounded-data-with-mini-batches-a416b29ae9a5438492663723dbeca805).

---

## 2. Advanced Creation Patterns

Beyond basic file reading, these patterns enable sophisticated data integration.

### Custom TableProviders

Build custom data sources when you need to read from APIs, databases, generated tables, or specialized storage systems.

#### Core Responsibilities

A custom [`TableProvider`] must:

1. **Provide schema**: Return an accurate `SchemaRef`
2. **Implement scan**: Return an `ExecutionPlan` for reading data
3. **Support pushdown**: Honor filter and projection pushdown when possible
4. **Provide statistics**: Return row counts and distinct counts for optimization
5. **Declare table type**: Return `TableType::Base` or `TableType::View`

#### Basic Implementation Pattern

```rust
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::arrow::datatypes::{Schema, SchemaRef, Field, DataType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::logical_expr::TableProviderFilterPushDown;
use std::sync::Arc;
use std::any::Any;

#[derive(Debug)]
struct MyCustomTable {
    schema: SchemaRef,
    // Your custom state (connection pool, file list, etc.)
}

#[async_trait::async_trait]
impl TableProvider for MyCustomTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // 1. Apply projection to read only needed columns
        // 2. Push down filters to minimize data read
        // 3. Return ExecutionPlan that produces RecordBatches

        todo!("Implement scan logic")
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> datafusion::error::Result<TableProviderFilterPushDown> {
        // Advertise which filters can be pushed down
        Ok(TableProviderFilterPushDown::Inexact)
    }
}
```

#### Registration and Usage

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register custom table
    let custom_table = Arc::new(MyCustomTable::new());
    ctx.register_table("my_data", custom_table)?;

    // Use like any other table
    let df = ctx.table("my_data").await?
        .filter(col("status").eq(lit("active")))?;

    df.show().await?;
    Ok(())
}
```

<!-- Suggestions ? -->

> **Complete Examples**:
>
> - [`custom_datasource`] - API-backed data source
> - [`custom_file_format.rs`] - Custom file format reader
>
> See the [datafusion-examples](https://github.com/apache/datafusion/tree/main/datafusion-examples/examples) directory for complete implementations.

#### Best Practices

- **Push down aggressively**: Minimize I/O by filtering and projecting early
- **Provide accurate statistics**: Enable better query planning
- **Stream, don't buffer**: Avoid materializing large intermediates
- **Use EXPLAIN**: Verify pushdown with `df.explain(false, false)?`
- **Consider simpler alternatives**: Use `MemTable` for small data, `ViewTable` for logical views, `ListingTable` for file-based sources

### Federated Query Patterns

Query multiple data sources in a single query:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register multiple sources
    ctx.register_parquet("sales_2023", "s3://data/sales/2023/", ParquetReadOptions::default()).await?;
    ctx.register_parquet("sales_2024", "s3://data/sales/2024/", ParquetReadOptions::default()).await?;

    // Custom API source for user data
    ctx.register_table("users", Arc::new(UserAPITable::new()))?;

    // Federated query across sources
    let result = ctx.sql("
        SELECT u.name, s.amount, s.year
        FROM (
            SELECT user_id, amount, 2023 as year FROM sales_2023
            UNION ALL
            SELECT user_id, amount, 2024 as year FROM sales_2024
        ) s
        JOIN users u ON s.user_id = u.id
        WHERE u.country = 'US'
    ").await?;

    result.show().await?;
    Ok(())
}
```

### Dynamic Schema Discovery

Handle sources with evolving or runtime-determined schemas:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

async fn discover_and_load(
    ctx: &SessionContext,
    api_endpoint: &str
) -> datafusion::error::Result<DataFrame> {
    // 1. Probe API for schema (example: inspect first response)
    // In production: call API metadata endpoint or inspect sample data
    let discovered_fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ];

    // 2. Build Arrow schema
    let schema = Arc::new(Schema::new(discovered_fields));

    // 3. Create TableProvider with discovered schema
    // In production: implement custom TableProvider that fetches from API
    // For now, use a placeholder or read from cached data
    let df = ctx.sql(&format!("SELECT * FROM api_cache WHERE endpoint = '{}'", api_endpoint)).await?;

    Ok(df)
}
```

> **Extension Point**: Patterns needed for schema caching, version negotiation, and graceful degradation when schemas change.

### Lazy Loading and Virtual Tables

Defer expensive operations until actually needed:

```rust
use datafusion::prelude::*;

// ViewTable: lazy evaluation wrapper around a LogicalPlan
let view_plan = df
    .filter(col("active").eq(lit(true)))?
    .aggregate(vec![col("category")], vec![count(col("*"))])?
    .into_optimized_plan().await?;

ctx.register_table("active_summary", Arc::new(ViewTable::new(view_plan)))?;

// Query executes only when accessed
let result = ctx.sql("SELECT * FROM active_summary WHERE category = 'Electronics'").await?;
```

### Incremental/Streaming Ingestion Patterns

Micro-batching pattern for event streams:

```rust
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::functions_aggregate::expr_fn::count;
use tokio::time::{interval, Duration};

async fn stream_to_datafusion(
    ctx: SessionContext,
    topic: &str,
) -> datafusion::error::Result<()> {
    let mut interval_timer = interval(Duration::from_secs(5));

    loop {
        interval_timer.tick().await;

        // Fetch batch from stream (Kafka, Kinesis, etc.)
        // In production: implement actual stream consumer
        let batch: RecordBatch = fetch_events_from_stream(topic).await?;

        // Process with DataFusion
        let df = ctx.read_batch(batch)?;
        let aggregated = df
            .aggregate(
                vec![col("event_type")],
                vec![count(col("*")).alias("count")]
            )?
            .collect().await?;

        // Write results or update state
        // In production: write to sink (database, file, another stream)
        for batch in aggregated {
            eprintln!("Processed batch with {} rows", batch.num_rows());
        }
    }
}

// Placeholder for actual stream consumer
async fn fetch_events_from_stream(topic: &str) -> datafusion::error::Result<RecordBatch> {
    // In production: consume from Kafka/Kinesis/etc.
    todo!("Implement stream consumer for topic: {}", topic)
}
```

> **Note**: DataFusion is batch-oriented. Native streaming support is under active development. See [Streaming Execution EPIC](https://synnada.notion.site/EPIC-Long-running-stateful-execution-support-for-unbounded-data-with-mini-batches-a416b29ae9a5438492663723dbeca805) and [issue #4285](https://github.com/apache/datafusion/issues/4285).

### External Index Integration

For large Parquet datasets, external indexes dramatically improve query performance.

#### How External Indexes Work

External indexes map values to row group locations, enabling DataFusion to:

- Skip entire row groups beyond basic Parquet statistics
- Answer min/max queries without scanning data
- Prune partitions more aggressively

Effective for:

- High-cardinality columns (user IDs, transaction IDs)
- Range queries on sorted or semi-sorted data
- Multi-column predicates where statistics aren't sufficient

#### Performance Impact

A 1TB Parquet table with external indexes can reduce selective query time from minutes to seconds.

#### Usage Pattern

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register table normally (indexes configured separately)
    ctx.register_parquet(
        "events",
        "s3://bucket/events/",
        ParquetReadOptions::default()
    ).await?;

    // Queries automatically benefit from indexes if present
    let df = ctx.table("events").await?
        .filter(col("timestamp").gt(lit("2024-01-01")))?
        .filter(col("user_id").eq(lit(12345)))?;

    df.show().await?;
    Ok(())
}
```

#### When to Use

✅ **Use when:**

- Queries filter on high-cardinality columns
- Dataset is large (100GB+) with high selectivity (< 10% of data)
- Storage overhead acceptable (5-15% of data size)
- Read patterns are predictable

❌ **Skip when:**

- Full table scans are common
- Dataset is small (< 10GB)
- Low cardinality columns (use partitioning instead)
- Very high write frequency

> **Implementation Details**: External indexes require setup and maintenance. See [External Indexes and Caches](https://datafusion.apache.org/contributor-guide/architecture.html#external-indexes) for patterns and tooling.

### Catalog and Schema Management

#### Custom Catalogs and Schemas

For multi-tenant deployments or complex namespace requirements:

```rust
use datafusion::prelude::*;
use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
use std::sync::Arc;

let ctx = SessionContext::new();

// Create custom catalog structure
let catalog = MemoryCatalogProvider::new();
let schema = MemorySchemaProvider::new();
catalog.register_schema("analytics", Arc::new(schema))?;
ctx.register_catalog("warehouse", Arc::new(catalog));

// Register tables in custom locations
ctx.register_parquet(
    "warehouse.analytics.sales",
    "sales.parquet",
    ParquetReadOptions::default()
).await?;

// Access with full path
let df = ctx.table("warehouse.analytics.sales").await?;
```

#### Multi-Tenant Catalog Patterns

Isolate data between tenants:

```rust
fn create_tenant_context(tenant_id: &str) -> SessionContext {
    let ctx = SessionContext::new();
    let catalog = MemoryCatalogProvider::new();
    ctx.register_catalog(tenant_id, Arc::new(catalog));
    ctx
}

// Tenant-specific data access
let tenant1_ctx = create_tenant_context("tenant_1");
tenant1_ctx.register_parquet(
    "tenant_1.public.sales",
    "tenant1/sales.parquet",
    ParquetReadOptions::default()
).await?;
```

#### information_schema for Metadata Discovery

```rust
use datafusion::execution::config::SessionConfig;

let ctx = SessionContext::with_config(
    SessionConfig::new().with_information_schema(true)
);

// Discover all tables and schemas
let tables_df = ctx.sql("
    SELECT table_catalog, table_schema, table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema != 'information_schema'
    ORDER BY table_catalog, table_schema, table_name, ordinal_position
").await?;

tables_df.show().await?;
```

> **Extension Point**: Custom catalog implementations for distributed catalogs, versioned schemas, and metadata stores. See [Hive Metastore integration](https://github.com/apache/datafusion/issues/example) for patterns.

---

## 3. Advanced Schema Operations

Handle evolving schemas, complex types, and cross-system compatibility.

### Schema Versioning and Migration

Production systems need to handle schema changes gracefully.

#### Forward-Compatible Schema Design

Design schemas that tolerate additions:

```rust
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

// Version 1: Initial schema
let schema_v1 = Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
]);

// Version 2: Added optional column
let schema_v2 = Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
    Field::new("email", DataType::Utf8, true),  // Nullable for compatibility
]);

// DataFusion automatically handles missing columns as NULL
```

#### Schema Adapter Layer

Transform data to match expected schema:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::cast;

async fn normalize_to_schema(
    df: DataFrame,
    target_schema: SchemaRef,
) -> datafusion::error::Result<DataFrame> {
    let current_schema = df.schema();

    // Build transformation for each target field
    let mut select_exprs = vec![];

    for target_field in target_schema.fields() {
        let field_name = target_field.name();

        if let Ok(current_field) = current_schema.field_with_name(field_name) {
            // Field exists: cast if needed
            if current_field.data_type() == target_field.data_type() {
                select_exprs.push(col(field_name));
            } else {
                select_exprs.push(
                    cast(col(field_name), target_field.data_type().clone())
                        .alias(field_name)
                );
            }
        } else {
            // Field missing: add as NULL
            let null_value = lit(ScalarValue::try_from(target_field.data_type())?);
            select_exprs.push(null_value.alias(field_name));
        }
    }

    df.select(select_exprs)
}
```

#### Schema Migration Testing

Validate migrations before deployment:

```rust
use datafusion::prelude::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_v1_to_v2_migration() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();

        // Load v1 data
        let df_v1 = ctx.read_parquet("data_v1.parquet", ParquetReadOptions::default()).await?;

        // Apply migration
        let df_v2 = normalize_to_schema(df_v1, schema_v2()).await?;

        // Verify schema
        assert_eq!(df_v2.schema().fields().len(), 3);
        assert!(df_v2.schema().field_with_name("email").is_ok());

        Ok(())
    }
}
```

### Complex Type Handling

Work with nested structures, arrays, and maps.

#### Nested Schemas (Structs)

```rust
use datafusion::arrow::datatypes::{Schema, Field, DataType, Fields};
use std::sync::Arc;

// Define nested schema
let address_struct = DataType::Struct(Fields::from(vec![
    Field::new("street", DataType::Utf8, false),
    Field::new("city", DataType::Utf8, false),
    Field::new("zip", DataType::Utf8, true),
]));

let schema = Schema::new(vec![
    Field::new("user_id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
    Field::new("address", address_struct, true),
]);
```

#### Querying Nested Fields

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_parquet("users.parquet", ParquetReadOptions::default()).await?;

    // Access nested fields using SQL syntax (most straightforward)
    let result = ctx.sql("
        SELECT
            name,
            address['street'] as street,
            address['city'] as city
        FROM users
        WHERE address['city'] = 'San Francisco'
    ").await?;

    result.show().await?;
    Ok(())
}
```

#### Arrays and Lists

```rust
use datafusion::prelude::*;

// Work with array columns
let df = ctx.sql("
    SELECT
        id,
        tags,
        array_length(tags) as tag_count,
        unnest(tags) as individual_tag
    FROM products
    WHERE array_contains(tags, 'electronics')
").await?;
```

#### Maps and Dictionaries

```rust
// Map type handling
let map_type = DataType::Map(
    Arc::new(Field::new("entries", DataType::Struct(Fields::from(vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Int64, true),
    ])), false)),
    false,
);
```

> **Extension Point**: Patterns needed for JSON flattening, schema-on-read for semi-structured data, and efficient nested column operations.

### Schema Inference Optimization

Balance flexibility with performance:

```rust
use datafusion::prelude::*;

// Inference with limited sample for performance
let df = ctx.read_csv(
    "large_file.csv",
    CsvReadOptions::new()
        .has_header(true)
        .schema_infer_max_records(10000)  // Limit inference sample
).await?;

// Or provide explicit schema to skip inference entirely
let explicit_schema = Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
    Field::new("amount", DataType::Decimal128(19, 2), false),
]);

let df = ctx.read_csv(
    "large_file.csv",
    CsvReadOptions::new()
        .has_header(true)
        .schema(&explicit_schema)
).await?;
```

### Cross-System Schema Mapping

Convert between different type systems:

```rust
// Example: Map PostgreSQL types to Arrow types
fn postgres_to_arrow_type(pg_type: &str) -> DataType {
    match pg_type {
        "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "numeric" => DataType::Decimal128(38, 10),
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "varchar" | "text" => DataType::Utf8,
        _ => DataType::Utf8,  // Fallback
    }
}
```

> **Extension Point**: Type mapping libraries needed for major databases (MySQL, Oracle, SQL Server), cloud warehouses (Snowflake, BigQuery), and data lakes (Delta Lake, Iceberg).

---

## 4. Advanced Transformations

Extend DataFusion with custom functions and sophisticated query patterns.

### User-Defined Functions (UDFs)

Create custom scalar functions for domain-specific logic.

#### Simple Scalar UDF

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::{create_udf, Volatility, ColumnarValue};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
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
        vec![DataType::Utf8],  // Input types
        DataType::Int64,  // Return type (not Arc)
        Volatility::Immutable,
        func,
    )
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register UDF
    ctx.register_udf(create_length_squared_udf());

    // Use in queries
    let df = ctx.sql("SELECT name, length_squared(name) as name_score FROM users").await?;
    df.show().await?;

    Ok(())
}
```

#### Advanced UDF with Multiple Arguments

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::{create_udf, Volatility, ColumnarValue};
use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_float64_array;
use std::sync::Arc;

fn create_weighted_score_udf() -> ScalarUDF {
    let func = Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let values = as_float64_array(&args[0])?;
        let weights = as_float64_array(&args[1])?;
        let base = as_float64_array(&args[2])?;

        let result: Float64Array = values
            .iter()
            .zip(weights.iter())
            .zip(base.iter())
            .map(|((v, w), b)| {
                match (v, w, b) {
                    (Some(val), Some(weight), Some(base_val)) =>
                        Some(val * weight + base_val),
                    _ => None,
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    });

    create_udf(
        "weighted_score",
        vec![DataType::Float64, DataType::Float64, DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        func,
    )
}
```

> **Complete Examples**: See [`simple_udf.rs`], [`advanced_udf.rs`], and [`async_udf.rs`] in datafusion-examples.

### User-Defined Aggregate Functions (UDAFs)

Implement custom aggregations:

```rust
use datafusion::logical_expr::Accumulator;
use datafusion::arrow::array::{ArrayRef, Float64Array, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{ScalarValue, cast::as_float64_array};
use std::sync::Arc;

// Example: Geometric mean aggregation accumulator
#[derive(Debug)]
struct GeometricMeanAccumulator {
    product: f64,
    count: u64,
}

impl GeometricMeanAccumulator {
    fn new() -> Self {
        Self {
            product: 1.0,
            count: 0,
        }
    }
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
            let result = self.product.powf(1.0 / self.count as f64);
            Ok(ScalarValue::Float64(Some(result)))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.product)),
            ScalarValue::UInt64(Some(self.count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        // Merge state from parallel partitions
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

// Note: Creating and registering UDAFs requires implementing the full
// AggregateUDFImpl trait. See datafusion-examples for complete patterns.
```

> **Extension Point**: UDAF examples needed for statistical functions (median, mode, percentile), time-series operations (EWMA, rolling correlations), and ML feature engineering.

### Window Function Customization

Advanced window operations:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::{sum, avg};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_parquet("sales.parquet", ParquetReadOptions::default()).await?;

    // Window functions with SQL for complex patterns
    let result = ctx.sql("
        SELECT
            region,
            amount,
            SUM(amount) OVER (PARTITION BY region ORDER BY sale_date) as running_total,
            RANK() OVER (PARTITION BY region ORDER BY sale_date) as rank,
            AVG(amount) OVER (
                PARTITION BY region
                ORDER BY sale_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as moving_avg_7d
        FROM sales
    ").await?;

    result.show().await?;
    Ok(())
}
```

### Complex Join Strategies

#### Broadcast vs Shuffle Joins

```rust
use datafusion::prelude::*;

// Small dimension table: broadcast to all partitions
let dim_table = ctx.read_csv("small_dim.csv", CsvReadOptions::new()).await?
    .cache().await?;  // Cache in memory

// Large fact table: keep partitioned
let fact_table = ctx.read_parquet("large_fact/", ParquetReadOptions::default()).await?;

// DataFusion automatically chooses broadcast join for small table
let result = fact_table
    .join(dim_table, JoinType::Inner, &["dim_id"], &["id"], None)?;
```

#### Semi-Join and Anti-Join

```rust
// Semi-join: return rows from left that have matches in right
let active_users = ctx.sql("
    SELECT * FROM users
    WHERE id IN (SELECT user_id FROM recent_activity)
").await?;

// Anti-join: return rows from left that have NO matches in right
let inactive_users = ctx.sql("
    SELECT * FROM users
    WHERE id NOT IN (SELECT user_id FROM recent_activity)
").await?;
```

### Subquery Optimization Patterns

#### Correlated Subqueries

```rust
let df = ctx.sql("
    SELECT u.name, u.total_purchases
    FROM users u
    WHERE u.total_purchases > (
        SELECT AVG(total_purchases)
        FROM users
        WHERE region = u.region
    )
").await?;
```

#### Common Table Expressions (CTEs)

```rust
let df = ctx.sql("
    WITH regional_stats AS (
        SELECT region, AVG(amount) as avg_amount
        FROM sales
        GROUP BY region
    ),
    top_regions AS (
        SELECT region FROM regional_stats
        WHERE avg_amount > 10000
    )
    SELECT s.* FROM sales s
    JOIN top_regions tr ON s.region = tr.region
").await?;
```

### Recursive Queries and Graph Operations

> **Coming Soon**: Recursive CTE support and graph traversal patterns.

> **Extension Point**: Graph algorithms (PageRank, shortest path, community detection) and hierarchical data processing patterns welcome.

### Custom Physical Operators

For maximum performance, implement custom physical operators:

```rust
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

// Custom operator that implements ExecutionPlan
// - Provides custom execution logic
// - Can leverage SIMD, GPU, or specialized algorithms
// - Integrates seamlessly into DataFusion's execution engine


```

---

## 5. Advanced Output & Integration

Beyond simple file writes, integrate DataFusion with external systems.

### Custom File Format Writers

Implement writers for specialized formats:

```rust
use datafusion::datasource::file_format::FileFormat;

// Custom FileFormat implementation
// - Implement serialize_batch() for custom encoding
// - Support compression, encryption, custom schemas
// - Register with SessionContext for SQL write support
```

### Partitioned Write Strategies

Optimize writes for downstream processing:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?;

    // Partition by year and month for efficient pruning
    df.write_parquet(
        "output/",
        DataFrameWriteOptions::new().with_partition_by(vec![
            "year".to_string(),
            "month".to_string(),
        ]),
        None,
    ).await?;

    // Result structure: output/year=2024/month=01/data.parquet
    Ok(())
}
```

### Transaction Support Patterns

Implement transactional writes:

```rust
use datafusion::prelude::*;

// Pattern: Two-phase commit with staging
async fn transactional_write(
    ctx: &SessionContext,
    df: DataFrame,
    path: &str
) -> datafusion::error::Result<()> {
    let staging_path = format!("{}.tmp", path);

    // 1. Write to staging location
    df.write_parquet(&staging_path, DataFrameWriteOptions::new(), None).await?;

    // 2. Validate write
    let validation_df = ctx.read_parquet(&staging_path, ParquetReadOptions::default()).await?;
    let count = validation_df.count().await?;

    if count > 0 {
        // 3. Atomic rename/move
        std::fs::rename(&staging_path, path)?;
        Ok(())
    } else {
        std::fs::remove_dir_all(&staging_path)?;
        Err(datafusion::error::DataFusionError::Execution(
            "Write validation failed".to_string()
        ))
    }
}
```

### Change Data Capture (CDC) Patterns

Process change streams:

```rust
use datafusion::prelude::*;

// Pattern: Merge CDC events into target table
async fn apply_cdc_events(
    ctx: &SessionContext,
    base_table: &str,
    output_path: &str,
) -> datafusion::error::Result<()> {
    // Assumes cdc_events table is already registered

    // Apply changes (INSERT, UPDATE, DELETE)
    let updated = ctx.sql(&format!("
        WITH deletes AS (
            SELECT id FROM cdc_events WHERE op = 'DELETE'
        ),
        upserts AS (
            SELECT id, name, value FROM cdc_events WHERE op IN ('INSERT', 'UPDATE')
        )
        SELECT * FROM {base_table}
        WHERE id NOT IN (SELECT id FROM deletes)
        UNION ALL
        SELECT * FROM upserts
    ")).await?;

    // Write back to new location
    updated.write_parquet(output_path, DataFrameWriteOptions::new(), None).await?;

    Ok(())
}
```

> **Extension Point**: Patterns for Delta Lake, Iceberg, and Hudi integration.

### Arrow Flight Serving

Serve DataFrames over the network:

```rust
use datafusion::prelude::*;
use arrow_flight::{FlightData, FlightServer};

// Create Arrow Flight server to serve DataFusion queries
// - Clients connect over gRPC
// - Efficient binary protocol for Arrow data
// - Supports streaming large results

// See creating-dataframes.md for client-side reading
```

### Distributed Execution with Ballista

Scale to multiple machines:

```rust
use ballista::prelude::*;

#[tokio::main]
async fn main() -> ballista::error::Result<()> {
    // Connect to Ballista cluster
    let ctx = BallistaContext::remote("localhost", 50050).await?;

    // Same DataFrame API, distributed execution
    let df = ctx.read_parquet("s3://data/", ParquetReadOptions::default()).await?;

    let result = df
        .filter(col("amount").gt(lit(1000)))?
        .aggregate(vec![col("region")], vec![sum(col("amount"))])?
        .collect().await?;

    Ok(())
}
```

> **Extension Point**: Patterns for Kubernetes deployment, resource management, and fault tolerance in distributed scenarios.

---

## 6. Performance Engineering

Tune DataFusion for maximum throughput and minimal latency.

### Cost-Based Optimization Tuning

Provide statistics for better query plans:

```rust
use datafusion::prelude::*;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::Statistics;

// TableProvider with statistics
impl TableProvider for MyTable {
    fn statistics(&self) -> Option<Statistics> {
        Some(Statistics {
            num_rows: Some(1_000_000),
            total_byte_size: Some(100_000_000),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: Some(ScalarValue::Int64(Some(999999))),
                    min_value: Some(ScalarValue::Int64(Some(1))),
                    distinct_count: Some(1_000_000),
                },
            ]),
        })
    }
}
```

### Statistics Collection and Maintenance

```rust
// Compute and cache statistics for better planning
async fn compute_statistics(ctx: &SessionContext, table: &str) -> datafusion::error::Result<()> {
    let df = ctx.table(table).await?;

    let stats = ctx.sql(&format!("
        SELECT
            COUNT(*) as row_count,
            COUNT(DISTINCT id) as unique_ids,
            MIN(amount) as min_amount,
            MAX(amount) as max_amount
        FROM {table}
    ")).await?;

    let result = stats.collect().await?;

    // Store statistics for future query planning
    cache_statistics(table, result)?;

    Ok(())
}
```

### Index-Aware Query Planning

Leverage external indexes in query planning:

> **Coming Soon**: API for registering and using external indexes.

### Cache Hierarchy Optimization

Layer caching strategies:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // L1: In-memory cache for hot data
    let hot_data = ctx.read_parquet("recent.parquet", ParquetReadOptions::default()).await?
        .cache().await?;  // Materialize in memory

    // L2: Registered tables for metadata caching
    ctx.register_parquet("warm_data", "warm.parquet", ParquetReadOptions::default()).await?;

    // L3: On-demand read for cold data
    let cold_data = ctx.read_parquet("archive.parquet", ParquetReadOptions::default()).await?;

    Ok(())
}
```

### NUMA-Aware Execution

> **Extension Point**: Patterns for NUMA-aware memory allocation and task scheduling in large-memory systems.

### GPU Acceleration Patterns

> **Extension Point**: Integration patterns for GPU-accelerated operations (cuDF, RAPIDS). Custom ExecutionPlan implementations welcome.

---

## 7. Production Patterns

Build robust, secure, and maintainable systems.

### Multi-Tenant Isolation Strategies

#### Catalog-Level Isolation

```rust
fn create_isolated_context(tenant_id: &str, data_root: &str) -> SessionContext {
    let ctx = SessionContext::new();

    // Each tenant gets own catalog
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();
    catalog.register_schema("public", Arc::new(schema))?;
    ctx.register_catalog(tenant_id, Arc::new(catalog));

    // Tenant can only access their data
    let tenant_path = format!("{}/{}", data_root, tenant_id);
    // Register tables under tenant catalog...

    ctx
}
```

#### Query-Level Isolation

```rust
// Add tenant_id filter to all queries
fn add_tenant_filter(plan: LogicalPlan, tenant_id: &str) -> LogicalPlan {
    // Rewrite plan to add WHERE tenant_id = 'xxx'
    // Ensures row-level security
}
```

### Resource Governance and Quotas

```rust
use datafusion::prelude::*;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeConfig};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::disk_manager::DiskManagerConfig;
use std::sync::Arc;
use std::time::Duration;

async fn run_query_with_limits(df: DataFrame) -> datafusion::error::Result<()> {
    // Per-query resource limits
    let memory_pool = Arc::new(GreedyMemoryPool::new(512 * 1024 * 1024)); // 512MB
    let disk_config = DiskManagerConfig::new_specified(vec!["/tmp/spill".into()]);

    let runtime = RuntimeEnv::new(
        RuntimeConfig::new()
            .with_memory_pool(memory_pool)
            .with_disk_manager_config(disk_config)
    )?;

    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::default(),
        Arc::new(runtime)
    );

    // Query timeout
    let result = tokio::time::timeout(
        Duration::from_secs(300),
        df.collect()
    ).await??;

    Ok(())
}
```

### Audit and Lineage Tracking

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlan;

// Track query lineage
struct QueryLogger {
    user: String,
    query: String,
}

impl QueryLogger {
    fn new(user: String, query: String) -> Self {
        Self { user, query }
    }

    fn log_query(&self, df: &DataFrame) {
        let plan = df.logical_plan();

        // Extract table references from the plan
        // In production, implement visitor pattern to walk LogicalPlan
        eprintln!("User: {} executed query", self.user);
        eprintln!("Query: {}", self.query);
        eprintln!("Plan: {:?}", plan.display());

        // In production: send to audit system (Kafka, database, log aggregator)
    }
}

// Usage
let logger = QueryLogger::new("user123".to_string(), "SELECT * FROM sales".to_string());
let df = ctx.table("sales").await?;
logger.log_query(&df);
```

### Fault Tolerance and Recovery

#### Checkpoint Pattern

```rust
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use std::fs::File;

async fn process_with_checkpoints(
    ctx: &SessionContext,
    batch_paths: Vec<String>,
    checkpoint_dir: &str,
) -> datafusion::error::Result<()> {
    for (idx, batch_path) in batch_paths.iter().enumerate() {
        let checkpoint_path = format!("{}/checkpoint_{}.parquet", checkpoint_dir, idx);

        // Check if already processed
        if std::path::Path::new(&checkpoint_path).exists() {
            continue;
        }

        // Process batch
        let df = ctx.read_parquet(batch_path, ParquetReadOptions::default()).await?;
        let result = df.filter(col("status").eq(lit("valid")))?;

        // Write checkpoint using DataFrame's write_parquet
        result.write_parquet(
            &checkpoint_path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None
        ).await?;
    }

    Ok(())
}
```

#### Retry with Exponential Backoff

```rust
use std::time::Duration;
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;

async fn query_with_retry(
    ctx: &SessionContext,
    sql: &str,
    max_retries: u32,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let mut retries = 0;

    loop {
        match ctx.sql(sql).await?.collect().await {
            Ok(result) => return Ok(result),
            Err(e) if retries < max_retries => {
                let delay = Duration::from_secs(2_u64.pow(retries));
                eprintln!("Query failed, retrying in {:?}: {}", delay, e);
                tokio::time::sleep(delay).await;
                retries += 1;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Distributed Transaction Patterns

> **Extension Point**: Patterns for distributed transactions, two-phase commit, and saga patterns across multiple data sources.

### Security and Encryption

#### Encryption at Rest

```rust
// Custom TableProvider with transparent decryption
struct EncryptedParquetTable {
    inner: Arc<dyn TableProvider>,
    key: EncryptionKey,
}

#[async_trait::async_trait]
impl TableProvider for EncryptedParquetTable {
    async fn scan(&self, ...) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(...).await?;
        // Wrap with decryption layer
        Ok(Arc::new(DecryptionExec::new(plan, self.key.clone())))
    }
}
```

#### Encryption in Transit

```rust
// TLS for Arrow Flight
use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::ServerTlsConfig;

let tls_config = ServerTlsConfig::new()
    .identity(Identity::from_pem(cert_pem, key_pem));

Server::builder()
    .tls_config(tls_config)?
    .add_service(FlightServiceServer::new(service))
    .serve(addr).await?;
```

> **Extension Point**: Integration patterns for HashiCorp Vault, AWS KMS, and other key management systems.

---

## 8. Debugging & Profiling

Diagnose and optimize query performance.

### Custom Metrics and Instrumentation

```rust
use datafusion::physical_plan::metrics::{MetricsSet, ExecutionPlanMetricsSet};

// Access execution metrics
let physical_plan = df.create_physical_plan().await?;
let result = physical_plan.execute(0, task_ctx)?;

// Collect metrics after execution
let metrics = physical_plan.metrics()?;
for metric in metrics.iter() {
    println!("{}: {:?}", metric.name(), metric.value());
}
```

### Query Plan Visualization

```rust
use datafusion::prelude::*;
use datafusion::physical_plan::displayable;

async fn visualize_plan(df: DataFrame) -> datafusion::error::Result<()> {
    // Logical plan
    println!("=== Logical Plan ===");
    println!("{}", df.logical_plan().display_indent());

    // Optimized logical plan
    let optimized = df.into_optimized_plan().await?;
    println!("\n=== Optimized Logical Plan ===");
    println!("{}", optimized.display_indent());

    // Physical plan
    let physical = df.create_physical_plan().await?;
    println!("\n=== Physical Plan ===");
    println!("{}", displayable(physical.as_ref()).indent(true));

    // Analyze mode: includes actual metrics
    let explain_df = df.explain(false, true)?;  // (verbose, analyze)
    explain_df.show().await?;

    Ok(())
}
```

### Performance Profiling Techniques

#### CPU Profiling

```rust
// Use tokio-console or pprof
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    // Enable tokio console
    console_subscriber::init();

    // Your DataFusion queries...
}
```

#### Memory Profiling

```rust
use datafusion::execution::memory_pool::MemoryPool;

let memory_pool = ctx.runtime_env().memory_pool.clone();

println!("Memory reserved: {} bytes", memory_pool.reserved());

// Run query
let result = df.collect().await?;

println!("Memory after query: {} bytes", memory_pool.reserved());
```

### Memory Leak Detection

```rust
use datafusion::prelude::*;

#[tokio::test]
async fn test_no_memory_leaks() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let initial_memory = ctx.runtime_env().memory_pool.reserved();

    // Run query multiple times
    for _ in 0..100 {
        let df = ctx.read_parquet("test.parquet", ParquetReadOptions::default()).await?;
        let _ = df.collect().await?;
    }

    // Memory should return to baseline (allow small overhead)
    let final_memory = ctx.runtime_env().memory_pool.reserved();
    assert!(final_memory - initial_memory < 1024 * 1024);  // < 1MB difference

    Ok(())
}
```

### Distributed Tracing Integration

```rust
use tracing::{info_span, Instrument};

async fn traced_query(df: DataFrame) -> datafusion::error::Result<Vec<RecordBatch>> {
    df.collect()
        .instrument(info_span!("datafusion_query",
            query_type = "aggregation",
            table = "sales"
        ))
        .await
}
```

> **Extension Point**: Integration patterns for OpenTelemetry, Jaeger, and cloud-native observability platforms.

---

## Related Documentation

- **[Concepts](concepts.md)** - Core architecture and lazy evaluation
- **[Creating DataFrames](creating-dataframes.md)** - Basic and intermediate patterns
- **[Schema Management](schema-management.md)** - Schema evolution and type systems
- **[Transformations](transformations.md)** - Filter, join, aggregate operations
- **[Writing DataFrames](writing-dataframes.md)** - Output and persistence
- **[Best Practices](best-practices.md)** - General optimization and debugging

## Contributing to This Guide

This guide is designed for community expansion. Each section has extension points marked with:

> **Extension Point**: [Description of needed content]

**How to contribute**:

1. Choose an extension point that matches your expertise
2. Add working, tested examples with clear explanations
3. Follow the documentation standards (conceptual overview, when to use, implementation, performance implications)
4. Submit a PR with your additions

See [CONTRIBUTING.md](../../../../CONTRIBUTING.md) for detailed guidelines.

---

**Prerequisites**: [Concepts](concepts.md), [Creating DataFrames](creating-dataframes.md)

**Next Steps**: Explore specific sections based on your needs, or dive into the [datafusion-examples](https://github.com/apache/datafusion/tree/main/datafusion-examples) for complete working code.

[`custom_datasource`]: https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/custom_data_source
[`custom_file_format.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_data_source/custom_file_format.rs
