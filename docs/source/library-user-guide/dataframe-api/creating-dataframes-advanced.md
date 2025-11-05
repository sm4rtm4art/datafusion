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

# Advanced DataFrame Creation Topics

Quick reference guide for advanced DataFrame creation patterns. For basic creation methods, see [Creating DataFrames](creating-dataframes.md).

## Catalog and Schema Management

### Custom Catalogs and Schemas

For complex deployments, you can create custom catalogs and schemas beyond the default `datafusion.public`:

```rust
use datafusion::prelude::*;
use datafusion::catalog::MemoryCatalogProvider;
use datafusion::catalog::MemorySchemaProvider;
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

### Using information_schema

Enable `information_schema` for SQL-based metadata discovery:

```rust
use datafusion::execution::config::SessionConfig;

let ctx = SessionContext::with_config(
    SessionConfig::new().with_information_schema(true)
);

// Discover all tables and their schemas
let tables_df = ctx.sql("
    SELECT table_catalog, table_schema, table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema != 'information_schema'
    ORDER BY table_catalog, table_schema, table_name, ordinal_position
").await?;

tables_df.show().await?;
```

### Multi-Tenant Catalog Patterns

Isolate data between tenants using separate catalogs:

```rust
// Each tenant gets their own catalog
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
    options
).await?;
```

## Schema Management

Handle evolving schemas, type conflicts, and multi-source data:

- **[Schema Evolution](creating-dataframes.md#schema-evolution-across-files)** - Reading files with different schemas
- **[Type Coercion](creating-dataframes.md#type-coercion-rules)** - Automatic type conversions and hierarchies
- **[Schema Conflicts](creating-dataframes.md#handling-schema-conflicts)** - Union strategies and explicit casting
- **[Schema Inspection](creating-dataframes.md#schema-inspection-and-introspection)** - Programmatic schema validation

## Error Handling & Recovery

Build resilient production systems:

- **[Common Errors](creating-dataframes.md#common-creation-errors)** - File not found, schema inference failures, pattern matching
- **[Schema Validation](creating-dataframes.md#schema-validation-and-recovery)** - Verify schemas before processing
- **[Partial Failures](creating-dataframes.md#handling-partial-file-read-failures)** - Continue when some files fail
- **[Timeout Handling](creating-dataframes.md#timeout-handling-for-remote-sources)** - Graceful timeouts for remote sources

## Real-World Integration Patterns

Production-ready examples for common scenarios:

- **[S3 and Cloud Storage](creating-dataframes.md#reading-from-cloud-storage-s3)** - Read from AWS S3, configure credentials
- **[Partitioned Data](creating-dataframes.md#incrementalpartitioned-data-loading)** - Daily/hourly partition loading
- **[Compressed Files](creating-dataframes.md#reading-compressed-files)** - GZIP, Bzip2, Zstandard support
- **[Glob Patterns](creating-dataframes.md#advanced-glob-patterns)** - Recursive and multi-pattern file discovery

## Performance Optimizations

Maximize throughput and minimize I/O:

- **[Column Projection](creating-dataframes.md#column-projection-reading-only-needed-columns)** - Read only required columns
- **[Predicate Pushdown](creating-dataframes.md#predicate-pushdown-filtering-at-source)** - Push filters to file readers
- **[Partition Pruning](creating-dataframes.md#partition-pruning)** - Skip irrelevant partitions
- **[Parquet Statistics](creating-dataframes.md#statistics-based-optimization-for-parquet)** - Leverage min/max/null statistics
- **[External Parquet Indexes](#external-parquet-indexes)** - Dramatically accelerate queries on large datasets

### External Parquet Indexes

For production workloads with large Parquet datasets, DataFusion supports external indexes to dramatically improve query performance beyond built-in Parquet statistics.

#### How External Indexes Work

When you register a Parquet table, the optimizer can use pre-built indexes to:

- **Skip entire row groups** that don't match your filters (beyond basic Parquet statistics)
- **Prune partitions more aggressively** based on indexed columns
- **Answer min/max queries** without scanning data

External indexes are separate data structures that map values to row group locations. They're particularly effective for:

- High-cardinality columns (user IDs, transaction IDs)
- Range queries on sorted or semi-sorted data
- Multi-column predicates where statistics alone aren't sufficient

#### Performance Impact

A 1TB Parquet table with an external index can reduce scan time from **minutes to seconds** for selective queries. The table registration remains the same—indexes are transparent to your application code.

#### Example Usage

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register table normally (indexes are configured separately)
    ctx.register_parquet(
        "events",
        "s3://bucket/events/",
        ParquetReadOptions::default()
    ).await?;

    // Queries automatically benefit from indexes if present
    let df = ctx.table("events").await?
        .filter(col("timestamp").gt(lit("2024-01-01")))?  // Index helps skip old data
        .filter(col("user_id").eq(lit(12345)))?;          // Index locates specific user

    df.show().await?;
    Ok(())
}
```

#### When to Use External Indexes

✅ **Use when:**

- Queries filter on high-cardinality columns
- Dataset is large (100GB+) and query selectivity is high (< 10% of data)
- You can afford the storage overhead (typically 5-15% of data size)
- Read patterns are predictable

❌ **Skip when:**

- Full table scans are common
- Dataset is small (< 10GB)
- Columns have low cardinality (better served by partitioning)
- Write frequency is very high (index maintenance overhead)

> **Implementation details**: External indexes require setup and maintenance. See the [External Parquet Indexes blog post](https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/) for implementation guides, index creation tools, and performance benchmarks.

### Parquet Metadata Caching and Pruning

DataFusion implements sophisticated Parquet optimization techniques that become especially powerful with registered tables:

#### Metadata Caching

When you register a Parquet table, DataFusion caches:

- File footers (schema, row group locations)
- Column statistics (min/max values per row group)
- Optional Bloom filters and page indexes

This avoids expensive re-parsing for each query. For remote storage (S3/GCS), this eliminates repeated network round-trips.

#### Hierarchical Pruning

DataFusion progressively narrows data reads through multiple levels:

1. **File pruning** - Skip entire files based on partitioning or statistics
2. **Row group pruning** - Skip row groups using min/max statistics
3. **Page pruning** - Skip individual pages within row groups (when page indexes exist)

See [Parquet Pruning in DataFusion](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/) for implementation details.

#### Advanced Metadata Strategies

For very large deployments, consider:

- **External metadata stores** - Cache Parquet metadata in Redis/PostgreSQL
- **Custom indexes** - Build value-to-rowgroup mappings for high-cardinality columns
- **Metadata-only tables** - Query statistics without touching data files

See [Using External Indexes, Metadata Stores, Catalogs and Caches](https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/) for production patterns.

## Configuration Tuning

Fine-tune DataFusion for your workload:

- **[Key Settings](creating-dataframes.md#key-configuration-options-for-creation)** - Batch size, partitions, repartitioning
- **[Batch Size Impact](creating-dataframes.md#batch-size-impact)** - Memory vs throughput trade-offs
- **[Parallelism Control](creating-dataframes.md#target-partitions-and-parallelism)** - Control concurrent operations
- **[Format Options](creating-dataframes.md#format-specific-configuration)** - CSV delimiters, Parquet pruning
- **[Memory Limits](creating-dataframes.md#memory-limits-and-spilling)** - Configure spilling to disk
- **[Connection Pooling](creating-dataframes.md#connection-pooling-for-remote-sources)** - S3/remote source optimization

## Ecosystem Interoperability

Integrate with other systems and frameworks:

- **[Arrow Flight](creating-dataframes.md#creating-dataframes-from-arrow-flight)** - Network data transfer with Flight servers
- **[Serde Structs](creating-dataframes.md#creating-from-serde-structs)** - Convert Rust structs to DataFrames
- **[Kafka/Streaming](creating-dataframes.md#streaming-sources-conceptual-example)** - Micro-batching pattern for event streams

> **Note on streaming**: DataFusion is batch-oriented. Native streaming support is under development—see the [Streaming Execution EPIC](https://synnada.notion.site/EPIC-Long-running-stateful-execution-support-for-unbounded-data-with-mini-batches-a416b29ae9a5438492663723dbeca805) and [issue #4285](https://github.com/apache/datafusion/issues/4285).

## Debugging & Best Practices

Avoid pitfalls and diagnose issues:

- **[Creation Anti-patterns](creating-dataframes.md#creation-anti-patterns)** - 6 common mistakes to avoid
- **[Performance Tips](creating-dataframes.md#performance-tips-summary)** - Do's and don'ts summary table
- **[Query Plan Inspection](creating-dataframes.md#inspecting-query-plans)** - Use `explain()` to debug
- **[Sampling Strategies](creating-dataframes.md#sampling-large-files)** - Test on samples before full scans
- **[File Validation](creating-dataframes.md#validating-file-contents)** - Check schemas, nulls, row counts
- **[Progress Monitoring](creating-dataframes.md#monitoring-progress)** - Track long-running operations

---

> **Related Guides:**
>
> - [Creating DataFrames](creating-dataframes.md) - Main creation guide (basic + advanced)
> - [Concepts](concepts.md) - SessionContext, LogicalPlan, lazy evaluation
> - [Best Practices](best-practices.md) - General performance and debugging
> - [Transformations](transformations.md) - Filter, join, aggregate operations
