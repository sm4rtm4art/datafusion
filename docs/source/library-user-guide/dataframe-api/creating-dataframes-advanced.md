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
