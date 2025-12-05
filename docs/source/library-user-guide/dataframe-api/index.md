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

# DataFrame API Documentation

This guide provides comprehensive documentation for using DataFusion's [`DataFrame`] API in Rust applications. For an introduction to DataFrames and their conceptual foundation, see the [User Guide](../../user-guide/dataframe.md).

## Overview

DataFusion [`DataFrame`]s are modeled after the [Pandas DataFrame] interface and implemented as a thin wrapper over a [`LogicalPlan`] that adds functionality for building and executing query plans.

**Key properties:**

- **Lazy evaluation**: Transformations build up an optimized query plan that executes only when you call an action method like [`.collect()`] or [`.show()`]
- **Arrow-based columnar structure**: DataFrames represent data in Apache Arrow's columnar format, where each column is stored as a contiguous, typed array
- **Strict type uniformity**: Each column enforces a single Arrow [`DataType`]—heterogeneous types within a column are not permitted, maintaining Arrow's type safety and vectorization guarantees
- **Immutable transformations**: DataFrame methods return new DataFrames, leaving the original unchanged (functional programming style)

```{contents}
:local:
:depth: 2
```

## The DataFrame Lifecycle

The documentation follows the **lifecycle of a DataFrame**—from creation to execution (Inspiered by [the desctiption of physics of photons][photon]):

| Phase             | Document                                      | What Happens                                |
| ----------------- | --------------------------------------------- | ------------------------------------------- |
| **Understanding** | [Concepts](concepts.md)                       | What are DataFrames and where do they live? |
| **Birth**         | [Creating DataFrames](creating-dataframes.md) | Instantiate from files, SQL, in-memory data |
| **Health**        | [Schema Management](schema-management.md)     | Inspect, validate, and evolve schema        |
| **Life**          | [Transformations](transformations.md)         | Filter, join, aggregate, sort, enrich       |
| **Death**         | [Writing & Executing](writing-dataframes.md)  | Materialize results or persist to storage   |
| **Wellness**      | [Best Practices](best-practices.md)           | Optimize performance and debug issues       |
| **Graduation**    | [Advanced Topics](dataframes-advance.md)      | S3, Kafka, Arrow Flight, custom execution   |

## Quick Navigation

| I want to...                            | Go to                                                                              |
| --------------------------------------- | ---------------------------------------------------------------------------------- |
| **Understand the basics**               |                                                                                    |
| Learn what DataFrames are conceptually  | [User Guide](../../user-guide/dataframe.md)                                        |
| Understand Arrow & RecordBatches        | [Arrow Introduction](../../user-guide/arrow-introduction.md)                       |
| Learn about data types                  | [Data Types](../../user-guide/sql/data_types.md)                                   |
| **Create DataFrames**                   |                                                                                    |
| Create my first DataFrame               | [Creating DataFrames](creating-dataframes.md)                                      |
| Understand SessionContext & LogicalPlan | [Concepts](concepts.md)                                                            |
| **Transform data**                      |                                                                                    |
| Filter, join, or aggregate data         | [Transformations](transformations.md)                                              |
| Mix SQL with DataFrames                 | [Transformations § SQL](transformations.md#mixing-sql-and-dataframes)              |
| **Execute and write results**           |                                                                                    |
| Execute DataFrames and get results      | [Writing & Executing](writing-dataframes.md#dataframe-execution-in-memory-results) |
| Save results to files                   | [Writing DataFrames](writing-dataframes.md#writing-dataframes-persistent-storage)  |
| Stream large results                    | [Streaming Execution](writing-dataframes.md#streaming-execution)                   |
| **Optimize & Debug**                    |                                                                                    |
| Improve query performance               | [Best Practices](best-practices.md)                                                |
| Debug query plans                       | [Best Practices § Debugging](best-practices.md#debugging-techniques)               |
| Configure batch sizes                   | [Best Practices § Configuration](best-practices.md#physical-optimizer-controls)    |

## Common Operations Quick Reference

```text
SQL                          DataFrame API
─────────────────────────────────────────────────────────────────────────
SELECT a, b                  df.select(vec![col("a"), col("b")])?
WHERE a > 5                  df.filter(col("a").gt(lit(5)))?
GROUP BY a, SUM(b)           df.aggregate(vec![col("a")], vec![sum(col("b"))])?
ORDER BY a DESC              df.sort(vec![col("a").sort(false, true)])?
LIMIT 10                     df.limit(0, Some(10))?
JOIN ... USING (id)          left.join(right, JoinType::Inner, &["id"], &["id"], None)?
DISTINCT                     df.distinct()?
UNION ALL                    df1.union(df2)?
```

**Execution actions:**

```text
Action                       DataFrame API
─────────────────────────────────────────────────────────────────────────
Show results                 df.show().await?
Collect to memory            df.collect().await?
Stream results               df.execute_stream().await?
Explain plan                 df.explain(false, false)?.show().await?
Cache for reuse              df.cache().await?
```

## I/O Quick Reference

**Reading data:**

```rust
// CSV
ctx.read_csv("data.csv", CsvReadOptions::new()).await?

// JSON (NDJSON)
ctx.read_json("data.json", NdJsonReadOptions::default()).await?

// Parquet
ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?

// In-memory
ctx.read_batch(record_batch)?
```

**Writing data:**

```rust
// Parquet (partitioned by year)
df.write_parquet(
    "out/",
    DataFrameWriteOptions::new().with_partition_by(vec!["year".to_string()]),
    None
).await?

// Parquet (single file)
df.write_parquet(
    "out/file.parquet",
    DataFrameWriteOptions::new().with_single_file_output(true),
    None
).await?

// CSV with compression
df.write_csv(
    "out.csv.gz",
    DataFrameWriteOptions::new(),
    Some(CsvOptions::default()
        .with_delimiter(b'\t')
        .with_has_header(true)
        .with_compression(CompressionTypeVariant::GZIP))
).await?
```

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`DataType`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html
[Pandas DataFrame]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[photon]: https://www.sciencedaily.com/releases/2007/04/070402122514.htm
