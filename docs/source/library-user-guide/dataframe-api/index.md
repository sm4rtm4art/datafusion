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

## Quick Navigation

### I want to...

- [Create my first DataFrame](creating-dataframes.md#quick-start) - Start here if you're new
- [Understand core concepts](concepts.md) - SessionContext, LogicalPlan, lazy evaluation
- [Transform data](transformations.md) - Filter, join, aggregate operations
- [Write results to files](writing-dataframes.md) - Export to Parquet, CSV, JSON
- [Optimize performance](best-practices.md) - Tuning & debugging

### Complete Guide

1. **[Concepts](concepts.md)** - Core foundations: SessionContext, LogicalPlan relationships, lazy evaluation, null values
2. **[Creating DataFrames](creating-dataframes.md)** - All creation methods (files, tables, SQL, in-memory) and execution methods
3. **[Transformations](transformations.md)** - Data manipulation: selection, filtering, aggregation, joins, sorting, set operations, subqueries, SQL mixing
4. **[Writing DataFrames](writing-dataframes.md)** - Persistence: writing to Parquet, CSV, JSON, and registered tables
5. **[Best Practices](best-practices.md)** - Performance tuning, debugging techniques, common pitfalls, and optimization strategies

## 🎯 Quick Navigation

| I want to...                            | Go to                                                                           |
| --------------------------------------- | ------------------------------------------------------------------------------- |
| **Understand the basics**               |                                                                                 |
| Learn what DataFrames are conceptually  | [User Guide](../../user-guide/dataframe.md)                                     |
| Understand Arrow & RecordBatches        | [Arrow Introduction](../../user-guide/arrow-introduction.md)                    |
| Learn about data types                  | [Data Types](../../user-guide/sql/data_types.md)                                |
| **Work with DataFrames**                |                                                                                 |
| Create my first DataFrame               | [Creating DataFrames](creating-dataframes.md)                                   |
| Understand SessionContext & LogicalPlan | [Concepts](concepts.md)                                                         |
| Filter, join, or aggregate data         | [Transformations](transformations.md)                                           |
| Mix SQL with DataFrames                 | [Transformations § SQL](transformations.md#mixing-sql-and-dataframes)           |
| Save results to files                   | [Writing DataFrames](writing-dataframes.md)                                     |
| **Optimize & Debug**                    |                                                                                 |
| Improve query performance               | [Best Practices](best-practices.md)                                             |
| Debug query plans                       | [Best Practices § Debugging](best-practices.md#debugging-techniques)            |
| Configure batch sizes                   | [Best Practices § Configuration](best-practices.md#physical-optimizer-controls) |

## Common Operations Quick Reference

- SELECT a,b → `df.select(vec![col("a"), col("b")])?`
- WHERE a > 5 → `df.filter(col("a").gt(lit(5)))?`
- GROUP BY a, SUM(b) → `df.aggregate(vec![col("a")], vec![sum(col("b"))])?`
- ORDER BY a DESC → `df.sort(vec![col("a").sort(false, true)])?`
- LIMIT 10 → `df.limit(0, Some(10))?`
- JOIN USING (id) → `left.join(right, JoinType::Inner, &["id"], &["id"], None)?`
- DISTINCT → `df.distinct()?`
- UNION → `df1.union(df2)?`
- SHOW → `df.show().await?`
- EXPLAIN → `df.explain(false, false)?`

## I/O Quick Reference

- Read CSV → `ctx.read_csv("data.csv", CsvReadOptions::new()).await?`
- Read JSON (NDJSON) → `ctx.read_json("data.json", NdJsonReadOptions::default()).await?`
- Read Parquet → `ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?`
- Write Parquet (partitioned) → `df.write_parquet("out/", DataFrameWriteOptions::new().with_partition_by(vec!["year".to_string()]), None).await?`
- Write Parquet (single file) → `df.write_parquet("out/file.parquet", DataFrameWriteOptions::new().with_single_file_output(true), None).await?`
- Write CSV (gzip, tab) → `df.write_csv("out.csv.gz", DataFrameWriteOptions::new(), Some(CsvOptions::default().with_delimiter(b'\t').with_has_header(true).with_compression(CompressionTypeVariant::GZIP))).await?`
- Register in-memory table → `ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![batches])?))?`
