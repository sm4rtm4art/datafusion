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

# Datafusion DataFrame API

A DataFrame represents a logical set of rows with the same named columns, similar to a [Pandas DataFrame] or [Spark DataFrame]. DataFusion's DataFrames combine the familiar, intuitive API you know with powerful database optimizations and Arrow-native execution.

## Why DataFrames? Bridging Data Science and Database Systems

The data science community innovated the DataFrame pattern, originated by the [Pandas DataFrame] API, as a way to work locally with structured and semi-structured data. However, early DataFrame implementations initially performed poorly because they didn't incorporate well-studied database techniques like query planning, optimization, and parallel vectorized execution. One step toward addressing this was the creation of [Apache Arrow](https://jorgecarleitao.github.io/arrow2/main/guide/arrow.html#what-is-apache-arrow)—a language-independent columnar memory format that enables zero-copy data sharing between systems—born from a desire to bring such well-studied database systems techniques to the data science ecosystem.

---

**New to Arrow?** Learn more about Arrow's columnar format:

- [Arrow Introduction](arrow-introduction.md) – DataFusion's gentle introduction to Arrow and RecordBatches
- [What is Apache Arrow?](https://jorgecarleitao.github.io/arrow2/main/guide/arrow.html#what-is-apache-arrow) – Deeper dive into Arrow's design
- [Official Apache Arrow docs](https://arrow.apache.org/) - Official Arrow docs

---

This trend toward building specialized systems from reusable components—often called the ["deconstructed database"](https://www.usenix.org/publications/login/winter2018/khurana)—enables teams to mix-and-match best-in-class parts instead of relying on a single monolith. Examples include:

- **Compute engines:** _Velox_ (C++ vectorized execution), _Apache Spark_ (distributed DataFrames), _DuckDB_ (embedded SQL analytics), and _Polars_ (Arrow-native DataFrames).
- **Columnar storage:** _Parquet_ on object stores complements Arrow's in-memory layout.
- **Open table formats:** _Apache Iceberg_ and _Delta Lake_ add ACID transactions, time-travel, and schema evolution—essential for governed data-mesh architectures.

### Where DataFusion and its DataFrame API fits

DataFusion is an _embeddable_, **Rust-native** query engine that bridges the familiar DataFrame interface with battle-tested database optimizations. Built on Apache Arrow, it provides:

- **Dual interfaces:** Ergonomic **DataFrame API** _and_ full SQL parser/planner in the **same** engine—use either or mix them freely
- **Arrow-native execution:** Zero-copy data sharing with other Arrow-compatible systems
- **Lightweight deployment:** Small, dependency-free binaries (no JVM)—ideal for embedded analytics, microservices, IoT, or WebAssembly
- **Flexible scaling:** From in-process analytics to distributed clusters via [DataFusion Ballista]; can also accelerate Apache Spark through [DataFusion Comet]

The DataFusion DataFrame API—modeled after pandas but built on Arrow—gives you an intuitive programmatic interface while leveraging query optimization, parallel execution, and zero-copy data sharing under the hood. Whether you're building a data-mesh compute kernel, an embedded analytics service, or a complex data pipeline, DataFusion integrates seamlessly with modern data infrastructure like Parquet, Apache Iceberg, Delta Lake, and Arrow Flight.

### How DataFrames Work: Lazy Evaluation and Arrow Output

A DataFrame is a **lazy description** of a computation, not the data itself. When you build a DataFrame (e.g., scanning a file or table, applying filters, joins), you're constructing a logical plan that DataFusion optimizes and executes only when you explicitly request results via methods like [`collect`], [`show()`], or file writes.

#### Understanding Lazy Evaluation

**What "lazy" means:**

When you call transformation methods like `filter()`, `select()`, or `join()`, DataFusion doesn't immediately process any data. Instead, it:

1. **Builds a query plan**: Each transformation adds nodes to a logical plan tree
2. **Defers execution**: No data is read or processed yet
3. **Optimizes holistically**: When you finally execute, DataFusion can optimize the entire plan as a whole

**Why lazy evaluation is powerful:**

```rust
// Example: None of this reads or processes data yet!
let df = ctx.read_parquet("sales_2024.parquet", ParquetReadOptions::default()).await?
    .filter(col("region").eq(lit("EMEA")))?              // Just adds a filter node
    .select(vec![col("product_id"), col("revenue")])?    // Just adds a projection node
    .aggregate(vec![col("product_id")], vec![sum(col("revenue"))])?; // Just adds aggregation

// The query plan is built, but DataFusion hasn't opened the file yet!
// You could inspect the plan with df.explain() or continue adding transformations...

// Only when you call an action does execution happen:
let results = df.collect().await?;

// What actually happened during execution:
// 1. Optimizer pushed the filter down to the Parquet reader
// 2. Optimizer determined only "region", "product_id", and "revenue" columns are needed
// 3. Parquet reader skipped irrelevant row groups and columns
// 4. Data processed in a single optimized pass
```

**Contrast with eager evaluation:**

In an eager system, each operation would:

- Process the entire dataset immediately
- Create intermediate results
- Require multiple passes over the data
- Use significantly more memory

```rust
// Hypothetical eager system (NOT how DataFusion works):
let df1 = read_parquet("sales_2024.parquet");     // Loads entire file into memory
let df2 = df1.filter(col("region").eq("EMEA"));    // Scans all data, creates new dataset
let df3 = df2.select(...);                         // Scans filtered data, creates another dataset
let df4 = df3.aggregate(...);                      // Scans projected data, final result
```

**Lazy vs Eager methods:**

DataFusion methods fall into two categories:

- **Lazy (transformations)**: `filter()`, `select()`, `join()`, `aggregate()`, `sort()` - build the plan
- **Eager (actions)**: `collect()`, `show()`, `execute_stream()`, `write_parquet()` - trigger execution

This lazy/eager distinction allows DataFusion to:

- See the entire query before execution
- Apply optimizations across all operations (e.g., predicate pushdown, projection pruning, join reordering)
- Minimize data movement and memory usage
- Push filters and projections down to the data source

#### Arrow Output Format

When executed, DataFusion produces results as Arrow RecordBatches: each batch contains a set of Arrow Arrays (one per column) that follow a shared Arrow Schema. Whether you build queries using DataFrames or SQL, DataFusion always produces results in Arrow's columnar format—enabling zero-copy integration with other Arrow-compatible tools and languages.

**Execution flow:**

```
SessionContext
  ↓ creates
DataFrame (lazy) ←─────────┐
  ↓ wraps                   │
LogicalPlan                 │ Logical optimizations/
  ↓                         │ transformations
Optimized LogicalPlan ──────┘
  ↓ plans into
ExecutionPlan ←────────────────┐
  ↓                            │ Physical optimizations/
Optimized ExecutionPlan ───────┘ transformations
  ↓ executes to
Streams → RecordBatch (Arrow Arrays)
```

For comprehensive API documentation and advanced usage patterns, see the [Library Users Guide].

### Understanding Null Values: None, Null, and NaN

When working with DataFrames, it's critical to understand the distinction between three concepts that represent "missing" or "special" values:

| Concept    | What it is                                     | Example                         | Arrow representation        |
| ---------- | ---------------------------------------------- | ------------------------------- | --------------------------- |
| **`None`** | Rust's way to represent absence in `Option<T>` | `None` in `Option<f64>`         | Maps to Arrow null          |
| **`Null`** | SQL/Arrow concept for missing data             | `NULL` in SQL                   | Validity bitmap (bit = 0)   |
| **`NaN`**  | IEEE 754 floating-point value (Not a Number)   | `f64::NAN`, result of `0.0/0.0` | A _present_ value (bit = 1) |

**Critical distinction:**

```rust
// These are DIFFERENT:
Some(f64::NAN)  // Present value that happens to be NaN (not null!)
None            // Absent value (null in SQL terms)

// Example in a DataFrame:
let df = dataframe!(
    "result" => [Some(1.0), Some(f64::NAN), None]  // 1.0, NaN, NULL
    //                      ^^^^^^^^^^^^^^  ^^^^
    //                      present NaN     absent (null)
)?;
```

**How this affects queries:**

- **`COUNT(*)`**: Counts NaN, excludes NULL
- **`SUM(col)`**: Propagates NaN (result is NaN), skips NULL
- **`IS NULL`**: Returns `false` for NaN, `true` for NULL
- **`IS NAN`**: Returns `true` for NaN, `false` for NULL

Understanding this distinction is essential when working with floating-point data and missing values.

## API at a Glance

This section provides a quick reference to the most commonly used methods. For the complete API, see the [API reference on docs.rs].

### DataFrame Methods

| Category                        | Methods                                                                          | SQL Equivalent                             | Notes                                |
| ------------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------ |
| **Transformations**             |                                                                                  |                                            |                                      |
| Selection                       | [`select()`], [`select_columns()`], [`drop_columns()`]                           | `SELECT`, `SELECT * EXCEPT`                | Choose/compute columns               |
| Filtering                       | [`filter()`]                                                                     | `WHERE`                                    | Row-level predicates                 |
| Aggregation                     | [`aggregate()`]                                                                  | `GROUP BY`                                 | Grouping with agg functions          |
| Joins                           | [`join()`]                                                                       | `JOIN`                                     | Inner, Left, Right, Full, Semi, Anti |
| Sorting                         | [`sort()`]                                                                       | `ORDER BY`                                 | Multi-column sorting                 |
| Limiting                        | [`limit()`]                                                                      | `LIMIT`, `OFFSET`                          | Pagination support                   |
| Set Operations                  | [`union()`], [`union_distinct()`], [`distinct()`], [`intersect()`], [`except()`] | `UNION`, `DISTINCT`, `INTERSECT`, `EXCEPT` | Combine DataFrames                   |
| **Actions** (trigger execution) |                                                                                  |                                            |                                      |
| Collect Results                 | [`collect()`], [`collect_partitioned()`]                                         | -                                          | Materialize to `Vec<RecordBatch>`    |
| Stream Results                  | [`execute_stream()`], [`execute_stream_partitioned()`]                           | -                                          | Incremental results                  |
| Cache                           | [`cache()`]                                                                      | -                                          | Materialize for reuse                |
| Display                         | [`show()`], [`show_limit()`]                                                     | -                                          | Print to stdout                      |
| **Introspection**               |                                                                                  |                                            |                                      |
| Schema                          | [`schema()`]                                                                     | `DESCRIBE`                                 | Get result schema                    |
| Explain                         | [`explain()`]                                                                    | `EXPLAIN`                                  | View query plan                      |
| **I/O**                         |                                                                                  |                                            |                                      |
| Write Files                     | [`write_parquet()`], [`write_csv()`], [`write_json()`]                           | `COPY TO` (SQL)                            | Export to files                      |
| Write Table                     | [`write_table()`]                                                                | `INSERT INTO`                              | Write to registered table            |

### SessionContext Methods

| Category               | Methods                                                              | Purpose                           | Example Use Case           |
| ---------------------- | -------------------------------------------------------------------- | --------------------------------- | -------------------------- |
| **DataFrame Creation** |                                                                      |                                   |                            |
| From Files             | [`read_csv()`], [`read_parquet()`], [`read_json()`], [`read_avro()`] | Load from file formats            | Reading datasets           |
| From Memory            | [`read_batch()`], [`read_batches()`]                                 | Create from Arrow RecordBatch     | Testing, in-memory data    |
| From Tables            | [`table()`]                                                          | Get registered table as DataFrame | Query existing tables      |
| From SQL               | [`sql()`]                                                            | Execute SQL, get DataFrame        | SQL → DataFrame workflows  |
| **Table Management**   |                                                                      |                                   |                            |
| Register Tables        | [`register_table()`], [`register_batch()`]                           | Make data queryable by name       | Share data between queries |
| Register Files         | [`register_csv()`], [`register_parquet()`]                           | Register files as tables          | Avoid re-reading files     |
| Deregister             | [`deregister_table()`]                                               | Remove table                      | Clean up                   |
| **Administration**     |                                                                      |                                   |                            |
| Config                 | [`with_config()`], [`state()`]                                       | Access configuration/state        | Performance tuning         |
| Table Providers        | [`register_table_provider()`]                                        | Custom data sources               | Advanced integrations      |

**Note**: This is not exhaustive. See the [Library Users Guide] for comprehensive examples and the [API reference on docs.rs] for all available methods.

## Example

The DataFrame struct is part of DataFusion's `prelude` and can be imported with
the following statement.

```rust
use datafusion::prelude::*;
```

Here is a minimal example showing the execution of a query using the DataFrame API.

Create DataFrame using macro API from in memory rows

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a new dataframe with in-memory data using macro
    let df = dataframe!(
        "a" => [1, 2, 3],
        "b" => [true, true, false],
        "c" => [Some("foo"), Some("bar"), None]
    )?;
    df.show().await?;
    Ok(())
}
```

Create DataFrame from file or in memory rows using standard API

```rust
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::min;
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Read the data from a csv file
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    let df = df.filter(col("a").lt_eq(col("b")))?
        .aggregate(vec![col("a")], vec![min(col("b"))])?
        .limit(0, Some(100))?;
    // Print results
    df.show().await?;

    // Create a new dataframe with in-memory data
    let schema = Schema::new(vec![
      Field::new("id", DataType::Int32, true),
      Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
      Arc::new(schema),
      vec![
          Arc::new(Int32Array::from(vec![1, 2, 3])),
          Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
      ],
    )?;
    let df = ctx.read_batch(batch)?;
    df.show().await?;

    Ok(())
}
```

[pandas dataframe]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
[spark dataframe]: https://spark.apache.org/docs/latest/sql-programming-guide.html
[datafusion ballista]: https://datafusion.apache.org/ballista/
[datafusion comet]: https://datafusion.apache.org/comet/#
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`read_csv`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`filter`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`select`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`aggregate`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[`limit`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit
[`collect`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`select_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_columns
[`drop_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns
[`filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`aggregate()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[`join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[`union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`union_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_distinct
[`distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[`intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`except()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[`collect_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect_partitioned
[`execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`execute_stream_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream_partitioned
[`cache()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.cache
[`show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`show_limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show_limit
[`schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`write_parquet()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`write_csv()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_csv
[`write_json()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_json
[`write_table()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_table
[`read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`read_avro()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`read_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batch
[`read_batches()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batches
[`table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table
[`sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`register_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch
[`register_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_csv
[`register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`deregister_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.deregister_table
[`with_config()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.with_config
[`state()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.state
[`register_table_provider()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table_provider
[library users guide]: ../library-user-guide/using-the-dataframe-api.md
[api reference on docs.rs]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[expressions reference]: expressions
