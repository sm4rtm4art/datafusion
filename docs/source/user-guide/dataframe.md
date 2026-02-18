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

## Why DataFrames?

_Bridging Data Science and Database Systems_

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

### Meet Your Data

**Real-world data is messy — we create a DataFrame with common data quality problems to clean throughout the following sections.**

Duplicates, inconsistent casing, nulls, missing values in essential columns, invalid formats, and outliers are everyday challenges. To highlight the benefits of the DataFrame API, the following sections form a narrative tutorial — we create our own dataset using the [`dataframe!`] macro with all these common problems baked in (see [Creating DataFrames](../library-user-guide/dataframe-api/creating-dataframes.md#5-from-inline-data-using-the-dataframe-macro) for details):

```rust
use datafusion::prelude::*;

// Create sample data (see Creating DataFrames for the dataframe! macro)
// This represents ORDER LINE ITEMS — multiple items can share the same order_id
let sales = dataframe!(
    "order_id" => [1, 1, 1, 2, 3, 4, 5, 6, 7, None::<i64>],
    "customer" => ["Alice", "Alice", "Alice", "Alice", "bob", "BOB", " Charlie ", "Dave", "Dave", "Eve"],
    "amount" => [100.0, 100.0, 10.0, 150.0, -50.0, 200.0, 180.0, 180.0, 300.0, 99999.0],
    "status" => ["complete", "complete", "complete", "pending", "pending", "PENDING", "complete", None, "complete", "pending"],
    "date" => ["2026-01-01", "2026-01-01", "2026-01-01", "2026-01-15", "2026-01-02", "2026-01-03", "invalid", "2026-01-04", "2026-01-20", "2026-01-06"]
)?;

sales.clone().show().await?;  // SQL: SELECT * FROM sales
```

> **Note:** All code snippets in this section continue from this setup — the [`prelude`] import and `sales` DataFrame are assumed available throughout.

The dataframe! macro results in the following DataFrame:

```text
+----------+------------+---------+----------+------------+
| order_id | customer   | amount  | status   | date       |
+----------+------------+---------+----------+------------+
| 1        | Alice      | 100.0   | complete | 2026-01-01 |
| 1        | Alice      | 100.0   | complete | 2026-01-01 |
| 1        | Alice      | 10.0    | complete | 2026-01-01 |
| 2        | Alice      | 150.0   | pending  | 2026-01-15 |
| 3        | bob        | -50.0   | pending  | 2026-01-02 |
| 4        | BOB        | 200.0   | PENDING  | 2026-01-03 |
| 5        |  Charlie   | 180.0   | complete | invalid    |
| 6        | Dave       | 180.0   |          | 2026-01-04 |
| 7        | Dave       | 300.0   | complete | 2026-01-20 |
|          | Eve        | 99999.0 | pending  | 2026-01-06 |
+----------+------------+---------+----------+------------+
```

> **Best Practice:** In production, define an explicit schema where `order_id` is non-nullable — the database would reject that last row at insert time. See [Schema Management](../library-user-guide/dataframe-api/schema-management.md) for how to enforce constraints upfront rather than cleaning them later.

### Exploring the Data: Cheap to Expensive

**Start with cheap operations, then sample, then analyze the full dataset.**

DataFusion DataFrames are **immutable** — every transformation (`.filter()`, `.select()`, `.with_column()`) creates a new DataFrame, leaving the original unchanged. Combined with **lazy execution**, transformations build a logical plan without touching data until you call a terminal action (`.show()`, `.collect()`, `.count()`). This means `.schema()` is free (reads plan metadata), while `.describe()` triggers a full scan. Use this to your advantage: the original data is always safe, and you can validate your approach on cheap operations before running expensive ones.
For more information, see the [DataFrame Execution part](../library-user-guide/dataframe-api/writing-dataframes.md#dataframe-execution)

| Operation         | Cost      | What it does                          |
| ----------------- | --------- | ------------------------------------- |
| [`.schema()`]     | Free      | Reads metadata only — no data scanned |
| [`.show_limit()`] | Cheap     | Preview first N rows                  |
| [`.show()`]       | Expensive | Shows all data                        |
| [`.count()`]      | Expensive | Scans all rows to count them          |
| [`.describe()`]   | Expensive | Multiple aggregations on all rows     |

(1-check-the-schema-free--understand-types-before-touching-data)=
#### 1. Check the schema (free) — understand types before touching data:

With the [`.schema()`] method, DataFusion only reads the metadata. For further reading, see the DataFusion DataFrame API documentation on [schema management](../library-user-guide/dataframe-api/schema-management.md).

```rust
println!("{}", sales.schema());
```

This results in the following schema:

```text
Schema {
    fields: [
        Field { name: "order_id", data_type: Int64, nullable: true },
        Field { name: "customer", data_type: Utf8, nullable: true },
        Field { name: "amount", data_type: Float64, nullable: true },
        Field { name: "status", data_type: Utf8, nullable: true },
        Field { name: "date", data_type: Utf8, nullable: true },
    ]
}
```

**Red flags from schema alone:**

- `date` is `Utf8` (string), not a proper date type — we'll need to convert it
- `order_id` is `nullable: true` — a primary key should never be null, yet our data has one

#### 2. Check the size (expensive, but necessary) — decide your strategy:

**[`.count()`]** returns the total number of rows. Knowing the dataset size helps you decide whether to run expensive operations directly or sample first — a 1,000-row dataset can be analyzed in full, but a 100-million-row dataset needs a different strategy:

```rust
let row_count = sales.clone().count().await?;  // Returns: 10 (usize)
```

With 10 rows, we can safely use [`.show()`] and [`.describe()`] on the full dataset. For larger datasets, you'd sample first, clean your data and then execute on the full dataset, to keep the iteration loop tight.

#### 3. Preview the data — spot obvious issues:

**Visual inspection catches problems that statistics miss — casing inconsistencies, whitespace, obviously wrong values.** Choose your preview method based on dataset size:

| Method                              | Executes                   | Use case                                      |
| ----------------------------------- | -------------------------- | --------------------------------------------- |
| [`.show_limit(n)`][`.show_limit()`] | Stops after first `n` rows | Large datasets, quick sanity checks           |
| [`.show()`]                         | Full plan, collects all    | Small datasets, complete view of data quality |

For large datasets, use [`.show_limit(n)`][`.show_limit()`] to peek at the first `n` rows without loading everything into memory:

```rust
df.show_limit(3).await?;  // Quick peek at first 3 rows
```

Results in our case for the first 3 rows (you will usually use 100 or 1000 rows):

```text
+----------+------------+---------+----------+------------+
| order_id | customer   | amount  | status   | date       |
+----------+------------+---------+----------+------------+
| 1        | Alice      | 100.0   | complete | 2026-01-01 |
| 2        | bob        | -50.0   | pending  | 2026-01-02 |
| 2        | BOB        | 200.0   | PENDING  | 2026-01-02 |
+----------+------------+---------+----------+------------+
```

Since our dataset is small (10 rows), we'll use [`.show()`] to see everything:

```rust
sales.clone().show().await?;
```

```text
+----------+------------+---------+----------+------------+
| order_id | customer   | amount  | status   | date       |
+----------+------------+---------+----------+------------+
| 1        | Alice      | 100.0   | complete | 2026-01-01 |
| 1        | Alice      | 100.0   | complete | 2026-01-01 |
| 1        | Alice      | 10.0    | complete | 2026-01-01 |
| 2        | Alice      | 150.0   | pending  | 2026-01-15 |
| 3        | bob        | -50.0   | pending  | 2026-01-02 |
| 4        | BOB        | 200.0   | PENDING  | 2026-01-03 |
| 5        |  Charlie   | 180.0   | complete | invalid    |
| 6        | Dave       | 180.0   |          | 2026-01-04 |
| 7        | Dave       | 300.0   | complete | 2026-01-20 |
|          | Eve        | 99999.0 | pending  | 2026-01-06 |
+----------+------------+---------+----------+------------+
```

**Issues visible in the full output:**

- Exact duplicate row (order_id=1, amount=100.0 appears twice)
- Multiple line items per order (order_id=1 has 3 items total — 2 duplicates + 1 different)
- Inconsistent casing (`bob` vs `BOB`, `pending` vs `PENDING`)
- Whitespace in names (`Charlie` has leading/trailing space)
- Negative amount (-50.0)
- Missing values (null `order_id`, null `status`)
- Invalid date format ("invalid")
- Suspicious outlier (99999.0)

> **Warning:** [`.show()`] collects _all_ results into memory — use [`.show_limit(n)`][`.show_limit()`] for large datasets. To explore different parts, use [`.limit(offset, count)`][`.limit()`] to skip and sample (e.g., `.limit(1000, Some(100))` skips first 1000, shows next 100). For complete large-dataset workflows, see [Advanced DataFrame Topics](../library-user-guide/dataframe-api/dataframes-advance.md).

#### 4. Analyze statistics (expensive) — reveal hidden issues:

**[`.describe()`]** creates a new Dataframe containing a summary statistics (count, null_count, mean, std, min, max, median) across all columns — revealing issues that visual inspection misses: outliers, skewed distributions, and null patterns hidden deep in your data.

For large datasets, start with a sample for fast iteration, then verify on the full dataset once your approach is validated:

```rust
// Step A: Analyze a sample first (fast feedback loop)
let sample = sales.clone().limit(0, Some(10_000))?;
sample.describe().await?.show().await?;

// Step B: Once satisfied, verify on the full dataset
sales.clone().describe().await?.show().await?;
```

This results in this new DataFrame and is represented as:

```text
+------------+----------+----------+-------------------+----------+------------+
| describe   | order_id | customer | amount            | status   | date       |
+------------+----------+----------+-------------------+----------+------------+
| count      | 9.0      | 10       | 10.0              | 9        | 10         |
| null_count | 1.0      | 0        | 0.0               | 1        | 0          |
| mean       | 3.33     | null     | 10116.9           | null     | null       |
| std        | 2.12     | null     | 31574.8           | null     | null       |
| min        | 1.0      | null     | -50.0             | null     | null       |
| max        | 7.0      | null     | 99999.0           | null     | null       |
| median     | 3.0      | null     | 165.0             | null     | null       |
+------------+----------+----------+-------------------+----------+------------+
```

**What [`.describe()`] reveals in our dataset:**

- **Null count**: 1 missing `order_id`, 1 missing `status` — need to filter or impute
- **Min amount**: -50.0 — negatives shouldn't exist in sales data
- **Max amount**: 99999.0 — suspicious outlier (data entry error?)
- **Mean vs median**: 14385 vs 175 — huge gap indicates outlier is skewing the mean

> **Note:** The output types differ by column type:
>
> | Column type | Output  | Supported stats                                | Example |
> | ----------- | ------- | ---------------------------------------------- | ------- |
> | Numeric     | Float64 | count, null_count, mean, std, min, max, median | `6.0`   |
> | String      | Utf8    | count, null_count, min, max                    | `7`     |
>
> String columns show `null` for mean, std, and median since those don't apply to text.

### Conclusion of the Happy little accidents in our dataset to fix:

This table provides a conclusive description of the common issues in our dataset and offers solutions or fixes for solving them, which should be discussed in the later subsections

| Issue               | Example                    | Fix                                    |
| ------------------- | -------------------------- | -------------------------------------- |
| Duplicates          | order_id 2, 4 appear twice | [`.distinct_on()`]                     |
| Inconsistent casing | "bob" vs "BOB"             | [`lower()`] or [`upper()`]             |
| Extra whitespace    | " Charlie "                | [`trim()`]                             |
| Invalid values      | negative amounts, NaN      | [`.filter()`]                          |
| Outliers            | 99999.0                    | [`.filter()`]                          |
| Nulls               | missing order_id, status   | [`.filter()`] or [`coalesce()`]        |
| Wrong type          | date as string, "invalid"  | [`to_date()`] with fail-safe filtering |

**Our goal**: Clean this into analyzable data with total sales by customer.

### Step 1: Filtering Out Invalid Records

**Filtering removes rows that would corrupt downstream analysis — nulls in primary keys, negative amounts, obvious outliers.**

In SQL, this would be a [`WHERE`] clause with multiple conditions joined by [`AND`]. With the DataFrame API, you have two equivalent options:

```rust
// Option 1: Chain multiple .filter() calls (readable, optimizer combines them)
let step1 = sales
    .filter(col("order_id").is_not_null())?
    .filter(col("amount").gt(lit(0)))?
    .filter(col("amount").lt(lit(10000)))?;

// Option 2: Combine predicates with .and() in a single filter
let step1 = sales.filter(
    col("order_id").is_not_null()
        .and(col("amount").gt(lit(0)))
        .and(col("amount").lt(lit(10000)))
)?;
```

Both produce the same execution plan. Use [`or()`] for [`OR`] logic. The chaining approach shines when filters are conditional — you can add or skip filters based on runtime logic without string concatenation.

```rust
step1.show().await?;
```

**Output after Step 1** (10 → 8 rows: removed null order_id, negative amount, outlier):

```
+----------+------------+--------+----------+------------+
| order_id | customer   | amount | status   | date       |
+----------+------------+--------+----------+------------+
| 1        | Alice      | 100.0  | complete | 2026-01-01 |
| 1        | Alice      | 100.0  | complete | 2026-01-01 |
| 1        | Alice      | 10.0   | complete | 2026-01-01 |
| 2        | Alice      | 150.0  | pending  | 2026-01-15 |
| 4        | BOB        | 200.0  | PENDING  | 2026-01-03 |
| 5        |  Charlie   | 180.0  | complete | invalid    |
| 6        | Dave       | 180.0  |          | 2026-01-04 |
| 7        | Dave       | 300.0  | complete | 2026-01-20 |
+----------+------------+--------+----------+------------+
```

> **Tip:** Notice we filter nulls first with [`.is_not_null()`]. If [`.filter()`] returns fewer rows than expected, nulls are often the culprit — comparisons with `NULL` return `NULL` (not `false`), so rows silently drop out. This is SQL's [three-valued logic] in action.

> **Best Practice:** Don't discard rejected rows silently! Capture them for review:
>
> ```rust
> let rejected = sales.filter(col("order_id").is_null()
>     .or(col("amount").lt_eq(lit(0)))
>     .or(col("amount").gt_eq(lit(10000))))?;
> rejected.write_parquet("rejected_rows.parquet", ...).await?;
> ```
>
> This creates an audit trail and helps identify upstream data quality issues.

> **Learn More:** For complex predicates and filter pushdown optimization, see [Filtering Rows](#filtering-rows-with-filter) in the Deep Dive section.

(step-2-cleaning-text-data)=
### Step 2: Cleaning Text Data

**Text normalization ensures consistent matching for a robust data pipeline.**

Text data requires special care: tabs and spaces cause invisible mismatches (`" Charlie"` ≠ `"Charlie"`), mixed casing breaks groupings (`"Bob"` ≠ `"bob"`), and encoding differences (UTF-8 vs ISO-8859) can corrupt comparisons entirely. DataFusion uses UTF-8 internally — if your source data uses a different encoding, convert it during ingestion before these cleaning steps. Use [`trim()`] for whitespace, [`lower()`] or [`upper()`] for casing.

In SQL, you'd write:

```sql
SELECT
order_id,
LOWER(TRIM(customer)) AS customer,
amount,
LOWER(status) AS status,
date
FROM step1
```

The goal: transform `customer` and `status` while keeping everything else unchanged. In SQL, you must explicitly list every column. The DataFrame API's [`.with_column()`] handles this automatically — it transforms the specified column, passes through all other columns AND all rows unchanged:

```rust
use datafusion::functions::string::expr_fn::{lower, trim};

let step2 = step1
    .with_column("customer", trim(vec![lower(col("customer"))]))?
    .with_column("status", lower(col("status")))?;

step2.show().await?;
```

**Output after Step 2** (8 → 8 rows: [`.with_column()`] transforms values, no rows lost):

```
+----------+----------+--------+----------+------------+
| order_id | customer | amount | status   | date       |
+----------+----------+--------+----------+------------+
| 1        | alice    | 100.0  | complete | 2026-01-01 |
| 1        | alice    | 100.0  | complete | 2026-01-01 |
| 1        | alice    | 10.0   | complete | 2026-01-01 |
| 2        | alice    | 150.0  | pending  | 2026-01-15 |
| 4        | bob      | 200.0  | pending  | 2026-01-03 |
| 5        | charlie  | 180.0  | complete | invalid    |
| 6        | dave     | 180.0  |          | 2026-01-04 |
| 7        | dave     | 300.0  | complete | 2026-01-20 |
+----------+----------+--------+----------+------------+
```

> **Tip:** Joins failing unexpectedly? Two common culprits: trailing spaces (`" Alice"` ≠ `"Alice"`) and case mismatches (`"Bob"` ≠ `"bob"`). Normalize with [`trim()`] and [`lower()`] or [`upper()`] before joining.

> **Learn More:** For the full range of string functions including [`substring()`], `replace()`, and regex operations, see **Step 2: Cleaning Text Data** in this guide.

### Step 3: Type Conversion with Fail-Safe Handling

**Type conversion transforms string data into proper types — enabling date arithmetic, correct sorting, and type-safe operations.**

Our schema inspection revealed `date` is `Utf8` (string), not a proper date type. This matters: string sorting puts "2024-12-01" before "2024-2-01" (lexicographic), while date sorting handles them correctly. Date arithmetic (`date + interval '1 day'`) only works on date types. Type conversion is where many pipelines silently fail — a single malformed value like "invalid" or "2024/01/01" (wrong separator) can crash the entire query.

> Recap from the [schema inspection](#1-check-the-schema-free--understand-types-before-touching-data):
>
> ```text
> Field { name: "date", data_type: Utf8, nullable: true },
> ```

In SQL, you'd use `CAST` or [`TO_DATE`]:

```sql
SELECT *,
TO_DATE(date, '%Y-%m-%d') AS parsed_date
FROM step2
-- But what happens when date = 'invalid'? The query fails!
```

The problem: [`TO_DATE('invalid')`][`to_date`] crashes the entire query. In production data, you'll encounter malformed dates, typos, legacy formats, and edge cases. The DataFrame API lets us handle this gracefully with two strategies:

#### Strategy 1: Filter first, then convert

Use [`.like()`] for pattern matching to keep only valid rows, then [`to_date()`] to parse. In SQL, you'd need a subquery or CTE to filter first, then convert. The DataFrame chain reads linearly and lets you debug each step independently — add [`.show()`] between filter and conversion to verify you're keeping the right rows:

```rust
use datafusion::functions::datetime::expr_fn::to_date;

let step3 = step2
    .filter(col("date").like(lit("____-__-__")))?  // Pattern: 4 chars, dash, 2, dash, 2
    // step2_filtered.show().await?;  // Debug: verify filter worked before conversion
    .with_column("parsed_date", to_date(vec![col("date")]))?;
```

#### Strategy 2: Conditional conversion

Use [`when()`] with [`.otherwise()`] to convert valid dates and set invalid ones to `NULL`, keeping all rows. This is equivalent to SQL's `CASE WHEN`:

```rust
let step3 = step2.with_column(
    "parsed_date",
    when(col("date").like(lit("____-__-__")), to_date(vec![col("date")]))
        .otherwise(lit::<&str>(None))?  // Invalid dates become NULL instead of causing errors
)?;
```

Strategy 2 preserves all rows — useful when you need to track _which_ records had invalid data, or when nulls are acceptable downstream. Strategy 1 is cleaner when invalid records should be excluded entirely.

For our cleaning journey, we'll use Strategy 1 — filter out invalid dates, then convert:

```rust
let step3 = step2
    .filter(col("date").like(lit("____-__-__")))?
    .with_column("parsed_date", to_date(vec![col("date")]))?;

step3.clone().show().await?;
```

**Output after Step 3** (8 → 7 rows: filtered invalid date format, added `parsed_date` column):

```
+----------+----------+--------+----------+------------+-------------+
| order_id | customer | amount | status   | date       | parsed_date |
+----------+----------+--------+----------+------------+-------------+
| 1        | alice    | 100.0  | complete | 2026-01-01 | 2026-01-01  |
| 1        | alice    | 100.0  | complete | 2026-01-01 | 2026-01-01  |
| 1        | alice    | 10.0   | complete | 2026-01-01 | 2026-01-01  |
| 2        | alice    | 150.0  | pending  | 2026-01-15 | 2026-01-15  |
| 4        | bob      | 200.0  | pending  | 2026-01-03 | 2026-01-03  |
| 6        | dave     | 180.0  |          | 2026-01-04 | 2026-01-04  |
| 7        | dave     | 300.0  | complete | 2026-01-20 | 2026-01-20  |
+----------+----------+--------+----------+------------+-------------+
```

Row 5 (Charlie with "invalid" date) is filtered out by Strategy 1. The `parsed_date` column is now `Date32`, enabling date arithmetic and proper sorting.

```
+----------+----------+--------+----------+------------+
| order_id | customer | amount | status   | date       |
+----------+----------+--------+----------+------------+
| 5        | charlie  | 180.0  | complete | invalid    |
+----------+----------+--------+----------+------------+
```

> **Tip:** Type conversion errors often surface late in pipelines. Validate early with [`.filter()`] or use [`when()`] with [`.otherwise()`] to make failures explicit rather than silent.

### Step 4: Handling Remaining Nulls

**Null handling fills missing values with sensible defaults — preserving rows that would otherwise break aggregations.**

After aggressive filtering and type conversion, you'll often have sparse columns where nulls are acceptable but inconvenient downstream. Aggregations like `SUM()` and `AVG()` skip nulls, which may be fine — but `COUNT(column)` vs `COUNT(*)` behaves differently, and joins on nullable columns can produce unexpected results. Decide per-column: filter nulls that indicate bad data, fill nulls that represent "unknown but acceptable."

In SQL, you'd use [`COALESCE`]:

```sql
SELECT
order_id,
customer,
amount,
COALESCE(status, 'unknown') AS status,
date,
parsed_date
FROM step3
```

Again, SQL requires listing every column. The DataFrame API's [`.with_column()`] combined with [`coalesce()`] is more concise:

```rust
use datafusion::functions::core::expr_fn::coalesce;

let step4 = step3.with_column(
    "status",
    coalesce(vec![col("status"), lit("unknown")])  // First non-null wins
)?;

step4.clone().show().await?;
```

**Output after Step 4** (7 → 7 rows: no rows lost, null `status` filled with "unknown"):

```
+----------+----------+--------+----------+------------+-------------+
| order_id | customer | amount | status   | date       | parsed_date |
+----------+----------+--------+----------+------------+-------------+
| 1        | alice    | 100.0  | complete | 2026-01-01 | 2026-01-01  |
| 1        | alice    | 100.0  | complete | 2026-01-01 | 2026-01-01  |
| 1        | alice    | 10.0   | complete | 2026-01-01 | 2026-01-01  |
| 2        | alice    | 150.0  | pending  | 2026-01-15 | 2026-01-15  |
| 4        | bob      | 200.0  | pending  | 2026-01-03 | 2026-01-03  |
| 6        | dave     | 180.0  | unknown  | 2026-01-04 | 2026-01-04  |
| 7        | dave     | 300.0  | complete | 2026-01-20 | 2026-01-20  |
+----------+----------+--------+----------+------------+-------------+
```

Order _#6_ had a null `status` — now filled with "unknown". This pattern is essential for real-world pipelines where nulls appear in optional columns.

**When to filter vs fill:**

| Scenario                  | Approach          | Why                                         |
| ------------------------- | ----------------- | ------------------------------------------- |
| Null in primary key       | Filter out        | Can't meaningfully process without identity |
| Null in optional field    | Fill with default | Preserve row, use sensible fallback         |
| Null indicates data issue | Filter out        | Bad data shouldn't propagate                |
| Null is valid state       | Keep as-is        | `NULL` has meaning in your domain           |

> **Tip:** [`coalesce()`] returns the first non-null value from a list. For two-argument cases, [`nvl()`] is a shorthand: `nvl(col("status"), lit("unknown"))`.

> **Learn More:** See [Step 4: Handling Remaining Nulls](#step-4-handling-remaining-nulls).

### Step 5: Removing Duplicates

**Deduplication resolves conflicting rows when the same logical entity appears multiple times — choosing which row "wins."**

Duplicates arise from retries, data merges, CDC (Change Data Capture) streams, or upstream bugs. Your strategy depends on your data semantics: keep the latest? The highest value? The first arrival? DataFusion's [`.distinct_on()`] lets you specify both the uniqueness key and the sort order that determines the survivor — more powerful than SQL's `DISTINCT` which only removes exact duplicates.

In SQL, you'd use [`DISTINCT`] or [`DISTINCT ON`] (DataFusion supports basic `DISTINCT ON`, but not combined with GROUP BY or aggregations):

```sql
-- Remove fully duplicate rows (exact matches on all columns)
SELECT DISTINCT * FROM step4

-- PostgreSQL/DataFusion: keep first row per key (when you DO want to collapse)
SELECT DISTINCT ON (order_id) *
FROM step4
ORDER BY order_id, amount DESC
```

For **line item data**, use [`.distinct()`] to remove exact duplicate rows while preserving intentionally different items within the same order:

```rust
// Remove exact duplicate rows only — preserves multiple items per order
let step5 = step4.distinct()?;

step5.clone().show().await?;
```

**Output after Step 5** (7 → 6 rows: removed 1 exact duplicate):

```
+----------+----------+--------+----------+------------+-------------+
| order_id | customer | amount | status   | date       | parsed_date |
+----------+----------+--------+----------+------------+-------------+
| 1        | alice    | 100.0  | complete | 2026-01-01 | 2026-01-01  |
| 1        | alice    | 10.0   | complete | 2026-01-01 | 2026-01-01  |
| 2        | alice    | 150.0  | pending  | 2026-01-15 | 2026-01-15  |
| 4        | bob      | 200.0  | pending  | 2026-01-03 | 2026-01-03  |
| 6        | dave     | 180.0  | unknown  | 2026-01-04 | 2026-01-04  |
| 7        | dave     | 300.0  | complete | 2026-01-20 | 2026-01-20  |
+----------+----------+--------+----------+------------+-------------+
```

The duplicate row (order_id=1, amount=100.0) that appeared twice is now collapsed to one. The 10.0 item remains because it's a different line item, not a duplicate.

> **Caution:** Using [`.distinct_on(order_id)`] here would collapse order #1's two items into one row, losing data! Use `.distinct_on()` only when you intentionally want to pick "one winner" per key (e.g., keeping the latest status update per order).

> **Tip:** Still seeing duplicates after [`.distinct()`]? It deduplicates on _all_ columns — rows that look identical but differ in one column aren't duplicates. Use [`.distinct_on()`] to specify exactly which columns define uniqueness.

> **Learn More:** See [Step 5: Removing Duplicates](#step-5-removing-duplicates).

### Step 6: Computing Derived Columns

**Derived columns transform raw data into business logic — fiscal quarters, customer tiers, time-based flags — without modifying source data.**

Computing derived values at query time keeps your source data clean while enabling flexible analysis.

- Need year-over-year comparisons?
- Extract the year. Customer segmentation?
- Compute tiers from amounts?

The DataFrame API's chained [`.with_column()`] calls read like a recipe, each step building on the last — no need to re-list all columns like in SQL's `SELECT`.

In SQL, you'd use [`EXTRACT`] or [`DATE_PART`]:

```sql
SELECT *,
EXTRACT(YEAR FROM parsed_date) AS year,
EXTRACT(MONTH FROM parsed_date) AS month
FROM step5
```

With `parsed_date` as a proper `Date32`, we can use [`date_part()`] to extract meaningful components:

```rust
use datafusion::functions::datetime::expr_fn::date_part;

let step6 = step5
    .with_column("year", date_part(lit("year"), col("parsed_date")))?
    .with_column("month", date_part(lit("month"), col("parsed_date")))?
    .with_column("day_of_week", date_part(lit("dow"), col("parsed_date")))?;

step6.clone().show().await?;
```

**Output after Step 6** (6 → 6 rows, +3 columns: `year`, `month`, `day_of_week`):

```
+----------+-----+-------------+--------+-------+-------------+
| order_id | ... | parsed_date | year   | month | day_of_week |
+----------+-----+-------------+--------+-------+-------------+
| 1        | ... | 2026-01-01  | 2026.0 | 1.0   | 1.0         |
| 1        | ... | 2026-01-01  | 2026.0 | 1.0   | 1.0         |
| 2        | ... | 2026-01-15  | 2026.0 | 1.0   | 1.0         |
| 4        | ... | 2026-01-03  | 2026.0 | 1.0   | 3.0         |
| 6        | ... | 2026-01-04  | 2026.0 | 1.0   | 4.0         |
| 7        | ... | 2026-01-20  | 2026.0 | 1.0   | 6.0         |
+----------+-----+-------------+--------+-------+-------------+
```

> **Note:** Output truncated for readability. Full DataFrame includes all columns from previous steps.

> **Note:** [`date_part()`] returns `Float64` for consistency across date components. Common parts: `year`, `month`, `day`, `hour`, `minute`, `second`, `dow` (day of week), `doy` (day of year).

> **Learn More:** See [Step 6: Computing Derived Columns](#step-6-computing-derived-columns).

### Step 7: Aggregating for Insights

**Aggregation is where all the cleaning pays off — collapsing rows into reliable totals, accurate counts, and trustworthy averages.**

This final step transforms cleaned detail rows into summary statistics grouped by business dimensions. Without the earlier cleaning, you'd have:

- Wrong totals (duplicates counted twice)
- Split groups ("bob" vs "BOB" as separate customers)
- Skewed averages (outliers like 99999.0 distorting means)

Since our data represents **line items**, we aggregate to **order level** — summing amounts per order while preserving the `order_id` for downstream joins with products, payments, or shipping tables.

In SQL:

```sql
SELECT order_id, customer, parsed_date AS order_date,
       SUM(amount) AS total_amount, COUNT(*) AS item_count
FROM step6
GROUP BY order_id, customer, parsed_date
ORDER BY order_id
```

The DataFrame API's [`.aggregate()`] separates grouping columns from aggregate expressions, making the structure explicit:

```rust
use datafusion::functions_aggregate::expr_fn::{sum, count};

let final_result = step6
    .aggregate(
        vec![col("order_id"), col("customer"), col("parsed_date")],  // GROUP BY order
        vec![
            sum(col("amount")).alias("total_amount"),
            count(lit(1)).alias("item_count")
        ]
    )?
    .with_column_renamed("parsed_date", "order_date")?
    .sort(vec![col("order_id").sort(true, true)])?;

final_result.show().await?;
```

**Final Output** (6 line items → 5 orders):

```
+----------+----------+------------+--------------+------------+
| order_id | customer | order_date | total_amount | item_count |
+----------+----------+------------+--------------+------------+
| 1        | alice    | 2026-01-01 | 110.0        | 2          |
| 2        | alice    | 2026-01-15 | 150.0        | 1          |
| 4        | bob      | 2026-01-03 | 200.0        | 1          |
| 6        | dave     | 2026-01-04 | 180.0        | 1          |
| 7        | dave     | 2026-01-20 | 300.0        | 1          |
+----------+----------+------------+--------------+------------+
```

Now we see **meaningful aggregation**:

- Order #1 has 2 line items totaling $110 (100 + 10).
- The `order_id` primary key is preserved for joins
- this table can link to products, payments, or shipping data.

**What happened to the rejected data?**

These 3 rows were filtered out. Here's the **summary of rejections**:

| order_id | rejection_reason              |
| -------- | ----------------------------- |
| 3        | negative_amount               |
| (null)   | null_order_id, outlier_amount |
| 5        | invalid_date                  |

The actual rejected DataFrame (from `rejected_step1.union(rejected_step3)?.show()`) contains all original columns plus the `rejection_reason`:

```text
+----------+-----+-------------------------------+
| order_id | ... | rejection_reason              |
+----------+-----+-------------------------------+
| 3        | ... | negative_amount               |
| (NUll)   | ... | null_order_id, outlier_amount |
| 5        | ... | invalid_date                  |
+----------+-----+-------------------------------+
```

This audit trail helps identify upstream data quality issues — if 30% of rows are rejected, you have a data source problem to fix!

> **Note:** The 4th "missing" row (Order #1 duplicate with amount=100.0) wasn't rejected as bad data — it was successfully handled by the deduplication step. Only 3 rows were truly rejected.

> **Tip:** Wrong totals? Verify your grouping keys first: `step6.select(vec![col("customer")]).distinct()?.show().await?` — hidden whitespace or case differences can split groups unexpectedly.

> **Learn More:** See [Step 7: Aggregating for Insights](#step-7-aggregating-for-insights).

### What We Learned

Through this cleaning journey, we transformed **10 line items** into **5 clean orders**:

| Step | Operation            | Method/Function                     | Rows   | Purpose                                               |
| ---- | -------------------- | ----------------------------------- | ------ | ----------------------------------------------------- |
| 1    | **Filtering**        | [`.filter()`]                       | 10 → 8 | Remove invalid data with predicates                   |
| 2    | **Text cleaning**    | [`lower()`], [`trim()`]             | 8 → 8  | Normalize strings for consistency                     |
| 3    | **Type conversion**  | [`to_date()`], [`.like()`]          | 8 → 7  | Parse strings to proper types with fail-safe handling |
| 4    | **Null handling**    | [`coalesce()`]                      | 7 → 7  | Fill nulls with sensible defaults                     |
| 5    | **Deduplication**    | [`.distinct()`]                     | 7 → 6  | Remove exact duplicate rows                           |
| 6    | **Computed columns** | [`.with_column()`], [`date_part()`] | 6 → 6  | Add derived values from existing data                 |
| 7    | **Aggregation**      | [`.aggregate()`]                    | 6 → 5  | Roll up line items to order totals                    |

All these operations produce the same result whether expressed as DataFrame methods or SQL—they're just different interfaces to the same underlying expressions. The DataFrame API adds **fail-safe patterns** (like conditional type conversion) that make production pipelines more robust.

**The complete pipeline** (all steps chained):

```rust
use datafusion::prelude::*;
use datafusion::functions::datetime::expr_fn::{to_date, date_part};
use datafusion::functions::core::expr_fn::{coalesce, concat_ws};
use datafusion::functions::string::expr_fn::{lower, trim};
use datafusion::functions_aggregate::expr_fn::{sum, count};

// Define rejection criteria
let null_order_id = col("order_id").is_null();
let invalid_amount = col("amount").lt_eq(lit(0)).or(col("amount").gt_eq(lit(10000)));
let invalid_date = col("date").not_like(lit("____-__-__"));

// Build rejection reason (concatenate all matching reasons)
let rejection_reason = concat_ws(
    lit(", "),
    vec![
        when(null_order_id.clone(), lit("null_order_id")).otherwise(lit(""))?,
        when(col("amount").lt_eq(lit(0)), lit("negative_amount")).otherwise(lit(""))?,
        when(col("amount").is_nan(), lit("nan_amount")).otherwise(lit(""))?,
        when(col("amount").gt_eq(lit(10000)), lit("outlier_amount")).otherwise(lit(""))?,
        when(invalid_date.clone(), lit("invalid_date")).otherwise(lit(""))?,
    ]
);

// Capture rejected rows WITH reason flag
let step1_reject = null_order_id.or(invalid_amount);
let rejected_step1 = sales.clone()
    .filter(step1_reject.clone())?
    .with_column("rejection_reason", rejection_reason.clone())?;

let after_step1 = sales.filter(step1_reject.not())?;

let rejected_step3 = after_step1.clone()
    .filter(invalid_date.clone())?
    .with_column("rejection_reason", lit("invalid_date"))?;

// Combine all rejected rows into one DataFrame for unified audit trail
// let all_rejected = rejected_step1.union(rejected_step3)?;

// Write rejected rows to parquet for review
// all_rejected.write_parquet("rejected_rows.parquet", ...).await?;

// Main pipeline: clean and aggregate
let clean_orders = after_step1
    // Step 2: Clean text data
    .with_column("customer", trim(vec![lower(col("customer"))]))?
    .with_column("status", lower(col("status")))?
    // Step 3: Type conversion (filter invalid dates, then parse)
    .filter(invalid_date.not())?
    .with_column("parsed_date", to_date(vec![col("date")]))?
    // Step 4: Handle remaining nulls
    .with_column("status", coalesce(vec![col("status"), lit("unknown")]))?
    // Step 5: Remove exact duplicate rows (preserves multiple items per order)
    .distinct()?
    // Step 6: Compute derived columns
    .with_column("year", date_part(lit("year"), col("parsed_date")))?
    // Step 7: Aggregate line items to order totals
    .aggregate(
        vec![col("order_id"), col("customer"), col("parsed_date")],
        vec![
            sum(col("amount")).alias("total_amount"),
            count(lit(1)).alias("item_count")
        ]
    )?
    .with_column_renamed("parsed_date", "order_date")?
    .sort(vec![col("order_id").sort(true, true)])?;
```

Notice how **method chaining** creates a readable, linear pipeline — each step flows naturally into the next. This is the DataFrame methodology in action: you can pause at any step with `.show()`, debug intermediate results, and DataFusion **optimizes the entire query** before execution.

### What We Covered: Shared Operations

- Every operation in this data cleaning journey has a direct SQL equivalent — [`.filter()`] is [`WHERE`], [`.aggregate()`] is [`GROUP BY`], [`.sort()`] is [`ORDER BY`].<br>
  **The logic is identical; only the syntax differs.** <br>

- But the DataFrame API adds something SQL strings cannot: <br>
  **composable, fail-safe patterns**. <br>

- The type conversion step showed how to gracefully handle parse failures instead of crashing — a pattern that's awkward to express in SQL but natural in DataFrames.

- Choose based on your context: SQL for ad-hoc queries and complex window functions, DataFrames for type-safe pipelines and dynamic composition. For detailed operation coverage and additional examples, see [Transformations](../library-user-guide/dataframe-api/transformations.md).

After that, [Advanced DataFrame Topics](../library-user-guide/dataframe-api/dataframes-advance.md) explores methods that have **no SQL equivalent** — like [`.with_column()`] for adding columns without re-selecting everything, [`.union_by_name()`] for schema-flexible unions, and [`.describe()`] for instant summary statistics.

---

## The DataFrame Detective Toolkit

When things go wrong, you need systematic debugging techniques. This section teaches you methodological approaches to diagnose and fix DataFrame issues.

### Understanding Query Plans

DataFrames build a logical plan that gets optimized before execution. Understanding plans is key to debugging.

#### Viewing the Logical Plan

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "price" => [1200, 25]
    )?
    .filter(col("price").gt(lit(50)))?
    .select(vec![col("product")])?;

    // View the logical plan (before optimization)
    println!("Logical Plan:\n{}", df.logical_plan().display_indent());

    Ok(())
}
```

**Output:**

```
Logical Plan:
Projection: product
  Filter: price > 50
    DataFrame: ...
```

#### Viewing the Optimized Plan

```rust
// See what optimizations were applied
let optimized_df = df.clone().explain(false, false)?;
optimized_df.show().await?;
```

This shows you:

- **Projection pushdown**: Reading only needed columns
- **Predicate pushdown**: Filtering as early as possible
- **Filter merging**: Combined multiple filters
- **Partition pruning**: Skipped irrelevant data

#### Analyzing Execution with EXPLAIN ANALYZE

```rust
// See actual execution statistics
let analyzed = df.explain(false, true)?;  // verbose=false, analyze=true
analyzed.show().await?;
```

This reveals:

- Actual row counts at each stage
- Execution time per operator
- Memory usage
- Partitioning information

_Use case_: Find bottlenecks in complex pipelines.

### Debugging Techniques

#### The "Break-the-Chain" Method

When a long pipeline fails, find the breaking point by progressively commenting out operations:

```rust
let result = df
    .filter(col("a").gt(lit(0)))?
    .with_column("b", col("a") * lit(2))?
    // .with_column("c", col("b") / col("zero"))?  // ← Comment this out
    .aggregate(vec![col("category")], vec![sum(col("b"))])?;

// If it works now, the commented line is the problem
```

Faster approach using [`.show()`] at each step:

```rust
let step1 = df.filter(col("a").gt(lit(0)))?;
step1.clone().show().await?;  // ← Verify step 1

let step2 = step1.with_column("b", col("a") * lit(2))?;
step2.clone().show().await?;  // ← Verify step 2

// Continue until you find the failing step
```

#### Binary Search Debugging

For very long pipelines (10+ operations), use binary search:

1. Comment out the second half
2. If it works, the problem is in the second half; if not, it's in the first half
3. Repeat within the problematic half
4. Converges quickly to the exact operation

#### Inspecting Intermediate Results

```rust
// Add .clone().show() to inspect without consuming the DataFrame
let df = original_df
    .filter(col("amount").gt(lit(0)))?;
df.clone().show().await?;  // ← Check what's here

let df = df.select(vec![col("customer"), col("amount")])?;
df.clone().show().await?;  // ← And here

let final_df = df.aggregate(vec![col("customer")], vec![sum(col("amount"))])?;
```

#### Schema Inspection

```rust
// Check the schema at any point
println!("Schema: {:?}", df.schema());
println!("Field names: {:?}", df.schema().field_names());

// Check a specific field's type
let field = df.schema().field_with_name("amount")?;
println!("Type of 'amount': {:?}", field.data_type());
```

#### Row Count Sanity Checks

```rust
// Quick row count at any stage
let count = df.clone().count().await?;
println!("Row count: {}", count);

// Expected vs actual
let expected = 100;
let actual = df.clone().count().await?;
assert_eq!(expected, actual, "Row count mismatch!");
```

### Common Debugging Scenarios

#### Scenario 1: "Why is my filter returning zero rows?"

**Diagnostic steps:**

1. Check for nulls: `df.filter(col("column").is_not_null())?.count().await?`
2. Inspect sample data: `df.limit(0, Some(5))?.show().await?`
3. Check data types: `df.schema()`
4. Verify filter logic with opposite condition: `col("x").lt_eq(lit(10))` instead of `.gt()`

#### Scenario 2: "My aggregation has wrong results"

**Diagnostic steps:**

1. Verify grouping keys: `df.select(vec![col("key")]).distinct()?.show().await?`
2. Check for nulls in group keys: `df.filter(col("key").is_null())?.count().await?`
3. Count rows per group: `df.aggregate(vec![col("key")], vec![count(lit(1))])?`
4. Inspect raw data before aggregation: `df.clone().show().await?`

#### Scenario 3: "Join returns unexpected row count"

**Diagnostic steps:**

1. Check for duplicates in keys: `left.select(vec![col("id")]).distinct().count()` vs `left.count()`
2. Inspect keys from both sides: `left.select(vec![col("id")]).show()` and `right.select(vec![col("id")]).show()`
3. Check for nulls in keys: `left.filter(col("id").is_null()).count()`
4. Verify key types match: `left.schema()` and `right.schema()`

#### Scenario 4: "Performance is terrible"

**Diagnostic steps:**

1. Check the optimized plan: `df.explain(false, false)?.show().await?`
2. Verify predicate pushdown happened (filter appears near data source)
3. Check partition count: Look for "Partitions" in explain output
4. Use `explain(false, true)?` to see actual execution times
5. Consider increasing parallelism: `SessionConfig::new().with_target_partitions(16)`

### Memory Management Debugging

#### Detecting Memory Issues

```rust
// DON'T: This loads all data into memory
// let all_data = huge_df.collect().await?;  // May OOM!

// DO: Stream the data
let mut stream = huge_df.execute_stream().await?;
use futures::StreamExt;
while let Some(batch) = stream.next().await {
    let batch = batch?;
    println!("Processing batch with {} rows", batch.num_rows());
    // Process batch by batch
}
```

#### Using show() vs collect()

```rust
// show() is for humans - limits rows and formats nicely
df.show().await?;  // Safe, only shows first 20 rows

// collect() is for programs - loads EVERYTHING
let batches = df.collect().await?;  // Dangerous with large data!

// Limit before collecting
let safe = df.limit(0, Some(1000))?.collect().await?;
```

## Anti-Pattern Gallery

Learn from common mistakes. Each anti-pattern shows what NOT to do and the correct approach.

### Anti-Pattern 1: Over-Selection Then Filter

```rust
// ❌ DON'T: Select all columns then filter (reads unnecessary data)
let bad = df
    .select(vec![col("*")])?
    .filter(col("price").gt(lit(100)))?;

// ✅ DO: Filter first, then select only what you need
let good = df
    .filter(col("price").gt(lit(100)))?
    .select(vec![col("product"), col("price")])?;
```

**Why**: Projection pushdown optimizes column reads, but only if you select early.

### Anti-Pattern 2: Multiple Sequential Filters

```rust
// ❌ DON'T: Chain filters (creates multiple plan nodes)
let bad = df
    .filter(col("price").gt(lit(50)))?
    .filter(col("quantity").lt(lit(100)))?
    .filter(col("category").eq(lit("Electronics")))?;

// ✅ DO: Combine into one filter
let good = df.filter(
    col("price").gt(lit(50))
        .and(col("quantity").lt(lit(100)))
        .and(col("category").eq(lit("Electronics")))
)?;
```

**Why**: Single filter is more efficient and easier to optimize.

### Anti-Pattern 3: Collecting Large Datasets

```rust
// ❌ DON'T: Collect millions of rows into memory
async fn process_big_data(df: DataFrame) -> Result<()> {
    let all_data = df.collect().await?;  // OOM risk!
    for batch in all_data {
        // process...
    }
    Ok(())
}

// ✅ DO: Stream the data
async fn process_big_data_streaming(df: DataFrame) -> Result<()> {
    let mut stream = df.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        // Process batch by batch
    }
    Ok(())
}
```

**Why**: Streaming handles arbitrarily large datasets without memory issues.

### Anti-Pattern 4: Ignoring Null Handling

```rust
// ❌ DON'T: Assume no nulls
let bad = df.filter(col("amount").gt(lit(0)))?;
// Silently drops null amounts (maybe not intended!)

// ✅ DO: Be explicit about null handling
let good = df.filter(
    col("amount").is_not_null()
        .and(col("amount").gt(lit(0)))
)?;
```

**Why**: [Three-valued logic] (SQL standard) can surprise you. Be explicit about null handling.

### Anti-Pattern 5: Cartesian Products

```rust
// ❌ DON'T: Forget join keys (Cartesian product!)
let bad = left.join(right, JoinType::Inner, &[], &[], None)?;
// With 1000 rows each = 1,000,000 rows!

// ✅ DO: Always specify join conditions
let good = left.join(right, JoinType::Inner, &["id"], &["user_id"], None)?;
```

**Why**: Cartesian products explode quickly and are rarely intended.

### Anti-Pattern 6: Not Using Lazy Evaluation

```rust
// ❌ DON'T: Execute prematurely
async fn bad_pipeline(df: DataFrame) -> Result<DataFrame> {
    let step1 = df.filter(col("a").gt(lit(0)))?;
    step1.show().await?;  // ← Executes!

    let step2 = step1.select(vec![col("b")])?;
    step2.show().await?;  // ← Executes again!

    Ok(step2)
}

// ✅ DO: Build the entire plan, execute once
async fn good_pipeline(df: DataFrame) -> Result<DataFrame> {
    let result = df
        .filter(col("a").gt(lit(0)))?
        .select(vec![col("b")])?;
    // Only execute when you need results
    result.show().await?;  // Single execution
    Ok(result)
}
```

**Why**: DataFusion optimizes the entire query plan; premature execution prevents optimization.

## Troubleshooting Guide

Quick solutions to common error messages and problems.

### Error: "Schema mismatch"

**Common causes:**

- Union of DataFrames with different schemas
- Join on columns with different types

**Solutions:**

```rust
// Use union_by_name for flexibility
let result = df1.union_by_name(df2)?;

// Cast columns to match types
let df2_fixed = df2.with_column("id", cast(col("id"), DataType::Int64))?;
```

### Error: "Column 'X' not found"

**Common causes:**

- Typo in column name
- Column was dropped in previous transformation
- Trying to access column after aggregation

**Solutions:**

```rust
// Check available columns
println!("{:?}", df.schema().field_names());

// After aggregation, only group keys and aggregates exist
// Include needed columns in group by or as aggregates
```

### Error: "Type mismatch"

**Common causes:**

- Comparing/computing with incompatible types
- String column used in numeric operation

**Solutions:**

```rust
// Cast to correct type
let df = df.with_column("price", cast(col("price_str"), DataType::Float64))?;

// Or use try_cast to handle failures gracefully
let df = df.with_column("price", try_cast(col("price_str"), DataType::Float64))?;
```

### Problem: Filter returns zero rows unexpectedly

**Diagnostic:**

```rust
// Check for nulls
df.filter(col("column").is_null())?.count().await?;

// Inspect sample data
df.limit(0, Some(10))?.show().await?;
```

**Solution:**

```rust
// Handle nulls explicitly
df.filter(col("column").is_not_null().and(col("column").gt(lit(0))))?;
```

### Problem: Join returns empty result

**Diagnostic:**

```rust
// Check keys from both sides
left.select(vec![col("id")]).distinct()?.show().await?;
right.select(vec![col("user_id")]).distinct()?.show().await?;

// Check for type mismatches
println!("Left: {:?}", left.schema().field_with_name("id")?.data_type());
println!("Right: {:?}", right.schema().field_with_name("user_id")?.data_type());
```

**Solution:**

```rust
// Ensure types match and check for trailing spaces
let left_clean = left.with_column("id", trim(vec![col("id")]))?;
let right_clean = right.with_column("user_id", trim(vec![col("user_id")]))?;
```

### Problem: Out of memory

**Solutions:**

```rust
// Use streaming instead of collect()
let stream = df.execute_stream().await?;

// Or limit the data
let sample = df.limit(0, Some(10000))?;

// Increase partition size for better parallelism
let config = SessionConfig::new().with_target_partitions(16);
let ctx = SessionContext::with_config(config);
```

## Quick Reference Cheat Sheet

Common DataFrame operations at a glance:

| Task                 |                                   Code                                   | Notes                 |
| -------------------- | :----------------------------------------------------------------------: | --------------------- |
| **Create from data** |                    `dataframe!("col" => [1, 2, 3])?`                     | In-memory data        |
| **Read CSV**         |       `ctx.read_csv("file.csv", CsvReadOptions::default()).await?`       |                       |
| **Read Parquet**     | `ctx.read_parquet("file.parquet", ParquetReadOptions::default()).await?` |                       |
| **Select columns**   |                    `df.select_columns(&["a", "b"])?`                     | By name               |
| **Computed column**  |               `df.with_column("c", col("a") + col("b"))?`                | Add/replace           |
| **Rename column**    |                 `df.with_column_renamed("old", "new")?`                  |                       |
| **Filter rows**      |                    `df.filter(col("a").gt(lit(10)))?`                    | WHERE                 |
| **Aggregate**        |        `df.aggregate(vec![col("dept")], vec![sum(col("sal"))])?`         | GROUP BY              |
| **Join**             |        `df1.join(df2, JoinType::Inner, &["id"], &["id"], None)?`         |                       |
| **Sort**             |               `df.sort(vec![col("a").sort(false, true)])?`               | DESC, nulls last      |
| **Limit**            |                      `df.limit(skip, Some(fetch))?`                      | Pagination            |
| **Union**            |                            `df1.union(df2)?`                             | UNION ALL             |
| **Distinct**         |                             `df.distinct()?`                             | Remove duplicates     |
| **Show results**     |                            `df.show().await?`                            | Display (limited)     |
| **Collect**          |                          `df.collect().await?`                           | Load all (careful!)   |
| **Stream**           |                       `df.execute_stream().await?`                       | Process incrementally |
| **Explain plan**     |                `df.explain(false, false)?.show().await?`                 | Debug                 |
| **Count rows**       |                       `df.clone().count().await?`                        |                       |
| **Get schema**       |                              `df.schema()`                               | Column info           |

### Common Aggregates

```rust
use datafusion::functions_aggregate::expr_fn::*;

sum(col("amount"))
avg(col("amount"))
min(col("amount"))
max(col("amount"))
count(col("id"))
count_distinct(col("user_id"))
stddev(col("amount"))
variance(col("amount"))
```

### Common Expressions

```rust
// Comparison
col("a").eq(lit(5))
col("a").gt(lit(5))
col("a").lt_eq(lit(10))
col("a").between(lit(5), lit(10))

// Logic
col("a").and(col("b"))
col("a").or(col("b"))
col("a").not()

// Null handling
col("a").is_null()
col("a").is_not_null()

// String operations
col("name").like(lit("%Smith%"))
lower(col("name"))
upper(col("name"))
trim(vec![col("name")])

// Math
col("a") + col("b")
col("a") * lit(2)
col("price") * col("quantity")

// Conditional
when(col("amount").gt(lit(1000)), lit("High"))
    .when(col("amount").gt(lit(100)), lit("Medium"))
    .otherwise(lit("Low"))?
```
