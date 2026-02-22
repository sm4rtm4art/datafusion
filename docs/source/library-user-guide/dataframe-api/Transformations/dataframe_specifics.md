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

(advanced-dataframe-patterns)=

## DataFrame-Unique Methods

**Some DataFrame methods have no SQL equivalent—these are the programmatic superpowers that justify using the DataFrame API.**

While the previous section covered operations available in _both_ APIs, this section highlights methods unique to DataFrames or methods where they shine due to their ergonomics. These exist because SQL's declarative grammar cannot express certain programmatic patterns that Rust handles naturally. For more see the [builder-methodology](#builder-methodology-architecting-with-dataframe) section.

For a more detailed overview, the following table list all the methods unique to the DataFrame API , a clustering of those in sections and the reasons why they are unique.

| Method                                                 | Purpose                                 | Why SQL Can't Express It                   |
| ------------------------------------------------------ | --------------------------------------- | ------------------------------------------ |
| **Schema Manipulation**                                |                                         |                                            |
| [`.with_column()`](#schema-manipulation)               | Add/replace a column keeping all others | SQL `SELECT` requires listing all columns  |
| [`.with_column_renamed()`](#schema-manipulation)       | Rename without expression               | SQL uses `AS` inside `SELECT`              |
| [`.drop_columns()`](#schema-manipulation)              | Remove columns by name                  | SQL has no direct equivalent               |
| **Set Operations by Name**                             |                                         |                                            |
| [`.union_by_name()`](#set-operations-by-name)          | Union aligned by name, not position     | SQL `UNION` is positional                  |
| [`.union_by_name_distinct()`](#set-operations-by-name) | Same with deduplication                 | SQL `UNION DISTINCT` is positional         |
| **SQL-DataFrame Hybrid**                               |                                         |                                            |
| [`.parse_sql_expr()`](#sql-dataframe-hybrid-methods)   | Parse SQL string into `Expr`            | Bridges SQL syntax into DataFrame code     |
| [`.select_exprs()`](#sql-dataframe-hybrid-methods)     | Select using SQL expression strings     | Combines SQL ergonomics with chaining      |
| [`.with_param_values()`](#parameter-binding)           | Bind parameter values to placeholders   | Plan-level operation, not a SQL clause     |
| **Data Exploration**                                   |                                         |                                            |
| [`.describe()`](#describing-data)                      | Summary statistics for all columns      | No single SQL statement equivalent         |
| **Convenience Methods**                                |                                         |                                            |
| [`.fill_null()`](#convenience-methods)                 | Fill nulls with default value           | Wrapper—SQL requires `COALESCE` per column |
| [`.cache()`](#convenience-methods)                     | Materialize DataFrame in memory         | Execution control—no SQL concept           |
| **Execution Control**                                  |                                         |                                            |
| [`.collect_partitioned()`](#execution-control)         | Collect preserving partitions           | Partition-aware execution                  |
| [`.execute_stream()`](#execution-control)              | Stream results without buffering        | Streaming execution control                |
| [`.execute_stream_partitioned()`](#execution-control)  | Stream per partition                    | Parallel streaming execution               |
| **Creation**                                           |                                         |                                            |
| [`.from_columns()`](#creating-from-columns)            | Create from column arrays               | Programmatic construction                  |
| **Array/Nested Data**                                  |                                         |                                            |
| [`.unnest_columns()`](#unnesting-arrays)               | Explode arrays into rows                | SQL `UNNEST` varies by database            |
| [`.unnest_columns_with_options()`](#unnesting-arrays)  | Unnest with fine-grained control        | Recursive depth, null handling             |
| **Bridging to SQL**                                    |                                         |                                            |
| [`.into_view()`](#bridging-to-sql)                     | Register DataFrame as SQL table         | Enables hybrid SQL/DataFrame workflows     |

> **Methods with SQL equivalents:** Some methods have SQL counterparts but offer ergonomic advantages:
>
> - **`.distinct_on()`** — DataFusion supports `SELECT DISTINCT ON (...)` in SQL (PostgreSQL-style, issues [#7827], [#7981])
> - **`.alias()`** — Equivalent to `SELECT * FROM (...) AS my_alias` subquery aliasing

[#7827]: https://github.com/apache/datafusion/issues/7827
[#7981]: https://github.com/apache/datafusion/issues/7981
[#12907]: https://github.com/apache/datafusion/issues/12907

> **Gap Note:** `unpivot`/`melt` (wide-to-long reshaping) is not yet available in DataFusion See [Issue #12907][#12907]. Workaround: manual `UNION ALL` of columns.

### Schema Manipulation

These methods modify column structure without requiring you to enumerate all columns—a common pain point in SQL. For more see the [Schema Management](schema-management.md).

#### Adding and Replacing Columns

[`.with_column()`] adds a new column or replaces an existing one _while keeping all other columns intact_. In SQL, you'd need to explicitly list every column in your `SELECT`.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "price" => [1200, 25]
    )?;

    // Add a computed column - all existing columns are preserved
    let df = df.with_column("discounted", col("price") * lit(0.9))?;

    df.clone().show().await?;
    // +---------+-------+------------+
    // | product | price | discounted |
    // +---------+-------+------------+
    // | Laptop  | 1200  | 1080.0     |
    // | Mouse   | 25    | 22.5       |
    // +---------+-------+------------+

    // Replace an existing column (same name overwrites)
    let df = df.with_column("price", col("price") * lit(1.1))?;

    df.show().await?;
    // +---------+--------+------------+
    // | product | price  | discounted |
    // +---------+--------+------------+
    // | Laptop  | 1320.0 | 1080.0     |
    // | Mouse   | 27.5   | 22.5       |
    // +---------+--------+------------+

    Ok(())
}
```

> **Why this matters:**
> With 2 columns the SQL is fine, but imagine a table with 20 columns—you'd have to list all 20 just to add one computed column. `.with_column()` scales effortlessly.

#### Renaming Columns

[`.with_column_renamed()`] renames a column without requiring an expression—just the old name and new name. Like all DataFrame methods, it's **lazy**: no execution happens until you call a terminal action.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "price" => [100, 200],
        "qty" => [5, 10]
    )?;

    // Rename is lazy - just updates the logical plan
    let df = df.with_column_renamed("price", "unit_price")?;

    // Chain multiple renames
    let df = df
        .with_column_renamed("qty", "quantity")?
        .with_column_renamed("unit_price", "cost")?;

    // Execute to see results
    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+------+----------+",
            "| cost | quantity |",
            "+------+----------+",
            "| 100  | 5        |",
            "| 200  | 10       |",
            "+------+----------+",
        ],
        &results
    );
    Ok(())
}
```

> **SQL equivalent:** `SELECT price AS unit_price, qty AS quantity FROM ...`
>
> The difference: SQL's `AS` is part of the projection—you must list all columns. `.with_column_renamed()` touches only the renamed column, passing others through unchanged.

#### Dropping Columns

[`.drop_columns()`] removes columns by name. SQL has no equivalent—you must list all columns you want to _keep_ instead.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2],
        "name" => ["Alice", "Bob"],
        "temp_id" => [999, 998],
        "category" => ["A", "B"]
    )?;

    // Remove multiple columns at once
    let df = df.drop_columns(&["category", "temp_id"])?;

    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  | Bob   |",
            "+----+-------+",
        ],
        &results
    );
    Ok(())
}
```

> **SQL workaround:** `SELECT id, name FROM ...` — must explicitly list every column to keep. With 20 columns, dropping 2 means listing 18.

---
