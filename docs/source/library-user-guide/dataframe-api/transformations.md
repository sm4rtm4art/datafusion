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

# DataFrame Transformations

Aside of [loading](./creating-dataframes.md) and [writing](./writing-dataframes.md) data, the efficient **transformation and alignment** of data is essential to DataFusion. This document serves as your guide to the DataFrame API's transformation capabilities, emphasizing its unique programmatic strengths alongside its shared foundation with datafusions [SQL-API](../using-the-sql-api.md). Because both APIs compile down to the same logical plan, you get the best of both worlds: the expressiveness of Rust and the performance of a world-class query optimizer. (See [DataFrame Concepts](./concepts.md#introduction))

> **Style Note:** In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

```{contents}
:local:
:depth: 2
```

Transformations allow you to shape, filter, enrich, and analyze your data through a series of composable, type-safe operations. Unlike SQL, where queries are often monolithic strings, DataFrames allow you to **build queries programmatically**. This approach shines when you need to:

- **Chain operations** into readable, logical pipelines.
- **Build queries dynamically** based on runtime conditions or configuration.
- **Leverage the Rust type system** to catch errors at compile time.
- **Reuse logic** by encapsulating complex transformations into functions.

The following diagram illustrates the conceptual position in the DataFrame architecture. Both the SQL API and the DataFrame API share the same [`SessionContext`] and converge to the same [`LogicalPlan`] with column-based operations.<br>

```text
           +------------------+
           |  SessionContext  |
           +------------------+
                    |
          +---------+---------+
          |                   |
          v                   v
    +-----------+       +-------------+
    |  SQL API  |       |  DataFrame  |
    | ctx.sql() |       |  API (lazy) |
    +-----------+       +-------------+
          \                   /
           \                 /
            v               v
       +------------------------+
       |      LogicalPlan       |  ← Same!
       +------------------------+
                  |
                  | Further processing...
                  v
```

For a deeper dive, see:

- [DataFrame Concepts](./concepts.md#introduction)
- [SIGMOD 2024 Paper][datafusion paper],
- [architecture overview on docs.rs][docs.rs]

For most data transformations, the choice between SQL-API and DataFrame-APIs is primarily about ergonomics—both produce identical execution plans. However, the DataFrame API is more than just "SQL with different syntax." The DataFrame API is a programmatic **builder** for query plans, whereas the SQL API is a declarative **parser** for query strings. Research on [DataFrame Algebra][dataframe algebra] shows that the DataFrame paradigm offers a distinct way of _expressing_ data transformations. These patterns were established by the [pandas library][pandas] and continuously refined by projects like [Apache Spark] for efficient parallel, multi-node computation.

## API Ergonomics: DataFrame vs SQL

**The DataFusion DataFrame-API and SQL-API share the same execution engine—but the _experience_ of writing them is fundamentally different.**

This table maps SQL operations to their DataFrame equivalents. Methods marked **Unique** have no direct SQL counterpart—these are where the DataFrame API provides capabilities beyond standard SQL.

| Category        |       SQL Operation        |             DataFrame Method             | Key Differences & Superpowers                                                               |
| --------------- | :------------------------: | :--------------------------------------: | ------------------------------------------------------------------------------------------- |
| **Filtering**   |         [`WHERE`]          |              [`.filter()`]               | Chainable predicates; programmatic filter building.                                         |
| **Selection**   |         [`SELECT`]         |   [`.select()`], [`.select_columns()`]   | [`.select()`] supports expressions; [`.select_columns()`] is a simple projection.           |
| **Selection**   |       [`AS`] (alias)       | [`.alias()`], [`.with_column_renamed()`] | [`.with_column_renamed()`] renames existing columns without expressions.                    |
| **Mutation**    |   (No direct equivalent)   |            [`.with_column()`]            | **Unique**: Add or replace a column while keeping all others.                               |
| **Aggregation** |        [`GROUP BY`]        |             [`.aggregate()`]             | Groups data and applies aggregate functions.                                                |
| **Joins**       |          [`JOIN`]          |       [`.join()`], [`.join_on()`]        | [`.join_on()`] allows arbitrary boolean expressions for join conditions.                    |
| **Sorting**     |        [`ORDER BY`]        |               [`.sort()`]                | Sort by one or multiple expressions.                                                        |
| **Limiting**    |   [`LIMIT`] / [`OFFSET`]   |               [`.limit()`]               | [`limit(skip, fetch)`][`.limit()`] handles both offset and limit.                           |
| **Set Ops**     |   [`UNION ALL`][`UNION`]   |               [`.union()`]               | concatenates DataFrames.                                                                    |
| **Set Ops**     |         [`UNION`]          |          [`.union_distinct()`]           | concatenates and removes duplicates.                                                        |
| **Set Ops**     |   (No direct equivalent)   |           [`.union_by_name()`]           | **Unique**: Unions based on column names, forgiving column order mismatches.                |
| **Distinct**    |   [`DISTINCT`][`SELECT`]   |             [`.distinct()`]              | Removes duplicate rows based on all columns.                                                |
| **Distinct**    | [`DISTINCT ON`] (Postgres) |            [`.distinct_on()`]            | **Unique**: Deduplicates based on specific columns, keeping the "first" row per sort order. |

> **Note:** While you can mix SQL and DataFrames (see [Concepts](../concepts.md)), mastering these native methods unlocks the full power of programmatic data manipulation.

### The Methodical Differences: Why DataFrames Feel Different

**The DataFusion DataFrame API isn't SQL with different syntax—it's a fundamentally different way of expressing data transformations.**

While both produce the same [`LogicalPlan`], DataFrames integrate with Rust's type system, control flow, and tooling in ways SQL strings cannot. As the [Towards Scalable Dataframe Systems][dataframe algebra] paper notes, SQL's declarative nature makes it "awkward to develop and debug queries in a piecewise, modular fashion."

| Aspect              |             DataFrame API              | SQL API                          |
| ------------------- | :------------------------------------: | :------------------------------- |
| **Interface**       | Method chaining (`.select().filter()`) | Declarative string (`SELECT...`) |
| **Error Detection** |     Compile-time (syntax & types)      | Runtime (parsing)                |
| **Composability**   |   High (variables, loops, functions)   | Low (string concatenation)       |
| **IDE Tooling**     | Full support (autocomplete, refactor)  | Limited (opaque strings)         |
| **Custom Logic**    | Can inject custom `LogicalPlan` nodes  | Limited to SQL grammar           |

Consider building a search API where filters depend on user input:

**SQL approach** — string concatenation:

```rust
let mut query = "SELECT * FROM employees WHERE 1=1".to_string();
if let Some(dept) = filter_department {
    query.push_str(&format!(" AND department = '{}'", dept));
}
```

This pattern has three problems:

1. **Injection vulnerability** — if `dept` contains [`'; DROP TABLE students; --`](https://xkcd.com/327/), you're in trouble.
2. **Runtime-only errors** — typos like `"deprtment"` won't surface until execution.
3. **The `WHERE 1=1` hack** — exists solely to simplify conditional string building.

**DataFrame approach** — type-safe, composable:

```rust
let mut result = employees_df;
if let Some(dept) = filter_department {
    result = result.filter(col("department").eq(lit(dept)))?;
}
```

Values pass through [`lit()`], never interpolated into strings — injection-proof by design. The optimizer automatically combines multiple [`.filter()`] calls into a single predicate.

### Finding Balance: When to Use Which

**Both APIs share far more than they differ.** They use the same expressions ([`col()`], [`lit()`], [`sum()`]), the same optimizer, and produce identical execution plans. The choice isn't "DataFrame vs SQL" — it's about picking the right tool for each situation.

#### DataFrame API Strengths

| Strength                     | What it means                                                                |
| ---------------------------- | ---------------------------------------------------------------------------- |
| **Type safety**              | Rust's type system catches errors before runtime                             |
| **Compile-time validation**  | No runtime "column not found" surprises — the compiler checks your code      |
| **No SQL injection risk**    | Values pass through [`lit()`], never string-interpolated — secure by design  |
| **IDE support**              | Full autocomplete, refactoring, go-to-definition for methods and expressions |
| **Programmatic composition** | Build queries with loops, conditionals, and reusable functions               |

#### Where SQL Shines

The DataFrame API isn't always the best choice. Be honest about trade-offs:

| Feature              | SQL API Strength                           | DataFrame Consideration             |
| -------------------- | ------------------------------------------ | ----------------------------------- |
| **Window Functions** | Concise `OVER (PARTITION BY ... ORDER BY)` | More verbose builder pattern        |
| **Complex Logic**    | Readable `CASE WHEN`, `GROUPING SETS`      | Deeply nested function calls        |
| **Prototyping**      | Instant feedback via REPL/CLI              | Requires Rust compilation cycle     |
| **Tool Integration** | Works with BI tools, JDBC/ODBC             | Requires custom integration         |
| **Team Familiarity** | Universal SQL knowledge                    | Rust + DataFrame API learning curve |

#### The Bigger Picture: DataFusion's Architecture

DataFusion is a **columnar query engine** optimized for analytical workloads. But not every operation suits columnar processing — some queries perform better on row-based systems like PostgreSQL or Oracle.

This is where DataFusion's [`TableProvider`] abstraction shines: you can **connect diverse data sources** and let each system do what it does best. Query your PostgreSQL tables alongside Parquet files, and DataFusion orchestrates the execution. The DataFrame API and SQL API are just two ways to express queries over this unified architecture.

> **Think outside the box:** If a particular query pattern is awkward in DataFrames _and_ slow in DataFusion's columnar engine, consider whether a row-based system or specialized database might be the right tool — and use DataFusion to connect them.

#### Use Case Recommendations

| Use Case                                   | Recommended API | Why                                      |
| ------------------------------------------ | :-------------: | ---------------------------------------- |
| Ad-hoc exploration, quick queries          |     SQL-API     | Familiar syntax, zero boilerplate        |
| Dynamic filters, user-driven queries       |  DataFrame-API  | Type-safe composition, no injection risk |
| Complex business logic, reusable pipelines |  DataFrame-API  | Composable functions, testable units     |
| Migrating existing SQL queries             |     SQL-API     | Copy-paste, immediate results            |
| IDE-heavy development                      |  DataFrame-API  | Autocomplete, refactoring support        |
| Complex window functions                   |     SQL-API     | More concise, readable syntax            |
| Rapid prototyping                          |     SQL-API     | No compilation required                  |

The true power emerges when you **mix both APIs**: use SQL for complex joins or window functions where declarative syntax shines, then switch to DataFrames for dynamic filtering or pipeline composition. DataFusion makes this seamless — a SQL query returns a DataFrame you can continue transforming.

### What's Ahead

This guide walks through DataFrame transformations with a practical lens:

1. **[Shared Transformations](#shared-transformations-a-data-cleaning-journey)** — A narrative data-cleaning journey showing transformations in context, not isolation.
2. **[Deep Dive: Transformation Reference](#deep-dive-transformation-reference)** — The shared expression vocabulary: filtering, selection, aggregation, joins — concepts that apply to both SQL and DataFrames.
3. **[Advanced DataFrame Patterns](#advanced-dataframe-patterns)** — Unique DataFrame capabilities: `.with_column()`, `.union_by_name()`, window functions, and more.
4. **[Mixing SQL and DataFrames](#mixing-sql-and-dataframes)** — Hybrid workflows that leverage the strengths of both APIs.

---

<!-- TODO: Place in the Dataframe user guide -->

## Shared Transformations: A Data Cleaning Journey

**These are the core operations that exist in both SQL and the DataFrame API — same logic, different syntax.**

If you know SQL, you already know _what_ these operations do. The difference is _how_ you express them. The naming conventions come from different traditions: SQL follows the ANSI standard, while DataFrame methods inherit from [Apache Spark] and [pandas] — hence `ORDER BY` becomes `.sort()`, and `WHERE` becomes `.filter()`.

| SQL Clause                         | DataFrame Method                             | Origin / Note                              |
| ---------------------------------- | -------------------------------------------- | ------------------------------------------ |
| [`WHERE condition`][`WHERE`]       | [`.filter(expr)`][`.filter()`]               | Functional programming tradition           |
| [`SELECT columns`][`SELECT`]       | [`.select(exprs)`][`.select()`]              | Same concept, expression-based             |
| [`SELECT col AS name`][`AS`]       | [`.alias("name")`][`.alias()`]               | Rename via expression                      |
| [`GROUP BY ... AGG()`][`GROUP BY`] | [`.aggregate(groups, aggs)`][`.aggregate()`] | Explicit grouping + aggregation separation |
| [`ORDER BY col`][`ORDER BY`]       | [`.sort(exprs)`][`.sort()`]                  | Spark naming convention                    |
| `ORDER BY col DESC`                | `.sort(col.sort(false, true))`               | `(ascending, nulls_first)` parameters      |
| `LIMIT n OFFSET m`                 | [`.limit(skip, Some(fetch))`][`.limit()`]    | Combined into single method                |
| `DISTINCT`                         | [`.distinct()`]                              | Same concept                               |
| `UNION ALL`                        | [`.union()`]                                 | Keeps duplicates                           |
| `UNION`                            | [`.union_distinct()`]                        | Removes duplicates                         |

<!--TODO: References !-->

> **Going deeper:** For comprehensive method documentation, see [docs.rs]. For SQL semantics, refer to [PostgreSQL docs] or [Spark SQL documentation][spark docs].

Rather than covering each operation in isolation, we'll demonstrate them together in a realistic **data cleaning workflow** — the kind of pipeline you'd actually build. This approach shows how DataFrame **ergonomics** shine: operations like [`.describe()`] give you summary statistics in one line, while SQL would require multiple aggregations (see below) and many more.

Along the way, you'll see **method chaining** in action — more on that pattern in [Advanced DataFrame Patterns](#advanced-dataframe-patterns). For a full comparison of when to use DataFrames vs SQL, see [Finding Balance](#finding-balance-when-to-use-which) above.

### Meet Your Data

**Real-world data is messy — we create a DataFrame with common data quality problems to clean throughout the following sections.**

Duplicates, inconsistent casing, nulls, missing values in essential columns, invalid formats, and outliers are everyday challenges. To highlight the benefits of the DataFrame API, the following sections form a narrative tutorial — we create our own dataset using the [`dataframe!`] macro with all these common problems baked in (see [Creating DataFrames](creating-dataframes.md#5-from-inline-data-using-the-dataframe-macro) for details):

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

> **Best Practice:** In production, define an explicit schema where `order_id` is non-nullable — the database would reject that last row at insert time. See [Schema Management](./schema-management.md) for how to enforce constraints upfront rather than cleaning them later.

### Exploring the Data: Cheap to Expensive

**Start with cheap operations, then sample, then analyze the full dataset.**

DataFusion DataFrames are **immutable** — every transformation (`.filter()`, `.select()`, `.with_column()`) creates a new DataFrame, leaving the original unchanged. Combined with **lazy execution**, transformations build a logical plan without touching data until you call a terminal action (`.show()`, `.collect()`, `.count()`). This means `.schema()` is free (reads plan metadata), while `.describe()` triggers a full scan. Use this to your advantage: the original data is always safe, and you can validate your approach on cheap operations before running expensive ones.
For more information, see the [DataFrame Execution part](./writing-dataframes.md#dataframe-execution)

| Operation         | Cost      | What it does                          |
| ----------------- | --------- | ------------------------------------- |
| [`.schema()`]     | Free      | Reads metadata only — no data scanned |
| [`.show_limit()`] | Cheap     | Preview first N rows                  |
| [`.show()`]       | Expensive | Shows all data                        |
| [`.count()`]      | Expensive | Scans all rows to count them          |
| [`.describe()`]   | Expensive | Multiple aggregations on all rows     |

#### 1. Check the schema (free) — understand types before touching data:

With the [`.schema()`] method, DataFusion only reads the metadata. For further reading, see the DataFusion DataFrame API documentation on [schema management](./schema-management.md).

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

> **Warning:** [`.show()`] collects _all_ results into memory — use [`.show_limit(n)`][`.show_limit()`] for large datasets. To explore different parts, use [`.limit(offset, count)`][`.limit()`] to skip and sample (e.g., `.limit(1000, Some(100))` skips first 1000, shows next 100). For complete large-dataset workflows, see [Advanced DataFrame Topics](./dataframes-advance.md).

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

> **Learn More:** For the full range of string functions including [`substring()`], `replace()`, and regex operations, see [String Functions](#string-operations) in the Deep Dive section.

### Step 3: Type Conversion with Fail-Safe Handling

**Type conversion transforms string data into proper types — enabling date arithmetic, correct sorting, and type-safe operations.**

Our schema inspection revealed `date` is `Utf8` (string), not a proper date type. This matters: string sorting puts "2024-12-01" before "2024-2-01" (lexicographic), while date sorting handles them correctly. Date arithmetic (`date + interval '1 day'`) only works on date types. Type conversion is where many pipelines silently fail — a single malformed value like "invalid" or "2024/01/01" (wrong separator) can crash the entire query.

> Recap from the [schema inspection](#1-check-the-schema-free--understand-types-before-touching-data:):
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

The problem: [`TO_DATE('invalid')`][`TO_DATE`] crashes the entire query. In production data, you'll encounter malformed dates, typos, legacy formats, and edge cases. The DataFrame API lets us handle this gracefully with two strategies:

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

> **Learn More:** See [Null Handling Patterns](#null-handling-patterns) in the Deep Dive section.

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

> **Learn More:** See [Deduplication Strategies](#deduplication-strategies) in the Deep Dive section.

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

> **Learn More:** See [Date and Time Operations](#date-and-time-operations) in the Deep Dive section.

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

> **Learn More:** See [Aggregation Patterns](#aggregation-patterns) in the Deep Dive section.

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

- Choose based on your context: SQL for ad-hoc queries and complex window functions, DataFrames for type-safe pipelines and dynamic composition. The next section, [Deep Dive: Transformation Reference](#deep-dive-transformation-reference), provides detailed coverage of each operation with more examples and edge cases.

After that, [Advanced DataFrame Patterns](#advanced-dataframe-patterns) explores methods that have **no SQL equivalent** — like [`.with_column()`] for adding columns without re-selecting everything, [`.union_by_name()`] for schema-flexible unions, and [`.describe()`] for instant summary statistics.

---

<!-- TODO moeve to the user guide Dataframes.md -->

## Deep Dive: Transformation Reference

**Every DataFrame transformation has a SQL equivalent—but the experience of writing them differs fundamentally.**

This reference covers operations that exist in _both_ the DataFrame API and SQL. For each operation, you'll find:

- **SQL equivalent** — The familiar SQL syntax as an anchor point
- **DataFrame implementation** — How to express it using method chaining
- **Trade-off analysis** — Where DataFrame shines vs where SQL shines
- **Quick Debug tips** — Common pitfalls and how to diagnose them

Use this section when you know _what_ operation you need but want to understand _how_ to express it idiomatically in DataFusion—and when to prefer one API over the other.

_For operations unique to DataFrames (no SQL equivalent), see [DataFrame-Unique Methods](#dataframe-unique-methods)._

**Jump to:**

| Data Shaping                                                  | Filtering & Combining                               | Advanced                              |
| ------------------------------------------------------------- | --------------------------------------------------- | ------------------------------------- |
| [Selection and Projection](#selection-and-projection-mastery) | [Filtering](#filtering-excellence)                  | [Window Functions](#window-functions) |
| [Aggregation](#aggregation-patterns)                          | [Joins](#when-dataframes-collide-join-patterns)     | [Subqueries](#subqueries)             |
| [Sorting and Limiting](#sorting-and-limiting)                 | [Set Operations](#set-operations-and-deduplication) | [Reshaping Data](#reshaping-data)     |

### Selection and Projection Mastery

**Projection controls the shape of your output—choosing which columns to keep, computing derived values, and reducing memory footprint by discarding what you don't need.**

DataFusions DataFrame-API provides methods for every projection need: simple name-based selection via [`.select_columns()`], expression-based computation with [`.select()`], adding columns with [`.with_column()`], renaming via [`.with_column_renamed()`], and removal with [`.drop_columns()`]. Projection pushdown ensures only requested columns are read from the data source.

[`.drop_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns

**SQL equivalent:**

```sql
SELECT
col_a,
col_b AS col_b_renamed,
col_a + col_b AS col_sum
FROM table
```

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Type-safe column references catch typos at compile time; programmatic column selection from schema; projection pushdown happens automatically
> - **SQL shines:** Familiar [`SELECT`] syntax; more readable for simple projections; [`SELECT *`][`SELECT`] for quick exploration

#### Basic Selection

**Which method to use:**

- **[`.select_columns()`]** — Pass column names as strings: `select_columns(&["a", "b"])`
- **[`.select()`]** — Pass expressions for computation: `select(vec![col("a"), (col("b") * lit(2)).alias("b_doubled")])`

Use `select_columns()` when you just need existing columns by name. Use [`.select()`] when you need to compute new values, rename with [`.alias()`], or apply functions.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

// Creating the dataframe with the dataframe! macro
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // select_columns(): simple name-based selection
    let result = sample_df.select_columns(&["product", "price"])?.collect().await?;

    assert_batches_eq!(
        &[
            "+----------+-------+",
            "| product  | price |",
            "+----------+-------+",
            "| Laptop   | 1200  |",
            "| Mouse    | 25    |",
            "| Keyboard | 75    |",
            "+----------+-------+",
        ],
        &result
    );

    Ok(())
}
```

#### Intermediate: Expressions and Computed Columns

[`.select()`] accepts any expression—arithmetic, conditionals, function calls—letting you compute new columns inline. Use [`.alias()`] to name computed results, [`.with_column()`] to add columns while keeping existing ones, and [`.with_column_renamed()`] to rename without recomputing.

```rust
// Using df from Basic Selection above

// select() with computed columns — replaces schema, only keeps what you list
let df = sample_df.select(vec![
    col("product"),
    (col("price") * col("quantity")).alias("revenue"),
])?;
assert_batches_eq!(&[
    "+----------+---------+",
    "| product  | revenue |",
    "+----------+---------+",
    "| Laptop   | 6000    |",
    "| Mouse    | 1250    |",
    "| Keyboard | 2250    |",
    "+----------+---------+",
], &df.clone().collect().await?);

// with_column() adds column while keeping all existing
let df = sample_df.with_column("tag", lit("2026"))?;
assert_batches_eq!(&[
    "+----------+---------+------+",
    "| product  | revenue | tag  |",
    "+----------+---------+------+",
    "| Laptop   | 6000    | 2026 |",
    "| Mouse    | 1250    | 2026 |",
    "| Keyboard | 2250    | 2026 |",
    "+----------+---------+------+",
], &df.clone().collect().await?);

// with_column_renamed() renames without recomputing
let df = sample_df.with_column_renamed("revenue", "total")?;
assert_batches_eq!(&[
    "+----------+-------+------+",
    "| product  | total | tag  |",
    "+----------+-------+------+",
    "| Laptop   | 6000  | 2026 |",
    "| Mouse    | 1250  | 2026 |",
    "| Keyboard | 2250  | 2026 |",
    "+----------+-------+------+",
], &df.clone().collect().await?);

// drop_columns() removes specific columns
let df = sample_df.drop_columns(&["tag"])?;
assert_batches_eq!(&[
    "+----------+-------+",
    "| product  | total |",
    "+----------+-------+",
    "| Laptop   | 6000  |",
    "| Mouse    | 1250  |",
    "| Keyboard | 2250  |",
    "+----------+-------+",
], &df.collect().await?);
```

> _Quick Debug_: **Columns missing after [`.select()`]?** <br>
> Unlike [`.with_column()`], [`.select()`] only keeps columns you explicitly list.

> **Ugly column names?** Without [`.alias()`], column names are the expression's string representation: `col("a") * col("b")` becomes `"a * b"`, `col("x").gt(lit(5))` becomes `"x > Int32(5)"` see [**g**rater **t**hen => `.gt()`][`.gt()`]. Always alias computed columns for readable output.

#### Advanced: Dynamic Column Selection

When column names aren't known until runtime—or you want to select by type—use [`.schema()`] to inspect the DataFrame's structure, then build expressions programmatically. This is where DataFrames shine over SQL: Rust's type system and iterators let you construct queries that would require dynamic SQL generation otherwise.

```rust
// Using sample_df: [product (Utf8), price (Int32), quantity (Int32), category (Utf8)]

// Get schema and filter to numeric columns only
let numeric_cols: Vec<_> = sample_df
    .schema()
    .fields()
    .iter()
    .filter(|f| f.data_type().is_numeric())
    .map(|f| col(f.name()))
    .collect();

let result = sample_df.clone().select(numeric_cols)?.collect().await?;
assert_batches_eq!(&[
    "+-------+----------+",
    "| price | quantity |",
    "+-------+----------+",
    "| 1200  | 5        |",
    "| 25    | 50       |",
    "| 75    | 30       |",
    "+-------+----------+",
], &result);
```

#### Anti-Pattern: Over-Selection

Selecting all columns with [`col("*")`][`col()`] defeats **projection pushdown**—an optimization where DataFusion tells the data source to only read requested columns. With Parquet files, this can mean reading 2 columns instead of 200, dramatically reducing I/O. Common SQL-Rule of not using [`SELECT *  FROM big_table`][`SELECT`]

```rust
// Using sample_df: [product, price, quantity, category]

// ❌ DON'T: Select all columns then filter
let bad = sample_df.clone()
    .select(vec![col("*")])?      // Reads ALL columns from source
    .filter(col("price").gt(lit(100)))?;

// ✅ DO: Select only what you need
let good = sample_df.clone()
    .select(vec![col("product"), col("price")])?  // Projection pushdown!
    .filter(col("price").gt(lit(100)))?;

// Both return same logical result, but 'good' reads less data
let result = good.collect().await?;
assert_batches_eq!(&[
    "+----------+-------+",
    "| product  | price |",
    "+----------+-------+",
    "| Laptop   | 1200  |",
    "+----------+-------+",
], &result);
```

_Quick Debug_: **Column not found error?** DataFusion is case-sensitive. Use [`sample_df.schema().field_names()`][`.schema()`] to list available columns.

#### Escape Hatch: SQL Syntax in Rust

Sometimes SQL syntax is just cleaner—especially for complex [`CASE`] expressions or nested functions. [`.select_exprs()`] parses SQL strings into expressions, giving you SQL's brevity with DataFrame's composability.

```rust
// Using sample_df: [product, price, quantity, category]

// SQL syntax inside Rust — parsed at runtime
let result = sample_df.clone()
    .select_exprs(&[
        "product",
        "price * 1.1 AS taxed_price",
        "CASE WHEN price > 100 THEN 'Premium' ELSE 'Standard' END AS tier"
    ])?
    .collect().await?;

assert_batches_eq!(&[
    "+----------+-------------+----------+",
    "| product  | taxed_price | tier     |",
    "+----------+-------------+----------+",
    "| Laptop   | 1320.0      | Premium  |",
    "| Mouse    | 27.5        | Standard |",
    "| Keyboard | 82.5        | Standard |",
    "+----------+-------------+----------+",
], &result);
```

> **Warning:** This loses compile-time safety. A typo like `"prodict"` compiles fine but fails at runtime. Use when SQL is genuinely clearer, not as a shortcut to avoid learning the expression API.

> **Going deeper:** See [`expr_api`] for complex expression patterns combining both approaches.

---

**Best Practice: Pick a Lane (or Document the Bridge)**

Mixing method chains with embedded SQL strings violates the [Single Level of Abstraction Principle][SLAP]—readers must context-switch constantly between abstraction levels. Choose _one_ approach:

| Approach                                                | Best For                               | Trade-off                            |
| :------------------------------------------------------ | :------------------------------------- | :----------------------------------- |
| **Full DataFrame**                                      | App logic, refactoring, IDE support    | Type-safe, but more verbose          |
| **Full SQL** via [`ctx.sql()`][`SessionContext::sql()`] | Ad-hoc queries, portability            | Familiar, but no compile-time checks |
| **Documented Constants**                                | Complex expressions reused across code | Traceable, but requires discipline   |

If you choose the third approach, extract SQL strings into named constants with doc comments:

```rust
/// Pricing tier based on unit price.
/// Business rule: ACME-1234
const TIER_EXPR: &str = "\
    CASE WHEN price > 100 \
         THEN 'Premium' \
         ELSE 'Standard' \
    END AS tier";

let df = sample_df.select_exprs(&["product", "price", TIER_EXPR])?;
```

This makes SQL expressions discoverable, testable, and traceable—rather than buried inline where they drift and multiply.

### Filtering Excellence

**Filtering controls which rows survive—applying predicates to discard irrelevant data early, before expensive joins or aggregations consume resources.**

Where [projection](#selection-and-projection-mastery) shapes columns, filtering shapes rows. The [`.filter()`] method accepts any boolean expression built from these building blocks:

| Predicate Type   | Methods                           | Example                                   |
| :--------------- | :-------------------------------- | :---------------------------------------- |
| Comparisons      | [`.gt()`], [`.lt()`], [`.eq()`]   | `col("price").gt(lit(100))`               |
| Logical          | [`.and()`], [`.or()`], [`.not()`] | `condition_a.and(condition_b)`            |
| Set membership   | [`in_list()`]                     | `col("status").in_list(vec![...], false)` |
| Pattern matching | [`.like()`], [`.ilike()`]         | `col("name").like(lit("A%"))`             |
| Range            | [`.between()`]                    | `col("age").between(lit(18), lit(65))`    |

Predicate pushdown ensures filters reach the data source, letting formats like Parquet skip entire row groups.

**SQL equivalent:** [`WHERE condition`][`WHERE`]

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Composable predicates built programmatically, Rust control flow for conditional logic, compile-time column checking, dynamic filters from runtime values
> - **SQL shines:** Familiar `WHERE` syntax, more readable for simple static conditions, clearer `AND`/`OR` precedence

#### Basic Filtering

**Start simple:** most filters are single-column comparisons. <br>
Build the predicate with [`col()`] for the column, a comparison method like [`.gt()`], and [`lit()`] for the literal value. The pattern reads naturally: `col("price").gt(lit(100))` means "price greater than 100".

```rust
// Using sample_df: [product, price, quantity, category]

// Filter: keep only rows where price > 100
let result = sample_df.clone()
    .filter(col("price").gt(lit(100)))?
    .collect().await?;

// Only Laptop (1200) survives — Mouse (25) and Keyboard (75) are filtered out
assert_batches_eq!(
    &[
        "+----------+-------+----------+-------------+",
        "| product  | price | quantity | category    |",
        "+----------+-------+----------+-------------+",
        "| Laptop   | 1200  | 5        | Electronics |",
        "+----------+-------+----------+-------------+",
    ],
    &result
);
```

#### Intermediate: Complex Predicates

**Real-world filters combine multiple conditions.** <br>
Chain predicates with [`.and()`] and [`.or()`], check set membership with [`in_list()`], match patterns with [`.like()`] (case-sensitive) or [`.ilike()`] (case-insensitive), and validate ranges with [`.between()`].

```rust
// Using sample_df: [product, price, quantity, category]

// AND/OR: (price > 50 AND quantity < 40) OR product = "Laptop"
let result = sample_df.clone()
    .filter(
        col("price").gt(lit(50))
            .and(col("quantity").lt(lit(40)))
            .or(col("product").eq(lit("Laptop")))
    )?
    .collect().await?;

// Keyboard matches (price=75 > 50, quantity=30 < 40)
// Laptop matches via OR clause
assert_batches_eq!(
    &[
        "+----------+-------+----------+-------------+",
        "| product  | price | quantity | category    |",
        "+----------+-------+----------+-------------+",
        "| Laptop   | 1200  | 5        | Electronics |",
        "| Keyboard | 75    | 30       | Accessories |",
        "+----------+-------+----------+-------------+",
    ],
    &result
);
```

**More examples for predicate patterns:**

```rust
// Using sample_df: [product, price, quantity, category]

// IN list: product IN ("Laptop", "Mouse")
let result = sample_df.clone()
    .filter(in_list(col("product"), vec![lit("Laptop"), lit("Mouse")], false))?
    .collect().await?;

assert_batches_eq!(
    &[
        "+----------+-------+----------+-------------+",
        "| product  | price | quantity | category    |",
        "+----------+-------+----------+-------------+",
        "| Laptop   | 1200  | 5        | Electronics |",
        "| Mouse    | 25    | 50       | Accessories |",
        "+----------+-------+----------+-------------+",
    ],
    &result
);

// Pattern matching: product LIKE '%board%' (contains "board")
let result = sample_df.clone()
    .filter(col("product").like(lit("%board%")))?
    .collect().await?;

assert_batches_eq!(
    &[
        "+----------+-------+----------+-------------+",
        "| product  | price | quantity | category    |",
        "+----------+-------+----------+-------------+",
        "| Keyboard | 75    | 30       | Accessories |",
        "+----------+-------+----------+-------------+",
    ],
    &result
);

// Case-insensitive matching: ILIKE or lower()
// Method 1: .ilike() — SQL's ILIKE equivalent
let result = sample_df.clone()
    .filter(col("product").ilike(lit("%BOARD%")))?  // Matches "Keyboard"
    .collect().await?;
assert_eq!(result[0].num_rows(), 1);

// Method 2: Normalize both sides with lower()
let result = sample_df.clone()
    .filter(lower(col("product")).eq(lit("keyboard")))?
    .collect().await?;
assert_eq!(result[0].num_rows(), 1);

// Range: price BETWEEN 50 AND 500
let result = sample_df.clone()
    .filter(col("price").between(lit(50), lit(500)))?
    .collect().await?;

assert_batches_eq!(
    &[
        "+----------+-------+----------+-------------+",
        "| product  | price | quantity | category    |",
        "+----------+-------+----------+-------------+",
        "| Keyboard | 75    | 30       | Accessories |",
        "+----------+-------+----------+-------------+",
    ],
    &result
);

// Null safety: price IS NOT NULL (all rows pass — no nulls in sample_df)
let result = sample_df.clone()
    .filter(col("price").is_not_null())?
    .collect().await?;

assert_batches_eq!(
    &[
        "+----------+-------+----------+-------------+",
        "| product  | price | quantity | category    |",
        "+----------+-------+----------+-------------+",
        "| Laptop   | 1200  | 5        | Electronics |",
        "| Mouse    | 25    | 50       | Accessories |",
        "| Keyboard | 75    | 30       | Accessories |",
        "+----------+-------+----------+-------------+",
    ],
    &result
);
```

> **Operator precedence:** [`.and()`] binds tighter than [`.or()`], just like SQL. Use parentheses (method chaining order) to make intent explicit: `a.and(b).or(c)` means `(a AND b) OR c`.

#### Advanced: Dynamic Filter Building

**This is where DataFrames truly outshine SQL.** When filter criteria come from user input, configuration, or runtime logic, building queries dynamically showcases two critical safety advantages:

1. **Rust's type system catches errors at compile time.** <br> _Misspell a column name?_ <br>
   The compiler tells you. Pass a string where a number is expected? Caught before your code ever runs. With dynamic SQL, these errors surface at runtime—often in production.

2. **SQL injection becomes impossible by design.** <br> Values flow through [`lit()`] as typed data, not string fragments. There's no way for user input like [`"; DROP TABLE users;--"`](https://xkcd.com/327/) to escape into query structure. You don't need to remember to sanitize—the API makes unsafe patterns unrepresentable.

```rust
// Using sample_df: [product, price, quantity, category]

/// Builds a filter expression from optional criteria.
/// Returns `lit(true)` if no criteria provided (matches all rows).
fn build_filter(min_price: Option<i32>, max_quantity: Option<i32>) -> Expr {
    let mut conditions: Vec<Expr> = Vec::new();

    if let Some(price) = min_price {
        conditions.push(col("price").gt_eq(lit(price)));
    }

    if let Some(qty) = max_quantity {
        conditions.push(col("quantity").lt_eq(lit(qty)));
    }

    // Fold conditions with AND; default to lit(true) if empty
    conditions
        .into_iter()
        .reduce(|acc, cond| acc.and(cond))
        .unwrap_or_else(|| lit(true))
}

// Example: min_price=50, no max_quantity constraint
let filter_expr = build_filter(Some(50), None);
let result = sample_df.clone().filter(filter_expr)?.collect().await?;

// Only Laptop (1200) and Keyboard (75) have price >= 50
assert_batches_eq!(
    &[
        "+----------+-------+----------+-------------+",
        "| product  | price | quantity | category    |",
        "+----------+-------+----------+-------------+",
        "| Laptop   | 1200  | 5        | Electronics |",
        "| Keyboard | 75    | 30       | Accessories |",
        "+----------+-------+----------+-------------+",
    ],
    &result
);

// Example: both constraints — price >= 50 AND quantity <= 10
let filter_expr = build_filter(Some(50), Some(10));
let result = sample_df.clone().filter(filter_expr)?.collect().await?;

// Only Laptop matches (price=1200 >= 50, quantity=5 <= 10)
assert_batches_eq!(
    &[
        "+----------+-------+----------+-------------+",
        "| product  | price | quantity | category    |",
        "+----------+-------+----------+-------------+",
        "| Laptop   | 1200  | 5        | Electronics |",
        "+----------+-------+----------+-------------+",
    ],
    &result
);
```

> **Why [`unwrap_or_else`] instead of [`unwrap()`]?** <br>
> Calling [`unwrap()`] on `None` panics—crashing your program. Here, [`reduce()`] returns `None` when the conditions vector is empty (no filters provided). Instead of panicking, [`unwrap_or_else`] lets us provide a fallback: [`lit(true)`][`lit()`] matches all rows. This is a common Rust pattern for gracefully handling "no input" cases.

#### Anti-Pattern: Multiple Sequential Filters

**Each [`.filter()`] call creates a separate node in the logical plan.** While DataFusion's optimizer _can_ merge adjacent filters, combining them yourself is clearer, guarantees a single predicate evaluation, and makes your intent explicit.

```rust
// Using sample_df: [product, price, quantity, category]

// ❌ DON'T: Chain multiple filter calls
let fragmented = sample_df.clone()
    .filter(col("price").gt(lit(50)))?
    .filter(col("quantity").lt(lit(100)))?
    .filter(col("product").is_not_null())?;

// ✅ DO: Combine into single filter
let combined = sample_df.clone()
    .filter(
        col("price").gt(lit(50))
            .and(col("quantity").lt(lit(100)))
            .and(col("product").is_not_null())
    )?;

// Both return the same result — Keyboard (price=75, quantity=30)
let result = combined.collect().await?;
assert_batches_eq!(
    &[
        "+----------+-------+----------+-------------+",
        "| product  | price | quantity | category    |",
        "+----------+-------+----------+-------------+",
        "| Keyboard | 75    | 30       | Accessories |",
        "+----------+-------+----------+-------------+",
    ],
    &result
);
```

> **Why it matters:** The fragmented version creates 3 filter nodes; the combined version creates 1. In complex queries, this compounds—affecting plan readability and optimization opportunities.

#### Filter Troubleshooting

| Symptom          | Cause                                                                  | Fix                                                 |
| :--------------- | :--------------------------------------------------------------------- | :-------------------------------------------------- |
| No rows returned | [three-valued logic]: `NULL > 5` → `NULL` (filtered out)               | Use [`.is_not_null()`] or [`coalesce()`]            |
| Nulls vanishing  | `col("x").eq(lit(false))` removes `NULL` too (`NULL = false` → `NULL`) | Add `.or(col("x").is_null())`                       |
| Slow filter      | Predicate not pushed to data source                                    | Check [`.explain()`]—filter should be _inside_ scan |

---

---

### Aggregation Patterns

**Aggregation collapses rows into summary statistics—transforming thousands of individual records into meaningful totals, averages, and counts that reveal patterns in your data.**

Aggregate functions like [`sum()`], [`avg()`], [`count()`], [`min()`], and [`max()`] reduce multiple values to a single result. The [`.aggregate()`] method takes two arguments:

1. **Grouping columns** — partition rows into groups (like SQL's `GROUP BY`)
2. **Aggregate expressions** — compute summaries per group

Without grouping columns (empty `vec![]`), aggregations summarize the entire DataFrame.

**SQL equivalent:** `SELECT dept, SUM(salary) FROM employees GROUP BY dept`

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Programmatic grouping keys, conditional aggregations via [`when()`], multiple aggregations in one call
> - **SQL shines:** Declarative [`GROUP BY`] syntax, [`HAVING`] clause more intuitive than chained [`.filter()`]

#### Basic Aggregation

Group rows by one or more columns, then compute summary statistics for each group. Import aggregate functions from `datafusion::functions_aggregate::expr_fn` and use [`.alias()`] to name the output columns.

This example establishes `employees_df`—used throughout this section.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Sample data: employees_df [department, employee, salary]
    let employees_df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Group by department, compute multiple aggregations
    let result = employees_df.clone().aggregate(
        vec![col("department")],
        vec![
            sum(col("salary")).alias("total_salary"),
            avg(col("salary")).alias("avg_salary"),
            count(col("employee")).alias("employee_count")
        ]
    )?;

    result.show().await?;
    // +-------------+--------------+------------+----------------+
    // | department  | total_salary | avg_salary | employee_count |
    // +-------------+--------------+------------+----------------+
    // | Engineering | 165000       | 82500.0    | 2              |
    // | Sales       | 105000       | 52500.0    | 2              |
    // +-------------+--------------+------------+----------------+

    Ok(())
}
```

> **Limitation:** The _result_ of [`.aggregate()`] contains only grouping columns and aggregated expressions—the `employee` column is gone. If you need it, include it in the group-by or aggregate it (e.g., `array_agg(col("employee"))`). The original DataFrame is immutable; `employees_df` still has all columns.

#### Intermediate: Multi-Level Grouping and HAVING-Style Filtering

To replicate SQL's [`HAVING`] clause, chain [`.filter()`] _after_ [`.aggregate()`]—the filter sees the aggregated column names.

```rust
// Using employees_df: [department, employee, salary]

// HAVING equivalent: filter on aggregated values
let high_budget_depts = employees_df.clone()
    .aggregate(
        vec![col("department")],
        vec![sum(col("salary")).alias("total_salary")]
    )?
    .filter(col("total_salary").gt(lit(100000)))?;  // HAVING total_salary > 100000

high_budget_depts.show().await?;
// +-------------+--------------+
// | department  | total_salary |
// +-------------+--------------+
// | Engineering | 165000       |
// +-------------+--------------+
// Sales (105000) filtered out — doesn't meet HAVING condition
```

> **Key insight:** In SQL, [`HAVING`] filters _after_ grouping while [`WHERE`] filters _before_ ([SQL clause order]). In DataFrames, method order achieves the same: `.filter().aggregate()` = WHERE, `.aggregate().filter()` = HAVING.

#### Advanced: All Aggregate Functions

DataFusion provides a comprehensive set of aggregate functions beyond the basics. Import them from [`datafusion::functions_aggregate::expr_fn`][expr_fn] and combine multiple aggregations in a single [`.aggregate()`] call for efficiency.

| Category    | Functions                                                                          |
| :---------- | :--------------------------------------------------------------------------------- |
| Basic       | [`count()`], [`sum()`], [`avg()`], [`min()`], [`max()`]                            |
| Statistical | [`stddev()`], [`var_sample()`], [`var_pop()`], [`median()`], [`approx_median()`]\* |
| Distinct    | [`count_distinct()`], [`approx_distinct()`]\*                                      |
| Conditional | `sum(when(...).otherwise(...))` — aggregate only matching rows                     |
| Collection  | [`array_agg()`], [`string_agg()`]                                                  |

_\*Approximate functions_ use probabilistic algorithms (e.g., [HyperLogLog] for [`approx_distinct()`]) that trade exactness for speed and memory. Use them on large datasets where exact results would be too expensive—typical error is <2%.

The following example demonstrates several aggregate functions in a single call:

```rust
// Using employees_df: [department, employee, salary]
use datafusion::functions_aggregate::expr_fn::*;

let stats = employees_df.clone().aggregate(
    vec![col("department")],
    vec![
        count(col("employee")).alias("count"),
        sum(col("salary")).alias("sum"),
        avg(col("salary")).alias("avg"),
        min(col("salary")).alias("min"),
        max(col("salary")).alias("max"),
        stddev(col("salary")).alias("stddev"),
        // Conditional: count employees earning > 70k
        sum(when(col("salary").gt(lit(70000)), lit(1))
            .otherwise(lit(0))?).alias("high_earners")
    ]
)?;

stats.show().await?;
// +-------------+-------+--------+---------+-------+-------+---------+--------------+
// | department  | count | sum    | avg     | min   | max   | stddev  | high_earners |
// +-------------+-------+--------+---------+-------+-------+---------+--------------+
// | Engineering | 2     | 165000 | 82500.0 | 80000 | 85000 | 3535.53 | 2            |
// | Sales       | 2     | 105000 | 52500.0 | 50000 | 55000 | 3535.53 | 0            |
// +-------------+-------+--------+---------+-------+-------+---------+--------------+
```

> **Tip:** See the full list of aggregate functions in the [Aggregate Functions Reference](../../user-guide/sql/aggregate_functions.md).

#### Advanced: Aggregation Without Grouping

Pass an empty `vec![]` as the grouping columns to aggregate the entire DataFrame into a single row—equivalent to SQL without a `GROUP BY` clause.

```rust
// Using employees_df: [department, employee, salary]

// Aggregate entire DataFrame (no GROUP BY) — summarize all rows
let company_totals = employees_df.clone().aggregate(
    vec![],  // Empty group by = entire DataFrame
    vec![
        sum(col("salary")).alias("company_total"),
        avg(col("salary")).alias("company_avg")
    ]
)?;

company_totals.show().await?;
// +---------------+-------------+
// | company_total | company_avg |
// +---------------+-------------+
// | 270000        | 67500.0     |
// +---------------+-------------+
```

#### Aggregation Troubleshooting

| Symptom          | Cause                                        | Fix                                                                           |
| :--------------- | :------------------------------------------- | :---------------------------------------------------------------------------- |
| Wrong results    | Unexpected grouping keys                     | Verify with `df.select(vec![col("key")]).distinct()?.show().await?`           |
| Column not found | Non-aggregated columns disappear             | Include in group-by or aggregate (e.g., [`array_agg()`])                      |
| Nulls skipped    | Aggregate functions ignore `NULL` by default | Use [`count(*)`][`count()`] for row count, or [`coalesce()`] to replace nulls |

#### Further Reading

**DataFusion Resources:**

- [Aggregate Functions Reference (SQL)](../../user-guide/sql/aggregate_functions.md) — Complete list of built-in aggregate functions with SQL examples
- [`datafusion-functions-aggregate` crate](https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/) — Rust API docs for all aggregate function implementations

**Concepts & Theory:**

- [SQL GROUP BY (PostgreSQL docs)](https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUP) — Canonical reference for grouping semantics
- [HyperLogLog Algorithm](https://en.wikipedia.org/wiki/HyperLogLog) — How `approx_distinct()` achieves O(1) memory

---

---

### When DataFrames Collide: Join Patterns

**Joins are the backbone of relational data processing—the operation that links separate tables into unified, queryable datasets by matching rows on shared keys.**

A join takes two DataFrames (or SQL tables) and produces a new one by comparing values in designated **key columns**—when values match (e.g., `customer.id = 1` on the left finds `order.customer_id = 1` on the right), the corresponding rows are stitched together. The result is a wider table combining columns from both sides, where related data now sits in the same row. Think of it as a lookup: for each row on the left, scan the right table for rows with matching key values, then concatenate them.

Whether you're enriching customer records with their orders, filtering products by inventory status, or reconciling data across systems, joins are the workhorse behind nearly every real-world data pipeline.

Master joins, and you unlock the full power of relational data processing.

> **DataFrame API coverage:** The DataFrame API supports all common join types ([`Inner`], [`Left`], [`Right`], [`Full`], [`LeftSemi`], [`LeftAnti`], and their right variants). Two SQL join types have **no direct DataFrame equivalent**:
>
> - [`NATURAL JOIN`] — use [`ctx.sql()`][`SessionContext::sql()`] or specify keys explicitly with [`.join()`]
> - [`CROSS JOIN`] — use [`.join()`] with empty key lists, or [`ctx.sql("... CROSS JOIN ...")`][`SessionContext::sql()`]
>
> For most workflows, the DataFrame API is fully sufficient. Fall back to SQL for these edge cases.

#### Why Joins Matter

Real-world data rarely lives in a single table. Customers are in one file, orders in another, products in a third. Joins let you:

- **Enrich** records by attaching related data (customer name → their orders)
- **Filter** by relationships (only customers _with_ orders, or _without_)
- **Reconcile** datasets (find what's in A but not B, or in both)
- **Validate** data quality—anti-joins reveal orphaned records (orders referencing non-existent customers), broken foreign keys, or rows dropped during ETL

Without joins, you'd be stuck writing nested loops or manual lookups. DataFusion's join engine handles the matching efficiently—you describe _what_ to combine, not _how_.

#### How Joins Work

Every join has three ingredients:

1. **Two tables** — left (your starting DataFrame) and right (the one you're joining)
2. **Join keys** — which columns to match (`customers.id = orders.customer_id`)
3. **Join type** — what to do with matches and non-matches

A very basic example is shown as the following as common in SQL :

```sql
-- SQL equivalent
SELECT *
FROM customers           -- left table
JOIN orders              -- right table
  ON customers.id = orders.customer_id   -- join keys
```

Since we cannot cover a tutorial for joins, please follow other tutorials as but not only the following resources:

| Resource                        | Focus                                                              |
| :------------------------------ | :----------------------------------------------------------------- |
| [Visual JOIN guide]             | Interactive visualization of all join types with animated examples |
| [Join tutorial]                 | Why Venn diagrams are misleading for understanding joins           |
| [Semi and Anti joins explained] | First-class existence checks that SQL forgot                       |
| [PostgreSQL JOIN docs]          | Authoritative reference—DataFusion follows PostgreSQL semantics    |
| [NULL handling in joins]        | Why `NULL = NULL` is `UNKNOWN`, not `TRUE`                         |

#### DataFrame API vs SQL

**Two paths, same destination.** Both APIs compile to the same internal [`LogicalPlan`] and benefit from identical optimizer passes—the difference is _how_ you construct the query:

| Aspect           | DataFrame API                                                 | SQL API                                           |
| ---------------- | ------------------------------------------------------------- | ------------------------------------------------- |
| **Construction** | Builder pattern—chain methods like [`.join()`], [`.filter()`] | Parser—write a query string, DataFusion parses it |
| **Type safety**  | Compile-time checks; typos caught by `rustc`                  | Runtime errors; typos discovered at execution     |
| **Composition**  | Programmatic; easy to build queries conditionally             | String-based; dynamic SQL requires concatenation  |
| **Result**       | [`LogicalPlan`] → Optimizer → Execution                       | [`LogicalPlan` ]→ Optimizer → Execution           |

**The multiplicity of SQL-Dialects**<br>
DataFusion's SQL parser ([`sqlparser`]) accepts syntax from multiple dialects—PostgreSQL, MySQL, Snowflake, and others. Throughout this documentation, we use **PostgreSQL syntax** as the reference standard: it's widely understood, well-documented, and DataFusion's join semantics (NULL handling, outer join behavior) closely follow PostgreSQL conventions. <br>
For more deeper insights follow [SQL Dialects][Understanding SQL Dialects (medium-article)]

Both SQL and the DataFrame API support the standard join families:

| Family      | SQL syntax                                   | DataFrame [`JoinType`]        | Purpose                                     |
| ----------- | -------------------------------------------- | ----------------------------- | ------------------------------------------- |
| **Inner**   | [`INNER JOIN`]                               | [`Inner`]                     | Only matching rows                          |
| **Outer**   | [`LEFT`] / [`RIGHT`] / [`FULL OUTER JOIN`]   | [`Left`], [`Right`], [`Full`] | Keep non-matches from one or both sides     |
| **Semi**    | [`LEFT / RIGHT SEMI JOIN`][`LEFT SEMI JOIN`] | [`LeftSemi`], [`RightSemi` ]  | Filter by existence (no columns from right) |
| **Anti**    | [`LEFT / RIGHT ANTI JOIN`][`LEFT ANTI JOIN`] | [`LeftAnti`], [`RightAnti` ]  | Filter by non-existence                     |
| **Cross**   | [`CROSS JOIN`]                               | _(none)_                      | Cartesian product (use empty keys)          |
| **Natural** | [`NATURAL JOIN`]                             | _(none)_                      | Auto-match same-named columns               |
| **Mark**    | _(internal)_                                 | [`LeftMark`], [`RightMark`]   | Adds boolean column for `EXISTS` subqueries |

> **SQL-only joins:**<br> > [`NATURAL JOIN`] and [`CROSS JOIN`] have no direct [`JoinType`] variant.
> Use [`ctx.sql()`][`SessionContext::sql()`] for natural joins; for cross joins, call [`.join()`] with empty key lists (see Anti-Pattern section).

The [`.join()`] method signature in the datafusion dataframe-API:

```rust
use datafusion::prelude::*;  // Includes JoinType

left_df.join(
    right_df,                    // 1. Right DataFrame
    JoinType::Inner,             // 2. Join type
    &["id"],                     // 3. Left key columns
    &["customer_id"],            // 4. Right key columns
    None,                        // 5. Optional filter expression
)?
```

**Two ways to specify joins:**

- [`.join()`] — Pass column names (`&[&str]`) for each side plus an optional `filter: Option<Expr>`. DataFusion builds equality predicates from the columns.
- [`.join_on()`] — Pass the full join condition as `Expr`s. Internally this wraps [`.join()`] with empty key lists and a combined filter expression (`expr_1 AND expr_2 ...`). Optimizer passes then extract equality predicates and treat them as equi-join keys.

After optimization, both methods produce equivalent plans—**no performance difference** for standard equi-joins. However, [`.join()`] is the "safer" choice: you explicitly declare equi-join keys, guaranteeing hash/sort-merge algorithms. With [`.join_on()`], if the optimizer can't extract equality predicates from your expression, it may fall back to nested loop joins.

Pick whichever reads better for your use case.

> **Gotcha: [`.join_on()`] uses AND, not OR**
>
> Multiple expressions passed to [`.join_on()`] are combined with [`AND`]:
>
> ```rust
> // This means: a = a2 AND b = b2 (not OR!)
> join_on(right, JoinType::Inner, [col("a").eq(col("a2")), col("b").eq(col("b2"))])
> ```
>
> For [`OR`] logic, build a single expression:
>
> ```rust
> // Match if EITHER a or b matches
> join_on(right, JoinType::Inner, [col("a").eq(col("a2")).or(col("b").eq(col("b2")))])
> ```

> **Trade-off: DataFrame vs SQL**
>
> | DataFrame API Advantages                                                                                                                                                   | SQL Advantages                                                        |
> | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
> | **First-class Semi/Anti joins** — `JoinType::LeftAnti`, `LeftSemi` etc. are explicit; no workarounds needed (unlike PySpark where you'd use `LEFT JOIN` + `WHERE IS NULL`) | **Visual clarity** — Multi-table joins read naturally in SQL syntax   |
> | **Type-safe composition** — Build joins conditionally with `if/else`; compiler catches column typos                                                                        | **Familiar syntax** — Standard `ON` clause understood by any SQL user |
> | **Chained transformations** — `.join().filter().select()` flows naturally                                                                                                  | **Copy-paste ready** — Test queries directly in SQL tools             |
> | **Complex conditions** — [`.join_on()`] accepts any `Expr`, not just column equality                                                                                       | **Self-documenting** — SQL is often readable by non-programmers       |
>
> **DataFusion-specific advantage:** Unlike many DataFrame libraries, DataFusion exposes the _full_ set of join types ([`LeftSemi`], [`RightSemi`], [`LeftAnti`], [`RightAnti`], [`LeftMark`], [`RightMark`]) as first-class operations—no need to emulate anti-joins with outer joins and null checks.

#### How Joins Execute

Under the hood, DataFusion selects from [several join algorithms] based on your data:

| Algorithm                  | When Used                                                                                      |
| :------------------------- | :--------------------------------------------------------------------------------------------- |
| [**Hash Join**]            | Default for equi-joins (`=`). Builds a hash table on the smaller side, probes with the larger. |
| [**Sort-Merge Join**]      | Pre-sorted inputs; can spill to disk for huge datasets.                                        |
| [**Symmetric Hash Join**]  | Streaming/unbounded data—both sides build hash tables, rows pruned via sliding windows.        |
| [**Nested Loop Join**]     | General non-equi conditions where hash-based algorithms don't apply.                           |
| [**Piecewise Merge Join**] | Single range filter (`<`, `>`, `<=`, `>=`)—much faster than nested loop for these cases.       |
| [**Cross Join**]           | Cartesian product—used for SQL [`CROSS JOIN`] and [`.join()`] with empty key lists.            |

The optimizer _can_ (based on configuration and statistics):

- **Swap sides** to put the smaller table on the build side
- **Choose partition mode**—broadcast small tables or hash-partition both sides
- **Push dynamic filters**—min/max bounds from the build side skip irrelevant probe data (e.g., Parquet row groups)

These behaviors are tunable via [`datafusion.optimizer`] settings.

All join algorithms leverage [Arrow]'s columnar format: instead of copying rows, DataFusion computes index arrays and uses vectorized [`take()`] operations to assemble results efficiently.

> **Why DataFusion Joins Are Fast**
>
> Unlike traditional row-based databases, DataFusion combines several modern techniques:
>
> | Technique                          | Benefit                                                                                                                                          |
> | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
> | **Columnar format (Arrow)**        | Read only the columns you need; SIMD instructions process thousands of keys in parallel                                                          |
> | **Vectorized execution**           | Joins process batches of rows, not one at a time—simple inner loops let CPUs parallelize at the instruction level                                |
> | **SQL = DataFrame**                | Both compile to the same `LogicalPlan`—identical optimizer benefits regardless of API choice                                                     |
> | **Statistics-driven optimization** | Table metadata (row counts, min/max) guide join order and algorithm selection—[**16x faster** on TPC-H benchmarks][DataFusion Join Optimization] |
> | **Late materialization**           | During joins, only key columns + row indices are processed; other columns are fetched afterward                                                  |
>
> The result: you describe _what_ to join, and the optimizer handles _how_—often matching or exceeding hand-tuned imperative code.

#### Join Types at a Glance

Joins control how rows from two tables are matched and combined. The key decisions are:

1.  what happens to rows that _don't_ match
2.  which columns appear in the result.

Inner joins discard non-matches; outer joins preserve them with NULLs. Semi and Anti joins answer existence questions without adding columns from the right table.

| Join Type            | Returns                   | Use Case                                    |
| :------------------- | :------------------------ | :------------------------------------------ |
| [`Inner`]            | Matches from both sides   | Standard join—only matching rows            |
| [`Left`]             | All left + matching right | Keep all left rows (NULL if no match)       |
| [`Right`]            | All right + matching left | Keep all right rows (NULL if no match)      |
| [`Full`]             | Everything from both      | See all data, matched or not                |
| [`LeftSemi`]         | Left rows WITH matches    | "Which left rows have a match?"             |
| [`LeftAnti`]         | Left rows WITHOUT matches | "Which left rows have NO match?"            |
| ~~Cross~~ (SQL only) | Cartesian product         | All combinations (see Anti-Pattern section) |

> **Note:** The DataFrame API has no `JoinType::Cross`. Cartesian products are represented as `Inner` joins with empty key lists or as [`CROSS JOIN`] in SQL.

> **Learn more:** You may want to check out this source [Join tutorial] or [Semi and Anti joins explained].

#### Basic: The Inner Join

This example establishes `customers_df` and `orders_df`—used throughout this section. Note: Carol has no orders, and order 104 has no matching customer (orphan).

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Sample data: customers_df [id, name] — Carol has no orders
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // Sample data: orders_df [order_id, customer_id, amount] — order 104 is orphaned
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Inner join: only matching rows (Carol excluded, order 104 excluded)
    let result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;

    result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**When to use Inner Join:**

- **Enrich data** — Attach related information (customer details → their orders)
- **Filter by relationship** — Keep only rows that have a match on the other side
- **Combine normalized tables** — Reassemble data split across multiple tables

**Not for set intersections!** <br>
If you need rows that exist in _both_ DataFrames (identical schemas, all columns compared), use [`.intersect()`] instead—that's a set operation, not a join.
<br> For more see the subsection [Dataframes unique methods](#dataframe-unique-methods)

> **⚠️ The hidden cost: [Survivorship bias][survivorship_bias]**
>
> Many join types silently drop non-matching rows—Inner, Semi, and Anti joins all filter out data. In the example above, Carol and order 104 simply vanish. Chain several such joins together and you may lose 60% of your data without noticing—you only see the "survivors" (rows that matched at every step).
>
> As a sanity check, if you need to see what's _missing_, use [Outer Joins](#intermediate-leftrightfull-joins) (or other oposit joins like left vs. right) instead—`NULL` values reveal exactly where data gaps exist.

#### Intermediate: Multi-Key Joins

Join on multiple columns when a single key isn't enough to uniquely identify matches—common with composite keys or temporal constraints.

```rust
// Regional sales: same product_id can exist in different regions
let inventory_df = dataframe!(
    "product_id" => [1, 1, 2, 2],
    "region" => ["East", "West", "East", "West"],
    "stock" => [100, 50, 200, 75]
)?;

let sales_df = dataframe!(
    "product_id" => [1, 1, 2],
    "region" => ["East", "West", "East"],
    "sold" => [30, 20, 80]
)?;

// Multi-key join: match on BOTH product_id AND region
let joined = inventory_df.join(
    sales_df,
    JoinType::Left,  // Keep all inventory, even unsold
    &["product_id", "region"],
    &["product_id", "region"],
    None
)?;

joined.show().await?;
// +------------+--------+-------+------------+--------+------+
// | product_id | region | stock | product_id | region | sold |
// +------------+--------+-------+------------+--------+------+
// | 1          | East   | 100   | 1          | East   | 30   |
// | 1          | West   | 50    | 1          | West   | 20   |
// | 2          | East   | 200   | 2          | East   | 80   |
// | 2          | West   | 75    |            |        |      |
// +------------+--------+-------+------------+--------+------+
// Notice: product_id and region appear TWICE (once from each table)
```

**The duplicate key columns are expected behavior.** DataFusion preserves all columns from both sides. Clean them up with `.select()`:

```rust
// Select only the columns you need
let result = joined.select(vec![
    col("product_id"),  // Picks the left-side column
    col("region"),
    col("stock"),
    col("sold"),
])?;

result.show().await?;
// +------------+--------+-------+------+
// | product_id | region | stock | sold |
// +------------+--------+-------+------+
// | 1          | East   | 100   | 30   |
// | 1          | West   | 50    | 20   |
// | 2          | East   | 200   | 80   |
// | 2          | West   | 75    |      |  ← No sales
// +------------+--------+-------+------+
```

**⚠️ Handling Same-Named Columns**

DataFusion's [`.join()`] preserves columns from both sides. When join keys share names, use one of these patterns:

| Pattern                        | When to use                                                |
| ------------------------------ | ---------------------------------------------------------- |
| **[`.select()`] after join**   | Simple joins—pick the columns you need                     |
| **[`.alias()`] before join**   | Complex multi-way joins—qualify with `col("alias.column")` |
| **[`.with_column_renamed()`]** | Rename conflicting columns before joining                  |

**Tip:** Call [`.schema()`] after joining to see actual column names.

> **Pro tip for time-dependent data:** <br>
> Multi-key joins on temporal columns work well when truncated to appropriate granularity using [`date_trunc()`]. Joining on `DATE` (day) has minimal edge cases (~0.004% at midnight); joining on raw `TIMESTAMP` (milliseconds) risks silent mismatches.

#### Intermediate: Left/Right/Full Joins

Where Inner Join keeps only the intersection (rows matching on both sides), **"partial" outer joins (left, right and full) preserve rows that don't match**—filling missing columns with `NULL`. This makes data gaps visible instead of silently dropping them.

| Join Type | Keeps                                           | Typical Use Case                                        |
| :-------- | :---------------------------------------------- | :------------------------------------------------------ |
| **Left**  | All left rows, matching right data if available | Customer reports—keep all customers, show orders if any |
| **Right** | All right rows, matching left data if available | Orphan detection—find orders without valid customers    |
| **Full**  | Everything from both sides                      | Data reconciliation—find ALL discrepancies              |

Left Join handles ~90% of outer join use cases. Right Join can usually be rewritten as Left Join by swapping tables. Full Join is for reconciliation scenarios.

##### Left Join — Enrich Your Primary Data

Keep **all rows from the left table**, enrich with matching data from the right table. No match? Right-side columns become `NULL`.

```rust
// customers_df:
// +----+-------+
// | id | name  |
// +----+-------+
// | 1  | Alice |
// | 2  | Bob   |
// | 3  | Carol |  ← Has no orders
// +----+-------+
//
// orders_df:
// +----------+-------------+--------+
// | order_id | customer_id | amount |
// +----------+-------------+--------+
// | 101      | 1           | 100    |
// | 102      | 1           | 200    |
// | 103      | 2           | 150    |
// | 104      | 99          | 300    |  ← Orphan
// +----------+-------------+--------+

// "All customers with their orders (if any)"
let left_result = customers_df.clone().join(
    orders_df.clone(),
    JoinType::Left,
    &["id"],
    &["customer_id"],
    None
)?;

left_result.show().await?;
// +----+-------+----------+-------------+--------+
// | id | name  | order_id | customer_id | amount |
// +----+-------+----------+-------------+--------+
// | 1  | Alice | 101      | 1           | 100    |
// | 1  | Alice | 102      | 1           | 200    |
// | 2  | Bob   | 103      | 2           | 150    |
// | 3  | Carol |          |             |        |  ← Preserved with NULLs
// +----+-------+----------+-------------+--------+
```

**Use Case: Self-Joins (Customer Referrals)**

A **self-join** joins a table with itself—essential for hierarchical data. Left Join preserves all rows even if they have no match (like Alice, who has no referrer).

```rust
// Extended customers: add referred_by (which customer referred them)
let customers_referrals = dataframe!(
    "id" => [1, 2, 3],
    "name" => ["Alice", "Bob", "Carol"],
    "referred_by" => [None::<i64>, Some(1), Some(1)]  // Alice referred Bob and Carol
)?;

// Self-join: alias both sides to disambiguate
let customer = customers_referrals.clone().alias("customer")?;
let referrer = customers_referrals.clone().alias("referrer")?;

let with_referrers = customer.join(
    referrer,
    JoinType::Left,  // Keep customers without referrers (Alice)
    &["referred_by"],
    &["id"],
    None
)?.select(vec![
    col("customer.name").alias("customer"),
    col("referrer.name").alias("referred_by"),
])?;

with_referrers.show().await?;
// +----------+-------------+
// | customer | referred_by |
// +----------+-------------+
// | Alice    | NULL        |  ← No referrer
// | Bob      | Alice       |
// | Carol    | Alice       |
// +----------+-------------+
```

**Key pattern:** <br>
Use [`.alias()`] to create two "views" of the same DataFrame, then join with qualified column names (`customer.name`, `referrer.name`).

**Common self-join patterns:**

- **Hierarchy traversal:** employees → managers, categories → parent categories
- **Sequential comparison:** this_year.sales vs last_year.sales (join on product_id)
- **Finding pairs:** "Which products are often bought together?" (order_items self-join)

##### Right Join — Find Orphaned Records

Keep all rows from the right table—useful for finding records that reference non-existent parents (like order 104 referencing customer 99).

```rust
// "All orders, showing customer info (if customer exists)"
let right_result = customers_df.clone().join(
    orders_df.clone(),
    JoinType::Right,
    &["id"],
    &["customer_id"],
    None
)?;

right_result.show().await?;
// +----+-------+----------+-------------+--------+
// | id | name  | order_id | customer_id | amount |
// +----+-------+----------+-------------+--------+
// | 1  | Alice | 101      | 1           | 100    |
// | 1  | Alice | 102      | 1           | 200    |
// | 2  | Bob   | 103      | 2           | 150    |
// |    |       | 104      | 99          | 300    |  ← Orphan! No customer 99
// +----+-------+----------+-------------+--------+
```

**Tip:** <br>
Right Join is just Left Join with swapped tables. `A.join(B, Right)` = `B.join(A, Left)`. Most teams use Left Join exclusively for consistency—put your "main" table first.

##### Full Join — Complete Reconciliation

Keep **all rows from both tables**. Where there's no match, fill the "other side" with NULLs. This is the only join that guarantees you see _everything_—matched, unmatched left, AND unmatched right.

**When to use Full Join:**

- **Data reconciliation** — Comparing two data sources to find ALL discrepancies
- **Migration validation** — Ensuring old and new systems have the same records
- **Audit trails** — "Show me what's in A but not B, what's in B but not A, and what's in both"

```rust
// "Show me everything: matched, unmatched left, AND unmatched right"
let full_result = customers_df.clone().join(
    orders_df.clone(),
    JoinType::Full,
    &["id"],
    &["customer_id"],
    None
)?;

full_result.show().await?;
// +----+-------+----------+-------------+--------+
// | id | name  | order_id | customer_id | amount |
// +----+-------+----------+-------------+--------+
// | 1  | Alice | 101      | 1           | 100    |  ← Matched
// | 1  | Alice | 102      | 1           | 200    |  ← Matched
// | 2  | Bob   | 103      | 2           | 150    |  ← Matched
// | 3  | Carol |          |             |        |  ← Left only (no orders)
// |    |       | 104      | 99          | 300    |  ← Right only (orphan)
// +----+-------+----------+-------------+--------+
```

**Data Quality Pattern:** Full Join + NULL filters = powerful reconciliation tool:

```rust
// Find customers WITHOUT orders (left-only)
let inactive = full_result.clone().filter(col("order_id").is_null())?;

// Find orphaned orders (right-only, invalid customer_id)
let orphans = full_result.clone().filter(col("id").is_null())?;

// Find matched records (both sides present)
let matched = full_result.filter(
    col("id").is_not_null().and(col("order_id").is_not_null())
)?;
```

This pattern is invaluable for ETL pipelines, data migration validation, and debugging referential integrity issues.

#### Intermediate: Semi and Anti Joins

**What makes them special?** <br>
Semi and Anti joins are **filtering joins**—they filter the left table based on existence in the right table, but **never add columns** from the right table. This is fundamentally different from Inner/Left/Right/Full joins which combine data.

| Join Type    | Question                         | Returns                           | SQL Equivalent                               |
| :----------- | :------------------------------- | :-------------------------------- | :------------------------------------------- |
| **LeftSemi** | "Which left rows HAVE a match?"  | Left columns only, matched rows   | `WHERE EXISTS (SELECT 1 FROM right ...)`     |
| **LeftAnti** | "Which left rows have NO match?" | Left columns only, unmatched rows | `WHERE NOT EXISTS (SELECT 1 FROM right ...)` |

**Why use them instead of alternatives?**

| Alternative                | Problem                                                             | Semi/Anti Advantage                                              |
| :------------------------- | :------------------------------------------------------------------ | :--------------------------------------------------------------- |
| Inner Join + Distinct      | Creates duplicates if right has multiple matches, then removes them | Semi join handles this automatically—one output row per left row |
| Left Join + WHERE NULL     | Joins everything first, then filters                                | Anti join filters during join—more efficient                     |
| `IN (SELECT ...)` subquery | Can be slower, harder to optimize                                   | Semi join is the optimized physical plan for `IN`                |

##### LeftSemi — "Which Rows Have Matches?"

Returns left rows that have **at least one match** in the right table. Even if a customer has 10 orders, they appear only once.

```rust
// Using customers_df and orders_df from Basic example:
//
// customers_df:                 orders_df:
// +----+-------+                +----------+-------------+--------+
// | id | name  |                | order_id | customer_id | amount |
// +----+-------+                +----------+-------------+--------+
// | 1  | Alice |                | 101      | 1           | 100    |
// | 2  | Bob   |                | 102      | 1           | 200    |
// | 3  | Carol |                | 103      | 2           | 150    |
// +----+-------+                | 104      | 99          | 300    |
//                               +----------+-------------+--------+

// "Which customers have placed at least one order?"
let active_customers = customers_df.clone().join(
    orders_df.clone(),
    JoinType::LeftSemi,
    &["id"],
    &["customer_id"],
    None
)?;

active_customers.show().await?;
// +----+-------+
// | id | name  |
// +----+-------+
// | 1  | Alice |  ← Has 2 orders, appears once
// | 2  | Bob   |  ← Has 1 order
// +----+-------+
// Note: Carol (id=3) excluded—no orders
// Note: No order columns! Just filtered customers.
```

**Use cases for LeftSemi:**

- Find active customers (have placed orders)
- Find products that have been sold (exist in order_items)
- Filter to "things that are referenced somewhere"

##### LeftAnti — "Which Rows Have No Matches?"

Returns left rows that have **zero matches** in the right table. The inverse of Semi join.

```rust
// Using customers_df and orders_df from Basic example:
//
// customers_df:                 orders_df:
// +----+-------+                +----------+-------------+--------+
// | id | name  |                | order_id | customer_id | amount |
// +----+-------+                +----------+-------------+--------+
// | 1  | Alice |                | 101      | 1           | 100    |
// | 2  | Bob   |                | 102      | 1           | 200    |
// | 3  | Carol |                | 103      | 2           | 150    |
// +----+-------+                | 104      | 99          | 300    |
//                               +----------+-------------+--------+

// "Which customers have NEVER placed an order?"
let inactive_customers = customers_df.clone().join(
    orders_df.clone(),
    JoinType::LeftAnti,
    &["id"],
    &["customer_id"],
    None
)?;

inactive_customers.show().await?;
// +----+-------+
// | id | name  |
// +----+-------+
// | 3  | Carol |  ← No orders found
// +----+-------+
// Alice and Bob excluded—they have orders
```

**Use cases for LeftAnti:**

- Find inactive customers (never ordered)
- Find dead inventory (products never sold)
- Data cleanup: "Find records missing required relationships"
- Complement of Semi: `Semi ∪ Anti = Full Left Table`

##### Why Not Just Use Left Join + Filter?

A common question: "Can't I just do Left Join and filter for NULLs?"

```rust
// Using customers_df and orders_df from Basic example:
//
// customers_df:                 orders_df:
// +----+-------+                +----------+-------------+--------+
// | id | name  |                | order_id | customer_id | amount |
// +----+-------+                +----------+-------------+--------+
// | 1  | Alice |                | 101      | 1           | 100    |
// | 2  | Bob   |                | 102      | 1           | 200    |
// | 3  | Carol |                | 103      | 2           | 150    |
// +----+-------+                | 104      | 99          | 300    |
//                               +----------+-------------+--------+

// ❌ Less efficient: Join everything, then filter
let inactive_v1 = customers_df.clone()
    .join(orders_df.clone(), JoinType::Left, &["id"], &["customer_id"], None)?
    .filter(col("order_id").is_null())?;

// ✅ More efficient: Anti join filters during the join
let inactive_v2 = customers_df.clone()
    .join(orders_df.clone(), JoinType::LeftAnti, &["id"], &["customer_id"], None)?;

// Both produce the same result:
// +----+-------+
// | id | name  |
// +----+-------+
// | 3  | Carol |
// +----+-------+
```

Both produce the same result, but Anti join:

- Doesn't create intermediate joined rows
- Doesn't add (and then ignore) right-side columns
- Optimizer can use more efficient algorithms (e.g., hash-based existence check)

> **Learn more:** See [Semi and Anti joins explained] for why these deserve first-class syntax in SQL.

> **Other variants:** <br>
> DataFusion's [`JoinType`] also includes `RightSemi`, `RightAnti`, and mark variants for advanced use cases. For most DataFrame work, stick to left variants and swap input tables if needed.

> **Mark joins:** <br> > [`LeftMark`]/[`RightMark`] are used internally to decorrelate `EXISTS` subqueries. They return all rows from one side plus an extra boolean "mark" column indicating whether any match exists on the other side. Most DataFrame code won't use them directly, but you may see them in `EXPLAIN` plans for complex SQL with `EXISTS` predicates.

#### Advanced: Multi-Way Joins

**Chain [`.join()`] calls to combine 3+ tables—each join produces a new DataFrame that feeds into the next.**

Real-world data is often normalized across multiple tables. A business question like "which customers have paid orders?" requires combining customers → orders → payments. Each chained Inner Join acts as a filter—only rows matching _all_ join conditions survive.

```rust
// Using customers_df and orders_df from Basic example:
//
// customers_df:                 orders_df:
// +----+-------+                +----------+-------------+--------+
// | id | name  |                | order_id | customer_id | amount |
// +----+-------+                +----------+-------------+--------+
// | 1  | Alice |                | 101      | 1           | 100    |
// | 2  | Bob   |                | 102      | 1           | 200    |
// | 3  | Carol |                | 103      | 2           | 150    |
// +----+-------+                | 104      | 99          | 300    |
//                               +----------+-------------+--------+

// Introduce a payments table: only orders 101 and 103 have payment records
//
// payments_df:
// +------------------+---------+
// | payment_order_id | status  |
// +------------------+---------+
// | 101              | paid    |
// | 103              | pending |
// +------------------+---------+
//
let payments_df = dataframe!(
    "payment_order_id" => [101, 103],
    "status" => ["paid", "pending"]
)?;

// 3-way join: customers → orders → payments
// Use .select() to keep only the columns we need (avoids duplicates)
let result = customers_df.clone()
    .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
    .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?
    .select(vec![ // for better visibility of the result df
        col("name"),
        col("order_id"),
        col("amount"),
        col("status"),
    ])?;

result.show().await?;
// +-------+----------+--------+---------+
// | name  | order_id | amount | status  |
// +-------+----------+--------+---------+
// | Alice | 101      | 100    | paid    |
// | Bob   | 103      | 150    | pending |
// +-------+----------+--------+---------+
//
// What got filtered out:
// - Alice's order 102: no payment record
// - Carol: no orders at all
// - Order 104: orphan (customer_id 99 doesn't exist)
```

> **Tip:** <br>
> Use Left Joins at intermediate steps if you need to preserve unmatched rows (e.g., customers without payments).

##### Join Order Matters

The order you chain joins affects both **readability** and **performance**. General principles:

| Principle                                     | Why                                                                        |
| --------------------------------------------- | -------------------------------------------------------------------------- |
| **Start with your "main" table (Left-Table)** | Reads naturally: "customers with their orders and payments"                |
| **Filter early**                              | Reduces intermediate result size before expensive joins                    |
| **Smaller tables on the right**               | Hash joins build from the right side—smaller = faster                      |
| **Let the optimizer help**                    | DataFusion may reorder joins, but good initial order reduces planning work |

```rust
// customers_df:
// +----+-------+
// | id | name  |
// +----+-------+
// | 1  | Alice |
// | 2  | Bob   |
// | 3  | Carol |
// +----+-------+
//
// orders_df:
// +----------+-------------+--------+
// | order_id | customer_id | amount |
// +----------+-------------+--------+
// | 101      | 1           | 100    |
// | 102      | 1           | 200    |
// | 103      | 2           | 150    |
// | 104      | 99          | 300    |
// +----------+-------------+--------+
//
// payments_df:
// +------------------+---------+
// | payment_order_id | status  |
// +------------------+---------+
// | 101              | paid    |
// | 103              | pending |
// +------------------+---------+

// ✅ Good: Start with the table you're "asking about"
// "Which customers have payments?"
let result = customers_df.clone()
    .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
    .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?;

// ✅ Also good: Start with filtered data to reduce intermediate size
let high_value_orders = orders_df.clone().filter(col("amount").gt(lit(100)))?;
let result = high_value_orders
    .join(customers_df.clone(), JoinType::Inner, &["customer_id"], &["id"], None)?
    .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?;
```

> **Performance tip:** <br>
> The optimizer reorders joins when beneficial, but good initial ordering reduces planning overhead. Use [`.explain()`] to see the actual execution plan.

##### Managing Column Proliferation

Multi-way joins accumulate columns from every table. With each join, you get **all columns from both sides**—including duplicate key columns. Chain [`.select()`] at the end to keep only what you need:

```rust
// customers_df (2 columns):
// +----+-------+
// | id | name  |
// +----+-------+
// | 1  | Alice |
// | 2  | Bob   |
// | 3  | Carol |
// +----+-------+
//
// orders_df (3 columns):
// +----------+-------------+--------+
// | order_id | customer_id | amount |
// +----------+-------------+--------+
// | 101      | 1           | 100    |
// | 102      | 1           | 200    |
// | 103      | 2           | 150    |
// | 104      | 99          | 300    |
// +----------+-------------+--------+
//
// payments_df (2 columns):
// +------------------+---------+
// | payment_order_id | status  |
// +------------------+---------+
// | 101              | paid    |
// | 103              | pending |
// +------------------+---------+

// Without .select(): 2 + 3 + 2 = 7 columns (with redundant id, customer_id, order_id, payment_order_id)
// With .select(): pick only the 4 columns that matter
let result = customers_df.clone()
    .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
    .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?
    .select(vec![     // <--- This select removes the cludder
        col("name").alias("customer"),
        col("order_id"),
        col("amount"),
        col("status").alias("payment_status"),
    ])?;

result.show().await?;
// +----------+----------+--------+----------------+
// | customer | order_id | amount | payment_status |
// +----------+----------+--------+----------------+
// | Alice    | 101      | 100    | paid           |
// | Bob      | 103      | 150    | pending        |
// +----------+----------+--------+----------------+
```

> **When SQL might be clearer:** <br>
> Multi-way joins with 4+ tables can become hard to read as chained method calls. Consider [`SessionContext::sql()`] for complex [star-schema queries][databricks_star_schema] where SQL's visual structure helps.

#### Advanced: Join with Complex Conditions

Sometimes you need more than simple column equality. Range joins ("orders placed within 7 days of signup"), inequality predicates ("amount > threshold"), or compound logic ("match on id AND status = 'active'") require expressions that [`.join()`] can't express with just column names.

[`.join_on()`] accepts arbitrary boolean expressions as join conditions. Internally it wraps [`.join()`] with empty key lists and passes your expressions as a filter—the optimizer then extracts any equality predicates for efficient hash/sort-merge execution.

| Use Case           | Example Condition                                                       |
| ------------------ | ----------------------------------------------------------------------- |
| **Range join**     | `order_date.between(start_date, end_date)`                              |
| **Inequality**     | `col("amount").gt(col("threshold"))`                                    |
| **Compound logic** | `col("id").eq(col("customer_id")).and(col("status").eq(lit("active")))` |

```rust
// customers_df:
// +----+-------+
// | id | name  |
// +----+-------+
// | 1  | Alice |
// | 2  | Bob   |
// | 3  | Carol |
// +----+-------+
//
// orders_df:
// +----------+-------------+--------+
// | order_id | customer_id | amount |
// +----------+-------------+--------+
// | 101      | 1           | 100    |  ← Alice, amount = 100
// | 102      | 1           | 200    |  ← Alice, amount = 200
// | 103      | 2           | 150    |  ← Bob, amount = 150
// | 104      | 99          | 300    |  ← Orphan
// +----------+-------------+--------+

// Give DataFrames aliases so we can qualify column references
let customers = customers_df.clone().alias("customers")?;
let orders = orders_df.clone().alias("orders")?;

// Join with compound condition: match on id AND filter amount > 100
let high_value = customers.join_on(
    orders,
    JoinType::Inner,
    [col("customers.id").eq(col("orders.customer_id"))
        .and(col("orders.amount").gt(lit(100)))]
)?;

high_value.show().await?;
// +----+-------+----------+-------------+--------+
// | id | name  | order_id | customer_id | amount |
// +----+-------+----------+-------------+--------+
// | 1  | Alice | 102      | 1           | 200    |
// | 2  | Bob   | 103      | 2           | 150    |
// +----+-------+----------+-------------+--------+
// Alice's order 101 (amount=100) excluded—doesn't meet amount > 100
// Carol excluded—no orders at all
```

> **Tip:** <br>
> When using [`.join_on()`], column names may clash between tables. Use [`.alias()`] to qualify references: `col("customers.id")` vs `col("orders.id")`.

##### The `filter` Argument on Outer Joins

The [`.join()`] method's fifth parameter is [`filter: Option<Expr>`][join_filter_param]—easy to overlook in the signature but powerful for outer joins. This filter has **subtle but important semantics**: it applies only to _matched_ rows, not to preserved unmatched rows.

This distinction matters because:

- A [`WHERE`] clause **after** the join would filter out unmatched rows (turning your Left Join into an Inner Join)
- The `filter` argument applies **during** the join, controlling which matches are considered valid while still preserving unmatched rows

| Approach                                      | Behavior                         | Result                              |
| --------------------------------------------- | -------------------------------- | ----------------------------------- |
| [`.join(..., Some(filter))`][`.join()`]       | Filter is part of join condition | Unmatched rows preserved with NULLs |
| [`.join(..., None).filter(...)`][`.filter()`] | Filter applied after join        | Unmatched rows may be removed       |

```rust
// customers_df:
// +----+-------+
// | id | name  |
// +----+-------+
// | 1  | Alice |
// | 2  | Bob   |
// | 3  | Carol |  ← Has no orders
// +----+-------+
//
// orders_df:
// +----------+-------------+--------+
// | order_id | customer_id | amount |
// +----------+-------------+--------+
// | 101      | 1           | 100    |  ← Alice, amount = 100 (will be filtered)
// | 102      | 1           | 200    |  ← Alice, amount = 200 (passes filter)
// | 103      | 2           | 150    |  ← Bob, amount = 150 (passes filter)
// | 104      | 99          | 300    |  ← Orphan
// +----------+-------------+--------+

// Left join with filter: Carol still appears, but only orders > 100 attach
let result = customers_df.clone().join(
    orders_df.clone(),
    JoinType::Left,
    &["id"],                           // left_cols
    &["customer_id"],                  // right_cols
    Some(col("amount").gt(lit(100))),  // filter (5th param) - applied only to matched rows
)?;

result.show().await?;
// +----+-------+----------+-------------+--------+
// | id | name  | order_id | customer_id | amount |
// +----+-------+----------+-------------+--------+
// | 1  | Alice | 102      | 1           | 200    |  ←  > 100 attached
// | 2  | Bob   | 103      | 2           | 150    |
// | 3  | Carol |          |             |        |  ← Preserved!
// +----+-------+----------+-------------+--------+
// Alice's order 101 (amount=100) excluded by filter
// Carol preserved because Left Join keeps all left rows
```

> **Mental model:** <br>
> Think of `filter` as part of the _join condition_, not a `WHERE` after the join. It controls which matches are valid during the join itself.

> **Also applies to [`.join_on()`]:** <br>
> Since [`.join_on()`] is implemented as [`.join()`] with empty key lists and combined `on_exprs` as `filter`, the same semantics apply.

#### Anti-Pattern: Accidental Cartesian Product

Empty join keys produce a **Cartesian product**—every left row paired with every right row. This is almost never intentional and can crash your query or exhaust memory.

| Left rows | Right rows | Result rows       | Scale                            |
| --------- | ---------- | ----------------- | -------------------------------- |
| 3         | 4          | 12                | Tiny dataset, still 4× larger    |
| 1,000     | 1,000      | 1,000,000         | 1 million rows                   |
| 1,000,000 | 1,000,000  | 1,000,000,000,000 | **1 trillion rows** — will crash |

```rust
// customers_df:
// +----+-------+
// | id | name  |
// +----+-------+
// | 1  | Alice |
// | 2  | Bob   |
// | 3  | Carol |
// +----+-------+
//
// orders_df:
// +----------+-------------+--------+
// | order_id | customer_id | amount |
// +----------+-------------+--------+
// | 101      | 1           | 100    |
// | 102      | 1           | 200    |
// | 103      | 2           | 150    |
// | 104      | 99          | 300    |
// +----------+-------------+--------+

// ❌ DANGEROUS: Empty keys = Cartesian product
let cartesian = customers_df.clone().join(
    orders_df.clone(),
    JoinType::Inner,
    &[],  // No join keys!
    &[],
    None
)?;

cartesian.show().await?;
// +----+-------+----------+-------------+--------+
// | id | name  | order_id | customer_id | amount |
// +----+-------+----------+-------------+--------+
// | 1  | Alice | 101      | 1           | 100    |  ← Alice × order 101
// | 1  | Alice | 102      | 1           | 200    |  ← Alice × order 102
// | 1  | Alice | 103      | 2           | 150    |  ← Alice × order 103 (not her order!)
// | 1  | Alice | 104      | 99          | 300    |  ← Alice × order 104 (not her order!)
// | 2  | Bob   | 101      | 1           | 100    |  ← Bob × order 101 (not his order!)
// | ... 7 more rows ... |
// +----+-------+----------+-------------+--------+
// Total: 3 × 4 = 12 rows — every combination!

// ✅ CORRECT: Always specify join keys
let correct = customers_df.clone().join(
    orders_df.clone(),
    JoinType::Inner,
    &["id"],         // left key
    &["customer_id"], // right key
    None
)?;

println!("Row count: {}", correct.clone().count().await?);
// Row count: 3  ← Only matching rows (Alice×2, Bob×1)
```

**⚠️ Warning:** <br>
If a join returns unexpectedly many rows, check your keys. An empty or mismatched key array silently produces a Cartesian product. Use [`.count()`] before [`.collect()`] to verify.

**If you need a Cartesian product:** <br>
Use SQL via [`ctx.sql("SELECT ... FROM a CROSS JOIN b")`][`SessionContext::sql()`]. The DataFrame API has no `JoinType::Cross`—empty keys with `Inner` produces the same result but reads like a bug.

#### Join Troubleshooting

Joins can silently produce unexpected results. When something looks wrong, check these common issues:

| Symptom             | Common Causes                                                             | Diagnosis                                                                   |
| :------------------ | :------------------------------------------------------------------------ | :-------------------------------------------------------------------------- |
| **Empty result**    | Key values don't match, trailing whitespace, case mismatch, NULLs in keys | Inspect both sides: `.select(vec![col("key")]).distinct().show().await?`    |
| **Too many rows**   | Duplicate keys create row multiplication, accidental Cartesian product    | Check key uniqueness: `.select(vec![col("key")]).distinct().count().await?` |
| **Missing columns** | Wrong column names after join, schema mismatch                            | Inspect schema: [`.schema()`] and use [`.alias()`] to qualify               |
| **Wrong matches**   | Keys have different types (string vs int), encoding issues                | Compare types: `df.schema().field_with_name("key")?.data_type()`            |

##### Sanity Check: Did the Join Drop Too Much Data?

DataFusion doesn't have built-in join validation, but you can build a simple check to catch silent data loss:

```rust
// Count rows before join
let left_count = customers_df.clone().count().await?;

// Perform the join
let joined = customers_df.clone().join(
    orders_df.clone(),
    JoinType::Inner,
    &["id"],
    &["customer_id"],
    None
)?;
let joined_count = joined.clone().count().await?;

// Calculate retention rate
let retention_pct = (joined_count as f64 / left_count as f64) * 100.0;
println!("Rows: {} → {} ({:.1}% retention)", left_count, joined_count, retention_pct);

// ⚠️ Alert if too much data was dropped
if retention_pct < 50.0 {
    eprintln!("WARNING: Join dropped {:.1}% of rows! Check keys, NULLs, types.",
        100.0 - retention_pct);
}
```

| Join Type    | Expected Retention     | Warning Sign                       |
| ------------ | ---------------------- | ---------------------------------- |
| **Inner**    | Varies by data overlap | < 50% often indicates key mismatch |
| **Left**     | 100% of left rows      | < 100% means something is wrong    |
| **LeftSemi** | ≤ 100% (filtered)      | 0% = no matches at all             |
| **LeftAnti** | Complement of Semi     | 100% = nothing matched             |

##### Quick Debugging Steps

**Step 1: Inspect inputs before joining**

Before joining, verify that key values actually overlap. Use [`.distinct()`] to see the unique key values on each side—if they don't match, your join will produce empty or unexpected results.

```rust
// Verify key values exist and match on both sides
println!("Left keys:");
customers_df.clone()
    .select(vec![col("id")])
    .distinct()?  // Unique values only
    .show().await?;
// +----+
// | id |
// +----+
// | 1  |
// | 2  |
// | 3  |
// +----+

println!("Right keys:");
orders_df.clone()
    .select(vec![col("customer_id")])
    .distinct()?
    .show().await?;
// +-------------+
// | customer_id |
// +-------------+
// | 1           |
// | 2           |
// | 99          |  ← No matching customer! Will be dropped in Inner join
// +-------------+
```

**Step 2: Check for NULL keys**

In SQL semantics, `NULL = NULL` returns `UNKNOWN` (not `TRUE`), so NULL keys **never match**. This silently drops rows.

```rust
// Count NULLs in join key
let null_count = df.clone()
    .filter(col("customer_id").is_null())?
    .count().await?;
println!("NULL keys: {}", null_count);

// Fix: Replace NULLs with sentinel value before joining
let df = df.with_column("customer_id", coalesce(vec![col("customer_id"), lit(-1)]))?;
```

> **Config option:** <br>
> DataFusion has [`datafusion.optimizer.filter_null_join_keys`][`datafusion.optimizer`] to automatically filter NULL keys.

**Step 3: Examine the execution plan**

DataFusion's [`.explain()`] is your window into how the query optimizer transformed your join. It reveals which algorithm was selected, whether predicates were pushed down, and potential performance issues.

```rust
// explain(verbose, analyze) - verbose=true shows optimized plan
joined_df.explain(true, false)?.show().await?;
```

**What to look for in the plan:**

| Node                 |                       Meaning                       | Performance                       |
| -------------------- | :-------------------------------------------------: | --------------------------------- |
| `HashJoinExec`       | Hash-based join (builds hash table from right side) | ✅ Fast for equi-joins            |
| `SortMergeJoinExec`  |             Sort both sides, then merge             | ✅ Good for large sorted data     |
| `NestedLoopJoinExec` |               Compares every row pair               | ⚠️ Slow — only for non-equi joins |
| `CrossJoinExec`      |                  Cartesian product                  | ❌ Usually a bug                  |

**Signs of a healthy plan:**

- Predicates pushed into `ParquetExec` or `CsvExec` (filter early)
- `HashJoinExec` or `SortMergeJoinExec` for equi-joins
- Smaller table on the **build side** (right side of hash join)

**⚠️ Warning signs:**

- `NestedLoopJoinExec` when you expected equi-join → check if optimizer couldn't extract equality predicates
- `CrossJoinExec` → accidental Cartesian product
- Filters appearing **after** the join instead of pushed down

```rust
// Example output (simplified):
// HashJoinExec: mode=Partitioned, join_type=Inner
//   left: ParquetExec: file=customers.parquet, predicate=id IS NOT NULL
//   right: ParquetExec: file=orders.parquet, predicate=customer_id IS NOT NULL
//                       ↑ Good! NULL filter pushed down
```

> **Pro tip:** Use `.explain(true, true)?` (analyze=true) to see actual row counts and timing after execution—helps identify which join leg is the bottleneck.

#### Join Cheat Sheet

Quick reference for choosing the right join pattern:

| Goal                        | Method                     | JoinType         |
| :-------------------------- | :------------------------- | :--------------- |
| Standard lookup             | [`.join()`]                | `Inner`          |
| Keep all primary records    | [`.join()`]                | `Left`           |
| Filter by existence         | [`.join()`]                | `LeftSemi`       |
| Filter by non-existence     | [`.join()`]                | `LeftAnti`       |
| See all data (reconcile)    | [`.join()`]                | `Full`           |
| Range/inequality conditions | [`.join_on()`]             | `Inner`          |
| Self-join (hierarchies)     | [`.alias()`] + [`.join()`] | `Inner/Left`     |
| Cartesian product           | Prefer SQL `CROSS JOIN`    | Empty keys = bug |

#### Further Reading

Joins are fundamental yet often misunderstood. These resources provide deeper understanding:

**DataFrame APIs** — Similar concepts in other libraries:

| Resource                 | Focus                                                        |
| :----------------------- | :----------------------------------------------------------- |
| [Spark Join Guide]       | Conceptually similar API with extensive examples             |
| [Polars Join Operations] | Rust-native DataFrame library, closest to DataFusion's model |
| [DataFusion `.join()`]   | Official Rust API documentation                              |

**Join Algorithms & Optimization** — How joins execute under the hood:

| Resource                           | Focus                                                                                    |
| :--------------------------------- | :--------------------------------------------------------------------------------------- |
| [Optimizing SQL & DataFrames Pt 1] | Andrew Lamb on DataFusion's optimizer—why SQL and DataFrames compile to the same plan    |
| [Optimizing SQL & DataFrames Pt 2] | Deep dive: predicate pushdown, projection pushdown, join ordering in DataFusion          |
| [DataFusion Join Optimization]     | How DataFusion uses table statistics to choose build/probe sides—**16x faster** on TPC-H |
| [CMU Join Algorithms]              | Andy Pavlo's database course—excellent video lectures on hash/sort-merge joins           |
| [Hash Join (Wikipedia)]            | How hash tables enable O(n+m) equi-joins                                                 |
| [Sort-Merge Join]                  | Why pre-sorted data enables efficient streaming joins                                    |
| [Join optimization strategies]     | How databases choose algorithms and what you can control                                 |

**SQL Semantics** — Conceptual foundations:

| Resource                                                                  | Focus                                                              |
| :------------------------------------------------------------------------ | :----------------------------------------------------------------- |
| [Visual JOIN guide]                                                       | Interactive visualization of all join types with animated examples |
| [Join tutorial]                                                           | Why Venn diagrams are misleading for understanding joins           |
| [Semi and Anti joins explained]                                           | First-class existence checks that SQL forgot                       |
| [PostgreSQL JOIN docs]                                                    | Authoritative reference—DataFusion follows PostgreSQL semantics    |
| [NULL handling in joins]                                                  | Why `NULL = NULL` is `UNKNOWN`, not `TRUE`                         |
| [Understanding SQL Dialects][Understanding SQL Dialects (medium-article)] | Medium article about different SQL dialects                        |

---

---

### Sorting and Limiting

Order results and implement pagination with [`sort()`] and [`limit()`].

**SQL equivalent:** `ORDER BY score DESC LIMIT 10 OFFSET 5`

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Explicit null placement control (`.sort(asc, nulls_last)`); combined skip+fetch in single `.limit()` call
> - **SQL shines:** Familiar `ORDER BY ... DESC` syntax; `NULLS FIRST/LAST` in modern SQL

#### Basic Sorting

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol", "Dave"],
        "score" => [85, 92, 78, 95]
    )?;

    // Sort ascending
    let df = df.sort(vec![col("score").sort(true, true)])?;  // ASC, nulls last

    df.show().await?;
    Ok(())
}
```

#### Intermediate: Multi-Column Sorting and Pagination

```rust
// Sort by multiple columns
let df = df.sort(vec![
    col("score").sort(false, true), // DESC, nulls last (primary sort)
    col("name").sort(true, false)   // ASC, nulls first (tie-breaker)
])?;

// Pagination: skip 1, take 2 (OFFSET 1 LIMIT 2)
let page = df.limit(1, Some(2))?;

// Quick preview without full execution
let preview = df.show_limit(10).await?;  // Show first 10 rows
```

#### Advanced: Null Handling in Sorts

```rust
let df = dataframe!(
    "name" => ["Alice", "Bob", None, "Dave"],
    "score" => [85, 92, 78, None]
)?;

// Control null placement
let nulls_first = df.sort(vec![col("name").sort(true, false)])?;  // ASC, nulls first
let nulls_last = df.sort(vec![col("name").sort(true, true)])?;    // ASC, nulls last
```

_Quick Debug_: **Sort not working as expected?** Check for nulls—they can appear first or last depending on the null ordering parameter. Use `filter(col("column").is_not_null())` before sorting if needed.

### Set Operations and Deduplication

Combine DataFrames or remove duplicates using union, intersection, and distinct operations.

**SQL equivalent:** `SELECT * FROM df1 UNION SELECT * FROM df2`

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Schema validation at build time; DataFrame-unique methods like `.union_by_name()` and `.distinct_on()` (see below)
> - **SQL shines:** Standard `UNION`, `INTERSECT`, `EXCEPT` syntax; portable across databases

#### Basic Set Operations

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df1 = dataframe!("id" => [1, 2, 3])?;
    let df2 = dataframe!("id" => [3, 4, 5])?;

    // Union (preserves duplicates) - SQL: UNION ALL
    let union_all = df1.clone().union(df2.clone())?;
    // Result: [1, 2, 3, 3, 4, 5]

    // Union with duplicate removal - SQL: UNION
    let union_distinct = df1.clone().union_distinct(df2.clone())?;
    // Result: [1, 2, 3, 4, 5]

    // Distinct values
    let distinct = df1.distinct()?;

    Ok(())
}
```

#### Intermediate: DataFrame-Only Operations

```rust
// Union by name: aligns columns by name, not position (DataFrame-only!)
// Similar to Spark's unionByName - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionByName.html
let sales_q1 = dataframe!(
    "product" => ["A", "B"],
    "revenue" => [100, 200]
)?;

let sales_q2 = dataframe!(
    "revenue" => [150, 250],  // Note: different column order
    "product" => ["A", "C"]
)?;

// Regular union would fail - different column order
// union_by_name works!
let combined = sales_q1.clone().union_by_name(sales_q2.clone())?;

// With deduplication
let combined_distinct = sales_q1.union_by_name_distinct(sales_q2)?;
```

#### Advanced: DISTINCT ON

```rust
// DISTINCT ON: keep first row for each unique value in specified columns
// PostgreSQL-specific feature - see: https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT
let df = dataframe!(
    "customer" => ["Alice", "Alice", "Bob", "Bob"],
    "order_date" => ["2026-01-01", "2026-01-05", "2026-01-02", "2026-01-03"],
    "amount" => [100, 200, 150, 175]
)?;

// Keep only the first order for each customer (PostgreSQL-style)
let first_orders = df.distinct_on(
    vec![col("customer")],           // Distinct on these columns
    vec![col("order_date").sort(true, true)],  // Order by (to control which row is kept)
    None
)?;
// Result: Alice's Jan 1 order, Bob's Jan 2 order
```

#### Other Set Operations

```rust
// Intersect: rows in both DataFrames
let common = df1.intersect(df2)?;

// Except: rows in df1 but not in df2 (SQL: EXCEPT / MINUS)
let df1_only = df1.except(df2)?;
```

_Quick Debug_: **Union fails with schema error?** The DataFrames must have the same schema (column names and types). Use `union_by_name()` for flexibility with column order, or use `select()` to align schemas before union.

_Quick Debug_: **distinct() not working?** It considers all columns. If you want distinct based on specific columns, use `distinct_on()` or select only those columns first.

---

---

<!--TODO New commers -->

<!-- Selection & Projection references -->

[`.gt()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.gt
[`CASE`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.when.html
[`expr_api`]: docs/source/library-user-guide/working-with-exprs.md
[`.select_exprs()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_exprs
[`SessionContext::sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`array_agg()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/array_agg/index.html

<!-- Filtering references -->

[`.not()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.not
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.lt()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.lt
[`.eq()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.eq
[`.and()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.and
[`.or()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.or
[`in_list()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.in_list.html
[`.like()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.like
[`.ilike()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.ilike
[`lower()`]: https://docs.rs/datafusion/latest/datafusion/functions/unicode/fn.lower.html
[`.between()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.between
[`.is_not_null()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.is_not_null
[`.is_null()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.is_null

<!--SQL Statements-->

[`SELECT`]: ../../user-guide/sql/select.md
[`WHERE`]: ../../user-guide/sql/select.md#where
[PostgreSQL]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN "PostgreSQL: The de facto standard for DataFusion SQL behavior"
[`AND`]: ../../user-guide/sql/operators.md#logical-operators#and
[`OR`]: ../../user-guide/sql/operators.md#logical-operators#or
[`AS`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/ast/struct.ExprWithAlias.html
[`GROUP BY`]: ../../user-guide/sql/select.md#group-by-clause
[`JOIN`]: ../../user-guide/sql/select.md#join-clause
[`ORDER BY`]: ../../user-guide/sql/select.md#order-by-clause
[`LIMIT`]: ../../user-guide/sql/select.md#limit-clause
[`OFFSET`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/dialect/keywords/constant.OFFSET.html
[`UNION`]: ../../user-guide/sql/select.md#union-clause
[`DISTINCT`]: ../../user-guide/sql/select.md#select-clause
[`DISTINCT ON`]: https://github.com/apache/datafusion/issues/7827
[`TO_DATE`]: ../../user-guide/sql/scalar_functions.md#to_date
[`NATURAL JOIN`]: ../../user-guide/sql/select.md#natural-join
[`CROSS JOIN`]: ../../user-guide/sql/select.md#cross-join
[`INNER JOIN`]: ../../user-guide/sql/select.md#inner-join
[`LEFT`]: ../../user-guide/sql/select.md#left-join
[`LEFT SEMI JOIN`]: ../../user-guide/sql/select.md#left-semi-join
[`LEFT ANTI JOIN`]: ../../user-guide/sql/select.md#left-anti-join√
[`FULL OUTER JOIN`]: ../../user-guide/sql/select.md#full-outer-join
[`HAVING`]: ../../user-guide/sql/select.md#having_clause

<!-- Dataframe Statements -->

[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.describe()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.describe
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.is_not_null()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Expr.html#method.is_not_null
[`.like()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Expr.html#method.like
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.select_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_columns
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.with_column_renamed()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed
[`.aggregate()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[`sum()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.sum.html
[`avg()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.avg.html
[`count()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.count.html
[`min()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.min.html
[`max()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.max.html
[`stddev()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.stddev.html
[`var_sample()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.var_sample.html
[`var_pop()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.var_pop.html
[`median()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.median.html
[`approx_median()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.approx_median.html
[`count_distinct()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.count_distinct.html
[`approx_distinct()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.approx_distinct.html
[`array_agg()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.array_agg.html
[`string_agg()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.string_agg.html
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[join_filter_param]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join "See the 'filter' parameter in the join() signature"
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`JoinType`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html
[`Inner`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Inner "Only rows with matches in both tables"
[`Left`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Left "All left rows + matching right rows (NULL if no match)"
[`Right`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Right "All right rows + matching left rows (NULL if no match)"
[`Full`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Full "All rows from both tables (NULL where no match)"
[`LeftSemi`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftSemi "Left rows that have a match (no right columns)"
[`LeftAnti`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftAnti "Left rows that have NO match (no right columns)"
[`LeftMark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftMark "Mark join for EXISTS subquery decorrelation"
[`RightMark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightMark "Mark join for EXISTS subquery decorrelation"
[Join tutorial]: https://blog.jooq.org/say-no-to-venn-diagrams-when-explaining-joins/ "Why Venn diagrams mislead when explaining joins"
[Semi and Anti joins explained]: https://blog.jooq.org/semi-join-and-anti-join-should-have-its-own-syntax-in-sql/ "Why Semi/Anti joins deserve first-class syntax"
[several join algorithms]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/index.html "DataFusion join implementations"
[Visual JOIN guide]: https://joins.spathon.com/ "Interactive visual guide to SQL joins"
[PostgreSQL JOIN docs]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN "Authoritative reference for join semantics"
[PostgreSQL semantics]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN "DataFusion follows PostgreSQL SQL semantics"
[`sqlparser`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/dialect/index.html "DataFusion's SQL parser supports multiple dialects"
[NULL handling in joins]: https://modern-sql.com/concept/null "Why NULL comparisons return UNKNOWN, not TRUE/FALSE"
[Join optimization strategies]: https://use-the-index-luke.com/sql/join "How databases optimize joins and what you can control"
[Spark Join Guide]: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html "Apache Spark SQL join syntax and examples"
[Polars Join Operations]: https://docs.pola.rs/user-guide/transformations/joins/ "Polars DataFrame join operations"
[DataFusion `.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join "DataFusion join API"
[Optimizing SQL & DataFrames Pt 1]: https://www.influxdata.com/blog/optimizing-sql-dataframes-part-one/ "Optimizing SQL (and DataFrames) in DataFusion: Part 1"
[Optimizing SQL & DataFrames Pt 2]: https://www.influxdata.com/blog/optimizing-sql-dataframes-part-two/ "Optimizing SQL (and DataFrames) in DataFusion: Part 2"
[DataFusion Join Optimization]: https://xebia.com/blog/making-joins-faster-in-datafusion-based-on-table-statistics/ "Making Joins Faster in DataFusion Based on Table Statistics"
[CMU Join Algorithms]: https://www.youtube.com/watch?v=YIdIaPopfpk&list=PLSE8ODhjZXjYMAgsGH-GtY5rJYZ6zjsd5&index=12 "CMU 15-445 Lecture 11: Join Algorithms (Andy Pavlo)"
[Hash Join (Wikipedia)]: https://en.wikipedia.org/wiki/Hash_join "Hash join algorithm explanation"
[Sort-Merge Join]: https://en.wikipedia.org/wiki/Sort-merge_join "Sort-merge join algorithm"
[**Hash Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.HashJoinExec.html "Equi-join using hash table on build side"
[**Sort-Merge Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SortMergeJoinExec.html "Join pre-sorted inputs with optional spilling"
[**Symmetric Hash Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SymmetricHashJoinExec.html "Streaming join for unbounded data"
[**Nested Loop Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.NestedLoopJoinExec.html "General non-equi join conditions"
[**Piecewise Merge Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.PiecewiseMergeJoinExec.html "Optimized for single range conditions"
[**Cross Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.CrossJoinExec.html "Cartesian product of two tables"
[Arrow]: https://arrow.apache.org/ "Apache Arrow: columnar in-memory format"
[`take()`]: https://docs.rs/arrow/latest/arrow/compute/kernels/take/fn.take.html "Arrow kernel: select elements by index"
[`.sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[`.limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_distinct
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[`.distinct_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct_on
[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`.except()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[`.window()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.window
[`.unnest_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.unnest_columns
[`.unnest_columns_with_options()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.unnest_columns_with_options
[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.show_limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show_limit
[`.count()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.count
[`.describe()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.describe
[`upper()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.upper.html
[`or()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.or
[`.like()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.like
[`.explain()`]: ../../user-guide/explain-usage.md

<!--Datafusion Core types -->

[`datafusion.optimizer`]: https://docs.rs/datafusion/latest/datafusion/optimizer/index.html
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`prelude`]: https://docs.rs/datafusion/latest/datafusion/prelude/index.html
[`RightSemi`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightSemi
[`RightAnti`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightAnti
[HyperLogLog]: https://en.wikipedia.org/wiki/HyperLogLog

<!--  standalone functions-->

[`coalesce()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.coalesce.html
[`reduce()`]: https://doc.rust-lang.org/core/option/enum.Option.html#method.reduce
[`unwrap()`]: https://doc.rust-lang.org/core/option/enum.Option.html#method.unwrap
[`unwrap_or_else`]: https://doc.rust-lang.org/std/option/enum.Option.html#method.unwrap_or_else
[`lit()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.lit.html
[`col()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.col.html
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`lower()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.lower.html
[`trim()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.trim.html
[`to_date()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.to_date.html
[`TO_DATE`]: ../../user-guide/sql/scalar_functions.md#to_date
[`coalesce()`]: https://docs.rs/datafusion/latest/datafusion/functions/core/expr_fn/fn.coalesce.html
[`COALESCE`]: ../../user-guide/sql/scalar_functions.md#coalesce
[`nvl()`]: https://docs.rs/datafusion/latest/datafusion/functions/core/expr_fn/fn.nvl.html
[`date_part()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.date_part.html
[`date_trunc()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.date_trunc.html
[`DATE_PART`]: ../../user-guide/sql/scalar_functions.md#date_part
[`EXTRACT`]: ../../user-guide/sql/scalar_functions.md#date_part
[`when()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.when.html
[`.otherwise()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.CaseBuilder.html#method.otherwise
[`substring()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.substring.html
[`replace()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.replace.html
[`date_part()`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/compute/fn.date_part.html

<!-- External resources-->

[datafusion paper]: https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf
[docs.rs]: https://docs.rs/datafusion/latest/datafusion/#architecture
[pandas]: https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe
[Apache Spark]: https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
[dataframe algebra]: https://arxiv.org/pdf/2001.00888
[postgres docs]: https://www.postgresql.org/docs/
[spark docs]: https://spark.apache.org/docs/latest/
[three-valued logic]: https://modern-sql.com/concept/three-valued-logic
[predicate pushdown]: https://docs.rs/datafusion/latest/datafusion/physical_plan/filter_pushdown/struct.PushedDownPredicate.html
[SLAP]: https://www.jameshw.dev/blog/2022-02-05/principles-from-clean-code#d69d60c9b3054d47816551afafcfa847 "Functions should SLAP! — from Clean Code principles"
[Apache Spark DataFrames]: https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes
[databricks_star_schema]: https://www.databricks.com/glossary/star-schema
[survivorship_bias]: https://en.wikipedia.org/wiki/Survivorship_bias
[anti_semi_joins]: https://blog.jooq.org/semi-join-and-anti-join-should-have-its-own-syntax-in-sql/
[Understanding SQL Dialects (medium-article)]: https://medium.com/@abhapratiti27/understanding-sql-dialects-a-deeper-dive-into-the-linguistic-variations-of-sql-e7e2fdb7509b
[expr_fn]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/index.html
[SQL clause order]: https://www.postgresql.org/docs/current/sql-select.html#SQL-HAVING

### Window Functions

Apply calculations across rows related to the current row, like running totals, rankings, and moving averages.

**SQL equivalent:** `SELECT name, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees`

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Builder pattern (`.partition_by().order_by().build()`) is composable and reusable; multiple windows in one `.window()` call
> - **SQL shines:** Declarative `OVER` clause is more readable for simple cases; frame specifications (`ROWS BETWEEN`) more intuitive

> **Note:** Window functions follow the [SQL-99 Standard] specification. See also: [Window Functions](../../user-guide/sql/window_functions.md) for all available window functions.

#### Basic: Ranking

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "department" => ["Sales", "Sales", "Eng", "Eng"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Rank employees within each department by salary
    let result = df.window(vec![
        row_number()
            .partition_by(vec![col("department")])
            .order_by(vec![col("salary").sort(false, true)])
            .build()?
            .alias("rank")
    ])?;

    result.show().await?;
    Ok(())
}
```

#### Intermediate: Running Totals and Aggregates

```rust
use datafusion::functions_window::expr_fn::*;
use datafusion::functions_aggregate::expr_fn::*;

// Running total of sales
let df = df.window(vec![
    sum(col("amount"))
        .order_by(vec![col("date").sort(true, true)])
        .build()?
        .alias("running_total")
])?;

// Custom window frame: moving average over last 3 rows (current + 2 preceding)
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion::common::ScalarValue;

let df = df.window(vec![
    avg(col("amount"))
        .order_by(vec![col("date").sort(true, true)])
        .window_frame(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2))),  // 2 rows before
            WindowFrameBound::CurrentRow,
        ))
        .build()?
        .alias("moving_avg_3")
])?;
```

#### Advanced: lead() and lag()

```rust
// Compare with previous/next row
let df = df.window(vec![
    lag(col("amount"), Some(1), None)
        .partition_by(vec![col("customer")])
        .order_by(vec![col("date").sort(true, true)])
        .build()?
        .alias("prev_amount"),
    lead(col("amount"), Some(1), None)
        .partition_by(vec![col("customer")])
        .order_by(vec![col("date").sort(true, true)])
        .build()?
        .alias("next_amount")
])?;
```

_Quick Debug_: **Window function not partitioning correctly?** Ensure your `partition_by()` columns are the grouping you want. Without `partition_by()`, the window applies to the entire DataFrame.

### Reshaping Data

> **Note:** `.unnest_columns()` is a DataFrame-unique method with limited SQL equivalent (`UNNEST` varies by database). See also the DataFrame-Unique Methods section.

#### Unnesting / Exploding Arrays

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sample data with array columns using SQL ARRAY syntax
    let df = ctx.sql("SELECT * FROM (VALUES
        ('Alice', ARRAY[1, 2, 3]),
        ('Bob', ARRAY[4, 5])
    ) AS t(customer, orders)").await?;

    // Unnest expands array elements into separate rows
    // Similar to PySpark explode: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html
    let expanded = df.unnest_columns(&["orders"])?;
    expanded.show().await?;
    // +----------+--------+
    // | customer | orders |
    // +----------+--------+
    // | Alice    | 1      |
    // | Alice    | 2      |
    // | Alice    | 3      |
    // | Bob      | 4      |
    // | Bob      | 5      |
    // +----------+--------+

    Ok(())
}
```

### Subqueries

Subqueries allow you to use the result of one query within another query. DataFusion supports scalar subqueries (returning a single value), IN subqueries (checking membership), and EXISTS subqueries (checking existence).

**SQL equivalent:** `WHERE amount > (SELECT AVG(amount) FROM orders)` or `WHERE id IN (SELECT customer_id FROM premium)`

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Type-safe subquery construction; reusable subquery plans as variables
> - **SQL shines:** More intuitive nested syntax; familiar to SQL users

#### Scalar Subqueries

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::avg;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register tables
    let orders = dataframe!(
        "order_id" => [1, 2, 3, 4],
        "amount" => [100, 200, 150, 300]
    )?;
    ctx.register_table("orders", orders.clone().into_view())?;

    // Find orders above average
    let avg_subquery = ctx.table("orders").await?
        .aggregate(vec![], vec![avg(col("amount"))])?
        .select(vec![avg(col("amount"))])?
        .into_unoptimized_plan();

    let result = ctx.table("orders").await?
        .filter(col("amount").gt(scalar_subquery(Arc::new(avg_subquery))))?;

    result.show().await?;
    Ok(())
}
```

#### IN Subqueries

```rust
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let customers = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let premium_customers = dataframe!(
        "customer_id" => [1, 3]
    )?;

    ctx.register_table("customers", customers.into_view())?;
    ctx.register_table("premium", premium_customers.into_view())?;

    // Find customers who are premium
    let premium_ids = ctx.table("premium").await?
        .select(vec![col("customer_id")])?
        .into_unoptimized_plan();

    let result = ctx.table("customers").await?
        .filter(in_subquery(col("id"), Arc::new(premium_ids)))?;

    result.show().await?;
    Ok(())
}
```

---

<!-- TODO:  intersect and intersect_distinct, except and except_distinct, union, union_by name (but this is schema relevant, i guss , unnest columns see : https://docs.rs/datafusion/latest/datafusion/common/struct.UnnestOptions.html, ) -->

## DataFrame-Unique Methods

**Some DataFrame methods have no SQL equivalent—these are the programmatic superpowers that justify using the DataFrame API.**

While the previous section covered operations available in _both_ APIs, this section highlights methods unique to DataFrames. These exist because SQL's declarative grammar cannot express certain programmatic patterns that Rust handles naturally.

| Method                                            | Purpose                                 | Why SQL Can't Express It                  |
| ------------------------------------------------- | --------------------------------------- | ----------------------------------------- |
| [`.with_column()`](#adding-and-replacing-columns) | Add/replace a column keeping all others | SQL `SELECT` requires listing all columns |
| [`.with_column_renamed()`](#renaming-columns)     | Rename without expression               | SQL uses `AS` inside `SELECT`             |
| [`.drop_columns()`](#dropping-columns)            | Remove columns by name                  | SQL has no direct equivalent              |
| [`.union_by_name()`](#union-by-column-name)       | Union aligned by name, not position     | SQL `UNION` is positional                 |
| [`.distinct_on()`](#distinct-on-postgresql-style) | Keep first row per group                | PostgreSQL-specific, not standard SQL     |
| [`.unnest_columns()`](#unnesting-arrays)          | Explode arrays into rows                | SQL `UNNEST` varies by database           |
| [`.into_view()`](#bridging-to-sql)                | Register DataFrame as SQL table         | Enables hybrid SQL/DataFrame workflows    |

> **Gap Note:** `unpivot`/`melt` (wide-to-long reshaping) is not yet available in DataFusion. Workaround: manual `UNION ALL` of columns.

### Adding and Replacing Columns

`.with_column()` adds a new column or replaces an existing one _while keeping all other columns intact_. In SQL, you'd need to explicitly list every column in your `SELECT`.

```rust
use datafusion::prelude::*;

let df = dataframe!(
    "product" => ["Laptop", "Mouse"],
    "price" => [1200, 25]
)?;

// Add a computed column - all existing columns are preserved
let df = df.with_column("discounted", col("price") * lit(0.9))?;
// Result: product, price, discounted

// Replace an existing column
let df = df.with_column("price", col("price") * lit(1.1))?;
// Result: product, price (updated), discounted
```

### Renaming Columns

`.with_column_renamed()` renames a column without requiring an expression:

```rust
let df = df.with_column_renamed("price", "unit_price")?;
```

### Dropping Columns

`.drop_columns()` removes columns by name—no SQL equivalent exists:

```rust
let df = df.drop_columns(&["category", "temp_id"])?;
```

### Union by Column Name

`.union_by_name()` aligns DataFrames by column _name_, not position. SQL `UNION` fails if column order differs:

```rust
let q1 = dataframe!(
    "product" => ["A"],
    "revenue" => [100]
)?;

let q2 = dataframe!(
    "revenue" => [200],  // Different order!
    "product" => ["B"]
)?;

// SQL UNION would fail or produce wrong results
// union_by_name handles it correctly
let combined = q1.union_by_name(q2)?;
```

### DISTINCT ON (PostgreSQL-Style)

`.distinct_on()` keeps the first row for each unique value in specified columns—a PostgreSQL feature not in standard SQL:

```rust
use datafusion::prelude::*;

let df = dataframe!(
    "customer" => ["Alice", "Alice", "Bob"],
    "order_date" => ["2024-01-01", "2024-01-05", "2024-01-02"],
    "amount" => [100, 200, 150]
)?;

// Keep only the earliest order per customer
let first_orders = df.distinct_on(
    vec![col("customer")],
    vec![col("order_date").sort(true, true)],
    None
)?;
```

### Unnesting Arrays

`.unnest_columns()` explodes array columns into multiple rows:

```rust
let df = dataframe!(
    "customer" => ["Alice", "Bob"],
    "tags" => [vec!["vip", "early"], vec!["new"]]
)?;

let expanded = df.unnest_columns(&["tags"])?;
// Alice | vip
// Alice | early
// Bob   | new
```

### Bridging to SQL

`.into_view()` registers a DataFrame as a table that SQL can query—enabling hybrid workflows:

```rust
let ctx = SessionContext::new();

let df = dataframe!(
    "id" => [1, 2, 3],
    "value" => [100, 200, 300]
)?
.filter(col("value").gt(lit(150)))?;

// Register the filtered DataFrame as a SQL-queryable table
ctx.register_table("filtered_data", df.into_view())?;

// Now query it with SQL
let result = ctx.sql("SELECT * FROM filtered_data WHERE id > 1").await?;
```

## Builder Methodology: Architecting with DataFrames

**The DataFrame API isn't just SQL with different syntax—it's a programmatic _builder_ for query plans that integrates with Rust's type system, control flow, and tooling.**

This section explains _how to think_ in DataFrames: leveraging Rust's strengths to build dynamic, reusable, and robust data pipelines that SQL strings cannot express.

| Pattern                                                         | What It Enables                           | SQL Limitation                       |
| --------------------------------------------------------------- | ----------------------------------------- | ------------------------------------ |
| [Builder Pattern & Laziness](#the-builder-pattern-and-laziness) | Reuse intermediate plans as variables     | CTEs are query-scoped                |
| [Dynamic Construction](#dynamic-pipeline-construction)          | Rust `if/else` modifies the plan          | String concatenation, injection risk |
| [Encapsulation](#encapsulation-and-reusability)                 | Functions returning `Expr` or `DataFrame` | UDFs are hard to deploy/test         |
| [Error Handling](#error-handling-and-observability)             | Compile-time + runtime error separation   | All errors at runtime                |

### The Builder Pattern and Laziness

**Every DataFrame method returns a new DataFrame wrapping an extended `LogicalPlan`—no data moves until you call an action like `.collect()` or `.show()`.**

```rust
// Each step builds a plan, doesn't execute
let step1 = df.filter(col("price").gt(lit(100)))?;      // Plan: Filter
let step2 = step1.select(vec![col("product")])?;        // Plan: Filter → Project
let step3 = step2.sort(vec![col("product").sort(true, true)])?;  // Plan: Filter → Project → Sort

// Inspect the plan without executing
println!("{}", step3.logical_plan().display_indent());

// Only NOW does execution happen
step3.show().await?;
```

**The Mindfight: Variables vs CTEs**

| Aspect               | DataFrame (Rust)                               | SQL (CTEs)              |
| -------------------- | ---------------------------------------------- | ----------------------- |
| Intermediate storage | Rust variables                                 | `WITH step1 AS (...)`   |
| Reuse across queries | Variable lives in scope                        | CTE is query-scoped     |
| Debugging            | `.schema()`, `.explain()` at any point         | Must execute to inspect |
| Branching            | `step1.filter(...)` and `step1.aggregate(...)` | Duplicate the CTE       |

> **Footgun:** DataFrame is _consumed_ by transformations. To reuse, call `.clone()`:
>
> ```rust
> let filtered = df.clone().filter(...)?;  // df still usable
> let aggregated = df.aggregate(...)?;     // df consumed here
> ```

### Dynamic Pipeline Construction

**Use Rust control flow (`if/else`, `match`, loops) to build query plans dynamically—something SQL strings make dangerous and error-prone.**

The "Dynamic SQL" anti-pattern (string concatenation) is prone to injection attacks and syntax errors. DataFrames eliminate both risks: values pass through `lit()`, and the plan is validated at build time.

**The Mindfight: Control Flow vs String Concatenation**

```rust
// ❌ SQL: String concatenation (injection risk, runtime errors)
let mut query = "SELECT * FROM users WHERE 1=1".to_string();
if let Some(dept) = filter_department {
    query.push_str(&format!(" AND department = '{}'", dept));  // 💀 Injection!
}

// ✅ DataFrame: Type-safe, validated at build time
let mut result = users_df;
if let Some(dept) = filter_department {
    result = result.filter(col("department").eq(lit(dept)))?;  // Safe
}
```

#### Runtime Schema Discovery

```rust
use datafusion::prelude::*;
use arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "price" => [1200, 25],
        "quantity" => [5, 50],
        "name" => ["Dell XPS", "Logitech"]
    )?;

    // Discover all numeric columns at runtime
    let schema = df.schema();
    let numeric_cols: Vec<_> = schema
        .fields()
        .iter()
        .filter(|f| matches!(
            f.data_type(),
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
        ))
        .map(|f| col(f.name()))
        .collect();

    println!("Found {} numeric columns", numeric_cols.len());

    // Select only numeric columns
    let numeric_df = df.select(numeric_cols)?;
    numeric_df.show().await?;

    Ok(())
}
```

#### Iterator Patterns Over Columns

```rust
// Apply the same transformation to all numeric columns
let schema = df.schema();
let transformed_cols: Vec<_> = schema
    .fields()
    .iter()
    .map(|f| {
        if f.data_type().is_numeric() {
            // Multiply numeric columns by 2
            (col(f.name()) * lit(2)).alias(format!("{}_doubled", f.name()))
        } else {
            // Keep non-numeric as-is
            col(f.name())
        }
    })
    .collect();

let result = df.select(transformed_cols)?;
```

### Encapsulation and Reusability

**Move complex transformation logic into reusable Rust functions—no UDF registration, no deployment headaches, full unit-testability.**

SQL UDFs require registration with the execution context and have limited composability. Rust functions are native citizens: they compose naturally, benefit from IDE tooling, and can be unit-tested in isolation.

**The Mindfight: Native Functions vs SQL UDFs**

| Aspect       | Rust Functions        | SQL UDFs                       |
| ------------ | --------------------- | ------------------------------ |
| Registration | None needed           | `ctx.register_udf(...)`        |
| Testing      | Standard `#[test]`    | Requires execution context     |
| IDE support  | Full autocomplete     | None                           |
| Composition  | Direct function calls | Limited nesting                |
| Distribution | Compiled into binary  | Must be registered per context |

#### Functions Returning `Expr` (Column-Level)

Create reusable column transformations:

```rust
use datafusion::prelude::*;

/// Clean and standardize a currency column
fn clean_currency(column: &str) -> Expr {
    // Remove $ prefix, trim whitespace, cast to float
    use datafusion::functions::string::expr_fn::*;
    cast(
        trim(vec![ltrim(col(column), lit("$"))]),
        arrow::datatypes::DataType::Float64
    )
}

// Usage: reusable across any DataFrame
let df = df.with_column("amount_clean", clean_currency("amount_raw"))?;
```

#### Functions Returning `DataFrame` (Table-Level)

Encapsulate multi-step transformations:

```rust
use datafusion::prelude::*;

// Reusable transformation function
fn normalize_text_columns(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    use datafusion::functions::string::expr_fn::*;
    use arrow::datatypes::DataType;

    let schema = df.schema();
    let mut result = df;

    // Find all string columns and normalize them
    for field in schema.fields() {
        if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            result = result.with_column(
                field.name(),
                trim(vec![lower(col(field.name()))])
            )?;
        }
    }

    Ok(result)
}

// Usage
let cleaned_df = normalize_text_columns(df)?;
```

#### Conditional Query Building

```rust
// Build different queries based on runtime parameters
fn build_query(
    df: DataFrame,
    filter_price: Option<i32>,
    filter_category: Option<String>,
    sort_desc: bool,
) -> datafusion::error::Result<DataFrame> {
    let mut result = df;

    // Apply filters conditionally
    if let Some(price) = filter_price {
        result = result.filter(col("price").gt(lit(price)))?;
    }

    if let Some(category) = filter_category {
        result = result.filter(col("category").eq(lit(category)))?;
    }

    // Apply conditional sorting
    if sort_desc {
        result = result.sort(vec![col("price").sort(false, true)])?;
    } else {
        result = result.sort(vec![col("price").sort(true, true)])?;
    }

    Ok(result)
}
```

_Quick Debug_: When building queries dynamically, verify the generated column list or expressions with `println!("{:?}", cols)` before passing to DataFrame methods.

> **See also:**
>
> - [Apache Spark DataFrames] for similar dynamic construction patterns
> - [`expr_api`] for advanced expression building examples
> - [`schema`](schema-management.md) API documentation for schema introspection

### Memory Management & Streaming

> **Note:** This section covers execution patterns. For production tuning, see [Best Practices](best-practices.md).

Handle large datasets efficiently by understanding when to collect vs stream, and how to process data incrementally.

#### Understanding collect() vs show() vs execute_stream()

```rust
use datafusion::prelude::*;
use futures::StreamExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3, 4, 5],
        "value" => [100, 200, 300, 400, 500]
    )?;

    // show() - For humans: limits to 20 rows, formatted output
    // Safe for any size dataset
    df.clone().show().await?;

    // collect() - For programs: loads ALL data into memory
    // Only use for small datasets or after limit()
    let batches = df.clone().limit(0, Some(100))?.collect().await?;
    println!("Collected {} batches", batches.len());

    // execute_stream() - For large data: processes incrementally
    // Memory-efficient, handles datasets larger than RAM
    let mut stream = df.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        println!("Processing batch with {} rows", batch.num_rows());
        // Process one batch at a time
    }

    Ok(())
}
```

#### Streaming Pattern for Large Datasets

```rust
use datafusion::prelude::*;
use futures::StreamExt;
use arrow::array::AsArray;

// Processes data using Arrow RecordBatch streaming
// See also: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
async fn process_large_dataset(df: DataFrame) -> datafusion::error::Result<()> {
    let mut stream = df.execute_stream().await?;
    let mut total_processed = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;

        // Process batch incrementally
        let id_array = batch.column(0).as_primitive::<arrow::datatypes::Int32Type>();

        for value in id_array.iter() {
            if let Some(val) = value {
                // Process each value
                total_processed += 1;
            }
        }

        // Optional: report progress
        if total_processed % 10000 == 0 {
            println!("Processed {} rows so far...", total_processed);
        }
    }

    println!("Total rows processed: {}", total_processed);
    Ok(())
}
```

#### Batch Processing Pattern

```rust
// Process data in manageable chunks
async fn process_in_batches(
    df: DataFrame,
    batch_size: usize,
) -> datafusion::error::Result<Vec<i64>> {
    let total_rows = df.clone().count().await?;
    let mut results = Vec::new();

    for offset in (0..total_rows).step_by(batch_size) {
        let batch_df = df.clone()
            .limit(offset, Some(batch_size))?;

        let batch_data = batch_df.collect().await?;

        // Process this batch
        for record_batch in batch_data {
            // Process record_batch
            results.push(record_batch.num_rows() as i64);
        }
    }

    Ok(results)
}
```

_Quick Debug_: If you're running out of memory, check if you're calling `collect()` on large datasets. Replace with `execute_stream()` or add `limit()` before collecting.

> **See also:**
>
> - [`execute_stream()`] API documentation
> - [`recordbatch`] - Arrow RecordBatch format for understanding batch processing
> - [Best Practices](best-practices.md) for memory configuration

### Error Handling and Observability

**DataFusion catches plan errors at build time—before any data moves. SQL catches everything at runtime.**

This separation is the DataFrame API's safety advantage: invalid column names, type mismatches, and schema errors surface immediately when you call `.filter()` or `.select()`, not when you finally execute.

**The Mindfight: Compile-Time vs Runtime Errors**

| Error Type         | DataFrame                   | SQL                           |
| ------------------ | --------------------------- | ----------------------------- |
| Unknown column     | `Err` at `.filter()` call   | Runtime parse/execution error |
| Type mismatch      | `Err` at `.with_column()`   | Runtime execution error       |
| File not found     | `Err` at `.collect().await` | Runtime (same)                |
| Invalid expression | `Err` at build time         | Runtime parse error           |

**The `Result<DataFrame>` Pattern:**

```rust
// Construction errors are synchronous (no await)
fn build_pipeline(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    df.filter(col("price").gt(lit(0)))?        // May fail: column not found
      .with_column("tax", col("price") * lit(0.1))?  // May fail: type mismatch
      .select(vec![col("product"), col("tax")])      // May fail: column not found
}

// Execution errors are asynchronous
async fn run_pipeline(df: DataFrame) -> datafusion::error::Result<()> {
    build_pipeline(df)?   // Construction errors caught here
        .show().await?;   // Execution errors caught here (IO, memory, etc.)
    Ok(())
}
```

#### The `explain()` Matrix: Debugging Plans

Use `df.explain(verbose, analyze)` to inspect what DataFusion will do:

| Call                    | Shows                                 | Use When                    |
| ----------------------- | ------------------------------------- | --------------------------- |
| `explain(false, false)` | Logical + Physical plan (basic)       | Quick sanity check          |
| `explain(true, false)`  | Verbose plan with details             | Understanding optimizations |
| `explain(false, true)`  | **EXPLAIN ANALYZE** (execution stats) | Performance debugging       |
| `explain(true, true)`   | Verbose + Analyzed                    | Deep investigation          |

```rust
// See what optimizations were applied
let plan = df.clone().explain(false, false)?;
plan.show().await?;

// See actual execution statistics
let analyzed = df.clone().explain(false, true)?;
analyzed.show().await?;
```

**What to look for in EXPLAIN output:**

- **Projection pushdown**: Are only needed columns being read?
- **Predicate pushdown**: Is the filter near the data source?
- **Filter merging**: Were multiple `.filter()` calls combined?
- **Partition pruning**: Were irrelevant partitions skipped?

#### Graceful Degradation

Build robust data pipelines with graceful error handling and recovery patterns.

#### Graceful Degradation

```rust
use datafusion::prelude::*;

async fn load_with_fallback() -> datafusion::error::Result<DataFrame> {
    // Try primary data source
    match SessionContext::new().read_csv("primary.csv", CsvReadOptions::default()).await {
        Ok(df) => {
            println!("Loaded primary source");
            Ok(df)
        }
        Err(_) => {
            println!("Primary source failed, using fallback");
            // Fallback to backup or synthetic data
            dataframe!(
                "id" => [1, 2, 3],
                "status" => ["fallback", "fallback", "fallback"]
            )
        }
    }
}
```

#### Soft Failures with try_cast

```rust
use datafusion::prelude::*;
use datafusion::functions::expr_fn::*;
use arrow::datatypes::DataType;

async fn safe_type_conversion(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    // Use try_cast instead of cast for soft failures
    // Invalid values become NULL instead of causing errors
    let result = df.with_column(
        "price_numeric",
        try_cast(col("price_string"), DataType::Float64)
    )?;

    // Then handle NULLs gracefully
    let cleaned = result.with_column(
        "price_final",
        coalesce(vec![col("price_numeric"), lit(0.0)])
    )?;

    Ok(cleaned)
}
```

#### Transaction-Like Patterns

```rust
use datafusion::prelude::*;

async fn atomic_transformation(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    // Build entire transformation pipeline
    let result = df
        .filter(col("amount").gt(lit(0)))?
        .with_column("processed", lit(true))?
        .aggregate(vec![col("category")], vec![sum(col("amount"))])?;

    // Validate before returning
    let count = result.clone().count().await?;

    if count == 0 {
        return Err(datafusion::error::DataFusionError::Execution(
            "Transformation resulted in empty dataset".to_string()
        ));
    }

    Ok(result)
}
```

#### Retry Pattern

```rust
use datafusion::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

async fn with_retry<F, Fut>(
    operation: F,
    max_retries: u32,
) -> datafusion::error::Result<DataFrame>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = datafusion::error::Result<DataFrame>>,
{
    let mut attempts = 0;

    loop {
        match operation().await {
            Ok(df) => return Ok(df),
            Err(e) if attempts < max_retries => {
                attempts += 1;
                println!("Attempt {} failed, retrying... Error: {}", attempts, e);
                sleep(Duration::from_secs(2_u64.pow(attempts))).await;
            }
            Err(e) => return Err(e),
        }
    }
}

// Usage
let df = with_retry(
    || async {
        SessionContext::new()
            .read_csv("unstable_source.csv", CsvReadOptions::default())
            .await
    },
    3
).await?;
```

_Quick Debug_: Wrap transformation steps in separate functions that return `Result<DataFrame>`. This makes it easier to add try-catch logic and provides better error messages with context.

### Data Quality & Bias Inspection

Inspired by research on [Blue Elephants Inspecting Pandas], you can track data distribution changes throughout your pipeline to detect technical biases or data quality issues.

#### Tracking Tuple Identity with row_number()

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

#### Computing Distribution Frequencies

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

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
        col("count").cast_to(
            &arrow::datatypes::DataType::Float64,
            &grouped.schema()
        )? / lit(total_count)
    )?;

    with_ratio.sort(vec![col("ratio").sort(false, true)])?.show().await?;

    Ok(with_ratio)
}
```

#### Detecting Bias: Before and After Comparison

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

#### Reusable Distribution Tracking Function

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

// Usage in a pipeline
async fn audited_pipeline() -> datafusion::error::Result<()> {
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

**Key Insights from the Paper:**

1. **SQL CTEs mirror DataFrame steps**: Each DataFrame transformation can be registered as a view/CTE for SQL-based inspection
2. **Tuple identifiers enable lineage**: Adding a `tid` column lets you track individual rows through transformations
3. **Distribution changes reveal bias**: Comparing group counts before/after operations detects disproportionate filtering
4. **Hybrid approach is powerful**: Use DataFrames for pipeline construction, SQL for auditing and inspection

> **See also:**
>
> - [Blue Elephants Inspecting Pandas] - Research on ML pipeline inspection in SQL
> - [mlinspect] - Python framework for ML pipeline inspection
> - [Mixing SQL and DataFrames](#mixing-sql-and-dataframes) section

## Mixing SQL and DataFrames

One of DataFusion's strengths is the ability to seamlessly mix SQL and DataFrame APIs. You can start with SQL and refine with DataFrames, or build DataFrames and query them with SQL.

### SQL to DataFrame

Execute SQL to get initial results, then use DataFrame methods for additional transformations:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a table
    ctx.sql("CREATE EXTERNAL TABLE users (
        id INT,
        name VARCHAR,
        age INT,
        city VARCHAR
    ) STORED AS CSV LOCATION 'users.csv'").await?.collect().await?;

    // Start with SQL, continue with DataFrame API
    let df = ctx.sql("SELECT * FROM users WHERE age > 21").await?
        .filter(col("city").eq(lit("NYC")))?
        .select(vec![col("name"), col("age")])?
        .sort(vec![col("age").sort(false, true)])?;

    df.show().await?;
    Ok(())
}
```

### DataFrame to SQL

Build a DataFrame, register it as a view, then query it with SQL:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create DataFrame programmatically
    let df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor"],
        "price" => [1200, 25, 75, 300],
        "quantity" => [5, 50, 30, 10]
    )?
    .filter(col("price").gt(lit(50)))?;

    // Register as a view for SQL access
    ctx.register_table("filtered_products", df.into_view())?;

    // Now query with SQL
    let result = ctx.sql("
        SELECT
            product,
            price * quantity as total_value
        FROM filtered_products
        ORDER BY total_value DESC
    ").await?;

    result.show().await?;
    Ok(())
}
```

### Combining Both Approaches

You can alternate between SQL and DataFrame operations as needed:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Step 1: SQL for initial complex join
    let df1 = ctx.sql("
        SELECT o.order_id, o.customer_id, p.product_name, o.quantity
        FROM orders o
        JOIN products p ON o.product_id = p.id
    ").await?;

    // Step 2: DataFrame for programmatic filtering
    let df2 = df1.filter(col("quantity").gt(lit(10)))?;

    // Step 3: Register and use SQL for aggregation
    ctx.register_table("large_orders", df2.into_view())?;

    let final_result = ctx.sql("
        SELECT customer_id, COUNT(*) as order_count
        FROM large_orders
        GROUP BY customer_id
    ").await?;

    final_result.show().await?;
    Ok(())
}
```

### When to Use DataFrames vs SQL

**Use DataFrames when you need:**

- Dynamic query construction based on runtime conditions
- Type-safe, compile-time validated code
- Reusable transformation functions
- Programmatic iteration over columns or tables
- Complex conditional logic in your pipeline

**Use SQL when:**

- You have a fixed, well-defined query
- Team members are more comfortable with SQL
- The query is simple and declarative
- You're prototyping or exploring data interactively

**Mix both when:**

- You want the best of both worlds: SQL for complex joins/aggregations, DataFrames for dynamic processing
- Different parts of your pipeline suit different approaches

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

---

> **Prerequisites**:
>
> - [Creating DataFrames](creating-dataframes.md) - Know how to create a DataFrame
>
> **Next Steps**:
>
> - [Writing DataFrames](writing-dataframes.md) - Save your transformed data
> - [Best Practices](best-practices.md) - Optimize your queries
> - [Index](index.md) - Return to overview

## Further Reading

### DataFusion Documentation

- [DataFrame API Overview](index.md)
- [Creating DataFrames](creating-dataframes.md)
- [DataFrame Concepts](concepts.md)
- [Writing DataFrames](writing-dataframes.md)
- [Best Practices](best-practices.md)
- [SQL User Guide](../../user-guide/sql/index.md)
- [Aggregate Functions](../../user-guide/sql/aggregate_functions.md)
- [Window Functions](../../user-guide/sql/window_functions.md)

### Examples

- [expr_api] - Complex expression patterns
- [dataframe.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/dataframe.rs) - Basic DataFrame operations
- [dataframe_transformations.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/dataframe_transformations.rs) - Examples from this guide
- [All Examples](https://github.com/apache/datafusion/tree/main/datafusion-examples/examples)

### External Resources

- [Apache Arrow] - Underlying columnar format
- [Apache Spark DataFrames] - Similar API and concepts
- [PySpark DataFrame API] - Python DataFrame inspiration
- [PostgreSQL DISTINCT ON] - DISTINCT ON semantics
- [Lazy Evaluation] - Concept explanation
- [Three-valued logic] - SQL null handling

### Research & Advanced Topics

- [Blue Elephants Inspecting Pandas] - ML pipeline inspection and bias detection in SQL
- [mlinspect] - Framework for inspecting ML pipelines

### Concepts & Standards

- [SQL-99 Standard] - SQL language specification
- Projection pushdown, predicate pushdown - Query optimization techniques
- Lazy evaluation vs eager evaluation - Performance implications

```{include} references.md

```
