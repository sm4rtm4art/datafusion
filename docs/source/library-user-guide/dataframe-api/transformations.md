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

Order #6 had a null `status` — now filled with "unknown". This pattern is essential for real-world pipelines where nulls appear in optional columns.

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

Now we see **meaningful aggregation**: Order #1 has 2 line items totaling $110 (100 + 10). The `order_id` primary key is preserved for joins — this table can link to products, payments, or shipping data.

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
|          | ... | null_order_id, outlier_amount |
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

Every operation in this data cleaning journey has a direct SQL equivalent — `.filter()` is `WHERE`, `.aggregate()` is `GROUP BY`, `.sort()` is `ORDER BY`. **The logic is identical; only the syntax differs.** But the DataFrame API adds something SQL strings cannot: **composable, fail-safe patterns**. The type conversion step showed how to gracefully handle parse failures instead of crashing — a pattern that's awkward to express in SQL but natural in DataFrames.

Choose based on your context: SQL for ad-hoc queries and complex window functions, DataFrames for type-safe pipelines and dynamic composition. The next section, [Deep Dive: Transformation Reference](#deep-dive-transformation-reference), provides detailed coverage of each operation with more examples and edge cases.

After that, [Advanced DataFrame Patterns](#advanced-dataframe-patterns) explores methods that have **no SQL equivalent** — like [`.with_column()`] for adding columns without re-selecting everything, [`.union_by_name()`] for schema-flexible unions, and [`.describe()`] for instant summary statistics.

---

<!--SQL Statements-->

[`SELECT`]: ../../user-guide/sql/select.md
[`WHERE`]: ../../user-guide/sql/select.md#where
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
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
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

<!--Datafusion Core types -->

[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`prelude`]: https://docs.rs/datafusion/latest/datafusion/prelude/index.html

<!--  standalone functions-->

[`lit()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.lit.html
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`lower()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.lower.html
[`trim()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.trim.html
[`to_date()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.to_date.html
[`TO_DATE`]: ../../user-guide/sql/scalar_functions.md#to_date
[`coalesce()`]: https://docs.rs/datafusion/latest/datafusion/functions/core/expr_fn/fn.coalesce.html
[`COALESCE`]: ../../user-guide/sql/scalar_functions.md#coalesce
[`nvl()`]: https://docs.rs/datafusion/latest/datafusion/functions/core/expr_fn/fn.nvl.html
[`date_part()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.date_part.html
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

## Deep Dive: Transformation Reference

This section provides comprehensive coverage of each DataFrame operation. Use it as a reference when you need detailed information about specific transformations.

### Selection and Projection Mastery

Select specific columns, add computed columns, or rename columns using [`select()`], [`select_columns()`], [`with_column()`], [`with_column_renamed()`], and [`drop_columns()`].

**SQL equivalent:** `SELECT a, b AS b_renamed, a + b AS sum FROM table`

#### Basic Selection

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // Select specific columns by name
    let df = df.select_columns(&["product", "price"])?;
    df.show().await?;

    Ok(())
}
```

#### Intermediate: Expressions and Computed Columns

```rust
// Select with expressions (computed columns)
let df = df.select(vec![
    col("product"),
    col("price"),
    (col("price") * col("quantity")).alias("total_value"),
    when(col("price").gt(lit(100)), lit("Premium"))
        .otherwise(lit("Standard"))?
        .alias("tier")
])?;

// Add a new column (or replace existing)
let df = df.with_column("discounted_price", col("price") * lit(0.9))?;

// Rename a column
let df = df.with_column_renamed("total_value", "revenue")?;

// Drop specific columns
let df = df.drop_columns(&["category"])?;
```

#### Advanced: Dynamic Column Selection

```rust
// Programmatically select columns based on conditions
let schema = df.schema();
let numeric_cols: Vec<_> = schema
    .fields()
    .iter()
    .filter(|f| matches!(f.data_type(), DataType::Int32 | DataType::Float64))
    .map(|f| col(f.name()))
    .collect();

let numeric_df = df.select(numeric_cols)?;
```

#### Anti-Pattern: Over-Selection

```rust
// DON'T: Select all columns when you only need a few
let bad = df.select(vec![col("*")])?  // Reads everything
    .filter(col("price").gt(lit(100)))?;

// DO: Select only what you need (projection pushdown optimization)
let good = df
    .select(vec![col("product"), col("price")])?
    .filter(col("price").gt(lit(100)))?;
```

_Quick Debug_: **Column not found error?** Check column names with `df.schema()`. DataFusion is case-sensitive. Use `df.schema().field_names()` to list all available columns.

> **Going deeper:**
>
> - **Advanced:** [`select_exprs()`] for SQL-like string expressions
> - **Examples:** See `datafusion-examples/examples/expr_api.rs` for complex expression patterns
> - **Performance:** Projection pushdown automatically optimizes column reads

### Filtering Excellence

Filter rows based on conditions using [`filter()`]. Predicates can be simple or arbitrarily complex.

**SQL equivalent:** `WHERE condition`

#### Basic Filtering

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor"],
        "price" => [1200, 25, 75, 300],
        "quantity" => [5, 50, 30, 10]
    )?;

    // Simple filter
    let df = df.filter(col("price").gt(lit(100)))?;
    df.show().await?;

    Ok(())
}
```

#### Intermediate: Complex Predicates

```rust
// Multiple conditions with AND/OR
let df = df.filter(
    col("price").gt(lit(50))
        .and(col("quantity").lt(lit(40)))
        .or(col("product").eq(lit("Laptop")))
)?;

// Null handling
let df = df.filter(col("price").is_not_null())?;

// IN list
let df = df.filter(in_list(
    col("product"),
    vec![lit("Laptop"), lit("Monitor")],
    false  // negated
)?)?;

// String matching
let df = df.filter(col("product").like(lit("%top%")))?;  // Contains "top"

// Range checks
let df = df.filter(col("price").between(lit(50), lit(500)))?;
```

#### Advanced: Dynamic Filter Building

```rust
// Build filters programmatically based on runtime conditions
fn build_filter(min_price: Option<i32>, max_quantity: Option<i32>) -> Expr {
    let mut conditions = vec![lit(true)];  // Start with always-true

    if let Some(price) = min_price {
        conditions.push(col("price").gt_eq(lit(price)));
    }

    if let Some(qty) = max_quantity {
        conditions.push(col("quantity").lt_eq(lit(qty)));
    }

    // Combine all conditions with AND
    conditions.into_iter().reduce(|acc, cond| acc.and(cond)).unwrap()
}

let filter = build_filter(Some(100), None);
let df = df.filter(filter)?;
```

#### Anti-Pattern: Multiple Sequential Filters

```rust
// DON'T: Chain multiple filter calls (creates separate plan nodes)
let bad = df
    .filter(col("price").gt(lit(50)))?
    .filter(col("quantity").lt(lit(100)))?
    .filter(col("product").is_not_null())?;

// DO: Combine into single filter (more efficient)
let good = df.filter(
    col("price").gt(lit(50))
        .and(col("quantity").lt(lit(100)))
        .and(col("product").is_not_null())
)?;
```

_Quick Debug_: **Filter returning no rows?** Check for nulls—[Three-valued logic] means `NULL > 5` is `NULL` (not true or false). Always handle nulls explicitly with `.is_not_null()` when needed.

_Quick Debug_: **Filter too slow?** Ensure filter columns are in your data source's partition scheme for predicate pushdown. Check the optimized plan with `df.explain(false, false)?` to verify pushdown happened.

### Aggregation Patterns

Use [`aggregate()`] for GROUP BY operations with aggregate functions to summarize data.

**SQL equivalent:** `SELECT dept, SUM(salary) FROM table GROUP BY dept`

#### Basic Aggregation

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Group by with multiple aggregations
    let result = df.aggregate(
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
    // | Engineering | 165000       | 82500      | 2              |
    // | Sales       | 105000       | 52500      | 2              |
    // +-------------+--------------+------------+----------------+

    Ok(())
}
```

#### Intermediate: Multi-Level Grouping and HAVING-Style Filtering

```rust
// Multi-column grouping
let df = df.aggregate(
    vec![col("department"), col("location")],  // GROUP BY dept, location
    vec![sum(col("salary")).alias("total")]
)?;

// HAVING clause equivalent: filter after aggregation
let result = df
    .aggregate(vec![col("department")], vec![sum(col("salary")).alias("total")])?
    .filter(col("total").gt(lit(100000)))?;  // HAVING total > 100000
```

#### Advanced: All Aggregate Functions

```rust
use datafusion::functions_aggregate::expr_fn::*;

let stats = df.aggregate(
    vec![col("department")],
    vec![
        count(col("employee")).alias("count"),
        sum(col("salary")).alias("sum"),
        avg(col("salary")).alias("avg"),
        min(col("salary")).alias("min"),
        max(col("salary")).alias("max"),
        stddev(col("salary")).alias("stddev"),
        variance(col("salary")).alias("variance"),
        // Count distinct
        count_distinct(col("employee")).alias("unique_employees"),
        // Conditional aggregation
        sum(when(col("salary").gt(lit(70000)), lit(1))
            .otherwise(lit(0))?).alias("high_earners")
    ]
)?;
```

#### Advanced: Aggregation Without Grouping

```rust
// Aggregate entire DataFrame (no GROUP BY)
let total = df.aggregate(
    vec![],  // Empty group by
    vec![
        sum(col("salary")).alias("company_total"),
        avg(col("salary")).alias("company_avg")
    ]
)?;
```

_Quick Debug_: **Wrong aggregation results?** Verify grouping keys before aggregating: `df.select(vec![col("dept")]).distinct()?.show().await?` to see unique values.

_Quick Debug_: **Column not found in aggregation?** After `aggregate()`, only the grouping columns and aggregated expressions are available. If you need other columns, include them in the group by or aggregate them.

### When DataFrames Collide: Join Patterns

DataFusion supports all standard [SQL-99 Standard] join types plus powerful Semi and Anti joins.

**SQL equivalent:** `SELECT * FROM left JOIN right ON left.id = right.user_id`

#### Understanding Join Types: Visual Guide

Given two tables:

**customers** (left):

```
+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
| 3  | Carol |
+----+-------+
```

**orders** (right):

```
+----------+-------------+--------+
| order_id | customer_id | amount |
+----------+-------------+--------+
| 101      | 1           | 100    |
| 102      | 1           | 200    |
| 103      | 2           | 150    |
| 104      | 99          | 300    |
+----------+-------------+--------+
```

**Join Results:**

| Join Type    | Returns                       | Use Case                                       | Result Rows                        |
| ------------ | ----------------------------- | ---------------------------------------------- | ---------------------------------- |
| **Inner**    | Matches from both sides       | Standard join, only matching records           | 3 (Alice×2, Bob×1)                 |
| **Left**     | All left + matches from right | Keep all customers, show their orders (if any) | 4 (Alice×2, Bob×1, Carol×0)        |
| **Right**    | All right + matches from left | Keep all orders, show customer (if exists)     | 4 (orders 101,102,103,104)         |
| **Full**     | Everything from both sides    | Union of Left and Right                        | 5 (all customers + orphaned order) |
| **LeftSemi** | Left rows that have matches   | "Which customers have orders?"                 | 2 (Alice, Bob)                     |
| **LeftAnti** | Left rows with no matches     | "Which customers have NO orders?"              | 1 (Carol)                          |
| **Cross**    | Cartesian product             | All combinations (use carefully!)              | 12 (3×4)                           |

#### Basic: Inner Join

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders = dataframe!(
        "order_id" => [101, 102, 103],
        "customer_id" => [1, 1, 2],
        "amount" => [100, 200, 150]
    )?;

    // Inner join: only customers with orders
    let df = customers.join(
        orders,
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;

    df.show().await?;
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

#### Intermediate: Left/Right/Full Joins

```rust
// Left join: all customers, even without orders
let left_df = customers.join(
    orders.clone(),
    JoinType::Left,
    &["id"],
    &["customer_id"],
    None
)?;
// Result includes Carol with NULL order values

// Right join: all orders, even orphaned ones
let right_df = customers.join(
    orders.clone(),
    JoinType::Right,
    &["id"],
    &["customer_id"],
    None
)?;
// Result includes order 104 with NULL customer

// Full outer join: everything
let full_df = customers.join(
    orders,
    JoinType::Full,
    &["id"],
    &["customer_id"],
    None
)?;
```

#### Intermediate: Semi and Anti Joins

```rust
// LeftSemi: customers who HAVE orders (like EXISTS in SQL)
let with_orders = customers.clone().join(
    orders.clone(),
    JoinType::LeftSemi,
    &["id"],
    &["customer_id"],
    None
)?;
// Returns: Alice, Bob (only customer columns, no order data)

// LeftAnti: customers with NO orders (like NOT EXISTS)
let without_orders = customers.join(
    orders.clone(),
    JoinType::LeftAnti,
    &["id"],
    &["customer_id"],
    None
)?;
// Returns: Carol
```

#### Advanced: Multi-Way Joins

```rust
let products = dataframe!(
    "product_id" => [1, 2],
    "product_name" => ["Widget", "Gadget"]
)?;

let order_items = dataframe!(
    "order_id" => [101, 102],
    "product_id" => [1, 2]
)?;

// Join three tables
let result = orders
    .join(customers, JoinType::Inner, &["customer_id"], &["id"], None)?
    .join(order_items, JoinType::Inner, &["order_id"], &["order_id"], None)?
    .join(products, JoinType::Inner, &["product_id"], &["product_id"], None)?;
```

#### Advanced: Join with Complex Conditions

```rust
// Join with additional filter (not just equality)
let df = customers.join_on(
    orders,
    JoinType::Inner,
    col("customers.id").eq(col("orders.customer_id"))
        .and(col("orders.amount").gt(lit(100)))
)?;
```

#### Anti-Pattern: Accidental Cartesian Product

```rust
// DANGEROUS: Cross join without filter (3 × 4 = 12 rows)
let bad = customers.join(
    orders,
    JoinType::Inner,
    &[],  // Empty keys = Cartesian product!
    &[],
    None
)?;

// DO: Always specify join keys
let good = customers.join(
    orders,
    JoinType::Inner,
    &["id"],
    &["customer_id"],
    None
)?;
```

_Quick Debug_: **Join returns empty?** Check for: (1) key column name mismatches, (2) trailing spaces in data (`trim()` both sides), (3) case sensitivity, (4) null values in keys. Use `show()` on both DataFrames before joining to inspect keys.

_Quick Debug_: **Too many rows after join?** You might have duplicates in the join keys. Check with `df.select(vec![col("key")]).distinct().count()` on both sides. Consider using `distinct_on()` before joining if needed.

_Quick Debug_: **Schema confusion?** After a join, both tables' columns are available, but if they have the same name, you need to qualify them: `col("customers.id")` vs `col("orders.id")`.

### Sorting and Limiting

Order results and implement pagination with [`sort()`] and [`limit()`].

**SQL equivalent:** `ORDER BY score DESC LIMIT 10 OFFSET 5`

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
let combined = sales_q1.union_by_name(sales_q2)?;

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

### Window Functions

Apply calculations across rows related to the current row, like running totals, rankings, and moving averages.

**SQL equivalent:** `SELECT name, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees`

> **Note:** Window functions follow the [SQL-99 Standard] specification. See also: [Window Functions](../user-guide/sql/window_functions.md) for all available window functions.

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

// Moving average (last 3 rows)
let df = df.window(vec![
    avg(col("amount"))
        .order_by(vec![col("date").sort(true, true)])
        .window_frame(WindowFrame::new(Some(false)))  // Customize frame
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

#### Unnesting / Exploding Arrays

```rust
// Expand array column into multiple rows (like SQL UNNEST or Spark explode)
// See also: PySpark explode - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html
let df = dataframe!(
    "customer" => ["Alice", "Bob"],
    "orders" => [vec![1, 2, 3], vec![4, 5]]
)?;

// Unnest the orders array
let expanded = df.unnest_columns(&["orders"])?;
// Result:
// customer | orders
// Alice    | 1
// Alice    | 2
// Alice    | 3
// Bob      | 4
// Bob      | 5
```

## Advanced DataFrame Patterns

### Dynamic DataFrame Construction

Build DataFrames and queries programmatically at runtime based on schema discovery and conditional logic.

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

#### Metaprogramming: Reusable Transformation Functions

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
> - [`expr_api.rs`] for advanced expression building examples
> - [`schema`] API documentation for schema introspection

### Memory Management & Streaming

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

### Error Handling & Recovery

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

### Subqueries

Subqueries allow you to use the result of one query within another query. DataFusion supports scalar subqueries (returning a single value), IN subqueries (checking membership), and EXISTS subqueries (checking existence).

#### Scalar Subqueries

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::expr::ScalarSubquery;
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

### IN Subqueries

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

Faster approach using `.show()` at each step:

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
- [SQL User Guide](../user-guide/sql/index.md)
- [Aggregate Functions](../user-guide/sql/aggregate_functions.md)
- [Window Functions](../user-guide/sql/window_functions.md)

### Examples

- [expr_api.rs] - Complex expression patterns
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
