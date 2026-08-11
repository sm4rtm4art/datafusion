<!--
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

# Null Value Handling

**Null value handling, essential for reliable data processing — neglecting Null values leads to disappearing rows, join match fails, and results mislead silently.**

Every query engine must resolve a fundamental tension: real-world data contains gaps, yet computation demands concrete values. DataFusion's DataFrame API inherits SQL-standard three-valued logic — where NULL represents an unknown rather than a value — and layers it on top of Apache Arrow's columnar validity bitmaps for high-performance null-aware execution. This page covers how three-valued logic silently shapes every transformation, from filters that discard rows to joins that refuse to match, and how DataFusion's optimizer exploits nullability metadata to eliminate redundant checks and prune entire file segments. Armed with these propagation rules and the null-handling toolkit — `is_null()`, `coalesce()`, `.fill_null()` — the most common source of DataFrame bugs becomes predictable, controllable behavior.

**Key functions and methods:**

| Function / Method                 | Purpose                            |
| --------------------------------- | ---------------------------------- |
| [`is_null()`] / [`is_not_null()`] | Test for null in predicates        |
| [`coalesce()`]                    | First non-null from a list         |
| [`.fill_null()`]                  | Replace nulls across columns       |
| [`nullif()`]                      | Convert a value to null            |
| [`nvl()`]                         | Two-argument null fallback         |
| `when().otherwise()`              | Conditional null logic (CASE WHEN) |

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::

```{contents} Table of Contents for Handling Null Values
:local:
:depth: 2
```

## The Null Contract — Three-Valued Logic

**NULL represents _unknown_, not zero or empty — and "unknown" combined with anything remains "unknown," silently discarding rows through three-valued logic.**

Every data pipeline encounters missing values: sensors fail, users skip form fields, outer joins introduce unmatched rows. Traditional programming languages represent absence as `null`, `None`, or `nil` — a simple marker. DataFusion and other SQL-based query engines go further: NULL follows [**three-valued logic**][three-valued logic] (3VL), where every boolean expression evaluates to `TRUE`, `FALSE`, or `NULL`. This distinction is the single most common source of unexpected query results.

DataFusion adheres to SQL-standard null semantics. Any arithmetic, comparison, or logical operation involving NULL propagates the unknown: `5 + NULL = NULL`, `NULL + NULL = NULL`, `NULL > 0 = NULL`. The consequence is that predicates in `.filter()`, join conditions, and `CASE WHEN` branches treat NULL as neither true nor false — they simply skip it. While these semantics originate in SQL, the DataFrame API exposes them through typed `Expr` methods — `.is_null()`, `.gt()`, `.or()` — providing compile-time safety that SQL strings lack.

At the physical level, Apache Arrow represents nulls through a [validity bitmap] — one bit per array slot, separate from the data buffer. This design avoids sentinel values (no special `-1` or `NaN` conventions), enables SIMD-accelerated null checks, and keeps memory overhead to 1 bit per row regardless of the data type. Nullable Arrow arrays carry this bitmap, making null-awareness a first-class property of the entire execution pipeline.

| Expression      | Result | Why                                     |
| --------------- | ------ | --------------------------------------- |
| `5 > NULL`      | `NULL` | Cannot compare with an unknown value    |
| `NULL = NULL`   | `NULL` | Two unknowns are not necessarily equal  |
| `NULL AND TRUE` | `NULL` | Unknown AND anything = unknown          |
| `NULL OR TRUE`  | `TRUE` | TRUE OR anything = TRUE (short-circuit) |
| `NULL OR FALSE` | `NULL` | Unknown OR FALSE = unknown              |

:::{admonition} SQL engineers: familiar territory, different storage
:class: tip
If you know PostgreSQL null semantics, DataFusion behaves identically at the logical level. The key difference is the physical representation: instead of per-row null flags in a heap tuple, Arrow stores a columnar validity bitmap that enables batch-level bitwise operations.
:::

---

## Nullability in the Schema

**Column nullability declaration in schema management acts as a first quality gate — enabling fail-fast validation at plan time and unlocking optimizer shortcuts that skip unnecessary null checks.**

Nullability is one of the four properties of an Arrow `Field` (alongside name, data type, and metadata). Declaring a column as non-nullable enables DataFusion to fail-fast during plan construction and empowers the optimizer to eliminate redundant null checks downstream. File formats like Parquet carry nullability in their schema; formats without a declared schema (CSV, JSON) require inference, which defaults all columns to nullable.

:::{admonition} Schema Inference
:class: caution
Inference by sampling rows cannot guarantee nullability — all inferred columns default to `nullable = true`. Explicit declarations via `SchemaBuilder` or `CsvReadOptions::new().schema(&schema)` override inference and unlock optimizer shortcuts (see [Optimizer Null-Awareness](#optimizer-null-awareness)).
:::

:::{admonition} Deep dive
:class: seealso
For the full structure of Arrow fields — name, data type, nullability, and metadata — see [Anatomy of a Schema — Arrow Field Properties][schema-anatomy].
:::

---

## Null Propagation in Transformations

**Null handling during data transformations is determined individually by each transformation method — respecting these differences prevents silent row drops downstream.**

Solid null propagation throughout a data pipeline depends on the transformation methods used. Filters drop null-predicate rows, aggregates skip nulls in computation, and joins refuse to match null keys. The combination of these rules across a pipeline determines which rows survive and which values change. The following table summarizes null behavior by transformation.

| Transformation                                | Null Behavior                | Surprise Risk |
| --------------------------------------------- | ---------------------------- | ------------- |
| [`.filter()`](#null-handling-in-filter)       | Discards NULL predicate rows | High          |
| [`.aggregate()`](#null-handling-in-aggregate) | Skips NULLs in computation   | Medium        |
| [`.join()`](#null-handling-in-join)           | NULL ≠ NULL, no match        | High          |
| [`.sort()`](#null-handling-in-sort)           | Placement convention         | Low           |
| [`.union()`](#null-handling-in-union)         | Widens nullability in schema | Low           |

The subsections below follow the typical transformation order in a data pipeline.

### Null Handling in `.filter()`

**Silently dropping rows — the worst case in filtering — originates from boolean matching with no awareness of NULL: null predicates are neither true nor false, so the row disappears.**

Rows with NULL in filtered columns disappear without warning — the most common source of "where did my data go?" bugs. At its core, filtering compares boolean values: the `.filter(predicate)` method evaluates each row's predicate expression to `TRUE`, `FALSE`, or `NULL` via three-valued logic, and only `TRUE` rows survive. A predicate is any boolean `Expr`, such as `col("age").gt(lit(18))`, that DataFusion evaluates against each batch.

The following illustration traces a filter on `age > 18` through all three outcomes:

```text
dataframe.filter(col("age").gt(lit(18)))
  age = 25   → TRUE   → kept  ✅
  age = 17   → FALSE  → dropped ❎
  age = NULL → NULL   → dropped (!)  ← silent data loss ❌
```

#### `is_null()` and `is_not_null()`

The `is_null()` method wraps any expression in `Expr::IsNull`, which evaluates to `TRUE` if the value is null and `FALSE` otherwise — crucially, the result itself is _never_ NULL. This converts a nullable expression into a non-nullable boolean, making it safe to combine with `.or()` or `.and()` in filter predicates. The counterpart `is_not_null()` works identically but inverted.

To retain rows with unknown values, explicitly include them using `is_null()`:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol"],
        "age" => [Some(25i32), Some(17i32), None]
    )?;

    // Standard filter: NULLs silently excluded (Carol disappears)
    let adults = df.clone().filter(col("age").gt(lit(18)))?;
    let result = adults.collect().await?;
    assert_batches_eq!(
        &[
            "+-------+-----+",
            "| name  | age |",
            "+-------+-----+",
            "| Alice | 25  |",
            "+-------+-----+",
        ],
        &result
    );

    // Include unknown ages explicitly
    let adults_or_unknown = df.filter(
        col("age").gt(lit(18)).or(col("age").is_null())
    )?;
    let result = adults_or_unknown.collect().await?;
    assert_batches_eq!(
        &[
            "+-------+-----+",
            "| name  | age |",
            "+-------+-----+",
            "| Alice | 25  |",
            "| Carol |     |",
            "+-------+-----+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} SQL equivalent
:class: tip
`WHERE age > 18 OR age IS NULL`
:::

### Null Handling in `.aggregate()`

**Aggregation skips null values — preventing undefined arithmetic but changing results in ways that catch most developers off guard.**

Aggregation functions must decide what to do with missing values — include them (and risk undefined arithmetic), or ignore them (and silently change the denominator). DataFusion ignores NULLs entirely, following the SQL standard. Aggregates over `[10, NULL, 20]` return `sum = 30` and `avg = 15.0` (dividing by 2 non-null values, not 3 rows). When all values are NULL, most aggregates return NULL, not zero.

| Expression   | With data `[10, NULL, 20]` | Notes                                  |
| ------------ | :------------------------: | -------------------------------------- |
| `sum(col)`   |            `30`            | Nulls skipped                          |
| `avg(col)`   |           `15.0`           | Divides by 2, not 3; returns `Float64` |
| `count(*)`   |            `3`             | Counts all rows                        |
| `count(col)` |            `2`             | Counts only non-null values            |
| `min(col)`   |            `10`            | Nulls ignored                          |

:::{admonition} Note on `avg()` dtype change
:class: caution
`avg()` returns `Float64` even when the input is `Int64` — the average of integers is not necessarily an integer. This schema change can surprise downstream consumers expecting the original type.
:::

#### `count(*)` vs. `count(col)`

The `count(*)` vs. `count(col)` distinction is the most common pitfall. `count(*)` is the idiomatic way to count all rows regardless of null values; `count(col)` counts only rows where that specific column is non-null.

:::{admonition} Defensive aggregation
:class: tip
Wrap aggregates with `coalesce()` when downstream code cannot handle NULL results: `coalesce(vec![sum(col("revenue")), lit(0)])` ensures a zero instead of NULL when all inputs are null.
:::

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use datafusion::functions_aggregate::expr_fn::{sum, avg, count, min};

#[tokio::main]
async fn main() -> Result<()> {
    let sales_df = dataframe!(
        "product" => ["Widget", "Widget", "Widget"],
        "revenue" => [Some(10i64), None, Some(20i64)]
    )?;

    // Aggregation skips NULLs: sum=30, avg=15, count(*)=3, count(col)=2
    let stats = sales_df.aggregate(
        vec![col("product")],
        vec![
            sum(col("revenue")).alias("total"),
            avg(col("revenue")).alias("average"),
            count(col("revenue")).alias("non_null_count"),
            min(col("revenue")).alias("minimum"),
        ],
    )?;

    let result = stats.collect().await?;
    assert_batches_eq!(
        &[
            "+---------+-------+---------+----------------+---------+",
            "| product | total | average | non_null_count | minimum |",
            "+---------+-------+---------+----------------+---------+",
            "| Widget  | 30    | 15.0    | 2              | 10      |",
            "+---------+-------+---------+----------------+---------+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} SQL equivalent
:class: tip
`SELECT product, SUM(revenue), AVG(revenue), COUNT(revenue), MIN(revenue) FROM sales GROUP BY product`
:::

### Null Handling in `.join()`

**`NULL ≠ NULL` in join conditions — two unknown values are not considered equal, causing silently missed matches in every join type.**

Join conditions evaluate equality the same way as any other expression: `NULL = NULL` produces `NULL`, which the join interprets as "no match." Rows with NULL keys on either side are excluded from the result in inner joins and appear as unmatched in outer joins.

```text
INNER JOIN ON left.id = right.id

LEFT TABLE               RIGHT TABLE
┌──────┬───────┐         ┌──────┬──────┐
│ id   │ value │         │ id   │ data │
├──────┼───────┤         ├──────┼──────┤
│ 1    │ 'a'   │         │ 1    │ 'x'  │
│ 2    │ 'c'   │         │ NULL │ 'y'  │
│ NULL │ 'b'   │         │ 3    │ 'z'  │
└──────┴───────┘         └──────┴──────┘

Evaluation:
  1    = 1    → TRUE  → match  ✅
  2    = NULL → NULL  → no match  ❌
  NULL = 3    → NULL  → no match  ❌
  NULL = NULL → NULL  → no match (!)  ← silent data loss  ❌

Result: only 1 of 3 possible matches survives
```

#### `.join_on()` with `Operator::IsNotDistinctFrom`

The string-based `.join()` method hardcodes standard equality (`NullEquality::NullEqualsNothing`) and does not expose a null-safe option. For **null-safe joins** where `NULL = NULL` should evaluate to `TRUE`, use `.join_on()` with an explicit `Operator::IsNotDistinctFrom` expression:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let left_table = dataframe!(
        "left_id" => [Some(1i32), Some(2i32), None],
        "value" => ["a", "b", "c"]
    )?;
    let right_table = dataframe!(
        "right_id" => [Some(1i32), None, Some(3i32)],
        "data" => ["x", "y", "z"]
    )?;

    // Null-safe join: NULL = NULL matches via IS NOT DISTINCT FROM
    let null_safe = left_table.join_on(
        right_table,
        JoinType::Inner,
        [binary_expr(
            col("left_id"),
            Operator::IsNotDistinctFrom,
            col("right_id"),
        )],
    )?;

    let result = null_safe.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+---------+-------+----------+------+",
            "| left_id | value | right_id | data |",
            "+---------+-------+----------+------+",
            "| 1       | a     | 1        | x    |",
            "|         | c     |          | y    |",
            "+---------+-------+----------+------+",
        ],
        &result
    );

    Ok(())
}
```

The `ExtractEquijoinPredicate` optimizer rule detects `IsNotDistinctFrom` predicates in the join filter and automatically promotes them to equijoin keys with `NullEquality::NullEqualsNull`. This enables the high-performance Hash Join algorithm instead of falling back to a slower Nested Loop Join.

:::{admonition} API gap: no convenience method
:class: caution
DataFusion does not yet provide a convenience method like `col("a").is_not_distinct_from(col("b"))`. The `binary_expr()` + `Operator::IsNotDistinctFrom` pattern shown above is the current DataFrame API approach. For advanced use cases, [`LogicalPlanBuilder::join_detailed()`] accepts a [`NullEquality`] parameter directly.
:::

:::{admonition} SQL equivalent
:class: tip
DataFusion supports two syntaxes for null-safe equality: `IS NOT DISTINCT FROM` (SQL standard) and `<=>` (MySQL/Spark-style spaceship operator). Both compile to the same `Operator::IsNotDistinctFrom`.

`SELECT * FROM left AS l JOIN right AS r ON l.left_id IS NOT DISTINCT FROM r.right_id`
:::

### Null Handling in `.sort()`

**NULL values have no inherent order — DataFusion places NULLs last in ascending sorts and first in descending sorts, following PostgreSQL conventions.**

Every sort algorithm must decide where to place incomparable values. Since `NULL < 5` evaluates to `NULL` — not `TRUE` or `FALSE` — the sort cannot determine rank by comparison alone. DataFusion resolves this with a placement convention controlled by two booleans in `SortExpr`: `asc` (sort direction) and `nulls_first` (null placement).

#### Sort Order and Null Position

| Sort Order          | Default Null Position | Override                             |
| ------------------- | --------------------- | ------------------------------------ |
| `ASC` (ascending)   | Nulls **last**        | `.sort(true, true)` for nulls first  |
| `DESC` (descending) | Nulls **first**       | `.sort(false, false)` for nulls last |

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol"],
        "score" => [Some(85i32), None, Some(92i32)]
    )?;

    // ASC, nulls last (default): 85, 92, NULL
    let result = df.clone()
        .sort(vec![col("score").sort(true, false)])?
        .collect().await?;
    assert_batches_eq!(
        &[
            "+-------+-------+",
            "| name  | score |",
            "+-------+-------+",
            "| Alice | 85    |",
            "| Carol | 92    |",
            "| Bob   |       |",
            "+-------+-------+",
        ],
        &result
    );

    // ASC, nulls first: NULL, 85, 92
    let result = df.clone()
        .sort(vec![col("score").sort(true, true)])?
        .collect().await?;
    assert_batches_eq!(
        &[
            "+-------+-------+",
            "| name  | score |",
            "+-------+-------+",
            "| Bob   |       |",
            "| Alice | 85    |",
            "| Carol | 92    |",
            "+-------+-------+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} SQL equivalent
:class: tip
`ORDER BY score ASC NULLS FIRST`
:::

### Null Handling in `.union()`

**Union operations widen nullability — if a column is nullable in _any_ input, the output schema marks it nullable, even if other inputs declare it non-nullable.**

When combining DataFrames via `.union()` or `.union_by_name()`, DataFusion derives the output schema by merging fields across all inputs. The nullability rule is conservative: `nullable = fields.iter().any(|f| f.is_nullable())`. A non-nullable `price` column in one DataFrame becomes nullable in the union output if the other DataFrame's `price` column is nullable.

For `.union_by_name()`, columns that exist in one input but not another are filled with `NULL` literals and forced nullable in the output schema — the column cannot be non-nullable if entire inputs lack it.

---

## The Null-Handling Toolkit

**DataFusion provides dedicated functions and methods for testing, replacing, and converting nulls — choosing the right tool depends on whether you need a predicate, a fallback value, or conditional logic.**

Null propagation during transformations determines _where_ nulls appear — the toolkit determines _what to do about them_. DataFusion offers a layered set of null-handling functions: predicate functions like `is_null()` for filtering decisions, replacement functions like `coalesce()` and `.fill_null()` for substituting fallback values, and conditional expressions via `CASE WHEN` for branching logic that depends on multiple columns or conditions. The earlier [defensive aggregation](#null-handling-in-aggregate) pattern — `coalesce(vec![sum(col("revenue")), lit(0)])` — is one example of combining aggregation with the replacement layer to guarantee non-null output.

### Null-Testing and Replacement Functions

| Function / Method                 | Purpose                                         | Example                                                              |
| --------------------------------- | ----------------------------------------------- | -------------------------------------------------------------------- |
| [`is_null()`] / [`is_not_null()`] | Testing for null in filters                     | `.filter(col("email").is_not_null())`                                |
| [`coalesce()`]                    | First non-null value from a list of expressions | `coalesce(vec![col("nickname"), col("name"), lit("Anonymous")])`     |
| [`nullif()`]                      | Convert a specific value to null                | `nullif(col("status"), lit("UNKNOWN"))` → NULL if status = "UNKNOWN" |
| [`nvl()`]                         | Simple two-argument null fallback               | `nvl(col("price"), lit(0))`                                          |
| [`.fill_null()`]                  | Replace nulls across multiple columns at once   | `df.fill_null(ScalarValue::from(0i64), vec!["qty".into()])?`         |

:::{admonition} NULL is not NaN
:class: warning
`NaN` (Not a Number) is a valid IEEE 754 floating-point value — it is **not** null. `is_null()` returns `FALSE` for NaN, and `.fill_null()` will not replace NaN values. Use [`isnan()`] to detect NaN and [`nanvl()`] (`nanvl(expr, replacement)`) to replace it.

| Check           | NULL   | NaN   |
| --------------- | ------ | ----- |
| `is_null()`     | TRUE   | FALSE |
| `isnan()`       | FALSE  | TRUE  |
| `.fill_null(0)` | → 0    | → NaN |
| `nanvl(x, 0)`   | → NULL | → 0   |

:::

### `CASE WHEN` — Conditional Null Logic

**`CASE WHEN` provides conditional branching where simpler replacement functions fall short — the general-purpose tool for null-aware logic that depends on multiple columns or conditions.**

Replacement functions like `coalesce()` and `nvl()` pick the first non-null value from a fixed list — they cannot evaluate arbitrary predicates or vary the replacement based on other columns. `CASE WHEN` fills this gap: each branch evaluates an independent boolean condition, and DataFusion short-circuits the evaluation — branches whose conditions are already determined are not evaluated. This makes patterns like `CASE WHEN d != 0 THEN n / d ELSE NULL END` safe from division-by-zero errors. The DataFrame API expresses this via `when(condition, value).otherwise(fallback)`:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "status" => [Some("active"), None, Some("inactive")]
    )?;

    let categorized = df.select(vec![
        when(col("status").is_null(), lit("UNKNOWN"))
            .otherwise(col("status"))?
            .alias("status_clean"),
    ])?;

    let result = categorized.collect().await?;
    assert_batches_eq!(
        &[
            "+--------------+",
            "| status_clean |",
            "+--------------+",
            "| active       |",
            "| UNKNOWN      |",
            "| inactive     |",
            "+--------------+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} SQL equivalent
:class: tip
`CASE WHEN status IS NULL THEN 'UNKNOWN' ELSE status END`
:::

### `DISTINCT` and Null Values

`DISTINCT` operations treat all NULL values as equal — multiple NULLs collapse into a single NULL in the output. This follows the SQL standard and differs from comparison semantics where `NULL ≠ NULL`.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "region" => [Some("EMEA"), None, Some("EMEA"), None, Some("APAC")]
    )?;

    // DISTINCT: both NULLs collapse into one
    let result = df.distinct()?.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+--------+",
            "| region |",
            "+--------+",
            "|        |",
            "| APAC   |",
            "| EMEA   |",
            "+--------+",
        ],
        &result
    );

    Ok(())
}
```

---

## Optimizer Null-Awareness

**DataFusion's optimizer leverages nullability metadata to eliminate redundant checks at plan time and skip entire file segments at execution time — explicit schemas unlock these optimizations.**

Two optimizer components exploit null information:

**`SimplifyExpressions`** rewrites expressions based on column nullability declared in the schema. If a column is marked `nullable = false`, the optimizer reduces `col.is_null()` to `FALSE` and `col.is_not_null()` to `TRUE`, eliminating the runtime check entirely. This optimization propagates: a filter on `WHERE non_nullable_col IS NOT NULL` is simplified to `WHERE TRUE` and then removed from the plan.

**`PruningPredicate`** uses file-level statistics (such as Parquet's `null_count` per row group) to skip entire row groups during physical execution. If a predicate requires non-null values and a row group's `null_count` equals the row count, the entire group is pruned without reading any data. This reduces I/O significantly for sparse datasets.

Beyond plan-level optimizations, marking a column `NOT NULL` has a physical benefit: Arrow's `NullBufferBuilder` skips validity bitmap allocation entirely when no nulls are present, saving memory and CPU cycles during scans and writes.

:::{admonition} Unlock optimizer benefits
:class: tip
Schema inference defaults all columns to `nullable = true`, disabling these optimizations. Declaring explicit schemas with accurate nullability constraints — via `SchemaBuilder` or read options — enables the optimizer to simplify expressions and prune data at the physical level. See [Schema Inference][schema-inference] for strategies.
:::

---

## Conclusion

**Three-valued logic governs every transformation: filters discard, aggregates skip, joins refuse to match, and sorts must place — mastering these behaviors turns null-related bugs into predictable, controllable outcomes.**

The key patterns to remember:

- **Test explicitly** with [`is_null()`] / [`is_not_null()`] rather than relying on equality checks
- **Replace defensively** with [`coalesce()`], [`nvl()`], or [`.fill_null()`] before downstream operations
- **Join carefully** using [`.join_on()`] with [`Operator::IsNotDistinctFrom`] (DataFrame API) or `IS NOT DISTINCT FROM` (SQL) when NULL keys should match
- **Declare nullability** in schemas to enable optimizer short-circuits and physical pruning

:::{admonition} Next steps
:class: seealso

- [Anatomy of a Schema][schema-anatomy] — how nullability is declared in Arrow fields
- [Schema Inference][schema-inference] — why inferred schemas default to `nullable = true`
- [Type Coercion][type-coercion] — how type mismatches interact with nullability during expression planning
- [Expressions][expressions] — how [`Expr`] trees propagate nullability through the plan
  :::

---

With null semantics understood, the next section covers what happens when you trigger execution — lazy plans become optimized physical operators and streaming results. Continue to [Execution Lifecycle][execution-lifecycle].

---

<!-- References -->

<!-- Internal documentation -->

[execution-lifecycle]: execution-lifecycle.md
[expressions]: expressions.md
[schema-anatomy]: ../Schema-Management/schema-anatomy.md
[schema-inference]: ../Schema-Management/schema-inference.md
[type-coercion]: ../Schema-Management/type-coercion.md

<!-- Core types -->

[`expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[`nullequality`]: https://docs.rs/datafusion/latest/datafusion/common/enum.NullEquality.html
[`operator::isnotdistinctfrom`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Operator.html#variant.IsNotDistinctFrom

<!-- Methods and functions -->

[`.fill_null()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.fill_null
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[`logicalplanbuilder::join_detailed()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html#method.join_detailed
[`coalesce()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.coalesce.html
[`is_not_null()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.is_not_null
[`is_null()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.is_null
[`isnan()`]: https://docs.rs/datafusion/latest/datafusion/functions/math/fn.isnan.html
[`nanvl()`]: https://docs.rs/datafusion/latest/datafusion/functions/math/fn.nanvl.html
[`nullif()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.nullif.html
[`nvl()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.nvl.html

<!-- External resources -->

[three-valued logic]: https://modern-sql.com/concept/three-valued-logic
[validity bitmap]: https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
