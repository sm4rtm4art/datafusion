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

Every query engine must resolve a fundamental tension: real-world data contains gaps, yet computation demands concrete values. DataFusion's DataFrame API inherits SQL-standard three-valued logic — where NULL represents an unknown rather than a value — and layers it on top of Apache Arrow's columnar validity bitmaps for high-performance null-aware execution. This guide covers how three-valued logic silently shapes every transformation, from filters that discard rows to joins that refuse to match, and how DataFusion's optimizer exploits nullability metadata to eliminate redundant checks and prune entire file segments. Armed with these propagation rules and the null-handling toolkit — `coalesce()`, `.fill_null()`, `IS NOT DISTINCT FROM` — the most common source of DataFrame bugs becomes predictable, controllable behavior.

**Correct null propagation through three-valued logic prevents the most common DataFrame bugs — silent row drops, missed matches, and unexpected results.**

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

Every data pipeline encounters missing values: sensors fail, users skip form fields, outer joins introduce unmatched rows. Traditional programming languages represent absence as `null`, `None`, or `nil` — a simple marker. SQL-based query engines, including DataFusion, go further: NULL follows **three-valued logic** (3VL), where every boolean expression evaluates to `TRUE`, `FALSE`, or `NULL`. This distinction is the single most common source of unexpected query results.

DataFusion adheres to SQL-standard null semantics. Any arithmetic, comparison, or logical operation involving NULL propagates the unknown: `5 + NULL = NULL`, `NULL + NULL = NULL`, `NULL > 0 = NULL`. The consequence is that predicates in `.filter()`, join conditions, and `CASE WHEN` branches treat NULL as neither true nor false — they simply skip it.

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

**Column nullability — declared via `Field::nullable` — controls whether the optimizer can short-circuit null checks and skip validity bitmap allocation.**

Nullability is one of the four properties of an Arrow `Field` (alongside name, data type, and metadata). When `Field::nullable` is `true`, the column may contain null values; when `false`, the query engine can guarantee every row has a valid value.

:::{admonition} Schema Inference
:class: tip
File formats without a declared schema (CSV, JSON) require inference by sampling rows — all columns default to `nullable = true`. Parquet preserves nullability from the writer's schema. Explicit declarations via `SchemaBuilder` or `CsvReadOptions::new().schema(&schema)` enable the optimizer to eliminate redundant null checks (see [Optimizer Null-Awareness](#optimizer-null-awareness)).
:::

:::{admonition} Deep dive
:class: seealso
For the full structure of Arrow fields — name, data type, nullability, and metadata — see [Anatomy of a Schema — Arrow Field Properties](../Schema-Management/anatomy-schema.md#arrow-field-the-four-properties).
:::

---

## Null Propagation in Transformations

**Each DataFrame transformation interacts with NULLs through distinct, SQL-standard rules that silently alter output rows, schema nullability, and result values.**

NULL propagation is not uniform — each transformation applies its own rule, and the combination of these rules across a pipeline determines which rows survive, which values change, and which matches occur. Understanding these rules prevents the most common DataFrame bugs.

| Transformation | Null Behavior                | Surprise Risk |
| -------------- | ---------------------------- | ------------- |
| `.filter()`    | Discards NULL predicate rows | High          |
| `.aggregate()` | Skips NULLs in computation   | Medium        |
| `.join()`      | NULL ≠ NULL, no match        | High          |
| `.sort()`      | Placement convention         | Low           |
| `.union()`     | Widens nullability in schema | Low           |

The subsections below follow the typical transformation order in a data pipeline.

### `.filter()` Handling Nulls

**`.filter()` keeps rows where the predicate is `TRUE` — both `FALSE` and `NULL` results are discarded, silently dropping rows with unknown values.**

Rows with NULL in filtered columns disappear without warning. The `.filter(predicate)` method evaluates each row's predicate expression to `TRUE`, `FALSE`, or `NULL` via three-valued logic — only `TRUE` rows survive. A predicate is any boolean `Expr`, such as `col("age").gt(lit(18))`, that DataFusion evaluates against each batch.

```text
WHERE age > 18
  ├── age = 25  → TRUE  → kept ✓
  ├── age = 17  → FALSE → filtered out
  └── age = NULL → NULL → filtered out (!)
```

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

**SQL equivalent:** `WHERE age > 18 OR age IS NULL`

### `.aggregate()` Handling Nulls

**Aggregation skips null values — preventing undefined arithmetic but changing results in ways that catch most developers off guard.**

Aggregates over `[10, NULL, 20]` return `sum = 30` and `avg = 15.0` (dividing by 2 non-null values, not 3 rows) — because DataFusion follows the SQL standard and ignores NULLs entirely. When all values are NULL, most aggregates return NULL, not zero.

| Expression   | With data `[10, NULL, 20]` | Notes                       |
| ------------ | :------------------------: | --------------------------- |
| `sum(col)`   |            `30`            | Nulls skipped               |
| `avg(col)`   |           `15.0`           | Divides by 2, not 3         |
| `count(*)`   |            `3`             | Counts all rows             |
| `count(col)` |            `2`             | Counts only non-null values |
| `min(col)`   |            `10`            | Nulls ignored               |

The `count(*)` vs. `count(col)` distinction is the most common pitfall. `count(*)` is the idiomatic way to count all rows regardless of null values; `count(col)` counts only rows where that specific column is non-null.

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

:::{admonition} Defensive aggregation
:class: tip
Wrap aggregates with `coalesce()` when downstream code cannot handle NULL results: `coalesce(sum(col("revenue")), lit(0))` ensures a zero instead of NULL when all inputs are null.
:::

### `.join()` Handling Nulls

**`NULL ≠ NULL` in join conditions — two unknown values are not considered equal, causing silently missed matches in every join type.**

Join conditions evaluate equality the same way as any other expression: `NULL = NULL` produces `NULL`, which the join interprets as "no match." Rows with NULL keys on either side are excluded from the result in inner joins and appear as unmatched in outer joins.

```text
LEFT TABLE        RIGHT TABLE       INNER JOIN RESULT
id | value        id | data
---|-------       ---|------
1  | 'a'          1  | 'x'          ← 1 = 1, match ✓
2  | 'c'          NULL| 'y'         ← 2 ≠ NULL, no match
NULL| 'b'         3  | 'z'          ← NULL ≠ 3, no match
```

For **null-safe joins** where `NULL = NULL` should evaluate to `TRUE`, the high-level DataFrame `.join()` method does not currently expose this option. Two alternatives exist:

- **SQL**: Use `IS NOT DISTINCT FROM` syntax
- **Programmatic**: Use [`LogicalPlanBuilder`] with [`NullEquality`] for full control

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let left_table = dataframe!(
        "left_id" => [Some(1i32), Some(2i32), None],
        "value" => ["a", "b", "c"]
    )?;
    let right_table = dataframe!(
        "right_id" => [Some(1i32), None, Some(3i32)],
        "data" => ["x", "y", "z"]
    )?;

    ctx.register_table("left", left_table.clone().into_view())?;
    ctx.register_table("right", right_table.clone().into_view())?;

    // Null-safe join via SQL: NULL = NULL matches
    let null_safe = ctx.sql(
        "SELECT *
        FROM left AS l
        JOIN right AS r
        ON l.left_id IS NOT DISTINCT FROM r.right_id"
    ).await?;

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

:::{admonition} Null-safe equality in SQL
:class: tip
DataFusion supports two equivalent syntaxes for null-safe equality: `IS NOT DISTINCT FROM` (SQL standard) and `<=>` (MySQL/Spark-style spaceship operator). Both compile to the same `Operator::IsNotDistinctFrom`. For programmatic null-safe joins without SQL, use [`LogicalPlanBuilder`] with [`NullEquality::NullEqualsNull`][`NullEquality`].
:::

### `.sort()` Handling Nulls

**NULL values have no inherent order — DataFusion follows PostgreSQL conventions, placing NULLs last in ascending sorts and first in descending sorts by default.**

Since `NULL` cannot be compared (`NULL < 5` is `NULL`), every sort must define a placement convention. DataFusion defaults to PostgreSQL semantics. The `.sort()` method accepts `SortExpr` with two booleans controlling behavior:

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

**SQL equivalent:** `ORDER BY score ASC NULLS FIRST`

### `.union()` Handling Nulls

**Union operations widen nullability — if a column is nullable in _any_ input, the output schema marks it nullable, even if other inputs declare it non-nullable.**

When combining DataFrames via `.union()` or `.union_by_name()`, DataFusion derives the output schema by merging fields across all inputs. The nullability rule is conservative: `nullable = fields.iter().any(|f| f.is_nullable())`. A non-nullable `price` column in one DataFrame becomes nullable in the union output if the other DataFrame's `price` column is nullable.

For `.union_by_name()`, columns that exist in one input but not another are filled with `NULL` literals and forced nullable in the output schema — the column cannot be non-nullable if entire inputs lack it.

---

## The Null-Handling Toolkit

**DataFusion provides dedicated functions and methods for testing, replacing, and converting nulls — choosing the right tool depends on whether you need a predicate, a fallback value, or conditional logic.**

### Null-Testing and Replacement Functions

| Function / Method                 | Purpose                                         | Example                                                              |
| --------------------------------- | ----------------------------------------------- | -------------------------------------------------------------------- |
| [`is_null()`] / [`is_not_null()`] | Testing for null in filters                     | `.filter(col("email").is_not_null())`                                |
| [`coalesce()`]                    | First non-null value from a list of expressions | `coalesce(col("nickname"), col("name"), lit("Anonymous"))`           |
| [`nullif()`]                      | Convert a specific value to null                | `nullif(col("status"), lit("UNKNOWN"))` → NULL if status = "UNKNOWN" |
| [`nvl()`] / [`ifnull()`]          | Simple two-argument null fallback               | `nvl(col("price"), lit(0))`                                          |
| [`.fill_null()`]                  | Replace nulls across multiple columns at once   | `df.fill_null(ScalarValue::from(0i64), vec!["qty".into()])?`         |

:::{admonition} NULL is not NaN
:class: warning
`NaN` (Not a Number) is a valid IEEE 754 floating-point value — it is **not** null. `is_null()` returns `FALSE` for NaN, and `.fill_null()` will not replace NaN values. Use `isnan()` to detect NaN and `nanvl(expr, replacement)` to replace it.

| Check           | NULL   | NaN   |
| --------------- | ------ | ----- |
| `is_null()`     | TRUE   | FALSE |
| `isnan()`       | FALSE  | TRUE  |
| `.fill_null(0)` | → 0    | → NaN |
| `nanvl(x, 0)`   | → NULL | → 0   |

:::

### `CASE WHEN` — Conditional Null Logic

For complex null-handling beyond simple replacement, use `when()` / `then()` / `otherwise()` to build `CASE WHEN` expressions:

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

**SQL equivalent:** `CASE WHEN status IS NULL THEN 'UNKNOWN' ELSE status END`

DataFusion short-circuits `CASE WHEN` evaluation — branches whose conditions are already determined are not evaluated. This makes patterns like `CASE WHEN d != 0 THEN n / d ELSE NULL END` safe from division-by-zero errors.

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
Schema inference defaults all columns to `nullable = true`, disabling these optimizations. Declaring explicit schemas with accurate nullability constraints — via `SchemaBuilder` or read options — enables the optimizer to simplify expressions and prune data at the physical level. See [Schema Inference](../Schema-Management/schema-inference.md) for strategies.
:::

---

## Conclusion

**Three-valued logic governs every transformation: filters discard, aggregates skip, joins refuse to match, and sorts must place — mastering these behaviors turns null-related bugs into predictable, controllable outcomes.**

The key patterns to remember:

- **Test explicitly** with `is_null()` / `is_not_null()` rather than relying on equality checks
- **Replace defensively** with `coalesce()`, `nvl()`, or `.fill_null()` before downstream operations
- **Join carefully** using `IS NOT DISTINCT FROM` (SQL) or `NullEquality` (programmatic) when NULL keys should match
- **Declare nullability** in schemas to enable optimizer short-circuits and physical pruning

:::{admonition} Next steps
:class: seealso

- [Anatomy of a Schema — Nullability](../Schema-Management/anatomy-schema.md#schema-field-nullability) — how nullability is declared in Arrow fields
- [Schema Inference](../Schema-Management/schema-inference.md) — why inferred schemas default to `nullable = true`
- [Type Coercion](../Schema-Management/type-coercion.md) — how type mismatches interact with nullability during expression planning
- [Expressions](expressions.md) — how `Expr` trees propagate nullability through the plan
  :::

---

With null semantics understood, the next section covers what happens when you trigger execution — lazy plans become optimized physical operators and streaming results. Continue to [Execution Lifecycle](execution-lifecycle.md).

<!-- Abstract written. Content finalized. -->

<!-- Link references -->

[validity bitmap]: https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
[`is_null()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ExprFuncExt.html#method.is_null
[`is_not_null()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ExprFuncExt.html#method.is_not_null
[`coalesce()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.coalesce.html
[`nullif()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.nullif.html
[`nvl()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.nvl.html
[`ifnull()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.ifnull.html
[`.fill_null()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.fill_null
[`NullEquality`]: https://docs.rs/datafusion/latest/datafusion/common/enum.NullEquality.html
[`isnan()`]: https://docs.rs/datafusion/latest/datafusion/functions/math/fn.isnan.html
[`nanvl()`]: https://docs.rs/datafusion/latest/datafusion/functions/math/fn.nanvl.html
[`LogicalPlanBuilder`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html
[`.sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
