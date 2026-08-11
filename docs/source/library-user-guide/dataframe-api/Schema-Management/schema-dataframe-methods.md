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

# Changing Schemas with DataFrame Methods

**DataFrame schema methods reshape the output contract lazily: they derive a new [`DFSchema`] from the columns you keep, compute, rename, remove, cast, or unnest.**

Structured data processing depends on a schema contract — the column names, types, and nullability the engine expects at each step. In DataFusion, every [`DataFrame`] carries that contract as a [`DFSchema`], derived automatically as the methods you chain extend the [`LogicalPlan`]. This page is the reference for the methods that deliberately reshape it — and the boundary around the many that leave it untouched.

The methods that change the schema fall into three families: projection-backed edits (`.select()`, `.with_column()`, renames, drops, and casts) that rebuild the field list on purpose; operation-derived changes (`.join()`, `.aggregate()`, `.window()`, set operations) where a new schema is a side effect of combining or grouping rows; and unnest-backed reshaping (`.unnest_columns()`) that flattens nested `Struct` and `List` fields. Each is shown in runnable code — and linked to the operation's own page where that operation is the real topic — because the recurring risk is the same: ambiguous or duplicate column names when fields are combined or renamed.

**Key methods:**

| Method                     | Schema effect                                                      |
| :------------------------- | :----------------------------------------------------------------- |
| [`.select()`]              | Replace the output schema with expression-derived fields           |
| [`.select_columns()`]      | Keep existing fields by name (errors on unknown names)             |
| [`.select_exprs()`]        | Project from SQL strings (requires the `sql` feature)              |
| [`.with_column()`]         | Append a field, or replace it in place if the name exists          |
| [`.with_column_renamed()`] | Rename a field (no-op if not found)                                |
| [`.drop_columns()`]        | Keep the complement; remove fields by name (ignores unknown names) |
| [`.fill_null()`]           | Replace NULLs; filled castable fields become NOT NULL              |
| [`.unnest_columns()`]      | Expand `List`/`Struct` fields into a new shape                     |

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `df.method()` (e.g., `df.select(...)`, `df.filter(...)`)
- **DFSchema instance methods:** `df.schema().method()` (e.g., `df.schema().fields()`)
- **DFSchema associated functions:** `DFSchema::function()` (e.g., `DFSchema::try_from(...)`)
- **Standalone functions:** `function()` (e.g., `col(...)`, `lit(...)`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::

```{contents} Table of Contents for Changing Schemas with DataFrame Methods
:local:
:depth: 2
:caption:
```

## DataFrame Methods and the Schema's Involvement

**Schema change can be a DataFrame method's purpose, a side effect of another operation, or absent altogether — and this page groups every method by which case applies.**

At the DataFrame layer the schema is never hand-written: the engine derives a new [`DFSchema`] for you as the plan is built. What changes from method to method is _why_ it moved. Most methods on this page move it **on purpose** — you call `.select()`, `.with_column()`, or `.unnest_columns()` to choose, add, or reshape fields. Others move it only as a **by-product** of an operation whose real story is rows, such as `.join()` or `.aggregate()`. And many methods — `.filter()`, `.sort()`, `.limit()` — leave the visible schema **unchanged**.

Wherever columns are combined or split — joining inputs or unnesting a nested column — watch for ambiguous or duplicate names, the most common schema surprise. The table below routes each method to where it is documented.

| Method family                                                                                                                                                                                            | Effect on the schema                                                                                                                                                | Where to read it                                                                  |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------------------------------------------------------------------- |
| **Schema-preserving methods** (**no visible effect**) — `.filter()`, `.sort()`, `.limit()`, `.distinct()`, `.repartition()`                                                                              | The visible `DFSchema` passes through unchanged                                                                                                                     | This page — [Schema-Preserving Methods](#schema-preserving-methods)               |
| **Projection-backed schema edits** — `.select()`, `.select_columns()`, `.select_exprs()`, `.with_column()`, `.with_column_renamed()`, `.drop_columns()`, `.fill_null()`, casts inside projection methods | A projection derives a new `DFSchema` from the output expressions: fields can be kept, computed, renamed, removed, cast, or normalized                              | This page — [Projection-Backed Column Edits](#projection-backed-column-edits)     |
| **Operation-derived schemas** — `.join()`, `.union()`, `.union_by_name()`, `.aggregate()`, `.window()`                                                                                                   | The operation derives its own output schema from input-combination, grouping, set, or window-expression rules; schema movement is a consequence, not the main topic | This page — [Operation-Derived Schema Changes](#operation-derived-schema-changes) |
| **Unnest-backed nested reshaping** — `.unnest_columns()`, `.unnest_columns_with_options()`                                                                                                               | An `Unnest` node expands selected nested fields: `Struct` fields split into child columns, while `List` fields expose their element type and may expand rows        | This page — [Reshaping Nested Fields](#reshaping-nested-fields)                   |

The rest of this page follows that order: the methods that leave the schema untouched, the projection edits you make on purpose, the operations that reshape it as a side effect, and finally nested reshaping with unnest.

---

## Schema-Preserving Methods

**Most DataFrame methods leave the schema untouched — they work on rows, not fields, so the `DFSchema` passes straight through.**

Before the methods that reshape the schema, note the common case: most of the API never touches the field list.

- **Row-shaping methods** — `.filter()`, `.sort()`, `.limit()`, `.distinct()`, `.repartition()` — change which rows appear or how they are arranged; the output schema matches the input.
- **Inspection actions** — `.describe()`, `.explain()` — hand back a separate diagnostic result (summary statistics or plan text), so they sit outside schema management too.

Everything below is the smaller set of methods that do move the schema.

---

## Projection-Backed Column Edits

**Projection-backed methods edit a `DataFrame` schema by rebuilding the output field list from selected, computed, renamed, or normalized columns.**

Under the hood almost all of these methods compile to a projection: [`.drop_columns()`] becomes a [`.select()`] of the columns you keep, and [`.fill_null()`] becomes a [`.select()`] of `coalesce()` expressions. [`.select()`] is the reference point because it exposes projection directly: each expression in the select list becomes one output field in the derived schema. The convenience methods differ mostly in how they build that field list and how they react to unknown column names.

:::{admonition} .alias() renames the qualifier, not the columns
:class: seealso

[`.alias()`] re-qualifies every output field through a `SubqueryAlias` node — a relation-level rename, not a projection — so it sits outside these column edits. See [Schema Transformation][schema-transformation] for qualifier handling.
:::

One sharp edge cuts across these methods: they disagree on what an **unknown column name** does. Check the call you are making against this table before relying on it.

| Method                     | Unknown column name |
| :------------------------- | :------------------ |
| [`.select_columns()`]      | Error               |
| [`.fill_null()`]           | Error               |
| [`.drop_columns()`]        | Silently ignored    |
| [`.with_column_renamed()`] | Silent no-op        |

[`.fill_null()`] is as strict as [`.select_columns()`]: a named column that does not exist is an error. Passing an empty column list instead targets every column and skips the name check entirely.

### Selecting Columns

**Selecting columns rebuilds the visible `DFSchema` from the columns and expressions that the projection names explicitly.**

[`.select()`] and [`.select_columns()`] both replace the current output field list; they differ only in how you author it. [`.select()`] is the general form: each projection expression — a bare column, a computed expression, or a wildcard — becomes one output field, so any input field you omit disappears. [`.select_columns()`] is the shorthand for the common “keep these existing columns” case, taking field names as strings.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let users_df = dataframe!(
        "id" => [1_i64, 2_i64, 3_i64],
        "name" => ["Alice", "Bob", "Carol"],
        "email" => ["a@x.com", "b@x.com", "c@x.com"],
        "temp_flag" => [true, false, true]
    )?;

    // select(): only the named projection expressions become output fields.
    let selected = users_df.clone().select(vec![col("id"), col("name")])?;
    assert_eq!(selected.schema().fields().len(), 2);
    assert!(selected.schema().field_with_unqualified_name("email").is_err());

    // select_columns(): the same schema shape, written as string names.
    let selected = users_df.clone().select_columns(&["id", "name"])?;
    assert_eq!(selected.schema().fields().len(), 2);

    // select_columns() is strict: an unknown name is an error.
    assert!(users_df.select_columns(&["nonexistent"]).is_err());

    Ok(())
}
```

For SQL expression strings, [`.select_exprs()`] parses `&["a * b", "c"]` into the same projection shape: one output field per parsed expression. It is gated behind the `sql` feature, so reach for [`.select()`] with `col()`/`lit()` when you cannot assume that feature is enabled.

:::{admonition} Duplicate projection names fail the plan
:class: warning

The output schema is a [`DFSchema`], which rejects duplicate field names. Two projection expressions that resolve to the same name therefore make the build fail rather than silently collide.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let metrics_df = dataframe!(
        "clicks" => [10_i64, 20_i64],
        "views" => [100_i64, 200_i64]
    )?;

    // Both expressions resolve to the name "rate" -> duplicate field -> error.
    let result = metrics_df.select(vec![
        col("clicks").alias("rate"),
        col("views").alias("rate"),
    ]);
    assert!(result.is_err());

    Ok(())
}
```

:::

### Adding and Replacing Fields

**Use [`.with_column()`] to append a computed field, or to replace an existing one in place when the name already exists.**

The method matches the given name against the current schema. A new name is appended as the last field; an existing name is replaced where it already sits, keeping field order and the field count stable.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "price" => [10.0_f64, 20.0_f64],
        "qty" => [2_i64, 1_i64]
    )?;

    // A new name is appended.
    let with_total = orders_df
        .clone()
        .with_column("total", col("price") * col("qty"))?;
    assert_eq!(with_total.schema().fields().len(), 3);
    assert!(with_total
        .schema()
        .field_with_unqualified_name("total")
        .is_ok());

    // An existing name is replaced in place — the field count is unchanged.
    let retyped = orders_df.with_column("qty", col("qty").gt(lit(0)))?;
    assert_eq!(retyped.schema().fields().len(), 2);

    // ...but the field's TYPE silently changed from Int64 to Boolean.
    assert_eq!(
        retyped
            .schema()
            .field_with_unqualified_name("qty")?
            .data_type(),
        &DataType::Boolean
    );

    Ok(())
}
```

:::{admonition} Replacement is a silent schema-drift vector
:class: caution

[`.with_column()`] adopts the replacement expression's type and nullability while keeping the name, so re-deriving an existing column can change its type with no error at the call site — and downstream code may break later. When a stable contract matters, prefer an explicit [`.select()`] list, where the named fields make the drift visible in review.

:::

### Renaming and Removing Fields

**Use [`.with_column_renamed()`] to rename a field and [`.drop_columns()`] to keep the complement — both project the surviving fields through unchanged.**

[`.with_column_renamed()`] changes one matching field name and leaves the field's value expression otherwise unchanged. The match may be qualified or unqualified; wrap the source name in `"`, `'`, or `` ` `` for a case-sensitive match, or set `datafusion.sql_parser.enable_ident_normalization` to `false` to make every rename case-sensitive. [`.drop_columns()`] keeps every field whose name is _not_ in the list.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let users_df = dataframe!(
        "user_id" => [1_i64, 2_i64],
        "name" => ["Alice", "Bob"],
        "debug_temp" => [true, false]
    )?;

    // Rename one field, then drop another — every surviving field projects through.
    let cleaned = users_df
        .with_column_renamed("user_id", "id")?
        .drop_columns(&["debug_temp"])?;

    assert_eq!(cleaned.schema().fields().len(), 2);
    assert!(cleaned.schema().field_with_unqualified_name("id").is_ok());
    assert!(cleaned
        .schema()
        .field_with_unqualified_name("user_id")
        .is_err());
    assert!(cleaned
        .schema()
        .field_with_unqualified_name("debug_temp")
        .is_err());

    Ok(())
}
```

### Normalizing Types and Nulls

**Change a field's type with a `cast()` expression inside a projection, and replace its NULLs with [`.fill_null()`] — both edits flow through the projection node.**

Type and NULL normalization both change the values a downstream plan node receives, but they enter the DataFrame API through different surfaces. Casts are expression-level edits, not DataFrame methods: you pass `cast()`, `try_cast()`, or `Expr::cast_to()` into [`.select()`] or [`.with_column()`], and the projected field takes the expression's type. The coercion rules that govern implicit casts live in [Type Coercion][type-coercion].

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let readings_df = dataframe!("sensor_id" => [1_i64, 2_i64])?;

    // cast() is an expression passed into a projection method; it changes the
    // field's type while keeping its name and position.
    let as_text =
        readings_df.with_column("sensor_id", cast(col("sensor_id"), DataType::Utf8))?;
    assert_eq!(
        as_text
            .schema()
            .field_with_unqualified_name("sensor_id")?
            .data_type(),
        &DataType::Utf8
    );

    Ok(())
}
```

[`.fill_null()`] is a DataFrame method, but it uses the same projection pattern internally: for each selected column whose type can accept the fill value, DataFusion projects a [`coalesce(column, value)`][coalesce()] expression under the original field name. Because the fill value is non-null, that projected field becomes NOT NULL.

```rust
use datafusion::prelude::*;
use datafusion::common::ScalarValue;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let scores_df = dataframe!(
        "player" => ["Alice", "Bob", "Carol"],
        "score" => vec![Some(85_i64), None, Some(90_i64)]
    )?;

    // Before: score is nullable and carries a NULL.
    assert!(scores_df
        .schema()
        .field_with_unqualified_name("score")?
        .is_nullable());

    let filled = scores_df.fill_null(ScalarValue::from(0_i64), vec!["score".to_string()])?;

    // After: the gap is filled AND the field is now NOT NULL.
    assert!(!filled
        .schema()
        .field_with_unqualified_name("score")?
        .is_nullable());

    Ok(())
}
```

:::{admonition} fill_null skips columns it cannot cast
:class: caution

[`.fill_null()`] fills a selected column only when the value casts to that column's type; otherwise it leaves the column untouched. A type mismatch therefore looks like a silent miss rather than a failure, so confirm the fill value's type matches the target columns.

:::

---

## Operation-Derived Schema Changes

**Some operations change the schema as a side effect of their real work: `.join()`, `.aggregate()`, `.window()`, and the set operations each derive a new field list from combining or grouping rows — so their schema rules live with the operation, not on this page.**

Some operations introduce new columns, combine columns from multiple inputs, or collapse rows into groups — and the output schema shifts to match, without you naming a single field to edit. Which methods behave this way? `.join()`, `.aggregate()`, `.window()`, and the set operations. Each derives its own `DFSchema` from its semantics — matched columns, grouping keys and aggregate expressions, set compatibility, or window expressions — so the field list is part of the operation's contract. The table routes each method to the page that owns its full rules; follow the link when the operation itself is your goal.

| Method family                                 | Schema effect                                                                           | Where the operation is documented                          |
| :-------------------------------------------- | :-------------------------------------------------------------------------------------- | :--------------------------------------------------------- |
| [`.aggregate()`]                              | Replaces the input fields with grouping expressions and aggregate-expression fields     | [Aggregations](../Transformations/aggregations.md)         |
| [`.window()`]                                 | Adds or projects window-expression fields through `.select()` or `.with_column()`       | [Window Functions](../Transformations/window-functions.md) |
| [`.join()`], [`.join_on()`]                   | Combines fields from both inputs and may introduce qualifier or duplicate-name concerns | [Joins](../Transformations/joins/index.md)                 |
| [`.union()`], [`.intersect()`], [`.except()`] | Requires compatible input schemas and preserves the set-operation output shape          | [Set Operations](../Transformations/set-operations.md)     |
| [`.union_by_name()`]                          | Aligns fields by name and may introduce NULLs for missing columns                       | [Schema Transformation][schema-transformation]             |

These effects span the full range: `.aggregate()` collapses rows into group and aggregate fields, while `.window()` is the mirror case — it appends its result to the existing fields and keeps every row. The aggregation below shows the boundary — the schema changes, but grouping and aggregate-expression rules govern the change, not a schema-editing method.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::min;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales_df = dataframe!(
        "region" => ["EU", "EU", "US"],
        "amount" => [100_i64, 250_i64, 80_i64],
        "order_id" => [1_i64, 2_i64, 3_i64]
    )?;

    // aggregate(): the output schema is derived from grouping and aggregate
    // expressions, not from a direct column-editing method.
    let summarized = sales_df.aggregate(
        vec![col("region")],
        vec![min(col("amount")).alias("min_amount")],
    )?;

    let schema = summarized.schema();

    assert_eq!(schema.fields().len(), 2);
    assert!(schema.field_with_unqualified_name("region").is_ok());
    assert!(schema.field_with_unqualified_name("min_amount").is_ok());
    assert!(schema.field_with_unqualified_name("order_id").is_err());

    Ok(())
}
```

:::{admonition} Colliding names are qualified, not renamed
:class: caution

DataFusion does not auto-suffix collisions the way some engines do — there is no `id_1`, `id_2`. When `.join()` brings a column named `id` from both inputs, each keeps its table qualifier (`left.id`, `right.id`), and a later bare reference to `id` fails with an ambiguous-reference error. Two colliding columns with no distinguishing qualifier — same-named computed fields, or an unaliased self-join — are rejected when the plan is built. Disambiguate with qualified references or `.alias()` before combining. See [Joins](../Transformations/joins/index.md) and [Schema Transformation][schema-transformation].
:::

---

## Reshaping Nested Fields

**Unnesting expands a nested column into flatter columns or rows: [`.unnest_columns()`] fans a `Struct` out into one child field per member, and unwraps a `List` to its element type while repeating the row once per element.**

A nested column packs several values into a single field, so ordinary column operations — filter, group, join — reach those inner values only through special struct or array expressions. [`.unnest_columns()`] lifts the contents into first-class columns or rows by building a dedicated `Unnest` plan node — the one schema edit on this page that does not ride on projection, because nested values can reshape both the schema and the row count. This convenience form is the default-option version of [`.unnest_columns_with_options()`], which additionally accepts an [`UnnestOptions`] to control list handling such as null preservation.

The schema effect depends on the nested type:

| Nested type | Schema effect                                                                      | Row effect                                       |
| :---------- | :--------------------------------------------------------------------------------- | :----------------------------------------------- |
| `Struct`    | Replaces the struct field with one field per child, named `parent.child`           | Row count is unchanged                           |
| `List`      | Keeps the selected column name but changes its field type to the list element type | Rows may expand, one output row per list element |

A `Struct` reshapes only the schema — its members fan out into top-level fields while the row count stays the same:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let people_df = ctx
        .sql("SELECT 1 AS id, named_struct('first', 'Alice', 'age', 30) AS person")
        .await?;

    // A Struct fans out into one top-level field per member, named parent.child.
    let unnested = people_df.unnest_columns(&["person"])?;
    assert_eq!(
        unnested.schema().field_names(),
        vec!["id", "person.first", "person.age"]
    );

    Ok(())
}
```

A `List` also multiplies rows — it unwraps to the element type and repeats the input row once per element:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // One row whose `scores` column is a List<Int64>.
    let scores_df = ctx
        .sql("SELECT 1 AS id, make_array(10, 20, 30) AS scores")
        .await?;

    let unnested = scores_df.unnest_columns(&["scores"])?;

    // A List unwraps to its element type and repeats the row once per element.
    let rows: usize = unnested
        .clone()
        .collect()
        .await?
        .iter()
        .map(|batch| batch.num_rows())
        .sum();
    assert_eq!(rows, 3);

    // The element type replaces the List type on the same column name.
    assert_eq!(
        unnested
            .schema()
            .field_with_unqualified_name("scores")?
            .data_type(),
        &DataType::Int64
    );

    Ok(())
}
```

:::{admonition} Unnest targets nested columns only
:class: caution

[`.unnest_columns()`] expects each named column to be a `List` or `Struct`. Naming a scalar column is an error, not a no-op, so unnest a column only after confirming it is nested.

:::

---

## Conclusion

**Every method on this page answers one question: does it change the visible `DFSchema`?**

Most do not — row-shaping and inspection methods pass it straight through. Those that do split by intent: projection-backed edits and unnesting reshape the schema on purpose, while joins, aggregations, and the other operation-derived methods reshape it as a side effect of their real work. Knowing which group a method falls into turns schema change from an accidental surprise into a contract you author deliberately. With the schema under control, the next step is composing these methods into full pipelines.

:::{admonition} Related documents
:class: seealso

- **Next:** [Transformations](../Transformations/index.md) — filter, join, aggregate, sort, and enrich data in the DataFrame lifecycle's "life" phase.
- [Schema Transformation][schema-transformation] — qualifiers, combining schemas, and the name collisions that operation-derived methods can trigger.
- [Type Coercion][type-coercion] — the cast rules behind expression-driven type changes.
  :::

---

---

<!-- References -->

<!-- Internal documentation -->

[schema-transformation]: schema-transformation.md
[type-coercion]: type-coercion.md

<!-- Core types -->

[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`dfschema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`logicalplan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`unnestoptions`]: https://docs.rs/datafusion/latest/datafusion/common/struct.UnnestOptions.html

<!-- Methods and functions -->

[`.aggregate()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.drop_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns
[`.except()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[`.fill_null()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.fill_null
[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.select_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_columns
[`.select_exprs()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_exprs
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.unnest_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.unnest_columns
[`.unnest_columns_with_options()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.unnest_columns_with_options
[`.window()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.window
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.with_column_renamed()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed

<!-- External resources -->

[coalesce()]: https://docs.rs/datafusion/latest/datafusion/functions/core/expr_fn/fn.coalesce.html
