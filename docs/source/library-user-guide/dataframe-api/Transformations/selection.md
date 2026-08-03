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

<!-- TODO (first restructuring draft — 2026-07-17)

1. Revisit the provisional abstract and conclusion after the H2-by-H2 review.
2. Verify every Rust block and rendered result against the exact target
   DataFusion branch, then run the registered single-file doctest and the full
   `cargo test --doc` suite.
3. Verify the target-branch signatures and edge behavior of
   `.select_columns()`, `.select()`, `.with_column()`,
   `.with_column_renamed()`, `.drop_columns()`, and `.schema()`.
4. Verify the generated Sphinx anchors, the Key Methods links, and the final
   destination anchors in `schema-dataframe-methods.md`,
   `schema-inspection.md`, `hybrid-sql.md`, and `filtering.md`.
5. Recheck the projection-pruning note against the target optimizer and the
   documented behavior of the relevant source or `TableProvider`.
6. Confirm that this file has a `doc_comment::doctest!` registration in
   `datafusion/core/src/lib.rs` before finalization.
7. Validate the cross references ! And overall references !
-->

# Selecting and Shaping Columns

**Shape a DataFrame’s output schema with complete projections when the result must be explicit and targeted column methods when most existing fields should pass through unchanged.**

Column-oriented transformations reshape the schema of a lazy [`DataFrame`] without changing what one row represents. This page shows how to define a complete output with `.select_columns()` or `.select()`, compute fields with `Expr`, generate projections from `.schema()`, and apply targeted additions, replacements, renames, or removals. It distinguishes Rust compile-time API checks from DataFusion’s planning-time column resolution and execution-time data processing, compares the method families with their SQL forms, and directs readers using SQL-expression strings to `.select_exprs()` in the hybrid SQL guidance. Detailed field metadata and schema-specific edge cases remain with Schema Management.

**Key Methods**

| Method                                                             | Purpose                                                                    |
| :----------------------------------------------------------------- | :------------------------------------------------------------------------- |
| [`.select_columns()`](#choosing-select_columns-or-select)          | Define the complete output from existing fields named explicitly           |
| [`.select()`](#choosing-select_columns-or-select)                  | Define the complete output from column references and computed expressions |
| [`.with_column()`](#adding-replacing-and-renaming-columns)         | Preserve surrounding fields while adding or replacing one named expression |
| [`.with_column_renamed()`](#adding-replacing-and-renaming-columns) | Preserve the column set while changing one field name                      |
| [`.drop_columns()`](#removing-columns)                             | Preserve the remaining fields while excluding named columns                |

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (for example, `.select()` and `.with_column()`)
- **Standalone functions:** `function()` (for example, `col()` and `lit()`)
- **Constructors:** `Type::new()` (for example, `SessionContext::new()`)
- **Types:** `TypeName` (for example, `DataFrame`, `DFSchema`, and `Expr`)
- **Lazy transformations:** return a `DataFrame` and extend its `LogicalPlan`
- **Actions:** methods such as `.collect()` execute the accumulated plan

```{contents} Table of Contents for Selecting and Shaping Columns
:local:
:depth: 2
```

## Defining the Output Projection

**A projection defines every column in the result, whether you list existing fields directly, compute expressions, or generate the list from schema metadata.**

A SQL `SELECT` list defines the columns and expressions in a query result. The DataFrame API provides two corresponding projection methods: `.select_columns()` is the concise form for existing fields named directly, while `.select()` is the general form for column references, computed expressions, aliases, and programmatically constructed selections.

Column selection changes the [schema dimension][structural-dimensions] of a [`DataFrame`] . Under the shared [transformation contract][transformation-contract], both methods return a new lazy [`DataFrame`] backed by an updated logical plan. The projection does not execute the query; an action such as `.collect()` triggers the result-producing computation.

### Choosing `.select_columns()` or `.select()`

**Both methods define the complete output column set; choose `.select_columns()` for a name-only projection and `.select()` when the projection needs expressions.**

[`.select_columns()`][select-columns-method] accepts existing column names and returns them in the requested order. [`.select()`][select-method] accepts projection selections, including ordinary [`Expr`][expr-type] values, and produces one output field for each supplied item.

| Method              | Input                           | Use when                                                           | Trade-off                                                 |
| :------------------ | :------------------------------ | :----------------------------------------------------------------- | :-------------------------------------------------------- |
| `.select_columns()` | Existing column names           | Every output field already exists                                  | Concise, but does not define computed fields              |
| `.select()`         | Column or expression selections | The projection needs expressions, aliases, or generated selections | More flexible, but more verbose for name-only projections |

:::{admonition} Column References Are Resolved During Planning
:class: note

The Rust compiler checks that the DataFrame methods exist and that their Rust argument types are valid before the application runs. When the running application calls `.select()` or `.select_columns()`, DataFusion resolves the referenced columns against the current `DFSchema` and builds a logical projection. A missing or ambiguous column can therefore make the transformation return an error through `?` even though the query has not executed or scanned its rows.

The following example expresses the same projection with both methods:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: one row per order with six columns.
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1],
        "order_date" => ["2024-01-05", "2024-02-11", "2024-01-20", "2024-03-02"]
    )?;

    // Build equivalent lazy projections.
    let by_name = orders_df
        .clone()
        .select_columns(&["order_id", "product", "amount"])?;

    let by_expression = orders_df.select(vec![
        col("order_id"),
        col("product"),
        col("amount"),
    ])?;

    // Execute.
    let by_name_batches = by_name.collect().await?;
    let by_expression_batches = by_expression.collect().await?;

    assert_batches_eq!(
        &[
            "+----------+---------+--------+",
            "| order_id | product | amount |",
            "+----------+---------+--------+",
            "| 101      | Widget  | 100    |",
            "| 102      | Gadget  | 200    |",
            "| 103      | Widget  | 150    |",
            "| 104      | Gizmo   | 300    |",
            "+----------+---------+--------+",
        ],
        &by_name_batches
    );

    assert_eq!(by_name_batches, by_expression_batches);

    Ok(())
}
```

The comparable SQL projection names the same fields in a `SELECT` list:

```sql
SELECT order_id, product, amount
FROM orders;
```

SQL is often the most concise form for a fixed declarative projection. `.select_columns()` is useful when Rust code already owns a list of field names, while `.select()` provides the expression-based form needed for computed or dynamically constructed output fields. Equivalent SQL and DataFrame plans use the same execution engine, so choose between them for ergonomics, composition, and error timing rather than execution speed.

### Computing Projection Fields with `Expr`

**Use `.select()` with `Expr` values when an output field must be calculated, transformed, or assigned a stable name.**

An [`Expr`][expr-type] describes a logical computation rather than evaluating a value immediately. A projection can combine existing fields with arithmetic, comparisons, functions, and other expression trees. For the complete expression model, see [Expressions][expressions].

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: one row per order with six columns.
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1],
        "order_date" => ["2024-01-05", "2024-02-11", "2024-01-20", "2024-03-02"]
    )?;

    // Build lazy plan.
    let classified = orders_df.select(vec![
        col("order_id"),
        col("product"),
        col("amount").gt(lit(150)).alias("large_order"),
    ])?;

    // Execute.
    let batches = classified.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+---------+-------------+",
            "| order_id | product | large_order |",
            "+----------+---------+-------------+",
            "| 101      | Widget  | false       |",
            "| 102      | Gadget  | true        |",
            "| 103      | Widget  | false       |",
            "| 104      | Gizmo   | true        |",
            "+----------+---------+-------------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} Expression Projections Replace the Column List
:class: caution

`.select()` keeps only the supplied selections; every unlisted input field is absent from the result. Give a computed expression an `.alias()` when downstream code, schemas, or readers need a stable and meaningful field name. Do not treat an automatically generated expression label as a long-lived interface unless its exact behavior has been verified for the target version.
:::

The equivalent SQL uses an expression and alias in the same complete projection:

```sql
SELECT
    order_id,
    product,
    amount > 150 AS large_order
FROM orders;
```

SQL can be clearer for a fixed declarative expression, while `.select()` composes with Rust variables, functions, loops, conditional logic, and reusable `Expr` builders.

:::{admonition} SQL Expressions Inside a DataFrame Pipeline
:class: seealso

With DataFusion's `sql` crate feature enabled, `.select_exprs()` accepts SQL expression strings and uses them to define the complete output projection. Its parsing behavior, error model, and trade-offs belong to [Mixing SQL and DataFrames][hybrid-sql].
:::

### Building a Projection from the Schema

**Use `.schema()` when Rust code must determine the projected fields from the current DataFrame rather than from a fixed list.**

[`.schema()`][schema-method] exposes the `DFSchema` produced by the current logical plan without executing the query. Rust code can inspect its fields, construct a `Vec<Expr>`, and pass that generated list to `.select()`.

The following example retains every numeric field from `orders_df`:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: one row per order with six columns.
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1],
        "order_date" => ["2024-01-05", "2024-02-11", "2024-01-20", "2024-03-02"]
    )?;

    let numeric_columns: Vec<Expr> = orders_df
        .schema()
        .fields()
        .iter()
        .filter(|field| field.data_type().is_numeric())
        .map(|field| col(field.name().as_str()))
        .collect();

    // Build lazy plan from the generated expression list.
    let numeric_orders = orders_df.select(numeric_columns)?;

    // Execute.
    let batches = numeric_orders.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+-------------+--------+----------+",
            "| order_id | customer_id | amount | quantity |",
            "+----------+-------------+--------+----------+",
            "| 101      | 1           | 100    | 2        |",
            "| 102      | 1           | 200    | 1        |",
            "| 103      | 2           | 150    | 3        |",
            "| 104      | 99          | 300    | 1        |",
            "+----------+-------------+--------+----------+",
        ],
        &batches
    );

    Ok(())
}
```

This remains a complete projection: `.schema()` supplies the candidate fields, Rust chooses which fields to retain, and `.select()` defines the output. For field metadata, qualification, nullability, and broader inspection patterns, see [Inspecting DataFrame Schemas][schema-management/schema-inspection.md].

:::{admonition} Projection Pruning Depends on the Source
:class: note

Selecting only required fields gives DataFusion's optimizer an opportunity to remove unused projections and push a narrower projection toward the scan. Whether that reduces parsing, decoding, transfer, or I/O depends on the data source and the capabilities of its [`TableProvider`][table-provider].

:::

---

## Adding, Replacing, and Renaming Columns

**When most fields should pass through unchanged, use `.with_column()` to add or replace one named expression and `.with_column_renamed()` to change one field name without rebuilding the complete projection.**

A complete `.select()` projection is appropriate when the expression list should define the entire output. When only one field needs to change, re-listing every surrounding field adds noise and makes the transformation harder to maintain. [`.with_column()`][with-column-method] and [`.with_column_renamed()`][with-column-renamed-method] focus the plan on the named field while preserving the rest of the column set.

| Operation | Method form                                    | Effect                                                |
| :-------- | :--------------------------------------------- | :---------------------------------------------------- |
| Add       | `.with_column("new_name", expr)`               | Appends a field when the supplied name is new         |
| Replace   | `.with_column("existing_name", expr)`          | Replaces the expression assigned to an existing field |
| Rename    | `.with_column_renamed("old_name", "new_name")` | Changes a field name while preserving its values      |

The following pipeline adds an order classification, replaces `amount` with a value expressed in cents, and renames the converted field to make its unit explicit:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: one row per order with six columns.
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1],
        "order_date" => ["2024-01-05", "2024-02-11", "2024-01-20", "2024-03-02"]
    )?;

    // Build lazy plan.
    let prepared_orders = orders_df
        .with_column("large_order", col("amount").gt(lit(150)))?
        .with_column("amount", col("amount") * lit(100))?
        .with_column_renamed("amount", "amount_cents")?;

    // Execute.
    let batches = prepared_orders.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+-------------+---------+--------------+----------+------------+-------------+",
            "| order_id | customer_id | product | amount_cents | quantity | order_date | large_order |",
            "+----------+-------------+---------+--------------+----------+------------+-------------+",
            "| 101      | 1           | Widget  | 10000        | 2        | 2024-01-05 | false       |",
            "| 102      | 1           | Gadget  | 20000        | 1        | 2024-02-11 | true        |",
            "| 103      | 2           | Widget  | 15000        | 3        | 2024-01-20 | false       |",
            "| 104      | 99          | Gizmo   | 30000        | 1        | 2024-03-02 | true        |",
            "+----------+-------------+---------+--------------+----------+------------+-------------+",
        ],
        &batches
    );

    Ok(())
}
```

Portable SQL expresses the same result by defining the complete output projection:

```sql
SELECT
    order_id,
    customer_id,
    product,
    amount * 100 AS amount_cents,
    quantity,
    order_date,
    amount > 150 AS large_order
FROM orders;
```

SQL keeps a fixed transformation declarative and explicit. The DataFrame methods keep the transformation focused on the fields being changed and compose naturally with programmatically constructed expressions. Both forms build logical plans for the same execution engine, so the choice is one of ergonomics and composition rather than execution speed.

Detailed behavior—including type and nullability changes, field metadata, qualification, identifier handling, replacement semantics, and missing rename targets—belongs to [Changing Schemas with DataFrame Methods][schema-dataframe-methods].

---

## Removing Columns

**When a pipeline needs most of its current fields, `.drop_columns()` expresses the change as a short exclusion list instead of rebuilding the complete projection.**

Transformation pipelines often carry temporary, sensitive, or no-longer-needed fields after those fields have served their purpose. A complete `.select()` or `.select_columns()` projection can remove them, but doing so requires listing every field that should remain.

[`.drop_columns()`][drop-columns-method] instead names the fields that should be absent. The method returns a new lazy `DataFrame` containing the remaining fields and leaves the source `DataFrame` unchanged.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: one row per order with six columns.
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1],
        "order_date" => ["2024-01-05", "2024-02-11", "2024-01-20", "2024-03-02"]
    )?;

    // Build lazy plan.
    let public_orders = orders_df.drop_columns(&["customer_id", "order_date"])?;

    // Execute.
    let batches = public_orders.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+---------+--------+----------+",
            "| order_id | product | amount | quantity |",
            "+----------+---------+--------+----------+",
            "| 101      | Widget  | 100    | 2        |",
            "| 102      | Gadget  | 200    | 1        |",
            "| 103      | Widget  | 150    | 3        |",
            "| 104      | Gizmo   | 300    | 1        |",
            "+----------+---------+--------+----------+",
        ],
        &batches
    );

    Ok(())
}
```

<!-- TODO: Verify `SELECT * EXCLUDE` support against the exact target branch. -->

DataFusion SQL can express the same exclusion-list operation with a wildcard projection:

```sql
SELECT * EXCLUDE (customer_id, order_date)
FROM orders;
```

Both forms pass through fields that are not named in the exclusion list. `.drop_columns()` composes directly with a DataFrame pipeline and can receive a programmatically constructed list, while SQL `* EXCLUDE` is concise when the excluded fields are fixed.

:::{admonition} Exclusion Lists Are Open to New Fields
:class: caution

`.drop_columns()` passes through every field that is not explicitly excluded. If the upstream schema gains a new field, that field also appears in the result. Use an explicit `.select()` or `.select_columns()` projection when the output schema is a stable external contract.

For missing, ambiguous, qualified, or otherwise schema-sensitive column references, see [Changing Schemas with DataFrame Methods][schema-dataframe-methods].

---

## Conclusion

<!-- Provisional conclusion — revisit after the H2-by-H2 review. -->

Complete projections and targeted column changes serve different stages of a transformation pipeline. Use `.select_columns()` or `.select()` when the supplied selections should define the entire output, including expression lists generated from `.schema()`. Use `.with_column()` and `.with_column_renamed()` when most fields should pass through unchanged, and `.drop_columns()` when the intended change is best expressed as an exclusion list. Each method builds a lazy logical plan; the next page applies the same transformation contract to the row dimension with [Filtering Rows][filtering].

### Further Reading

- [SQL `SELECT` syntax][sql-select-reference] — DataFusion SQL projection expressions, wildcard selection, and column exclusion.
- [Building Logical Plans][building-logical-plans] — How DataFrame transformations accumulate into a logical plan before execution.

[sql-select-reference]: https://datafusion.apache.org/user-guide/sql/select.html
[building-logical-plans]: https://datafusion.apache.org/library-user-guide/building-logical-plans.html

<!-- DataFusion types and functions -->

[expr-type]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[table-provider]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

<!-- DataFrame methods -->

[select-columns-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_columns
[select-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[with-column-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[with-column-renamed-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed
[drop-columns-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns
[schema-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema

<!-- Internal pages -->

[transformation-contract]: transformation-concepts.md#the-transformation-contract
[structural-dimensions]: transformation-concepts.md#what-transformations-can-change
[expressions]: ../Concepts/expressions.md
[schema-inspection]: ../Schema-Management/schema-inspection.md
[schema-dataframe-methods]: ../Schema-Management/schema-dataframe-methods.md
[hybrid-sql]: hybrid-sql.md
[filtering]: filtering.md
