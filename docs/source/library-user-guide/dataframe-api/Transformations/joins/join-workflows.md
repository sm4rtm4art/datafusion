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

# Join Workflows

**Compose joins into readable [`DataFrame`] pipelines by controlling input roles, field identity, and the output schema carried into later transformations.**

Relational pipelines often combine data from separate sources before a later transformation consumes the result. In DataFusion, a join workflow is a sequence of lazy [`DataFrame`] transformations whose intermediate schemas make later relationships possible. This page shows how to name and qualify fields, shape joined output, relate two roles of one source, and chain multiple joins. For match coverage, multiplication, Cartesian products, and NULL-key policy, continue with [Join Validation].

**Key Methods**

| Method                     | Purpose                                    | When to use                                                   |
| :------------------------- | :----------------------------------------- | :------------------------------------------------------------ |
| [`.alias()`]               | Assign role qualifiers to an input         | Distinguish fields from inputs that represent different roles |
| [`.with_column_renamed()`] | Give a field a shared domain name          | Make a key name clearer before joining when useful            |
| [`.join()`]                | Add one Extension DataFrame                | Compose the next relationship in a workflow                   |
| [`.select()`]              | Define an explicit stable output allowlist | Set the fields required by downstream transformations         |
| [`.drop_columns()`]        | Exclude a small field list                 | Remove a few qualified fields from a wide joined result       |

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the [`LogicalPlan`]
- **Actions:** (`.collect()`, `.show()`) trigger execution
- **Input roles:** Base DataFrame = method receiver/left input; Extension DataFrame = argument/right input

:::

```{contents} Table of Contents for Join Workflows
:local:
:depth: 2
```

## Compose Joins as a Workflow

**Within a lazy DataFrame pipeline, each join produces another `DataFrame` whose schema and rows feed the next transformation.**

Joins relate two logical inputs, which can come from different sources or represent two roles of the same source. At each call, the Base DataFrame is the method receiver and left input; the Extension DataFrame is the argument and right input. [Join Concepts] provides the binary-composition model, while [Join Conditions] and [Join Types] cover match construction and row preservation.

The joined output must retain unambiguous fields and keys needed by later lazy transformations. Missing or ambiguous references can fail while the plan is constructed.

:::{admonition} Choose the workflow shape
:class: note

**SQL parser path:** A fixed multi-way relationship can be easier to scan when `FROM`, `JOIN`, and `SELECT` appear in one statement.

**DataFrame builder path:** Relationships assembled with Rust control flow, reusable expressions, or named intermediate [`DataFrame`] values can be easier to compose as method chains.

**Both paths** converge on the same [`LogicalPlan`], optimizer, and executor, so choose by workflow clarity rather than execution speed.

See [Builder vs. Parser][builder-parser] for the detailed architecture comparison.
:::

### Prepare, Join, and Shape Two Inputs

**Name the inputs for their domain roles, join the Base DataFrame to its Extension DataFrame, then project the fields needed downstream.**

The example continues the paired-key construction owned by [Join Conditions]. Distinct right-side key names avoid qualifier concerns in this simplest path, while [`.select()`] intentionally defines the downstream schema and demonstrates method composition.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: inventory is the Base DataFrame and sales is the Extension DataFrame.
    let inventory_df = dataframe!(
        "product_id" => [1_i64, 1, 2],
        "region" => ["east", "west", "east"],
        "stock" => [100_i64, 50, 200]
    )?;
    let sales_df = dataframe!(
        "sale_product_id" => [1_i64, 2],
        "sale_region" => ["west", "east"],
        "sold" => [20_i64, 80]
    )?;

    // Build the lazy join, then shape its output for the next workflow step.
    let result = inventory_df.join(
        sales_df,
        JoinType::Inner,
        &["product_id", "region"],
        &["sale_product_id", "sale_region"],
        None,
    )?
        .select(vec![
            col("product_id"),
            col("region"),
            col("stock"),
            col("sold"),
        ])?;

    // Execute and assert the shaped result.
    let batches = result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+------------+--------+-------+------+",
            "| product_id | region | stock | sold |",
            "+------------+--------+-------+------+",
            "| 1          | west   | 50    | 20   |",
            "| 2          | east   | 200   | 80   |",
            "+------------+--------+-------+------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} Preserve the intended method order
:class: caution

A method like [`.select()`] after the join can use fields from both inputs and shape the joined result; moving it before the join applies it to one input and must retain required join keys.
Filter placement has separate ON-like and WHERE-like semantics described in [Join Conditions].
:::

---

## Keep the Joined Schema Unambiguous and Focused

**Qualify or rename overlapping input fields before joining, then project the result to the fields and names required downstream.**

At plan-build time, a [`DataFrame`] join derives a result [`DFSchema`]; its inputs do not need fully aligned schemas. Same or overlapping field names can prevent join-plan construction. Distinct qualifiers can permit construction while later bare references remain ambiguous.

Aliases qualify fields by input role, renames can make a field's domain meaning explicit, and projections retain the fields required downstream. General column-method behavior belongs to [Selecting and Shaping Columns] and [DataFrame Schema Methods].

Join key pairs must resolve to compatible types; [Type Coercion] describes coercion and planning errors.

### Qualify or Rename Input Fields

**Give a shared domain key a clear name, use aliases for role qualifiers, and project the payload fields that the next step needs.**

Rename when you want one domain name; [`.join()`] already accepts differently named key pairs. [`.alias()`] assigns role qualifiers, and the qualified projection chooses the payload fields that persist.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: customer IDs and order customer IDs use different source names.
    let customers_df = dataframe!(
        "id" => [1_i64, 2],
        "name" => ["Ada", "Bina"]
    )?;
    let orders_df = dataframe!(
        "order_id" => [100_i64, 101],
        "customer_id" => [1_i64, 2],
        "amount" => [40_i64, 75]
    )?;

    // Give the Base key a shared domain name; paired keys may also have distinct names.
    let customer_base = customers_df
        .with_column_renamed("id", "customer_id")?
        .alias("customer")?;
    // Role aliases qualify fields for the joined projection.
    let order_extension = orders_df.alias("order")?;

    // Join and select with role-qualified fields.
    let result = customer_base
        .join(
            order_extension,
            JoinType::Inner,
            &["customer_id"],
            &["customer_id"],
            None,
        )?
        .select(vec![
            col("customer.name").alias("customer"),
            col("order.amount"),
        ])?;

    let batches = result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+--------+",
            "| customer | amount |",
            "+----------+--------+",
            "| Ada      | 40     |",
            "| Bina     | 75     |",
            "+----------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

### Select or Drop Result Fields

**Produce a stable downstream schema with `.select()` for an explicit field list or `.drop_columns()` for a small, qualified exclusion list.**

Two-sided joins often retain keys and payload fields from both inputs. Redundant or ambiguous fields, and upstream additions, can otherwise change the schema contract that downstream code consumes.

Use [`.select()`] as an explicit allowlist, or [`.drop_columns()`] for a small qualified exclusion list. An unqualified exclusion can remove every same-named field, while exclusions pass newly added fields through; [Selecting and Shaping Columns] and [DataFrame Schema Methods] own the general behavior.

---

## Relate Two Roles with a Self-Join

**Alias one source by role to attach related-row data—such as each employee’s manager—without ambiguous field references.**

A self-join relates two roles from one source; it is not inherently a filter. This example answers who each employee's manager is and preserves an employee without one: `employee` is the Base role and `manager` is the Extension role. [Join Types] explains the left-join preservation behavior.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: employee IDs and manager references share Int64.
    let employees_df = dataframe!(
        "employee_id" => [1_i64, 2, 3],
        "name" => ["Ada", "Bina", "Cora"],
        "manager_id" => [None::<i64>, Some(1), Some(1)]
    )?;

    let employee_base = employees_df.clone().alias("employee")?;
    let manager_extension = employees_df.alias("manager")?;

    // Join the two roles, then keep a role-specific output schema.
    let result = employee_base
        .join(
            manager_extension,
            JoinType::Left,
            &["manager_id"],
            &["employee_id"],
            None,
        )?
        .select(vec![
            col("employee.name").alias("employee"),
            col("manager.name").alias("manager"),
        ])?;

    let batches = result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+---------+",
            "| employee | manager |",
            "+----------+---------+",
            "| Ada      |         |",
            "| Bina     | Ada     |",
            "| Cora     | Ada     |",
            "+----------+---------+",
        ],
        &batches
    );

    Ok(())
}
```

Use [`.filter()`] when a NULL `manager_id` predicate is sufficient. Use a self-join to attach related-role fields or check whether non-null references resolve. One self-join resolves one level; use SQL [`WITH RECURSIVE`][with-recursive] or explicit iteration for arbitrary depth.

---

## Chain Joins Across Multiple DataFrames

**Chain `.join()` calls to extend a relationship one DataFrame at a time while retaining the keys and fields required by each later leg.**

Chaining keeps each relationship beside the call that adds it. Use a named intermediate [`DataFrame`] instead when one leg needs separate shaping for readability.

At every leg, the current result is the Base DataFrame and the new input is the Extension DataFrame. Keep the fields and keys required by later legs.

Each two-sided join can widen the intermediate schema, so project between legs or at the end while preserving later keys. Keep outer-join grouping explicit because regrouping can change which unmatched rows survive.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: customers have orders, and some orders have payment records.
    let customers_df = dataframe!(
        "id" => [1_i64, 2, 3],
        "name" => ["Ada", "Bina", "Cora"]
    )?;
    let orders_df = dataframe!(
        "order_id" => [101_i64, 102, 103],
        "customer_id" => [1_i64, 1, 2],
        "amount" => [100_i64, 200, 150]
    )?;
    let payments_df = dataframe!(
        "payment_order_id" => [101_i64, 103],
        "status" => ["paid", "pending"]
    )?;

    // The second join's Base DataFrame is the customer-order result.
    let result = customers_df
        .join(
            orders_df,
            JoinType::Inner,
            &["id"],
            &["customer_id"],
            None,
        )?
        .join(
            payments_df,
            JoinType::Inner,
            &["order_id"],
            &["payment_order_id"],
            None,
        )?
        .select(vec![
            col("name").alias("customer"),
            col("order_id"),
            col("amount"),
            col("status"),
        ])?;

    let batches = result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+----------+--------+---------+",
            "| customer | order_id | amount | status  |",
            "+----------+----------+--------+---------+",
            "| Ada      | 101      | 100    | paid    |",
            "| Bina     | 103      | 150    | pending |",
            "+----------+----------+--------+---------+",
        ],
        &batches
    );

    Ok(())
}
```

For coverage, multiplication, Cartesian-product, and NULL-key checks, continue with [Join Validation].

---

## Conclusion

Qualify or rename fields before joining when needed, preserve the keys and fields required by each later leg, and shape the result for its next consumer. Then validate the joined output against the intended relationship.

---

<!-- References -->

<!-- Internal documentation -->

[builder-parser]: ../../Concepts/builder-parser.md#choosing-the-right-api-for-the-task
[dataframe schema methods]: ../../Schema-Management/schema-dataframe-methods.md
[join concepts]: join-concepts.md
[join conditions]: join-conditions.md
[join types]: join-types.md
[join validation]: join-validation.md
[selecting and shaping columns]: ../selection.md
[type coercion]: ../../Schema-Management/type-coercion.md
[with-recursive]: ../../../../user-guide/sql/select.md#with-clause

<!-- Core types -->

[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`dfschema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html

<!-- Methods and functions -->

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.drop_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.with_column_renamed()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed
