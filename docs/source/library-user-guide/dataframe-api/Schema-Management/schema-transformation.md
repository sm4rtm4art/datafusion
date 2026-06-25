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

# Transforming Schemas in DataFusion

**Schema transformation is a layer decision: let [`DataFrame`] operations derive ordinary output schemas, and reach into a [`DFSchema`] or Arrow [`Schema`] only when you must preserve planning context yourself.**

As data flows through a pipeline, its schema rarely holds still: columns are projected, renamed, added, combined, requalified, or converted for Arrow interop. DataFusion derives most of those changes for you as [`DataFrame`] operations extend the [`LogicalPlan`] — but some transformations belong to a more specific API layer.

This page covers those exceptions across three layers: transforming a [`DFSchema`] directly (qualifiers, combining, functional dependencies), evolving schemas at the DataFrame layer with name-based unions across independently evolving inputs, and Arrow interop where planning context must be rebuilt. Each section names the layer that owns the change and shows it in runnable code, because the real risk is silent context drift — nullable columns introduced by schema evolution, dependencies cleared by combination, or qualifiers dropped on the way to Arrow.

**Key operations:**

| Operation                                                     | API Level        | Schema Effect                                             | Section                                                                                                                 |
| ------------------------------------------------------------- | ---------------- | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [`.strip_qualifiers()`]                                       | [`DFSchema`]     | Remove all table qualifiers                               | [Rewriting Qualifiers](#rewriting-qualifiers-to-control-column-resolution)                                              |
| [`.replace_qualifier()`]                                      | [`DFSchema`]     | Replace every qualifier with one value                    | [Rewriting Qualifiers](#rewriting-qualifiers-to-control-column-resolution)                                              |
| [`.with_field_specific_qualified_schema()`]                   | [`DFSchema`]     | Rebuild per-field qualifiers on an existing schema        | [Rewriting Qualifiers](#rewriting-qualifiers-to-control-column-resolution)                                              |
| [`.join()`]                                                   | [`DFSchema`]     | Strictly concatenate two schemas                          | [Combining Schemas](#joining-and-merging-schemas)                                                                       |
| [`.merge()`]                                                  | [`DFSchema`]     | Permissively append non-duplicate fields                  | [Combining Schemas](#joining-and-merging-schemas)                                                                       |
| [`.with_functional_dependencies()`]                           | [`DFSchema`]     | Attach optimizer key relationships                        | [Annotating Functional Dependencies](#annotating-functional-dependencies)                                               |
| [`.union_by_name()`]                                          | [`DataFrame`]    | Combine rows by column name, filling missing columns NULL | [Unioning DataFrames by Column Name](#unioning-dataframes-by-column-name)                                               |
| [`.union_by_name_distinct()`]                                 | [`DataFrame`]    | Name-based union with duplicate-row removal               | [Unioning DataFrames by Column Name](#unioning-dataframes-by-column-name)                                               |
| [`.inner()`] / [`.as_arrow()`] plus [`DFSchema`] constructors | Arrow [`Schema`] | Convert to Arrow and rebuild lost context                 | [Handling Schema Transformation at the Arrow Interop Layer](#handling-schema-transformation-at-the-arrow-interop-layer) |

:::{admonition} Style Note
:class: note
:collapsible: open

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

```{contents} Transforming Schemas
:local:
:depth: 2
```

## Schema Transformation: Where the API Layer Is Key

**Every schema change — reshaping columns, rebuilding qualifiers, combining evolving inputs, or dropping to the Arrow interop layer — has a natural owner in the API.**

A **schema transformation** derives a new contract from an existing one and leaves the original untouched — a column projected, renamed, added, combined, or requalified. DataFusion recomputes that contract inside the [`LogicalPlan`] as the plan grows; for the conceptual model of how a schema propagates as that plan is built, see [Schema Concepts — Schema Propagation Through Transformations](schema-concepts.md#schema-propagation-through-transformations).

DataFusion spreads those changes across distinct layers: most ride along with ordinary [`DataFrame`] operations and let the engine derive the new schema, some touch metadata that only the [`DFSchema`] carries — table qualifiers and functional dependencies — and a few surface only when you hand the schema down to Arrow and bring it back. Reaching for the wrong layer is the usual friction, so treat this page as a routing contract: choose the layer whose abstractions already own the task. The table below maps each change to its layer and where to find it — column edits on their own page, the rest in the sections below.

| Desired Change                                      | Layer            | Find It In                                                                                                                                                                                    |
| --------------------------------------------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Add, rename, project, or unnest columns             | [`DataFrame`]    | [Schema Methods](schema-methods.md) — separate page                                                                                                                                           |
| Combine independently evolving DataFrames by name   | [`DataFrame`]    | [Unioning DataFrames by Column Name](#unioning-dataframes-by-column-name) — below                                                                                                             |
| Requalify, combine, or annotate the schema directly | [`DFSchema`]     | [Rewriting Qualifiers](#rewriting-qualifiers-to-control-column-resolution), [Combining Schemas](#joining-and-merging-schemas), [Functional Dependencies](#annotating-functional-dependencies) |
| Hand a schema to Arrow and bring it back            | Arrow [`Schema`] | [Handling Schema Transformation at the Arrow Interop Layer](#handling-schema-transformation-at-the-arrow-interop-layer) — below                                                               |

:::{admonition} Construction is a separate step
:class: seealso
Constructors such as [`DFSchema::try_from_qualified_schema()`] and [`DFSchema::from_field_specific_qualified_schema()`] build a [`DFSchema`] from an Arrow [`Schema`] — the starting point a transformation works on, not a transformation itself. They appear in the examples below only to set up a schema worth transforming; reach for them whenever you need a schema from scratch, and find the full reference in [Creating Schemas — Defining a `DFSchema` Directly](schema-creation.md#defining-a-dfschema-directly). Note the near-namesakes: the constructor [`DFSchema::from_field_specific_qualified_schema()`] _builds_ a schema from Arrow, while the transform [`.with_field_specific_qualified_schema()`] in the next section _rewrites_ qualifiers on an existing one.
:::

---

## Transforming a DFSchema Directly

**Transform a [`DFSchema`] as an in-memory object — requalify, combine, or annotate it — without scanning or rewriting a single row.**

This layer owns schema-object work: rewriting qualifiers, combining schema objects, and attaching optimizer metadata. These methods operate on a [`DFSchema`] value in memory and are common in custom logical plan code, schema-dependent tests, and advanced tooling that manipulates planning contracts directly.

### Rewriting Qualifiers to Control Column Resolution

**Qualifiers are the only thing standing between two identically named columns — rewriting them decides which references resolve and which collide.**

A self-join is the clearest case: join one table to itself and both sides contribute every column, so two `order_id` fields land in the same schema and only their qualifiers (`left.order_id` versus `right.order_id`) keep them apart. The same overlap arises from any join across tables that share column names, or from hand-built plan nodes that concatenate two schemas. Strip or overwrite those qualifiers carelessly and the columns collapse into one ambiguous name; rebuild them deliberately and each stays resolvable. The three transformation methods trade off granularity and ownership:

| Method                                      | Receiver        | Qualifier Result                    | Main Risk                                          |
| ------------------------------------------- | --------------- | ----------------------------------- | -------------------------------------------------- |
| [`.strip_qualifiers()`]                     | consumes `self` | all qualifiers become `None`        | Can create duplicate unqualified names             |
| [`.replace_qualifier()`]                    | consumes `self` | every field gets the same qualifier | Can create duplicate qualified names               |
| [`.with_field_specific_qualified_schema()`] | borrows `&self` | one qualifier per field             | Errors if qualifier count differs from field count |

Pick the method by the qualifier result you want — the table's middle column. The **Receiver** column then settles one Rust mechanic: whether you must clone before transforming.

- **Consuming (`self`):** [`.strip_qualifiers()`] and [`.replace_qualifier()`] take an owned [`DFSchema`] and rewrite every qualifier at once. Because [`DataFrame::schema()`][`.schema()`] hands back a `&DFSchema`, clone before calling them: `df.schema().clone().strip_qualifiers()`.
- **Borrowing (`&self`):** [`.with_field_specific_qualified_schema()`] reads a borrowed schema and returns a new one, so it runs directly on `df.schema()` — no clone.

The example below stages that self-join, strips the qualifiers to expose the collision, then requalifies per field to resolve it:

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, TableReference};

fn main() -> datafusion::error::Result<()> {
    // A self-join surfaces the same column from both sides of one table.
    let order = Schema::new(vec![Field::new("order_id", DataType::Int64, false)]);
    let left = DFSchema::try_from_qualified_schema("left", &order)?;
    let right = DFSchema::try_from_qualified_schema("right", &order)?;

    // Qualifiers are the only thing keeping the two order_id columns apart.
    let joined = left.join(&right)?;
    assert_eq!(joined.field_names(), vec!["left.order_id", "right.order_id"]);

    // Strip them and the columns collapse into one ambiguous "order_id".
    assert!(joined.clone().strip_qualifiers().check_names().is_err());

    // Requalify per field to rename each side while keeping them distinct.
    let requalified = joined.with_field_specific_qualified_schema(vec![
        Some(TableReference::bare("orders")),
        Some(TableReference::bare("related_orders")),
    ])?;
    assert_eq!(
        requalified.field_names(),
        vec!["orders.order_id", "related_orders.order_id"]
    );

    Ok(())
}
```

:::{admonition} Validate after broad requalification
:class: warning
[`DFSchema::check_names()`] catches duplicate qualified fields, duplicate unqualified fields, and ambiguous references between qualified and unqualified fields. Call it after stripping or replacing qualifiers if the transformed schema will be used for column resolution. [`DFSchema::strip_qualifiers()`] and [`DFSchema::replace_qualifier()`] do not call [`DFSchema::check_names()`] for you. Neither does [`DFSchema::with_field_specific_qualified_schema()`][`.with_field_specific_qualified_schema()`]; assigning one qualifier per field lowers the collision risk but does not remove it.
:::

:::{seealso}
The [`DFSchema`] methods above reshape a schema object directly. At the DataFrame layer, [`DataFrame::alias()`] is their plan-level counterpart: [`.alias()`] replaces every output column's qualifier with one alias, much like [`.replace_qualifier()`] does on a schema object. Reach for [`.alias()`] when renaming relation context on a [`DataFrame`]; reach for the [`DFSchema`] methods when transforming a schema object directly.
:::

### Joining and Merging Schemas

**Combining two schema objects is a planning operation, not a row operation — [`DFSchema::join()`] forbids name collisions while [`DFSchema::merge()`] absorbs them.**

Plan builders and custom plan nodes sometimes need to derive one output schema from two input schemas — a join node, for instance, must publish the columns of both children. The open question is what to do with duplicate field names: treat them as a bug, or as structure that already exists? [`DFSchema`] supports both answers, and neither touches data — these methods never scan rows, fill missing values, or run type coercion.

:::{admonition} `DFSchema::join()` is not `DataFrame::join()`
:class: caution
[`DFSchema::join()`] and [`DataFrame::join()`] share a name but act on different layers. [`DFSchema::join()`] — with [`DFSchema::merge()`] — combines schema _objects_, field lists and qualifiers, and never reads a row; [`DataFrame::join()`] combines _rows_, matching records across two DataFrames. This section covers the schema-object methods; for row joins see [Join Patterns](../Transformations/joins.md).
:::

| Method       | Combination                                 | Duplicate Names                                          | Result                  |
| ------------ | ------------------------------------------- | -------------------------------------------------------- | ----------------------- |
| [`.join()`]  | Concatenates every field from both inputs   | Rejected — returns `Err` via [`DFSchema::check_names()`] | New [`DFSchema`]        |
| [`.merge()`] | Appends only the fields not already present | Silently skipped (`self` wins)                           | Mutates `self` in place |

Both methods keep each field's existing qualifier and let the other schema's metadata overwrite matching keys. They diverge on optimizer state, which the two cases below make concrete.

#### DFSchema::join(): Strict Concatenation

[`.join()`] returns a new schema containing every field from both inputs and rejects overlap: duplicate or ambiguous names would make later column resolution unsafe, so it calls [`DFSchema::check_names()`] and resets functional dependencies to empty. Reach for it when overlapping names signal a mistake — a derived schema that should have stayed disjoint.

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;

fn main() -> datafusion::error::Result<()> {
    let orders = DFSchema::try_from(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))?;
    let status = DFSchema::try_from(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        true,
    )]))?;

    // .join() concatenates every field from both inputs.
    let combined = orders.join(&status)?;
    assert_eq!(combined.field_names(), vec!["order_id", "amount", "status"]);

    // Re-joining overlapping schemas is rejected: duplicate names are treated as a bug.
    assert!(combined.join(&combined).is_err());

    Ok(())
}
```

#### DFSchema::merge(): Permissive Append

[`.merge()`] mutates `self` in place, appending only the fields not already present and keeping `self`'s version on conflict; the functional dependencies already on `self` are left untouched. Reach for it when overlap is expected and the goal is to accumulate fields from several sources into one schema. The example keeps `self`'s duplicate `amount` and appends only the new `status`:

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;

fn main() -> datafusion::error::Result<()> {
    let mut accumulated = DFSchema::try_from(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))?;
    let incoming = DFSchema::try_from(Schema::new(vec![
        Field::new("amount", DataType::Int64, true), // duplicate: skipped
        Field::new("status", DataType::Utf8, true),  // new: appended
    ]))?;

    // .merge() mutates `accumulated` in place instead of returning a new schema.
    accumulated.merge(&incoming);
    assert_eq!(
        accumulated.field_names(),
        vec!["order_id", "amount", "status"]
    );

    Ok(())
}
```

:::{admonition} `.merge()` is not `.union_by_name()`
:class: caution
[`DFSchema::merge()`] only accumulates field definitions on one schema object. It does not combine rows, reorder inputs, fill missing columns, or run [`TypeCoercion`]. The metadata rules differ too: [`.merge()`] lets the other schema win on conflicting keys, while [`.union_by_name()`] keeps only the keys that are identical across every branch. Use [`.union_by_name()`] when two DataFrames should produce one row stream with a schema derived by column name.
:::

For row-producing alignment across independently evolving inputs, continue with [Unioning DataFrames by Column Name](#unioning-dataframes-by-column-name).

### Annotating Functional Dependencies

**Functional dependencies tell the optimizer which columns a key determines — get them right and DataFusion skips redundant work; get them wrong and it can quietly skew results.**

A functional dependency records that one column determines others: `order_id` fixes the `customer_id` and `amount` on its row. The optimizer spends that guarantee two ways — a `GROUP BY` on the key can keep dependent columns without aggregating them, and a `DISTINCT` a unique key already enforces is dropped.

You rarely author these by hand — DataFusion derives them from primary-key and unique constraints on a [`TableProvider`] (see the tip below). Use [`.with_functional_dependencies()`] only when you build a [`DFSchema`] yourself; it attaches a [`FunctionalDependencies`] value and validates only that the indices fit the schema width. The example attaches one, reads it back the way the planner does, then sees the bounds check reject an out-of-range index:

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{
    get_target_functional_dependencies, DFSchema, Dependency, FunctionalDependence,
    FunctionalDependencies,
};

fn main() -> datafusion::error::Result<()> {
    let schema = DFSchema::try_from(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))?;

    // order_id is unique, so it determines customer_id and amount.
    let dependencies = FunctionalDependencies::new(vec![
        FunctionalDependence::new(vec![0], vec![1, 2], false)
            .with_mode(Dependency::Single),
    ]);
    let schema = schema.with_functional_dependencies(dependencies)?;

    // The payoff: grouping by the determinant frees its dependent columns,
    // so the planner reports them as usable without aggregation.
    let usable =
        get_target_functional_dependencies(&schema, &["order_id".to_string()]);
    assert_eq!(usable, Some(vec![1, 2]));

    // Validation guards the indices, not the claim: index 3 is outside a
    // three-field schema, so it is rejected.
    let invalid = FunctionalDependencies::new(vec![FunctionalDependence::new(
        vec![3],
        vec![0],
        false,
    )]);
    assert!(schema.with_functional_dependencies(invalid).is_err());

    Ok(())
}
```

:::{admonition} A false dependency corrupts results
:class: warning
Validation checks only that indices fit the schema width — never that the relationship is true. Declare that a non-unique column determines others and the optimizer will trust the claim: it may drop a `DISTINCT` that was doing real work or surface rows a `GROUP BY` never collapsed. Attach a dependency only when your code can prove it holds for every row.
:::

Several transformations clear functional dependencies on purpose. [`DFSchema::join()`] empties them immediately, and the name-based unions in [Unioning DataFrames by Column Name](#unioning-dataframes-by-column-name) also produce empty dependencies, because the original key relationships may no longer hold once inputs are combined. Reattach them only after reasoning about the transformed schema, never as a mechanical copy.

:::{admonition} Prefer source constraints when possible
:class: tip
For normal table scans, declare primary-key and unique constraints on the [`TableProvider`] instead of hand-authoring functional dependencies; DataFusion converts those constraints into [`FunctionalDependencies`] during plan construction. Keep [`.with_functional_dependencies()`] for custom logical plans, schema-dependent tests, or advanced optimizer work.
:::

---

## Evolving Schemas at the DataFrame Layer

**At the DataFrame layer, the [`DFSchema`] is reshaped in the background — schema changes ride along with the row-producing operations you call, and the engine derives the new contract.**

Transforming data at the DataFrame layer still moves the [`DFSchema`]. [`.union_by_name()`] is the clearest case: it reconciles two inputs by column name, and that reconciliation reshapes the output schema — adding columns, relaxing nullability, and dropping functional dependencies. Those changes surface as NULLs, which the second half of this section addresses. Single-DataFrame column edits such as [`.select()`] and [`.with_column()`] stay in [Schema Methods](schema-methods.md); here the inputs are plural.

### Unioning DataFrames by Column Name

**[`.union_by_name()`] combines two DataFrames by matching columns on name rather than position — the resulting [`DFSchema`] changes are derived in the background.**

[`.union()`] stacks two DataFrames vertically by matching columns on position; [`.union_by_name()`] does the same but matches on column name. Reconciling differing column sets reshapes the output [`DFSchema`], so the variants below differ mainly in how they align columns and whether they drop duplicate rows:

| Method                        | Aligns columns by | Differing width | Missing column                  | Duplicate rows |
| ----------------------------- | ----------------- | --------------- | ------------------------------- | -------------- |
| [`.union()`]                  | position          | rejected        | not allowed                     | kept           |
| [`.union_distinct()`]         | position          | rejected        | not allowed                     | removed        |
| [`.union_by_name()`]          | name              | allowed         | filled with NULL, made nullable | kept           |
| [`.union_by_name_distinct()`] | name              | allowed         | filled with NULL, made nullable | removed        |

For row-level set-operation semantics — `UNION` versus `UNION ALL`, deduplication — see [Set Operations by Name](../Transformations/set-operations.md). For the horizontal counterpart — combining columns instead of stacking rows — see [Join Patterns](../Transformations/joins.md).

:::{admonition} SQL equivalent: `UNION BY NAME`
:class: note
DataFusion SQL exposes the same operation as `UNION BY NAME`. Reach for the DataFrame method when the union sits inside Rust pipeline logic, and for SQL when the surrounding query is already written as text.
:::

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let historical_orders = dataframe!(
        "order_id" => [1_i64, 2],
        "amount" => [100_i64, 150]
    )?;

    let current_orders = dataframe!(
        "status" => ["paid", "open"],
        "amount" => [200_i64, 250],
        "order_id" => [3_i64, 4]
    )?;

    let unified_orders = historical_orders.union_by_name(current_orders)?;

    // Schema effect: columns align by name, first-seen order wins, and the
    // column missing from one input is appended.
    assert_eq!(
        unified_orders.schema().field_names(),
        vec!["order_id", "amount", "status"]
    );

    // Data effect: rows missing `status` are filled with NULL.
    let batches = unified_orders.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+--------+--------+",
            "| order_id | amount | status |",
            "+----------+--------+--------+",
            "| 1        | 100    |        |",
            "| 2        | 150    |        |",
            "| 3        | 200    | paid   |",
            "| 4        | 250    | open   |",
            "+----------+--------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} Name-based union reshapes the schema silently
:class: caution
[`.union_by_name()`] derives the combined schema from rules you do not control. A column missing from one input is filled with NULL and marked nullable, so incomplete data can look complete and a column you relied on as NOT NULL may start carrying NULLs. Columns whose types differ are reconciled by the [`TypeCoercion`] analyzer and may be widened. Functional dependencies are dropped, exactly as with [`DFSchema::join()`]. Inspect the combined schema before downstream logic depends on it.

That relaxation is easy to miss — here an `order_id` that is NOT NULL in one input turns nullable because the other omits it:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let recent_orders = dataframe!("order_id" => [1_i64, 2])?;
    let legacy_notes = dataframe!("note" => ["archived", "migrated"])?;

    let combined = recent_orders.union_by_name(legacy_notes)?;

    // order_id was NOT NULL in recent_orders, but legacy_notes omits it,
    // so the union relaxes it to nullable.
    assert!(combined
        .schema()
        .field_with_unqualified_name("order_id")?
        .is_nullable());

    Ok(())
}
```

:::

### Managing NULLs Introduced by Schema Evolution

**When a name-based union can't find a column in one input, it adds that column as nullable — that schema change is this section's concern, while deciding what the NULL means is domain logic.**

When [`.union_by_name()`] meets a column absent from one input, it projects a NULL literal and marks the column nullable — a schema effect, not a data decision. Whether a missing `status` means "unknown", "not yet collected", or "not applicable" is knowledge the engine cannot supply, so pick a strategy deliberately:

| Strategy            | Use When                                        | Example                              |
| ------------------- | ----------------------------------------------- | ------------------------------------ |
| Fill with a default | A domain default is honest and useful           | Missing status becomes `"pending"`   |
| Fill with logic     | A value can be derived from other fields        | Missing display name from first/last |
| Drop rows           | Missing value invalidates the record            | Missing primary key                  |
| Preserve NULL       | Unknown is meaningful and should remain visible | Missing survey response              |

The default-fill case is the most common — `coalesce()` replaces the NULL with a domain default in one step:

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::functions::expr_fn::coalesce;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let historical_orders = dataframe!("order_id" => [1_i64, 2])?;
    let current_orders = dataframe!(
        "order_id" => [3_i64, 4],
        "status" => ["paid", "open"]
    )?;

    // union_by_name leaves status NULL for the historical rows; coalesce
    // fills those gaps with a domain default.
    let cleaned = historical_orders
        .union_by_name(current_orders)?
        .with_column("status", coalesce(vec![col("status"), lit("pending")]))?;

    let batches = cleaned.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+---------+",
            "| order_id | status  |",
            "+----------+---------+",
            "| 1        | pending |",
            "| 2        | pending |",
            "| 3        | paid    |",
            "| 4        | open    |",
            "+----------+---------+",
        ],
        &batches
    );

    Ok(())
}
```

For expression-level NULL handling (`coalesce`, `CASE`), see [Handling Null Values](../Concepts/null-handling.md). For nullability flags and widening, see [Anatomy of a Schema — Nullability](schema-anatomy.md#nullability).

---

## Handling Schema Transformation at the Arrow Interop Layer

**The Arrow [`Schema`] carries names, types, nullability, and metadata — but not qualifiers or functional dependencies, so dropping to the Arrow layer loses planning context you must rebuild before returning to a [`DFSchema`].**

DataFusion is built on Arrow: every [`DFSchema`] wraps an Arrow [`Schema`] and adds the planning context — table qualifiers and functional dependencies — that the optimizer needs but Arrow does not model. Some work happens below that wrapper, at the bare Arrow layer: feeding Arrow compute kernels, serializing through IPC or Flight, or handing the schema to another Arrow-based library. You reach that layer with [`.inner()`] or [`.as_arrow()`], and when you rebuild a fresh [`DFSchema`] from the result, the added context is gone — columns come back unqualified and optimizer metadata is empty. Rebuild both from application context before the schema re-enters a plan.

The same [`.inner()`] / [`.as_arrow()`] accessors are introduced for inspection in [Inspecting and Validating Schemas — Arrow Interop](schema-inspection.md#arrow-interop); this section covers what the conversion drops and how to restore it. The example below walks the three steps — export the Arrow schema, rebuild a [`DFSchema`] that has lost its qualifiers, then requalify per field to restore resolution:

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, TableReference};

fn main() -> datafusion::error::Result<()> {
    let arrow_schema = Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]);

    let qualified =
        DFSchema::try_from_qualified_schema("orders", &arrow_schema)?;

    // Step 1 — export to Arrow: qualifier and dependency context is dropped.
    let exported = qualified.as_arrow().clone();

    // Step 2 — rebuild a DFSchema from Arrow: columns come back unqualified.
    let unqualified = DFSchema::try_from(exported)?;
    assert!(unqualified.iter().all(|(qualifier, _)| qualifier.is_none()));

    // Step 3 — rebuild qualifiers from application context (functional
    // dependencies, if any, need .with_functional_dependencies() here too).
    let rebuilt = unqualified.with_field_specific_qualified_schema(vec![
        Some(TableReference::bare("orders")),
        Some(TableReference::bare("orders")),
    ])?;

    assert_eq!(
        rebuilt.field_names(),
        vec!["orders.order_id", "orders.amount"]
    );

    Ok(())
}
```

Use this pattern when an Arrow-only API sits between two DataFusion planning steps. The same rebuild applies to optimizer metadata: [`FunctionalDependencies`] cannot be recovered from the Arrow [`Schema`], so reattach them with [`.with_functional_dependencies()`] when they still hold for the rebuilt schema.

:::{seealso}
To build a [`DFSchema`] from Arrow from scratch rather than recovering one, see [Creating Schemas — Defining a `DFSchema` Directly](schema-creation.md#defining-a-dfschema-directly). To reattach optimizer metadata after the conversion, see [Annotating Functional Dependencies](#annotating-functional-dependencies).
:::

---

## Conclusion & Further Reading

**You now have a layer for every schema change: reshape rows with DataFrame methods, rewrite planning context on the [`DFSchema`], and treat Arrow as a physical-only boundary whose context you rebuild on return.**

On the [`DFSchema`] layer, requalification controls how DataFusion resolves columns and [`.join()`] / [`.merge()`] build a planning contract without touching rows. At the DataFrame layer, [`.union_by_name()`] evolves independently growing inputs by aligning columns on name and filling the gaps with NULL. Across both, qualifiers and functional dependencies are DataFusion-only metadata that no Arrow conversion preserves — carry them deliberately and rebuild them after the Arrow layer drops them. For the everyday DataFrame methods that add, remove, rename, or reshape columns on a single frame, continue with [Schema Methods](schema-methods.md).

:::{admonition} Related documents
:class: seealso

- [Schema Concepts](schema-concepts.md) — conceptual propagation model and schema lifecycle
- [Creating Schemas](schema-creation.md) — Arrow and [`DFSchema`] construction from scratch
- [Inspecting and Validating Schemas](schema-inspection.md) — display, access, validation, and basic Arrow interop
- [Anatomy of a Schema](schema-anatomy.md) — field-level properties, qualifiers, nullability, and metadata
- [Schema Methods](schema-methods.md) — DataFrame methods that add, remove, rename, or reshape columns
  :::

---

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DataFrame::alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`DataFrame::join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`DataFrame::schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`FunctionalDependencies`]: https://docs.rs/datafusion/latest/datafusion/common/struct.FunctionalDependencies.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`Schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`SchemaRef`]: https://docs.rs/arrow/latest/arrow/datatypes/type.SchemaRef.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`TypeCoercion`]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/type_coercion/struct.TypeCoercion.html
[`DFSchema::check_names()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.check_names
[`DFSchema::from_field_specific_qualified_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.from_field_specific_qualified_schema
[`DFSchema::join()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.join
[`DFSchema::merge()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.merge
[`DFSchema::replace_qualifier()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.replace_qualifier
[`DFSchema::strip_qualifiers()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.strip_qualifiers
[`DFSchema::try_from_qualified_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.try_from_qualified_schema
[`.as_arrow()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.as_arrow
[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.inner()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.join
[`.merge()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.merge
[`.replace_qualifier()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.replace_qualifier
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.strip_qualifiers()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.strip_qualifiers
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.union_by_name_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name_distinct
[`.union_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_distinct
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.with_field_specific_qualified_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.with_field_specific_qualified_schema
[`.with_functional_dependencies()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.with_functional_dependencies
[Creating Schemas]: schema-creation.md
