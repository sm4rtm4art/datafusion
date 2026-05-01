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


<--TODO 
1. ABSTRACT
2. Fix cross-references to schema-anatomy.md and schema-creation.md (anchors moved across files)
3. Add cross-ref to schema-concepts.md "Schema Propagation Through Transformations" for the conceptual overview
4. Add .union_by_name() operational content (moved from schema-anatomy.md): NULL filling for missing columns, type mismatch handling with TypeCoercion analyzer, positional .union() vs name-based .union_by_name() comparison
5. Add .with_functional_dependencies() method documentation to the DFSchema Transform Methods table
6. Add deeper Arrow interop section (DFSchema ↔ Arrow Schema conversion patterns,
   qualifier/functional-dependency loss, round-trip considerations). The basic overview
   lives in schema-inspection.md "Arrow Interop" — this file should cover
   transformation-specific patterns (e.g., rebuilding qualifiers after Arrow round-trip).
7. Cross-reference the DFSchema *construction* constructors (try_from_qualified_schema,
   from_field_specific_qualified_schema) that live in schema-creation.md
   "When You Need a DFSchema Directly". This file should cover transformation of an
   existing DFSchema, not construction from scratch — link rather than duplicate.
--> 


# Transforming Schemas

**Schema transformation adapts an existing [`DFSchema`] or DataFrame schema contract without pretending the original schema was mutable.**

[TODO: Abstract is written last]

**Key operations:**

| Operation                                                     | API Level     | Schema Effect                                             | Section                                                                   |
| ------------------------------------------------------------- | ------------- | --------------------------------------------------------- | ------------------------------------------------------------------------- |
| [`.strip_qualifiers()`]                                       | [`DFSchema`]  | Remove all table qualifiers                               | [Requalifying Existing Schemas](#requalifying-existing-schemas)           |
| [`.replace_qualifier()`]                                      | [`DFSchema`]  | Replace every qualifier with one value                    | [Requalifying Existing Schemas](#requalifying-existing-schemas)           |
| [`.with_field_specific_qualified_schema()`]                   | [`DFSchema`]  | Rebuild per-field qualifiers on an existing schema        | [Requalifying Existing Schemas](#requalifying-existing-schemas)           |
| [`.join()`]                                                   | [`DFSchema`]  | Strictly concatenate two schemas                          | [Combining `DFSchema` Values](#combining-dfschema-values)                 |
| [`.merge()`]                                                  | [`DFSchema`]  | Permissively append non-duplicate fields                  | [Combining `DFSchema` Values](#combining-dfschema-values)                 |
| [`.union_by_name()`]                                          | [`DataFrame`] | Combine rows by column name, filling missing columns NULL | [Combining DataFrames by Name](#combining-dataframes-by-name)             |
| [`.with_functional_dependencies()`]                           | [`DFSchema`]  | Attach optimizer key relationships                        | [Preserving Planning Metadata](#preserving-planning-metadata)             |
| [`.inner()`] / [`.as_arrow()`] plus [`DFSchema`] constructors | Interop bridge | Round-trip through Arrow while rebuilding lost context    | [Arrow Round Trips and Lost Context](#arrow-round-trips-and-lost-context) |


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


```{contents} Transforming Schemas
:local:
:depth: 2
```

## Where Schema Transformations Fit

**Schema transformation sits between inspection and ordinary DataFrame methods: inspect the current contract, then adapt the contract at the layer that actually owns the change.**

Every [`DataFrame`] exposes a [`DFSchema`] through [`.schema()`], but the [`DataFrame`] does not own a mutable schema object. The schema belongs to the current [`LogicalPlan`] node. When a lazy DataFrame method runs, DataFusion builds a new plan node and derives a new output schema from the input schema. For the conceptual overview of that propagation model, see [Schema Concepts — Schema Propagation Through Transformations](schema-concepts.md#schema-propagation-through-transformations).

The practical rule is simple: use DataFrame methods when you are transforming data, and use [`DFSchema`] transformation methods when you are implementing or testing planning behavior. The table below keeps the two layers separate:

| Need                                                                  | Use                                      | Why                                                              |
| --------------------------------------------------------------------- | ---------------------------------------- | ---------------------------------------------------------------- |
| Add, rename, project, or unnest columns in an application pipeline    | DataFrame methods                        | The new [`LogicalPlan`] derives the new schema automatically     |
| Combine rows from independently evolving DataFrames                   | [`.union_by_name()`]                     | Missing columns are filled with NULL and aligned by column name  |
| Rebuild qualifiers or functional dependencies in planning code        | [`DFSchema`] methods                     | These fields live only in the DataFusion query-planning layer    |
| Construct a [`DFSchema`] from scratch                                 | Constructors in [Creating Schemas]       | Construction is separate from transforming existing state        |
| Hand schema data to Arrow libraries, then bring it back to DataFusion | Arrow interop plus explicit reconstruction | Arrow schemas do not carry qualifiers or functional dependencies |

:::{admonition} Constructors live in Creating Schemas
:class: seealso
Use [`DFSchema::try_from_qualified_schema()`] and [`DFSchema::from_field_specific_qualified_schema()`] when you are building a [`DFSchema`] from an Arrow [`Schema`] or [`SchemaRef`]. This page uses those constructors in examples, but the constructor reference belongs in [Creating Schemas — Defining a `DFSchema` Directly](schema-creation.md#defining-a-dfschema-directly).
:::

---

## Requalifying Existing Schemas

**Qualifier transformations change how columns are resolved, not what values the columns contain.**

Table qualifiers distinguish same-named fields from different relations: `orders.order_id` and `payments.order_id` can coexist because their qualifiers differ. Requalification is useful in custom logical plan code, subquery aliasing, and tests that need to simulate relation context. The three transformation methods differ mainly in ownership and granularity:

| Method                                      | Receiver        | Qualifier Result                   | Main Risk                                          |
| ------------------------------------------- | --------------- | ---------------------------------- | -------------------------------------------------- |
| [`.strip_qualifiers()`]                     | consumes `self` | all qualifiers become `None`       | Can create duplicate unqualified names             |
| [`.replace_qualifier()`]                    | consumes `self` | every field gets the same qualifier | Can create duplicate qualified names               |
| [`.with_field_specific_qualified_schema()`] | borrows `&self` | one qualifier per field            | Errors if qualifier count differs from field count |

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, TableReference};

fn main() -> datafusion::error::Result<()> {
    let orders_arrow = Schema::new(vec![Field::new("order_id", DataType::Int64, false)]);
    let payments_arrow = Schema::new(vec![Field::new("order_id", DataType::Int64, false)]);

    let orders_schema =
        DFSchema::try_from_qualified_schema("orders", &orders_arrow)?;
    let payments_schema =
        DFSchema::try_from_qualified_schema("payments", &payments_arrow)?;

    let joined_schema = orders_schema.join(&payments_schema)?;
    assert_eq!(
        joined_schema.field_names(),
        vec!["orders.order_id", "payments.order_id"]
    );

    // Stripping qualifiers keeps both field names but removes the disambiguation.
    let stripped = joined_schema.clone().strip_qualifiers();
    assert!(stripped.check_names().is_err());

    // Per-field requalification preserves disambiguation.
    let requalified = joined_schema.with_field_specific_qualified_schema(vec![
        Some(TableReference::bare("left_orders")),
        Some(TableReference::bare("right_payments")),
    ])?;

    assert_eq!(
        requalified.field_names(),
        vec!["left_orders.order_id", "right_payments.order_id"]
    );

    Ok(())
}
```

:::{admonition} Validate after broad requalification
:class: warning
[`DFSchema::check_names()`] catches duplicate qualified fields, duplicate unqualified fields, and ambiguous references between qualified and unqualified fields. Call it after stripping or replacing qualifiers if the transformed schema will be used for column resolution. [`DFSchema::strip_qualifiers()`] and [`DFSchema::replace_qualifier()`] do not call [`DFSchema::check_names()`] for you.
:::

Two ownership details matter in ordinary Rust code. [`DataFrame::schema()`][`.schema()`] returns `&DFSchema`, so consuming methods require a clone: `df.schema().clone().strip_qualifiers()`. [`DFSchema::with_field_specific_qualified_schema()`] borrows `&self`, so it can be called directly on a borrowed schema when you only need to rebuild qualifier metadata.

---

## Combining `DFSchema` Values

**Use strict combination when duplicate names are bugs; use permissive merging only when duplicate fields should be treated as already-known structure.**

[`DFSchema`] combination methods operate on schema objects, not rows. They are used by logical plan builders, custom plan nodes, and tests that need to construct an output schema from input schemas. They do not scan data, do not fill missing values, and do not perform type coercion.

| Method       | Behavior                                      | Duplicate Handling                                  | Metadata Handling                                  | Functional Dependencies |
| ------------ | --------------------------------------------- | --------------------------------------------------- | -------------------------------------------------- | ----------------------- |
| [`.join()`]  | Returns a new schema with fields from both inputs | Calls [`DFSchema::check_names()`] and returns `Err` | `other` schema metadata overwrites matching keys   | Reset to empty          |
| [`.merge()`] | Mutates `self`, appending fields from `other` | Silently skips duplicates                           | `other` schema metadata overwrites matching keys   | Existing value on `self` remains; `other` is not merged |

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;

fn main() -> datafusion::error::Result<()> {
    let orders_schema = DFSchema::try_from(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))?;

    let status_schema = DFSchema::try_from(Schema::new(vec![
        Field::new("status", DataType::Utf8, true),
    ]))?;

    let combined = orders_schema.join(&status_schema)?;
    assert_eq!(
        combined.field_names(),
        vec!["order_id", "amount", "status"]
    );

    let duplicate_result = combined.join(&combined);
    assert!(duplicate_result.is_err());

    Ok(())
}
```

[`DFSchema::join()`] is strict because it models a schema derivation where duplicate or ambiguous field names would make later column resolution unsafe. [`DFSchema::merge()`] is deliberately more permissive:

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;

fn main() -> datafusion::error::Result<()> {
    let mut accumulated_schema = DFSchema::try_from(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))?;

    let next_schema = DFSchema::try_from(Schema::new(vec![
        Field::new("amount", DataType::Int64, true), // duplicate: skipped
        Field::new("status", DataType::Utf8, true),  // new: appended
    ]))?;

    accumulated_schema.merge(&next_schema);

    assert_eq!(
        accumulated_schema.field_names(),
        vec!["order_id", "amount", "status"]
    );

    Ok(())
}
```

:::{admonition} `.merge()` is not `.union_by_name()`
:class: caution
[`DFSchema::merge()`] only accumulates field definitions on one schema object. It does not combine rows, reorder inputs, fill missing columns, or run [`TypeCoercion`]. Use [`.union_by_name()`] when two DataFrames should produce one row stream with a schema derived by column name.
:::

---

## Combining DataFrames by Name

**Use [`.union_by_name()`] when input schemas evolve independently and column identity matters more than column position.**

Positional [`.union()`] requires the same number of columns and aligns them by index. That is safe only when every input schema has the same column order and width. Name-based [`.union_by_name()`] derives a schema from column names, projects each input into that schema order, and inserts NULL literals for columns missing from an input.

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

The output schema follows these rules:

| Property                  | Rule                                                                 |
| ------------------------- | -------------------------------------------------------------------- |
| Column order              | First appearance across inputs wins; missing columns are appended when first seen |
| Missing columns           | Input is wrapped in a projection that emits NULL under the missing name |
| Nullability               | A column becomes nullable if it is nullable in any input or missing from any input |
| Type mismatch handling    | The union schema starts from input types and the [`TypeCoercion`] analyzer inserts safe widening casts where possible |
| Field and schema metadata | Only metadata keys present with identical values in all union branches are preserved |
| Functional dependencies   | Not preserved after the union operation                              |

:::{admonition} SQL equivalent: `UNION BY NAME`
:class: note
DataFusion SQL supports `UNION BY NAME` syntax. The DataFrame API is more convenient when the union is embedded in Rust pipeline logic; SQL is often more compact for ad-hoc queries or when the whole transformation is already expressed as SQL text.
:::

---

## Managing NULLs Introduced by Schema Evolution

**NULLs introduced by name-based schema evolution are structural placeholders; fill, filter, or preserve them according to domain meaning.**

When [`.union_by_name()`] sees a column that is absent from one input, DataFusion projects a NULL literal for the missing column. That behavior keeps the row stream valid, but it does not decide what the missing value means. A missing status in historical data might mean "unknown", "not collected yet", or "not applicable"; the schema can only mark the column nullable.

```rust
use datafusion::assert_batches_eq;
use datafusion::functions::expr_fn::coalesce;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders = dataframe!(
        "email" => [Some("a@example.com"), None, Some("c@example.com")],
        "status" => [Some("active"), None, Some("inactive")]
    )?;

    let cleaned = orders
        .with_column("status", coalesce(vec![col("status"), lit("pending")]))?
        .with_column(
            "email",
            when(col("email").is_null(), lit("unknown@example.com"))
                .otherwise(col("email"))?,
        )?;

    let batches = cleaned.collect().await?;
    assert_batches_eq!(
        &[
            "+---------------------+----------+",
            "| email               | status   |",
            "+---------------------+----------+",
            "| a@example.com       | active   |",
            "| unknown@example.com | pending  |",
            "| c@example.com       | inactive |",
            "+---------------------+----------+",
        ],
        &batches
    );

    Ok(())
}
```

| Strategy            | Use When                                         | Example                                |
| ------------------- | ------------------------------------------------ | -------------------------------------- |
| Fill with a default | A domain default is honest and useful            | Missing status becomes `"pending"`     |
| Fill with logic     | A value can be derived from other fields         | Missing display name from first/last   |
| Drop rows           | Missing value invalidates the record             | Missing primary key                    |
| Preserve NULL       | Unknown is meaningful and should remain visible  | Missing survey response                |

For expression-level NULL behavior, see [Handling Null Values](../Concepts/null-handling.md). For schema-level nullability flags and widening, see [Anatomy of a Schema — Nullability](schema-anatomy.md#nullability).

---

## Preserving Planning Metadata

**Functional dependencies are optimizer metadata on [`DFSchema`]; preserve or rebuild them only when your code can prove the relationship still holds.**

Functional dependencies describe determinant relationships such as "column 0 uniquely determines column 1." DataFusion derives them from table constraints and propagates them through many plan nodes. When you construct or transform a [`DFSchema`] directly, [`.with_functional_dependencies()`] lets you attach a replacement [`FunctionalDependencies`] value. The method validates that dependency indices fit the schema width.

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{
    Dependency, DFSchema, FunctionalDependence, FunctionalDependencies,
};

fn main() -> datafusion::error::Result<()> {
    let schema = DFSchema::try_from(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]))?;

    // order_id is unique, so it determines the other fields.
    let dependencies = FunctionalDependencies::new(vec![
        FunctionalDependence::new(vec![0], vec![1, 2], false)
            .with_mode(Dependency::Single),
    ]);

    let schema = schema.with_functional_dependencies(dependencies)?;
    assert_eq!(schema.functional_dependencies().len(), 1);

    // Index 3 is outside a three-field schema, so validation fails.
    let invalid = FunctionalDependencies::new(vec![
        FunctionalDependence::new(vec![3], vec![0], false),
    ]);
    assert!(schema.clone().with_functional_dependencies(invalid).is_err());

    Ok(())
}
```

:::{admonition} Prefer source constraints when possible
:class: tip
For normal table scans, declare primary-key and unique constraints on the [`TableProvider`] instead of hand-authoring functional dependencies. DataFusion converts table constraints into [`FunctionalDependencies`] during plan construction. Use [`.with_functional_dependencies()`] mainly for custom logical plans, schema-dependent tests, or advanced optimizer work.
:::

Several transformations intentionally clear functional dependencies. [`DFSchema::join()`] and union schema derivation both produce schemas with empty functional dependencies because the original key relationships may no longer be valid after combining inputs. Reattach dependencies only after reasoning about the transformed schema, not as a mechanical copy.

---

## Arrow Round Trips and Lost Context

**Arrow round trips preserve physical fields but drop DataFusion planning context; rebuild qualifiers and functional dependencies explicitly when returning to [`DFSchema`].**

The basic interop path is covered in [Inspecting and Validating Schemas — Arrow Interop](schema-inspection.md#arrow-interop): [`.inner()`] and [`.as_arrow()`] expose the Arrow [`Schema`] inside a [`DFSchema`]. That Arrow schema is the right shape for Arrow compute kernels, IPC writers, and libraries outside DataFusion. The trade-off is context loss: Arrow knows field names, data types, nullability, and metadata, but not table qualifiers or functional dependencies.

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

    // Leaving DFSchema for Arrow drops qualifier and dependency context.
    let exported_arrow_schema = qualified.inner().clone();
    let unqualified = DFSchema::try_from(exported_arrow_schema.as_ref().clone())?;
    assert!(unqualified.iter().all(|(qualifier, _)| qualifier.is_none()));

    // Returning to DataFusion: rebuild qualifiers from application context.
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

Use this pattern when an Arrow-only API sits between two DataFusion planning steps. If you are constructing a fresh [`DFSchema`] from Arrow rather than transforming an existing one, start with the constructor reference in [Creating Schemas](schema-creation.md#defining-a-dfschema-directly). If you are preserving optimizer constraints, rebuild [`FunctionalDependencies`] after the Arrow round trip; they cannot be recovered from the Arrow [`Schema`].

---

## Conclusion & Further Reading

**Transform schemas at the layer that owns the change: DataFrame methods for data-producing plans, [`DFSchema`] methods for planning context, and Arrow APIs only for physical-schema interop.**

Requalification changes how DataFusion resolves columns. Schema combination methods build planning contracts without touching rows. [`.union_by_name()`] is the operational tool for evolving input schemas because it aligns rows by column name and fills missing columns with NULL. Functional dependencies and qualifiers remain DataFusion-only planning metadata, so preserve them deliberately and rebuild them after Arrow round trips.

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
[`.inner()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.join
[`.merge()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.merge
[`.replace_qualifier()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.replace_qualifier
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.strip_qualifiers()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.strip_qualifiers
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.with_field_specific_qualified_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.with_field_specific_qualified_schema
[`.with_functional_dependencies()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.with_functional_dependencies
[Creating Schemas]: schema-creation.md
