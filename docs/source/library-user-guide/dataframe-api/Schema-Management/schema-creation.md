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

# Creating Schemas

**Explicit schema creation gives you full control over column types, precision, metadata, and the [`DFSchema`] contract — replacing inference guesswork with a declared structure.**

<!-- TODO: Write the abstract last. Needle-tip scope: this page covers (a) the three
Arrow primitives used to define a schema, (b) the three `DataType` families that need
extra parameters or composition (decimal, timestamp, nested), (c) attaching metadata,
and (d) when to reach for `DFSchema` construction. Keep it tighter than the
schema-management abstract in `index.md` — this is a leaf, not the hub. -->

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

```{contents} Table of Contents for Creating Schemas
:local:
:depth: 2
```

## Schema Creation: From Automatic to Explicit

**Most schemas arrive automatically — but the automatic path has limits that explicit schema definition resolves.**

Every [`DataFrame`] carries a [`DFSchema`] inside its [`LogicalPlan`] — the structural contract that the query engine validates at every plan node. The [`DFSchema`] connects the Arrow [`Schema`] underneath to the plan, adding the relational context that column resolution, type coercion, and optimization depend on.

| Layer            | Role                     | Contents                                                      |
| :--------------- | :----------------------- | :------------------------------------------------------------ |
| Arrow [`Schema`] | Physical column contract | Column name, [`DataType`], nullability, metadata              |
| [`DFSchema`]     | Query-planning wrapper   | Arrow [`Schema`] + table qualifiers + functional dependencies |

The Arrow [`Schema`] defines how each column is stored and processed. The [`DFSchema`] extends it with table qualifiers for cross-table column disambiguation and functional dependencies for optimizer reductions. For the full structural breakdown, see [Anatomy of a Schema](schema-anatomy.md).

### Automatic and Explicit Creation Paths

**Schema creation has two entry points: reader methods can derive a schema for you, or your code can define the schema before reading begins.**

Most of the time, schema creation is handled automatically when loading data into the query engine. Self-describing formats — Parquet, Avro, Arrow IPC — carry their schema in file metadata, which gets read directly as an Arrow [`Schema`] and wrapped into a [`DFSchema`]. For line-delimited formats — CSV, NDJSON — [schema inference](schema-inference.md) scans the first N rows (N = 1,000 by default) to derive column types. Either way, reader methods like [`ctx.read_csv()`] and [`ctx.read_parquet()`] handle schema creation and wrapping transparently.

```text
        ┌──────────────────────┐
        │ Data source / reader │
        └──────────┬───────────┘
                   │
        ┌─────────────────────┐
        ▼                     ▼
┌───────────────────┐ ┌────────────────┐
│ Automatic schema  │ │ Explicit schema│
│ creation          │ │ definition     │
├───────────────────┤ ├────────────────┤
│ Parquet / Avro /  │ │ Field          │
│ Arrow IPC metadata│ │ Schema         │
│ CSV / NDJSON      │ │ SchemaRef      │
│ inference         │ │ reader options │
└─────────┬─────────┘ └─────────┬──────┘
          └─────────┬───────────┘
                    ▼
             ┌──────────────┐
             │ Arrow Schema │
             └──────┬───────┘
                    ▼
             ┌──────────────┐
             │   DFSchema   │
             └──────┬───────┘
                    ▼
             ┌──────────────┐
             │ LogicalPlan  │
             └──────┬───────┘
                    ▼
             ┌──────────────┐
             │  DataFrame   │
             └──────────────┘
```

The explicit path is the focus of this document. Once you define the Arrow [`Schema`] yourself, DataFusion can use the same declared contract for file readers, in-memory tables, custom [`TableProvider`] implementations, and plan-level tests.

### When Explicit Schemas Are Worth Defining

**The automatic path breaks in predictable ways — recognizing these patterns tells you when an explicit schema is required.**

| Failure mode          | What happens                                                  | Affected formats                |
| :-------------------- | :------------------------------------------------------------ | :------------------------------ |
| Type guessing         | Currency → `Float64` instead of `Decimal128`, dates → `Utf8` | CSV, NDJSON                     |
| Sparse columns        | Fields appearing after the sample window are missed entirely  | CSV, NDJSON                     |
| Sample budget sharing | Later files never contribute to the inferred schema           | Multi-file CSV / NDJSON         |
| Schema divergence     | Conflicting types for the same field trigger merge failures   | Multi-file Parquet              |
| Custom sources        | No embedded schema or inference path exists                   | `TableProvider` implementations |

For the full catalog of inference failure modes, see [Schema Inference — Risks and Failure Modes](schema-inference.md#inference-risks-and-failure-modes). To validate a defined schema before execution and catch mismatches early, see [Inspecting and Validating Schemas](schema-inspection.md).

Once an explicit schema is the right choice, the usual entry point is the Arrow layer: [`Field`], [`Schema`], and [`SchemaRef`]. DataFusion wraps the Arrow [`Schema`] in [`DFSchema`] automatically when it is passed to a reader or registered as a table. Direct [`DFSchema`] construction is reserved for custom [`TableProvider`]s, [`LogicalPlan`] node authoring, and test fixtures (covered in [Defining a `DFSchema` Directly](#defining-a-dfschema-directly)). The sections below start with the minimal Arrow [`Schema`] recipe and progress through parameterized types, metadata, [`DFSchema`] construction, and applying schemas to readers.

---

## Building an Arrow Schema

**The entry-level recipe is three lines: list fields, wrap in `Schema::new`, share via `Arc`.**

A DataFusion-ready schema is built from three Arrow primitives. The table below summarizes them; [Anatomy of a Schema](schema-anatomy.md) covers each property in depth.

| Primitive        | Role                                                       | Key properties                                                            |
| :--------------- | :--------------------------------------------------------- | :------------------------------------------------------------------------ |
| [`Field`]        | One column                                                 | [name](schema-anatomy.md#name), [data type](schema-anatomy.md#data-type), [nullability](schema-anatomy.md#nullability), [metadata](schema-anatomy.md#metadata) |
| [`Schema`]       | Ordered list of [`Field`]s plus schema-level metadata      | [Field order](schema-anatomy.md#field-order), [field count](schema-anatomy.md#field-count) |
| [`SchemaRef`]    | `Arc<Schema>` for cheap sharing                            | `O(1)` clone, thread-safe                                                 |

### Composing the Minimal Schema

Build an Arrow [`Schema`] by listing its [`Field`]s in order. Each field declares a name, a [`DataType`], and a nullable flag. [`Schema::new`] accepts any `impl Into<Fields>`, so a plain `Vec<Field>` works:

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

fn main() {
    // Define once, share via Arc — cloning the Arc is O(1).
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),     // primary key, not nullable
        Field::new("name", DataType::Utf8, true),     // display name, nullable
        Field::new("active", DataType::Boolean, false),
    ]));

    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
}
```

The example covers the common case: scalar types, explicit nullability, no metadata. For the typing rules behind each [`DataType`] variant and the SQL-to-Arrow mapping, see [Arrow Data Types in DataFusion](type-coercion.md#arrow-data-types-in-datafusion) and [SQL Data Types](../../../user-guide/sql/data_types.md).

### Sharing Schemas with `Arc`

Readers, plan nodes, and custom [`TableProvider`]s all accept [`SchemaRef`] — the idiomatic type for a shared Arrow schema. Wrapping the schema in [`Arc`] once means every downstream consumer holds a reference, not a copy. Passing `Arc::clone(&schema)` is `O(1)` regardless of field count and metadata size.

:::{admonition} Avoid rebuilding schemas inside loops
:class: tip
Construct the schema once at pipeline setup and pass [`SchemaRef`] clones. Rebuilding a [`Schema`] per batch or per file allocates the full [`Fields`] list every time and defeats the point of the [`Arc`] layer.
:::

:::{admonition} Applying a schema to a reader
:class: seealso
To wire a defined [`Schema`] into [`ctx.read_csv()`], [`ctx.read_json()`], or a [`ListingTable`], see [Applying Schemas](schema-application.md).
:::

## Parameterized and Composite Types

**Three [`DataType`] families need more than a bare variant: decimals and timestamps take parameters that change semantics; structs, lists, and maps compose other types.**

The common scalar variants ([`Int64`], [`Utf8`], [`Boolean`], [`Float64`]) are self-describing — pass the variant and move on. The three families below are where schema definitions quietly go wrong. Decimal precision silently truncates; timestamp timezone presence breaks comparisons; nested types require explicit field composition.

### Decimals: Precision and Scale

Floating-point types (`Float32`, `Float64`) accumulate rounding error that compounds in financial and scientific calculations. [`Decimal128`] and [`Decimal256`] provide exact base-10 arithmetic at the cost of two parameters:

- **Precision** — total number of decimal digits stored. [`Decimal128`] supports `1..=38`; [`Decimal256`] supports `1..=76`.
- **Scale** — number of digits to the right of the decimal point. Must satisfy `0 <= scale <= precision`.

`Decimal128(10, 2)` can store `12345678.99` (8 integer digits + 2 decimal = 10 total), rejects `123456789.99` (11 digits exceed precision), and cannot preserve `1234567.999` (3 decimal digits exceed scale).

```rust
use datafusion::arrow::datatypes::{DataType, Field};

fn main() {
    // Currency: 2 decimal places, room for trillion-dollar values.
    let _price = Field::new("price", DataType::Decimal128(19, 2), false);

    // Rates and ratios: more scale for sub-percentage precision.
    let _conversion_rate = Field::new("conversion_rate", DataType::Decimal128(10, 6), false);

    // Scientific values needing more than 38 digits: Decimal256.
    let _ledger_balance = Field::new("ledger_balance", DataType::Decimal256(76, 10), false);
}
```

:::{admonition} Narrowing a decimal can fail at runtime
:class: warning
Casting `Decimal128(10, 2)` to `Decimal128(8, 2)` fails for any value with more than 6 integer digits. Reduce precision only when the value range bounds it.
:::

### Timestamps and Time Zones

A [`DataType::Timestamp`] represents either an **absolute instant** (with a non-empty timezone) or a **local wall-clock value** (without a timezone). The two are incompatible in arithmetic and comparisons — mixing them silently produces wrong answers or errors at execution time.

| Type                 | Example                               | Semantics                                                    | Use for                                   |
| :------------------- | :-----------------------------------: | :----------------------------------------------------------- | :---------------------------------------- |
| **With timezone**    | `Timestamp(Microsecond, Some("UTC"))` | Absolute UTC instant; the timezone string is display metadata | Server logs, transactions, event streams  |
| **Without timezone** | `Timestamp(Microsecond, None)`        | Wall-clock value relative to the producer's local time        | Scheduled events, opening hours, calendar |

At the Arrow level, any timestamp with a non-empty timezone is stored as a UTC instant — the timezone string is metadata for display and interpretation. Changing between two non-empty timezones (for example `"UTC"` → `"America/New_York"`) is a metadata-only operation. Timestamps without a timezone cannot be compared to timestamped instants without an explicit cast.

```rust
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

fn main() {
    // Absolute instant — stored as UTC regardless of the displayed timezone.
    let event_time = Field::new(
        "event_time",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    );

    // Local wall-clock — no absolute reference, cannot compare to `event_time`
    // without an explicit cast.
    let scheduled_at = Field::new(
        "scheduled_at",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    );

    let schema = Arc::new(Schema::new(vec![event_time, scheduled_at]));

    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
}
```

:::{admonition} Pick one strategy per pipeline
:class: tip
Most systems standardize on UTC timestamps end-to-end. When joining columns with different timezone settings, cast both to the same [`DataType::Timestamp`] variant first — type coercion does not reconcile timezone presence implicitly.
:::

### Nested Types: Struct, List, Map

Hierarchical data — nested JSON, Parquet groups, event payloads — is preserved losslessly by Arrow's [`Struct`][`DataType::Struct`], [`List`][`DataType::List`], and [`Map`][`DataType::Map`] types. Flattening into parallel scalar columns drops the relationship between fields; nested types keep it intact. For the high-level placement of nested types inside a schema, see [Nested Types in Anatomy of a Schema](schema-anatomy.md#nested-types).

| Type       | Shape                                 | Arrow variant                | Use for                                |
| :--------- | :------------------------------------ | :--------------------------- | :------------------------------------- |
| **Struct** | Fixed set of named fields             | [`DataType::Struct`]         | Heterogeneous record inside a column   |
| **List**   | Variable-length homogeneous sequence  | [`DataType::List`]           | Tags, multi-value attributes           |
| **Map**    | Key-value pairs                       | [`DataType::Map`]            | Labels, sparse attribute bags          |

```rust
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};

fn main() {
    // Struct: a nested object with its own field list.
    let metadata_type = DataType::Struct(Fields::from(vec![
        Field::new("source", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
    ]));

    // List: the child field carries the element type and its nullability.
    let tags_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

    // Map: encoded as List<Struct<key, value>>; the bool is `keys_sorted`.
    let attributes_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),    // map keys are never null
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        )),
        false,
    );

    let schema = Schema::new(vec![
        Field::new("metadata", metadata_type, true),
        Field::new("tags", tags_type, true),
        Field::new("attributes", attributes_type, true),
    ]);

    assert_eq!(schema.fields().len(), 3);
}
```

:::{admonition} Querying nested fields
:class: seealso
For extracting values from nested columns (`get_field()` on struct, `array_element()` on list), see [Applying Schemas and Modeling Data — Strategy 4: Nested Data](schema-application.md#strategy-nested-data).
:::

:::{admonition} Use `Large*` variants only when needed
:class: caution
[`LargeUtf8`], [`LargeBinary`], and [`LargeList`] use 64-bit offsets and cost more memory per array. Use them only when a single value might exceed 2 GB. DataFusion does not enforce key uniqueness in [`DataType::Map`] — duplicate keys must be resolved in query logic if the source format permits them.
:::

Types and nullability define the structural contract. The next layer — metadata — adds semantic context that the optimizer ignores but humans and governance systems depend on.

## Attaching Metadata

**Field- and schema-level metadata embed context (descriptions, lineage, PII classification) as key-value strings; DataFusion preserves them end-to-end but the optimizer never reads them.**

Attach metadata with [`Field::with_metadata()`] on individual fields and with [`Schema::new_with_metadata()`] at the schema level. Both accept `HashMap<String, String>`:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() {
    // Field-level: lineage and classification.
    let id_field = Field::new("user_id", DataType::Int64, false)
        .with_metadata(HashMap::from([
            ("source_system".to_string(), "crm".to_string()),
        ]));

    let email_field = Field::new("email", DataType::Utf8, true)
        .with_metadata(HashMap::from([
            ("pii".to_string(), "true".to_string()),
        ]));

    // Schema-level: dataset-wide annotations.
    let schema_metadata = HashMap::from([
        ("schema_version".to_string(), "v2.1".to_string()),
    ]);

    let schema = Arc::new(Schema::new_with_metadata(
        vec![id_field, email_field],
        schema_metadata,
    ));

    assert_eq!(
        schema.field(0).metadata().get("source_system"),
        Some(&"crm".to_string())
    );
    assert_eq!(
        schema.metadata().get("schema_version"),
        Some(&"v2.1".to_string())
    );
}
```

Not all formats preserve metadata equally. Arrow IPC round-trips it losslessly. Parquet can embed it, but DataFusion skips file-level schema metadata by default — set `.skip_metadata(false)` on [`ParquetReadOptions`] if your pipeline relies on it. CSV and NDJSON carry no metadata at all.

:::{admonition} Metadata is not a constraint system
:class: warning
Setting `primary_key=true` or `unique=true` in field metadata is documentation only — the DataFusion optimizer does not read these keys. To enable optimizer benefits (join elimination, distinct pushdown), express constraints through the [`Constraints`] API on the table or plan, not through metadata.
:::

:::{admonition} Metadata taxonomy and reading patterns
:class: seealso
For how metadata is classified as "secondary" schema information and how it propagates through the plan, see [Schema Concepts — What the Contract Contains](schema-concepts.md#what-the-contract-contains) and [Metadata in Anatomy of a Schema](schema-anatomy.md#metadata).
:::

## Defining a `DFSchema` Directly

**[`DFSchema`] adds table qualifiers and functional dependencies to an Arrow [`Schema`] — defining one by hand is reserved for custom [`TableProvider`]s, [`LogicalPlan`] node authoring, and test fixtures.**

For ordinary DataFrame work (reading files, running transformations, writing output), DataFusion builds [`DFSchema`] for you. You reach for these constructors in three scenarios:

1. **Implementing a custom [`TableProvider`]** — `schema()` returns a [`SchemaRef`], but plan-facing helpers may need a qualified [`DFSchema`].
2. **Authoring a [`LogicalPlan`] node** — each plan node derives its own output [`DFSchema`] from input schemas and projected expressions.
3. **Building test fixtures** — simulating query context for unit tests of schema-dependent code.

### Constructor Reference

| Constructor                                                                                                                | Input                                                | Qualifiers         | `check_names()` | Purpose                                    |
| -------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- | ------------------ | :-------------: | ------------------------------------------ |
| [`DFSchema::try_from(schema)`][`DFSchema::try_from`]                                                                        | [`Schema`] or [`SchemaRef`]                          | all `None`         | —               | Wrap an Arrow schema without qualifiers    |
| [`DFSchema::empty()`]                                                                                                       | —                                                    | —                  | —               | Zero-field schema                          |
| [`DFSchema::from_unqualified_fields(fields, metadata)`][`DFSchema::from_unqualified_fields`]                                | [`Fields`] + `HashMap<String, String>`               | all `None`         | ✓               | Arrow fields with schema-level metadata    |
| [`DFSchema::new_with_metadata(qualified_fields, metadata)`][`DFSchema::new_with_metadata`]                                  | `Vec<(Option<TableReference>, Arc<Field>)>` + meta   | per-field          | ✓               | Full control over qualifier per field      |
| [`DFSchema::try_from_qualified_schema(qualifier, &schema)`][`DFSchema::try_from_qualified_schema`]                          | `impl Into<TableReference>` + `&Schema`              | same for all       | ✓               | Qualify every field with one table name    |
| [`DFSchema::from_field_specific_qualified_schema(qualifiers, &schema)`][`DFSchema::from_field_specific_qualified_schema`]   | `Vec<Option<TableReference>>` + `&SchemaRef`         | per-field          | ✓               | Different qualifier per field              |

:::{admonition} `try_from` allows duplicate field names
:class: caution
[`DFSchema::try_from`] intentionally skips [`check_names()`] to support intermediate plan stages (for example, partial aggregates) where duplicate field names legitimately occur — see [apache/datafusion#17715](https://github.com/apache/datafusion/issues/17715). The five constructors that call [`check_names()`] return `Err(DuplicateQualifiedField)` or `Err(DuplicateUnqualifiedField)` when names collide under their qualifier rules.
:::

```rust
use std::sync::Arc;
use datafusion::common::{DFSchema, TableReference};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

fn main() -> datafusion::error::Result<()> {
    let arrow_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(19, 2), true),
    ]);

    // Common case: unqualified wrap, no name check.
    let unqualified = DFSchema::try_from(arrow_schema.clone())?;
    assert_eq!(unqualified.fields().len(), 2);

    // Single-qualifier: every field becomes `orders.id`, `orders.amount`.
    let qualified = DFSchema::try_from_qualified_schema("orders", &arrow_schema)?;
    let (qualifier, _) = qualified.qualified_field(0);
    assert_eq!(qualifier, Some(&TableReference::bare("orders")));

    // Per-field qualifiers: used when a schema represents a joined result.
    let schema_ref: SchemaRef = Arc::new(arrow_schema);
    let mixed = DFSchema::from_field_specific_qualified_schema(
        vec![
            Some(TableReference::bare("users")),
            Some(TableReference::bare("orders")),
        ],
        &schema_ref,
    )?;
    let (q_users, _) = mixed.qualified_field(0);
    let (q_orders, _) = mixed.qualified_field(1);
    assert_eq!(q_users, Some(&TableReference::bare("users")));
    assert_eq!(q_orders, Some(&TableReference::bare("orders")));

    Ok(())
}
```

:::{admonition} Transforming an existing `DFSchema`
:class: seealso
To re-qualify, strip qualifiers, merge, or join [`DFSchema`] values after construction, see [Transforming Schemas](schema-transformation.md).
:::

## Conclusion & Further Reading

**An explicit schema replaces inference guesswork with a contract — column types, nullability, and metadata are locked before the first byte of data is read.**

The automatic path covered in the opening section works for exploration and uniform data. When it falls short — precision-sensitive types, multi-file consistency, custom sources — the primitives in this document give you full control: [`Field`], [`Schema`], and [`SchemaRef`] for the Arrow layer; parameterized types and metadata for semantic precision; [`DFSchema`] constructors for plan-level work. With the schema defined, the next step is wiring it into concrete readers or validating it against inferred results.

:::{admonition} Related documents
:class: seealso

- [Schema Inference](schema-inference.md) — the inference path and its failure modes
- [Applying Schemas](schema-application.md) — format-specific wiring (CSV, NDJSON, Parquet, partitions)
- [Inspecting and Validating Schemas](schema-inspection.md) — checking a defined schema before execution
- [Transforming Schemas](schema-transformation.md) — qualifiers, combining, nullability on existing schemas
- [Anatomy of a Schema](schema-anatomy.md) — field-level reference for [`DataType`], nullability, metadata
:::

<!-- Link references -->

[`Schema`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html
[`Schema::new`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html#method.new
[`Schema::new_with_metadata()`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html#method.new_with_metadata
[`SchemaRef`]: https://docs.rs/arrow-schema/latest/arrow_schema/type.SchemaRef.html
[`Field`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Field.html
[`Field::with_metadata()`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Field.html#method.with_metadata
[`Fields`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Fields.html
[`DataType`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html
[`DataType::Struct`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Struct
[`DataType::List`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.List
[`DataType::Map`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Map
[`DataType::Timestamp`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Timestamp
[`Decimal128`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Decimal128
[`Decimal256`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Decimal256
[`Int64`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Int64
[`Utf8`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Utf8
[`Boolean`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Boolean
[`Float64`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.Float64
[`LargeUtf8`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.LargeUtf8
[`LargeBinary`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.LargeBinary
[`LargeList`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html#variant.LargeList
[`Arc`]: https://doc.rust-lang.org/std/sync/struct.Arc.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html
[`DFSchema::try_from`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#impl-TryFrom%3CSchema%3E-for-DFSchema
[`DFSchema::empty()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.empty
[`DFSchema::from_unqualified_fields`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.from_unqualified_fields
[`DFSchema::new_with_metadata`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.new_with_metadata
[`DFSchema::try_from_qualified_schema`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.try_from_qualified_schema
[`DFSchema::from_field_specific_qualified_schema`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.from_field_specific_qualified_schema
[`check_names()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.check_names
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/provider/trait.TableProvider.html
[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`ParquetReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`Constraints`]: https://docs.rs/datafusion/latest/datafusion/common/struct.Constraints.html
[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`ctx.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`ctx.read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`ctx.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
