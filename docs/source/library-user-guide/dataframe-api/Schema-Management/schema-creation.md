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

**Defining schemas explicitly makes data pipelines reliable, testable, and predictable — replacing inference guesswork with a declared contract.**

Schemas give information its meaning — converting raw values into typed, structured data. Creating schemas is a foundational practice in data engineering, reliable production systems, and reproducible analysis. DataFusion handles schema creation automatically in most cases, but the best practice for predictable results is defining schemas explicitly. This document walks the construction end to end: Arrow primitives ([`Field`], [`Schema`], [`SchemaRef`]), parameterized and composite data types (decimal, timestamp, nested), field and schema metadata, and the [`DFSchema`] wrapper that connects the schema to the query engine's [`LogicalPlan`].

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

Every [`DataFrame`] carries a [`DFSchema`] inside its [`LogicalPlan`] — the structural contract that the query engine validates at every plan node. The [`DFSchema`] wraps an Arrow [`SchemaRef`] (the physical column definitions) and adds table qualifiers plus functional dependencies for query planning:

```text
┌───────────────────────────────────────────────────────┐
│ DataFrame                                             │
│   └── LogicalPlan                                     │
│        └── DFSchema                                   │
│             ├── inner: Arc<Schema>    (Arrow Schema)  │
│             │        └── Field[]      (Arrow Fields)  │
│             │             ├── name                    │
│             │             ├── data_type               │
│             │             ├── nullable                │
│             │             └── metadata                │
│             ├── field_qualifiers (TableReference)     │
│             └── functional_dependencies               │
└───────────────────────────────────────────────────────┘
```

The Arrow [`Schema`] defines the physical column contract — names, types, nullability, metadata. The [`DFSchema`] wraps that contract and connects it to the [`LogicalPlan`], adding table qualifiers and functional dependencies that column resolution, type coercion, and optimization rely on. For the full structural breakdown, see [Anatomy of a Schema](schema-anatomy.md).

| Layer            | Role                     | Contents                                                      |
| :--------------- | :----------------------- | :------------------------------------------------------------ |
| Arrow [`Schema`] | Physical column contract | Column name, [`DataType`], nullability, metadata              |
| [`DFSchema`]     | Query-planning wrapper   | Arrow [`Schema`] + table qualifiers + functional dependencies |

How that [`DFSchema`] reaches the [`DataFrame`] depends on the source — file metadata, row-sample inference, a [`TableProvider`] implementation, or explicit construction in code. The next subsection traces these paths before the document concentrates on the explicit one.

### Automatic and Explicit Creation Paths

**Schema creation is automatic in most cases — DataFusion derives the schema from the source without explicit definition.**

Schema creation happens inside the reader or table registration. The path DataFusion takes depends on the source:

- **Self-describing formats** — Parquet, Avro, Arrow IPC — carry the schema in file metadata, which the engine reads directly.
- **Line-delimited formats** — CSV, JSON — have no embedded schema, so the engine infers one by scanning the first 1,000 rows by default (configurable via [`CsvReadOptions::schema_infer_max_records()`] or [`NdJsonReadOptions::schema_infer_max_records()`]).
- **Table registration** — [`TableProvider`] implementations and [`MemTable`] supply a [`SchemaRef`] at construction; the engine reads it as-is, but the schema itself was declared in code at the source.

All three paths converge at an Arrow [`Schema`], which DataFusion wraps into a [`DFSchema`] and attaches to the [`LogicalPlan`]:

```text
  AUTOMATIC PATH        TABLE PROVIDER         EXPLICIT PATH
┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐
│ Parquet / Avro /  │ │ TableProvider /   │ │ Code construction │
│ Arrow IPC file    │ │ MemTable          │ │ Field::new(..)    │
│ (read metadata)   │ │ (declared at the  │ │ Schema::new(..)   │
│ CSV / JSON file │ │  source, read     │ │ Arc::new(..)      │
│ (sample N=1k)     │ │  automatically)   │ │                   │
└────────┬──────────┘ └────────┬──────────┘ └─────────┬─────────┘
         │ read / infer        │ register             │ construct
         └─────────────────────┼──────────────────────┘
                               ▼
                      ┌───────────────┐
                      │  Arrow Schema │
                      └───────┬───────┘
                              ▼ DataFusion wraps
                      ┌───────────────┐
                      │    DFSchema   │
                      └───────┬───────┘
                              ▼ embedded in
                      ┌───────────────┐
                      │  LogicalPlan  │
                      └───────┬───────┘
                              ▼ exposed via
                      ┌───────────────┐
                      │   DataFrame   │
                      └───────────────┘
```

Automatic creation covers the common case — but it falls short under predictable conditions that the next subsection maps out.

### The Case for Explicit Definition

**Explicit schemas add trust, robustness, and validation to the data flow — capabilities that the automatic path cannot guarantee.**

File reading is the most visible failure surface, but not the only one. [`TableProvider`] implementations that bridge external systems (PostgreSQL, MySQL, REST APIs) must translate source-native types into Arrow types — a mapping that can lose precision or fail on unsupported types. In-memory data and write targets carry their own variants. Explicit definition fixes the contract before execution begins, regardless of the source.

| Failure mode          | What happens                                                    | Affected sources                                                 |
| :-------------------- | :-------------------------------------------------------------- | :--------------------------------------------------------------- |
| Type guessing         | Currency → `Float64` instead of `Decimal128`, dates → `Utf8`    | CSV, JSON                                                        |
| Sparse columns        | Fields appearing after the sample window are missed entirely    | CSV, JSON                                                        |
| Sample budget sharing | Later files contribute only if earlier files leave budget       | Multi-file CSV / JSON                                            |
| Schema divergence     | Conflicting types for the same field trigger merge failures     | Multi-file Parquet                                               |
| Damaged metadata      | Read fails outright — no row-sample fallback exists             | Parquet, Avro, Arrow IPC                                         |
| Type translation      | Source-native types map incorrectly or incompletely to Arrow    | [`TableProvider`] bridging external databases (PostgreSQL, etc.) |
| Code-only sources     | No file, no metadata — the schema must be authored from scratch | `MemTable`, `RecordBatch` registration, plan-level test fixtures |

:::{admonition} Beyond file errors
:class: caution

Failure modes are not the only motivators. Several workflows require explicit schemas by construction:

- **Production contract enforcement.** A declared schema rejects upstream type drift at plan time rather than at execution. Pipelines that promise stable output benefit even when the source is technically inferable.
- **Pre-write output control.** Writers honor the schema they are given — explicit construction is how you pin Parquet logical types, dictionary encodings, or timezone tags on the output.
- **Type precision beyond inference.** [`Decimal128`] precision and scale, timezone-tagged [`DataType::Timestamp`] variants, and per-field nullability inside [`DataType::Struct`], [`DataType::List`], and [`DataType::Map`] are not reliably inferred — they have to be declared.
  :::

For the inference path's full failure catalog, see [Schema Inference — Risks and Failure Modes](schema-inference.md#inference-risks-and-failure-modes). To validate a schema against execution-time data and catch mismatches early, see [Inspecting and Validating Schemas](schema-inspection.md).

The remainder of this document walks explicit creation end to end — from Arrow primitives ([`Field`], [`Schema`], [`SchemaRef`]) through parameterized types, metadata, [`DFSchema`] construction, and wiring a defined schema into format-specific readers.

---

## Building an Arrow Schema

**The entry-level recipe is three primitives: define each column as a [`Field`], collect them in a [`Schema`], and share the result as a [`SchemaRef`].**

The previous section showed where the schema lives and how it reaches the [`DataFrame`]. This section moves from understanding to construction — the fine-grained Arrow layer where every column name, type, and nullability flag is declared. Three Arrow primitives compose every schema; [Anatomy of a Schema](schema-anatomy.md) covers each property in depth.

| Primitive     | Role                                                  | Key properties                                                                                                                                                 |
| :------------ | :---------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`Field`]     | One column                                            | [name](schema-anatomy.md#name), [data type](schema-anatomy.md#data-type), [nullability](schema-anatomy.md#nullability), [metadata](schema-anatomy.md#metadata) |
| [`Schema`]    | Ordered list of [`Field`]s plus schema-level metadata | [Field order](schema-anatomy.md#field-order), [field count](schema-anatomy.md#field-count)                                                                     |
| [`SchemaRef`] | `Arc<Schema>` for cheap sharing                       | `O(1)` clone, thread-safe                                                                                                                                      |

### Composing the Minimal Schema

**A minimal working example is the fastest path from concept to hands-on — three lines of code produce a complete, shareable schema.**

The three primitives from the table above map directly to code. Each [`Field`] declares a column name, a [`DataType`], and a nullable flag. [`Schema::new`] collects the fields into an ordered list, and wrapping the result in `Arc` produces the [`SchemaRef`] that DataFusion's readers and plan nodes expect:

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use std::sync::Arc;

fn main() -> Result<()> {
    // Define once, share via Arc — cloning the Arc is O(1).
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),     // primary key, not nullable
        Field::new("name", DataType::Utf8, true),     // display name, nullable
        Field::new("active", DataType::Boolean, false),
    ]));

    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());

    Ok(())
}
```

The example covers the common case: scalar types, explicit nullability, no metadata. For the typing rules behind each [`DataType`] variant and the SQL-to-Arrow mapping, see [Arrow Data Types in DataFusion](type-coercion.md#arrow-data-types-in-datafusion) and [SQL Data Types](../../../user-guide/sql/data_types.md).

### Sharing Schemas with `Arc`

**[`SchemaRef`] (`Arc<Schema>`) is the currency of schema sharing — construct once, pass everywhere at zero copy cost.**

Readers, plan nodes, and custom [`TableProvider`]s all accept [`SchemaRef`] — the idiomatic type for a shared Arrow schema. Wrapping the schema in [`Arc`] once means every downstream consumer holds a reference, not a copy. Passing `Arc::clone(&schema)` is `O(1)` regardless of field count and metadata size. Sharing a schema eliminates the cost of rebuilding it for every new reader or plan node.

:::{admonition} Avoid rebuilding schemas inside loops
:class: tip
Construct the schema once at pipeline setup and pass [`SchemaRef`] clones. Rebuilding a [`Schema`] per batch or per file allocates the full [`Fields`] list every time and defeats the point of the [`Arc`] layer.
:::

:::{admonition} Applying a schema to a reader
:class: seealso
To wire a defined [`Schema`] into [`ctx.read_csv()`], [`ctx.read_json()`], or a [`ListingTable`], see [Applying Schemas](schema-application.md).
:::

---

## Parameterized and Composite Data Types

**Decimals, timestamps, and nested types require parameters or composition that scalar variants do not — getting these wrong silently breaks precision, comparisons, or data relationships.**

Scalar types — [`Int64`], [`Utf8`], [`Boolean`], [`Float64`] — are self-describing: pass the variant to `Field::new` and the definition is complete. Parameterized and composite types build on top of scalars with additional constraints that shape how values are stored and compared: a [`Decimal128(19, 2)`] pins the number of digits, a [`Timestamp(Microsecond, Some("UTC"))`] pins the timezone interpretation, a `Struct` composes child fields into a nested column. These additions make the types more expressive — and more error-prone when declared incorrectly. Each of the three families below carries its own failure mode.

### Decimals: Precision and Scale

**Decimal types provide exact base-10 arithmetic — but precision and scale must be declared correctly, or values silently truncate or overflow at runtime.**

Financial calculations — currency totals, tax computations, ledger balances — require exact decimal arithmetic. Floating-point types (`Float32`, `Float64`) accumulate rounding error that compounds across operations: `0.1 + 0.2` evaluates to `0.30000000000000004`, not `0.3`. [`Decimal128`] and [`Decimal256`] avoid this by storing values as scaled integers with two declared parameters:

- **Precision** — total number of decimal digits stored. [`Decimal128`] supports `1..=38`; [`Decimal256`] supports `1..=76`.
- **Scale** — number of digits to the right of the decimal point. Must satisfy `0 <= scale <= precision`.

`Decimal128(10, 2)` can store `12345678.99` (8 integer digits + 2 decimal = 10 total), rejects `123456789.99` (11 digits exceed precision), and cannot preserve `1234567.999` (3 decimal digits exceed scale).

```rust
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::Result;

fn main() -> Result<()> {
    // Currency: 2 decimal places, room for trillion-dollar values.
    let _price = Field::new("price", DataType::Decimal128(19, 2), false);

    // Rates and ratios: more scale for sub-percentage precision.
    let _conversion_rate = Field::new("conversion_rate", DataType::Decimal128(10, 6), false);

    // Scientific values needing more than 38 digits: Decimal256.
    let _ledger_balance = Field::new("ledger_balance", DataType::Decimal256(76, 10), false);

    Ok(())
}
```

:::{admonition} Narrowing a decimal can fail at runtime
:class: warning
Casting `Decimal128(10, 2)` to `Decimal128(8, 2)` fails for any value with more than 6 integer digits. Reduce precision only when the value range bounds it. For how DataFusion widens and narrows decimal types in expressions, see [The Coercion Hierarchy](type-coercion.md#the-coercion-hierarchy).
:::

### Timestamps and Time Zones

**A timestamp with a timezone is an absolute instant; without one, it is a wall-clock reading — mixing the two silently produces wrong results.**

Server logs, transactions, and event streams record _when_ something happened — an absolute instant, independent of the observer's location. Scheduled events, business hours, and calendar entries record _what time the clock shows_ — a local reading tied to a specific timezone. Arrow's [`DataType::Timestamp`] encodes this distinction through a timezone parameter: a non-empty timezone makes the value an absolute UTC instant; `None` makes it a wall-clock value. The two are incompatible in arithmetic and comparisons — mixing them silently produces wrong answers or errors at execution time.

| Type                 |                 Example                 | Semantics                                                     | Use for                                   |
| :------------------- | :-------------------------------------: | :------------------------------------------------------------ | :---------------------------------------- |
| **With timezone**    | [`Timestamp(Microsecond, Some("UTC"))`] | Absolute UTC instant; the timezone string is display metadata | Server logs, transactions, event streams  |
| **Without timezone** |    [`Timestamp(Microsecond, None)`]     | Wall-clock value relative to the producer's local time        | Scheduled events, opening hours, calendar |

At the Arrow level, any timestamp with a non-empty timezone is stored as a UTC instant — the timezone string is metadata for display and interpretation. Changing between two non-empty timezones (for example `"UTC"` → `"America/New_York"`) is a metadata-only operation. Timestamps without a timezone cannot be compared to timestamped instants without an explicit cast.

```rust
use std::sync::Arc;
use datafusion::arrow::array::TimestampMicrosecondArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

fn main() -> Result<()> {
    // Same integer value — different semantic interpretation.
    let micros: i64 = 1_700_000_000_000_000; // 2023-11-14T22:13:20 as µs since epoch

    // Absolute instant — the timezone makes this a UTC point in time.
    let event_time = Field::new(
        "event_time",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    );

    // Local wall-clock — same digits, but no absolute reference.
    let scheduled_at = Field::new(
        "scheduled_at",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    );

    let schema = Arc::new(Schema::new(vec![event_time, scheduled_at]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                TimestampMicrosecondArray::from(vec![micros])
                    .with_timezone("UTC".to_string()),
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(micros)])),
        ],
    )?;

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        batch.schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );

    Ok(())
}
```

:::{admonition} Pick one strategy per pipeline
:class: tip
Most systems standardize on UTC timestamps end-to-end. When joining columns with different timezone settings, cast both to the same [`DataType::Timestamp`] variant first — type coercion does not reconcile timezone presence implicitly. For how DataFusion coerces temporal types, see [The Coercion Hierarchy](type-coercion.md#the-coercion-hierarchy). For the SQL-side temporal type mapping, see [SQL Data Types](../../../user-guide/sql/data_types.md).
:::

### Nested Types: Struct, List, Map

**Struct, List, and Map preserve hierarchical relationships that flattening into parallel scalar columns would destroy.**

Event payloads, nested JSON, and Parquet groups carry fields that belong together — a user's address is a single object with street, city, and postal code, not three unrelated columns. Flattening those into top-level scalars discards the grouping and makes schema evolution fragile. Arrow's [`Struct`][`DataType::Struct`], [`List`][`DataType::List`], and [`Map`][`DataType::Map`] types encode the hierarchy directly in the schema, keeping the relationship between fields intact and queryable. For the high-level placement of nested types inside a schema, see [Nested Types in Anatomy of a Schema](schema-anatomy.md#nested-types).

| Type       | Shape                                | Arrow variant        | Use for                              |
| :--------- | :----------------------------------- | :------------------- | :----------------------------------- |
| **Struct** | Fixed set of named fields            | [`DataType::Struct`] | Heterogeneous record inside a column |
| **List**   | Variable-length homogeneous sequence | [`DataType::List`]   | Tags, multi-value attributes         |
| **Map**    | Key-value pairs                      | [`DataType::Map`]    | Labels, sparse attribute bags        |

```rust
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::error::Result;

fn main() -> Result<()> {
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

    Ok(())
}
```

:::{admonition} Querying nested fields
:class: seealso
For extracting values from nested columns (`get_field()` on struct, `array_element()` on list), see the JSON section in [Applying Schemas](schema-application.md#json-name-based-alignment), which demonstrates nested type support in JSON sources.
:::

:::{admonition} Use `Large*` variants only when needed
:class: caution
[`LargeUtf8`], [`LargeBinary`], and [`LargeList`] use 64-bit offsets and cost more memory per array. Use them only when a single value might exceed 2 GB. DataFusion does not enforce key uniqueness in [`DataType::Map`] — duplicate keys must be resolved in query logic if the source format permits them.
:::

With types fully defined — scalar, parameterized, and composite — the structural contract is in place. The next layer adds _semantic_ context: schema and field metadata that the optimizer ignores but humans and governance systems depend on.

---

## Attaching Metadata

**Metadata — units, lineage, PII classifications, descriptions — adds interpretive value that schema fields alone cannot express; DataFusion preserves it end-to-end as key-value strings on fields and schemas.**

Schema fields — column name, data type, nullability — serve the query engine: the optimizer reads them, type coercion depends on them, plan validation enforces them. But they say nothing about **what the data means** to human or businesslogic. Without a standard place to record units, source-system lineage, or PII status, that knowledge lives in wikis, Slack threads, or tribal memory — disconnected from the data it describes. Arrow's metadata layer keeps that context attached to the data itself as `HashMap<String, String>` on both individual fields and the schema as a whole. DataFusion preserves metadata through the plan but does not interpret it, so creation and editing follow their own pattern.

Attach metadata with [`Field::with_metadata()`] on individual fields and with [`Schema::new_with_metadata()`] at the schema level:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result;

fn main() -> Result<()> {
    // Field-level: track which upstream system produced this field.
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

    Ok(())
}
```

Metadata survives only if the output format supports it — writing to the wrong format silently drops every annotation you attached. Arrow IPC round-trips metadata losslessly. Parquet can embed it, but DataFusion skips file-level schema metadata by default; set `.skip_metadata(false)` on [`ParquetReadOptions`] if your pipeline relies on it. CSV and JSON carry no metadata at all — any annotations (PII flags, lineage, units) must be stored out-of-band if the data passes through these formats.

:::{admonition} Metadata is not a constraint system
:class: warning
Setting `primary_key=true` or `unique=true` in field metadata is documentation only — the DataFusion optimizer does not read these keys. To enable optimizer benefits (join elimination, distinct pushdown), express constraints through the [`Constraints`] API on the table or plan, not through metadata.
:::

:::{admonition} Metadata taxonomy and reading patterns
:class: seealso
For how metadata is classified as "secondary" schema information and how it propagates through the plan, see [Schema Concepts — What the Contract Contains](schema-concepts.md#what-the-contract-contains) and [Metadata in Anatomy of a Schema](schema-anatomy.md#metadata).
:::

With fields, types, and metadata in place, the Arrow-level schema definition is complete. The next section wraps that Arrow [`Schema`] into a [`DFSchema`] — the query-planning layer that adds table qualifiers and functional dependencies.

---

## Defining a `DFSchema` Directly

**[`DFSchema`] bridges the Arrow [`Schema`] to the [`LogicalPlan`] — wrapping the physical column definitions with table qualifiers and functional dependencies that column resolution, type coercion, and optimization depend on.**

The previous sections built the Arrow-level definition: fields, data types, nullability, metadata. That definition describes _what the columns are_, but the query engine also needs to know _which table each column belongs to_ and _which columns uniquely determine others_. [`DFSchema`] adds that relational context. Table qualifiers (via [`TableReference`]) disambiguate columns when multiple tables are involved — `orders.id` vs. `users.id` after a join. Functional dependencies express constraints like primary keys, enabling optimizer transformations such as join elimination and distinct pushdown.

For ordinary DataFrame work — reading files, running transformations, writing output — DataFusion builds [`DFSchema`] automatically. You construct one by hand in three scenarios:

1. **Implementing a custom [`TableProvider`]** — `schema()` returns a [`SchemaRef`], but plan-facing helpers may need a qualified [`DFSchema`].
2. **Authoring a [`LogicalPlan`] node** — each plan node derives its own output [`DFSchema`] from input schemas and projected expressions.
3. **Building test fixtures** — simulating query context for unit tests of schema-dependent code.

### Constructor Reference

**The right constructor depends on two decisions: whether fields need table qualifiers and whether duplicate field names should be rejected at construction time.**

Six constructors cover the combinations. Qualifier strategy determines how fields are scoped — no qualifier (bare column names), a single shared qualifier (all fields belong to one table), or per-field qualifiers (join results with columns from different tables). The `check_names()` column shows which constructors validate uniqueness and return an error on duplicates.

| Constructor                                                                                                               | Input                                              | Qualifiers   | `check_names()` | Purpose                                 |
| ------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- | ------------ | :-------------: | --------------------------------------- |
| [`DFSchema::try_from(schema)`][`DFSchema::try_from`]                                                                      | [`Schema`] or [`SchemaRef`]                        | all `None`   |        —        | Wrap an Arrow schema without qualifiers |
| [`DFSchema::empty()`]                                                                                                     | —                                                  | —            |        —        | Zero-field schema                       |
| [`DFSchema::from_unqualified_fields(fields, metadata)`][`DFSchema::from_unqualified_fields`]                              | [`Fields`] + `HashMap<String, String>`             | all `None`   |        ✓        | Arrow fields with schema-level metadata |
| [`DFSchema::new_with_metadata(qualified_fields, metadata)`][`DFSchema::new_with_metadata`]                                | `Vec<(Option<TableReference>, Arc<Field>)>` + meta | per-field    |        ✓        | Full control over qualifier per field   |
| [`DFSchema::try_from_qualified_schema(qualifier, &schema)`][`DFSchema::try_from_qualified_schema`]                        | `impl Into<TableReference>` + `&Schema`            | same for all |        ✓        | Qualify every field with one table name |
| [`DFSchema::from_field_specific_qualified_schema(qualifiers, &schema)`][`DFSchema::from_field_specific_qualified_schema`] | `Vec<Option<TableReference>>` + `&SchemaRef`       | per-field    |        ✓        | Different qualifier per field           |

:::{admonition} `try_from` allows duplicate field names
:class: caution
[`DFSchema::try_from`] intentionally skips [`check_names()`] to support intermediate plan stages (for example, partial aggregates) where duplicate field names legitimately occur — see [apache/datafusion#17715](https://github.com/apache/datafusion/issues/17715). The five constructors that call [`check_names()`] return `Err(DuplicateQualifiedField)` or `Err(DuplicateUnqualifiedField)` when names collide under their qualifier rules.
:::

```rust
use std::sync::Arc;
use datafusion::common::{DFSchema, TableReference};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;

fn main() -> Result<()> {
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

With both the Arrow [`Schema`] and its [`DFSchema`] wrapper defined, the schema definition is complete.

---

## Conclusion

**An explicit schema replaces inference guesswork with a contract — column types, nullability, and metadata are locked before the first byte of data is read.**

The automatic path covered in the opening section works for exploration and uniform data. When it falls short — precision-sensitive types, multi-file consistency, custom sources — the primitives in this document give you full control: [`Field`], [`Schema`], and [`SchemaRef`] for the Arrow layer; parameterized types and metadata for semantic precision; [`DFSchema`] constructors for plan-level work.

The natural next step is [Applying Schemas](schema-application.md) — wiring the defined schema into CSV, JSON, and Parquet readers via format-specific read options. To verify a schema against inferred results before execution, see [Inspecting and Validating Schemas](schema-inspection.md).

### Further Reading

- [Applying Schemas](schema-application.md) — format-specific wiring (CSV, JSON, Parquet, partitions)
- [Schema Inference](schema-inference.md) — the inference path and its failure modes
- [Inspecting and Validating Schemas](schema-inspection.md) — checking a defined schema before execution
- [Transforming Schemas](schema-transformation.md) — qualifiers, combining, nullability on existing schemas
- [Anatomy of a Schema](schema-anatomy.md) — field-level reference for [`DataType`], nullability, metadata

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
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`DFSchemaRef`]: https://docs.rs/datafusion/latest/datafusion/common/type.DFSchemaRef.html
[`DFSchema::try_from`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#impl-TryFrom%3CSchema%3E-for-DFSchema
[`DFSchema::empty()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.empty
[`DFSchema::from_unqualified_fields`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.from_unqualified_fields
[`DFSchema::new_with_metadata`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.new_with_metadata
[`DFSchema::try_from_qualified_schema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.try_from_qualified_schema
[`DFSchema::from_field_specific_qualified_schema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.from_field_specific_qualified_schema
[`check_names()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.check_names
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/provider/trait.TableProvider.html
[`MemTable`]: https://docs.rs/datafusion/latest/datafusion/catalog/struct.MemTable.html
[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`ParquetReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`Constraints`]: https://docs.rs/datafusion/latest/datafusion/common/struct.Constraints.html
[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`ctx.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`ctx.read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`ctx.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`CsvReadOptions::schema_infer_max_records()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`NdJsonReadOptions::schema_infer_max_records()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.schema_infer_max_records
