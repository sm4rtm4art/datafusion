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

# Anatomy of a Schema

<!--TODO

1. ABSTRACT (ALWAYS DEFINED LAST)

-->
**Detailed disection of the DFSchema from dataframe to arrow filed.**

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


```{contents} Table of Contents for the Anatomy of a Schema
:local:
:depth: 2
```

## DFSchema: Structure and Components

**[`DFSchema`] bridges the Arrow [`Schema`] and the [`LogicalPlan`] — connecting physical data definitions with DataFusion's query planning and optimization.**

Every [`DataFrame`] carries a [`DFSchema`] at the core of its [`LogicalPlan`], accessible via [`df.schema()`][`.schema()`]. The [`DFSchema`] provides the relational context that the query engine needs for column resolution, plan optimization, and schema validation — context that the Arrow [`Schema`] alone cannot supply.

[`DFSchema`] achieves this by wrapping the physical Arrow data definition with plan-level metadata. Here is where [`DFSchema`] sits within the [`DataFrame`]:

```text
┌───────────────────────────────────────────────────────────┐
│ DataFrame                                                 │
│   └── LogicalPlan         CURRENT TOPIC (YOU ARE HERE!)   │
│            └── DFSchema  <----┘                           │
│                 ├── inner: Arc<Schema>   (Arrow Schema)   │
│                 │        └── Field[]     (Arrow Fields)   │
│                 │             ├── name                    │
│                 │             ├── data_type               │
│                 │             ├── nullable                │
│                 │             └── metadata                │
│                 ├── field_qualifiers ([`TableReference`]) │
│                 └── functional_dependencies               │
└───────────────────────────────────────────────────────────┘
```

[`DFSchema`] connects the physical data layout to the [`LogicalPlan`] through three components:

- **`inner` (Arrow Schema):** The physical column contract — field names, data types, nullability, and metadata — shared with Arrow compute kernels and file writers.
- **`field_qualifiers`:** Maps each field to its source table or relation ([`TableReference`]), preventing column ambiguity after joins.
- **`functional_dependencies`:** Captures key constraints (primary keys, unique constraints) that the optimizer uses to simplify query plans.

:::{admonition} Schema Concepts
:class: seealso
For a broader conceptual overview — ownership chain, immutability guarantees, and schema propagation — see [Schema Concepts](schema-concepts.md).
:::


### Schema in Practice

**Inspecting a schema makes the structural concepts concrete — here is how the Arrow [`Schema`] appears in a running [`DataFrame`].**

The [`DFSchema`] is accessed via [`df.schema()`][`.schema()`], which returns `&DFSchema`. To reach the underlying Arrow [`Schema`], use [`.inner()`] (returns `&SchemaRef`) or [`.as_arrow()`] (returns `&Schema`). The example below creates a [`DataFrame`], casts a column, and inspects the resulting Arrow Schema:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "user_id"    => [1_i64],
        "email"      => [Some("alice@example.com")],
        "created_at" => [1735689600_i64],
        "active"     => [true]
    )?;

    // Cast created_at from Int64 to Timestamp
    let df = df.with_column(
        "created_at",
        cast(col("created_at"), DataType::Timestamp(TimeUnit::Second, None))
    )?;

    // Access the Arrow Schema inside DFSchema
    println!("{:#?}", df.schema().inner());

    Ok(())
}
```

**Output** — each field shows its four properties (name, data_type, nullable, metadata):

```text
Schema {
    fields: [
        Field { name: "user_id", data_type: Int64, nullable: true, metadata: {} },
        Field { name: "email", data_type: Utf8, nullable: true, metadata: {} },
        Field { name: "created_at", data_type: Timestamp(Second, None), nullable: true, metadata: {} },
        Field { name: "active", data_type: Boolean, nullable: true, metadata: {} },
    ],
    metadata: {},
}
```

:::{admonition} Why types matter
:class: tip
Correct types unlock query optimization. A `Timestamp` column enables date-range pruning, while the same bytes as `Int64` only support numeric comparisons.
:::

For comprehensive inspection patterns — human-readable display, programmatic field access, existence checks, and schema comparison — see [Inspecting and Validating Schemas](inspecting-and-validating.md).

### DFSchema Components

**The `inner` Arrow [`Schema`] serves compute kernels and file writers; `field_qualifiers` and `functional_dependencies` serve the [`LogicalPlan`] exclusively.**

To successfully bridge physical data and query planning, [`DFSchema`] must serve two distinct audiences. The `inner` Arrow [`Schema`] dictates the physical column contract — names, types, nullability — demanded by Arrow compute kernels and file writers. However, physical layouts lack relational context. [`DFSchema`] adds two query-planning layers exclusively for the [`LogicalPlan`]: `field_qualifiers` to track table provenance, and `functional_dependencies` to encode key constraints. Together, these three components form the complete schema carried by every [`LogicalPlan`] node:

| Component                 | Access via                                                                                                            | Purpose                                                                  |
| :------------------------ | :-------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------- |
| `inner` (Arrow Schema)    | [`df.schema().inner()`][`.inner()`] returns `&SchemaRef`, [`df.schema().as_arrow()`][`.as_arrow()`] returns `&Schema` | Field definitions (name, type, nullable, metadata)                       |
| `field_qualifiers`        | [`df.schema().iter()`][`.iter()`] yields `(Option<&TableReference>, &Arc<Field>)` pairs                               | Maps each field to its source table (e.g., `users.id` vs `orders.id`)   |
| `functional_dependencies` | Set during schema construction via `DFSchema::with_functional_dependencies(self, deps)`; read via [`df.schema().functional_dependencies()`][`.functional_dependencies()`] | Captures key relationships within a table for optimizer reductions       |

The Arrow [`Schema`] inside [`DFSchema`] originates from the data source. How it arrives depends on the source type:

| Source                                                   | Returns                           | Example                             |
| :------------------------------------------------------- | :-------------------------------- | :---------------------------------- |
| [`TableProvider::schema()`]                              | `SchemaRef` (Arrow)               | Custom data sources, catalog tables |
| [`ctx.read_parquet(...)`][`.read_parquet()`]             | Arrow Schema from file metadata   | Self-describing formats             |
| `CsvReadOptions::new().schema(&schema)`                  | Explicit Arrow Schema you provide | Text formats requiring schema       |
| [`Schema::new(vec![Field::new(...)])`][`Schema::new()`]  | Constructed Arrow Schema          | Programmatic schema definition      |

:::{admonition} Accessing the Arrow Schema
:class: note
When you call [`df.schema()`][`.schema()`], you get a `&DFSchema`. To access the underlying Arrow Schema, use [`.inner()`] (returns `&SchemaRef`) or [`.as_arrow()`] (returns `&Schema`). The Arrow Schema is what file writers (Parquet, IPC) and Arrow compute kernels expect.
:::

### Table Qualifiers

**Table qualifiers solve column ambiguity after joins by mapping each field to its source table — `users.id` and `orders.id` become distinct entries in the schema, even though the bare field name is the same.**

When two tables are joined and both have an `id` column, `col("id")` alone is ambiguous. Qualifiers resolve this: each field carries a [`TableReference`] that identifies its origin, so the query engine can match `col("users.id")` against the correct field. This mapping is what makes multi-table queries reliable — without it, any overlapping column name would be unresolvable.

Structurally, [`DFSchema`] stores qualifiers as a `Vec<Option<TableReference>>` parallel to the field list. `Some(TableReference)` means the field comes from a registered table; `None` means it's a computed expression (like `sum(amount)` or a `CASE` result) with no table origin. DataFusion sets qualifiers automatically when building a `TableScan` node — you rarely create them manually.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Int32Array, Float64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register tables — the table name becomes the qualifier
    let users_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    )?;
    ctx.register_batch("users", users_batch)?;

    let orders_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("user_id", DataType::Int32, false),
            Field::new("amount", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![99.50, 250.00])),
        ],
    )?;
    ctx.register_batch("orders", orders_batch)?;

    // Join produces qualified fields: users.id, users.name, orders.order_id, ...
    let joined = ctx.table("users").await?
        .join(
            ctx.table("orders").await?,
            JoinType::Inner,
            &["id"],
            &["user_id"],
            None,
        )?;

    // Inspect qualifiers: each field carries its source table
    for (qualifier, field) in joined.schema().iter() {
        println!(
            "qualifier: {:?}, field: {}",
            qualifier.map(|q| q.to_string()),
            field.name()
        );
    }

    Ok(())
}
```

For qualifier manipulation methods (`.strip_qualifiers()`, `.replace_qualifier()`, per-field qualifiers), see [Transforming Schemas — Aligning Qualifiers](schema-transformation.md#aligning-qualifiers).

### Functional Dependencies

**Functional dependencies enable the optimizer to remove redundant GROUP BY columns — by encoding which columns within a table are uniquely determined by other columns (typically through primary key or unique constraints).**

When `order_id` is a primary key, knowing `order_id = 1` automatically determines the `region` and `amount` for that order. Functional dependencies encode this "determines" relationship: `{order_id} → {region, amount}`. The `OptimizeProjections` optimizer rule reads these dependencies to shrink GROUP BY clauses — if `order_id` is already grouped, `region` and `amount` are redundant and can be removed, reducing the hash aggregation's memory footprint.

Unlike table qualifiers, which map fields across tables (cross-table disambiguation), functional dependencies describe relationships *within* a single table. [`DFSchema`] stores them as [`FunctionalDependencies`], derived automatically from primary key and unique constraints declared on the [`TableProvider`] when the `TableScan` node is built.

The example below registers a table with a primary key constraint and shows how the optimizer uses functional dependencies:

```rust
use datafusion::prelude::*;
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Int32Array, Float64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Constraints;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["east", "west", "east"])),
            Arc::new(Float64Array::from(vec![100.0, 200.0, 150.0])),
        ],
    )?;

    // Register with a PRIMARY KEY on order_id (column index 0)
    let constraints = Constraints::new_unverified(vec![
        datafusion::common::Constraint::PrimaryKey(vec![0]),
    ]);
    let provider = datafusion::datasource::MemTable::try_new(
        schema, vec![vec![batch]],
    )?.with_constraints(constraints);
    ctx.register_table("sales", Arc::new(provider))?;

    // GROUP BY order_id — since it's the PK, the optimizer knows
    // region and amount are functionally determined
    let df = ctx.sql(
        "SELECT order_id, region, amount 
        FROM sales 
        GROUP BY order_id, region, amount"
    ).await?;

    // The optimized plan shows reduced grouping
    let plan = df.logical_plan();
    println!("{}", plan.display_indent());

    Ok(())
}
```

The `OptimizeProjections` rule detects that `order_id` (the primary key) functionally determines `region` and `amount`. When all source indices of a functional dependency are present in the GROUP BY, the target indices are redundant and can be removed from the grouping set.

:::{admonition} Functional dependencies are structural, not statistical
:class: note
Functional dependencies express **logical guarantees** (primary key, unique constraint) — not statistical correlations. They are set via [`TableProvider::constraints()`] and converted to [`FunctionalDependencies`] during plan construction. For setting functional dependencies on a [`DFSchema`] directly, see [Transforming Schemas](schema-transformation.md).
:::

---

<!--NEXT SECTION: WE Work on the above!  -->

## Arrow Field Properties

**Three primary properties (`name`, `data_type`, `nullable`) drive query planning and execution; one secondary property (`metadata`) preserves semantic context for humans — together they form the per-column contract that the query engine enforces.**

Every column in a [`DataFrame`] is defined by four properties on the Arrow [`Field`] struct. The three primary properties must be defined — the query engine depends on them for kernel selection, type coercion, validity bitmaps, and column resolution. The secondary property (`metadata`) is a free-form `HashMap<String, String>` with no validation or constraints from Arrow — DataFusion preserves it through transformations but never uses it for optimization. Getting the primary properties right prevents schema mismatch errors; maintaining the secondary metadata enables data governance (lineage, PII classification, units of measure).

:::{admonition} Field is an Arrow type
:class: note
[`Field`] is defined in [`arrow::datatypes::Field`], not in DataFusion. [`DFSchema`] _wraps_ an Arrow [`Schema`] (which contains `Field[]`) and adds query-planning context on top. For how [`DFSchema`] adds qualifiers and functional dependencies, see [Schema Concepts — DFSchema](schema-concepts.md#dfschema-the-query-planning-layer).
:::

| Property                     | Role                                     | Operations Affected                              |
| :--------------------------- | :--------------------------------------- | :----------------------------------------------- |
| **Primary**                  | Essential for performant data processing | Query engine                                     |
| [`field.name`][`field`]      | Column identity                          | joins, selects, filters, group by, union_by_name |
| [`field.data_type`][`field`] | Storage & compute                        | kernel selection, type coercion, optimization    |
| [`field.nullable`][`field`]  | Null handling                            | validity bitmaps, null-safe operations           |
| **Secondary**                | _Essential for giving data meaning_      | _Human understanding & tuning_                   |
| [`field.metadata`][`field`]  | Semantic context                         | descriptions, units, lineage, PII classification |

The query engine uses the **primary properties** to plan and execute queries efficiently. **Metadata** (secondary property), while preserved throughout processing, serves a different purpose: it gives your data _meaning_ so you (or downstream systems) can interpret results correctly and make informed decisions about schema evolution.

:::{admonition} Implementation detail
:class: note
Primary metadata (`name`, `data_type`, `nullable`) consists of first-class typed fields on the Arrow `Field` struct. Secondary metadata is physically stored in a `metadata: HashMap<String, String>` — a free-form key-value map with no schema enforcement or validation from Arrow. DataFusion preserves this metadata through transformations like projections and aggregations.
:::

The same bytes (`1735689600_i64`) become a timestamp (`2025-01-01T00:00:00`) when the field declares `Timestamp` as its type. The following sections examine each property and its practical implications.

(column-names)=

### Column Names

**Column names are the primary identifier for every DataFrame operation — case-sensitive matching in the Rust API catches most developers off guard.**

The column [`field.name`][`field`] is the primary identifier for a column in the DataFrame API. Operations like [`.select()`], [`.with_column()`], and [`.union_by_name()`] all rely on the column name to perform their work.

Case-sensitivity is the most common source of schema mismatch errors. `col("Region")` and `col("region")` reference different columns — DataFusion's DataFrame API performs exact string matching, with no normalization:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "region" => ["east", "west"],
        "amount" => [100, 200]
    )?;

    // Exact case match — "region" matches the schema field name
    let result = df.clone()
        .filter(col("region").eq(lit("east")))?
        .collect().await?;
    assert_batches_eq!(
        &[
            "+--------+--------+",
            "| region | amount |",
            "+--------+--------+",
            "| east   | 100    |",
            "+--------+--------+",
        ],
        &result
    );

    // "Region" (uppercase R) would reference a different, non-existent column
    // and fail during query analysis — col("Region") != col("region")

    Ok(())
}
```

:::{admonition} SQL parser behavior differs
:class: note
DataFusion's SQL parser normalizes unquoted identifiers to lowercase by default. When mixing DataFrame API calls with SQL queries, be aware of this distinction.
:::

:::{admonition} Best practice
:class: tip
Enforce a consistent naming convention (e.g., all **snake_case** or **camelCase**) at your ingestion boundary.
:::

(column-order)=

### Column Order

**Column order in the schema defines the physical position of fields — but DataFusion's DataFrame API resolves columns by name, making most operations resilient to upstream reordering.**

The Arrow [`Schema`] stores fields in an ordered list, and `RecordBatch` columns follow that order. However, the DataFrame API uses column names — not positions — for resolution. Operations like [`.select()`], [`.filter()`], and [`.join()`] reference columns by name via `col("...")`, so reordering fields in the source schema does not break downstream transformations.

Where column order *does* matter: positional operations like [`.union()`] match columns by index, not name. If the source schema reorders, a positional union produces silently wrong results. Name-based [`.union_by_name()`] eliminates this risk.

:::{admonition} Best practice
:class: tip
Prefer name-based operations ([`.union_by_name()`], `col("...")`) over positional access. This makes pipelines resilient to upstream schema evolution — new columns, reordered columns, and renamed sources are handled gracefully.
:::

:::{admonition} SQL equivalent: UNION BY NAME
:class: note
DataFusion's SQL parser supports `UNION BY NAME` syntax (inspired by [DuckDB]), which matches columns by name rather than position. Both the DataFrame API and SQL produce the same `LogicalPlan`.
:::

(column-count)=

### Column Count

**The number of fields in a schema determines compatibility between DataFrames — strict operations require identical counts, while flexible operations handle differences by filling missing columns with NULL.**

Schema field count is a compatibility constraint when combining DataFrames. Positional operations like [`.union()`] require identical column counts — any mismatch fails at plan-build time. Name-based operations like [`.union_by_name()`] are more flexible: missing columns are filled with NULL, supporting schema evolution where new columns appear over time or sources have different field sets.

This count flexibility extends to joins as well: the output schema's field count is the sum of both input schemas (minus join keys in some join types), and each field retains its source qualifier for disambiguation.

:::{admonition} Schema evolution pattern
:class: tip
When combining DataFrames from sources that evolve independently (e.g., daily exports with new columns), prefer [`.union_by_name()`]. Missing columns are filled with NULL — new columns appear for historical rows without breaking the pipeline. For the operational details of `.union_by_name()`, see [Transforming Schemas](schema-transformation.md).
:::

:::{admonition} Type mismatches require explicit resolution
:class: important
Count differences are handled automatically, but type mismatches are not. When the same column name appears with different types (e.g., `Int32` vs `Int64`), DataFusion's [`TypeCoercion`] analyzer attempts to find a common type. If no safe coercion path exists, the query fails during analysis. See [Type Coercion](type-coercion.md).
:::

(column-types)=

### Column Types

**Types drive planning-time validation, coercion, and operator selection — prefer widening over narrowing.**

In DataFusion's DataFrame API, every column must have a specific [Apache Arrow `DataType`][arrow dtype] that determines its storage format and computational behavior.

**Common Arrow data types in DataFusion:**

| Category           | Arrow Types                                                                | Example Values                        |           Common Use Cases            |
| :----------------- | -------------------------------------------------------------------------- | ------------------------------------- | :-----------------------------------: |
| **Integers**       | `Int8`, `Int16`, `Int32`, `Int64`<br>`UInt8`, `UInt16`, `UInt32`, `UInt64` | `42`, `-100`, `0`                     |        IDs, counts, quantities        |
| **Floating-Point** | `Float32`, `Float64`                                                       | `3.14`, `-0.001`                      | Measurements, scientific data, ratios |
| **Decimal**        | `Decimal128(precision, scale)`                                             | `99.99`, `1234.5678`                  |   Financial data, currency, prices    |
| **Strings**        | `Utf8`, `LargeUtf8`                                                        | `"hello"`, `"データ融合"`             |    Names, descriptions, categories    |
| **Temporal**       | `Date32`, `Date64`<br>`Timestamp(unit, timezone)`                          | `2024-01-15`<br>`2024-01-15 14:30:00` |          Event times, dates           |
| **Boolean**        | `Boolean`                                                                  | `true`, `false`                       |           Flags, conditions           |
| **Binary**         | `Binary`, `LargeBinary`                                                    | `[0x12, 0x34]`                        |           Raw data, hashes            |
| **Nested Types**   | `Struct(Fields)`, `List(Field)`                                            | `{"a": 1}`, `[1, 2, 3]`               |  JSON/Parquet data, complex objects   |

For a complete reference of all supported types, see the [SQL Data Types guide](../../user-guide/sql/data_types.md).

#### Nested Types

Beyond primitive types, DataFusion fully supports Arrow's nested types: [`List`], [`Struct`], [`Map`], and [`Union`]. These enable complex data structures like JSON objects, arrays of values, or key-value maps — common in Parquet files and semi-structured data.

Nested types follow the same schema rules but add complexity in coercion and comparison. For a comprehensive reference on nested type structures and memory layouts, see the [Apache Arrow Data Types documentation][arrow data types].

(schema-field-nullability)=

### Nullability

**The nullable flag controls validity bitmaps and schema merging — when schemas combine, nullability always widens to the most permissive side.**

The [`field.nullable`][`field`] flag declares whether a column may contain NULL values. Arrow uses a **validity bitmap** — one bit per row, separate from the data buffer — to track NULLs efficiently (no per-null object overhead like Python's `None`).

When schemas combine (for example, via [`.union_by_name()`] or [`.join()`]), DataFusion follows a widening rule:

:::{admonition} The Golden Rule of Nullability
:class: important
If a column is nullable in any of the input schemas, it will be nullable in the output schema.
:::

A non-nullable column can always be represented in a nullable one, but not the other way around. The example below shows how nullability propagates when DataFrames are combined:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df_a = dataframe!(
        "id" => [1, 2],
        "name" => ["Alice", "Bob"]
    )?;

    let df_b = dataframe!(
        "id" => [3],
        "name" => [Some("Carol")],
        "email" => [Some("carol@example.com")]
    )?;

    // union_by_name: "email" is missing in df_a → filled with NULL → nullable
    let combined = df_a.union_by_name(df_b)?;

    for field in combined.schema().fields() {
        println!("{}: nullable={}", field.name(), field.is_nullable());
    }

    Ok(())
}
```

For a deeper discussion of how NULL values behave in expressions, filters, and joins, see [Handling Null Values](../Concepts/null-handling.md).

### Metadata

**Metadata preserves semantic context — column descriptions, units, lineage, and PII classifications — that the query engine does not use for optimization but that is essential for data governance.**

The [`field.metadata`][`field`] property provides semantic annotations beyond types. DataFusion **preserves** metadata when reading from formats like Parquet that embed it, and through transformations like projections and aggregations.

Unlike the three primary properties (`name`, `data_type`, `nullable`) which are first-class typed fields on the Arrow `Field` struct, metadata is physically stored as a `HashMap<String, String>` — a free-form key-value map with no schema enforcement or validation from Arrow. Any string key and value are accepted without constraints.

**Common metadata use cases:**

| Use Case           | Example Key     | Example Value                     |
| ------------------ | --------------- | --------------------------------- |
| Column description | `description`   | `"User's primary email address"`  |
| Units of measure   | `unit`          | `"milliseconds"`                  |
| Data lineage       | `source_system` | `"orders_db.public.transactions"` |
| PII classification | `pii_level`     | `"sensitive"`                     |

**Two levels of metadata:**

DataFusion supports metadata at two levels, both accessible via [`DFSchema`]:

- **Schema-level**: [`df.schema().metadata()`][`dfschema::metadata`] — annotations for the entire dataset
- **Field-level**: [`field.metadata()`][`field.metadata()`] — annotations per column (accessed via [`df.schema().fields()`] or [`df.schema().inner().fields()`][`.inner()`])

The example below creates a schema with field-level metadata, registers it as a table, and reads the metadata back through the DataFrame API:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Float64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Field-level metadata: annotate columns with semantic context
    let mut temp_meta = HashMap::new();
    temp_meta.insert("unit".to_string(), "celsius".to_string());
    temp_meta.insert("pii_level".to_string(), "none".to_string());

    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("temperature", DataType::Float64, true)
            .with_metadata(temp_meta),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["sensor_a"])),
            Arc::new(Float64Array::from(vec![22.5])),
        ],
    )?;

    ctx.register_batch("readings", batch)?;
    let df = ctx.table("readings").await?;

    // Read field-level metadata back through the DataFrame schema
    let temp_field = df.schema().field_with_unqualified_name("temperature")?;
    assert_eq!(temp_field.metadata().get("unit"), Some(&"celsius".to_string()));
    assert_eq!(temp_field.metadata().get("pii_level"), Some(&"none".to_string()));

    Ok(())
}
```

For the full metadata API and inspection patterns, see [Inspecting and Validating Schemas](inspecting-and-validating.md).

---

## Conclusion

**[`DFSchema`] combines Arrow's per-column contract with query-planning context — table qualifiers and functional dependencies — to form the complete schema that every [`LogicalPlan`] node validates against.**

The Arrow [`Field`] properties (`name`, `data_type`, `nullable`, `metadata`) define what each column contains and how the query engine processes it. [`DFSchema`]'s additions — `field_qualifiers` for disambiguating columns across tables and `functional_dependencies` for enabling optimizer reductions — connect the raw data description to the query plan. Together, these components form the structural contract accessible via [`df.schema()`][`.schema()`].

:::{admonition} Next steps
:class: seealso
- [Schema Concepts](schema-concepts.md) — ownership chain, propagation, and the big-picture lifecycle
- [Type Coercion](type-coercion.md) — how DataFusion reconciles types when they don't match
- [Transforming Schemas](schema-transformation.md) — qualifier manipulation, schema combining, and functional dependency methods
- [Inspecting and Validating Schemas](inspecting-and-validating.md) — hands-on schema inspection and validation patterns
:::

<!--
 TODO: IF YOU REACHED THIS POINT, ABSTRACT SHOULD BE WRITTEN! 
-->

<!-- Literature references -->