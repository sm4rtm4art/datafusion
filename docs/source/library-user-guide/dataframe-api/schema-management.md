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

# Schema Management with DataFrameSchema

**The “health” phase of the DataFrame lifecycle: inspect, validate, and evolve schema.**

Schema management defines the structural contract of your data—names, types, and nullability—as it flows through DataFusion. Explicit schemas are critical for both correctness and performance, enabling the optimizer to push down predicates, select vectorized kernels, and prevent silent schema drift. You manage this contract via the [`DFSchema`] API, which wraps underlying Arrow types with the query-planning context needed for robust, predictable execution.

In this guide, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, method notation follows a consistent pattern:

- **DataFrame methods** use `df.method()` (for example, `df.select(...)`)
- **DFSchema method**s use `df.schema().method()` (for example, `df.schema().fields()`)
- **Associated functions** use `DFSchema::method()` (for example, `DFSchema::try_from(...)`).
- **Standalone functions** use `function()` (for example, `col()`), and constructors use `Type::new()` (for example, `SessionContext::new()`).

:::

</details>

```{contents}
:local:
:depth: 2
```

## Introduction

**Schema management connects data modeling, query planning, and execution correctness across the DataFusion ecosystem.**

In analytical systems, schema is the contract that binds source data, planner decisions, and runtime behavior. In DataFusion, that contract flows from data sources into the [`LogicalPlan`] and surfaces as [`DFSchema`] on each [`DataFrame`], where you inspect, validate, and evolve structure safely.

DataFusion uses the term "schema" for four distinct concepts. They fall into two layers:

### Where Schemas Come From

**A schema is the structural contract of your data—it defines column names, types, and constraints that enable the query engine to plan and execute efficiently.**

Without a schema, the query engine cannot validate your operations, optimize execution, or guarantee consistent results. Every DataFrame, every table, and every query plan carries a schema that describes "What shape and character are these data?"

### The Schema Ownership Flow

Understanding where schemas live—and how they flow through the system—is key to working with DataFusion. The diagram below illustrates the three stages:

1. **Origin (Catalog)**: <br>
   Registered tables store their Arrow [`Schema`] in the catalog via [`TableProvider`]. This is the source of truth for table definitions.

2. **Ownership (LogicalPlan)**: <br>
   When you build a query, the [`LogicalPlanBuilder`] takes the Arrow [`Schema`] from the [`TableProvider`], wraps it in a [`DFSchema`] (adding table qualifiers), and embeds it in the plan node (e.g., [`TableScan.projected_schema`]). Each transformation creates a new plan node with its own derived schema.

3. **Access (DataFrame)**: <br>
   The DataFrame wraps the [`LogicalPlan`] and delegates [`df.schema()`] to [`LogicalPlan.schema()`]. The DataFrame itself does not store the schema—it lives in the plan.

```text
DataFusion Schema Ownership Flow

┌───────────────────────────────────────────────────────┐
│ 1. SCHEMA ORIGIN (SessionState / Catalog)             │
│    Source of truth for *registered* tables.           │
│                                                       │
│   SessionState                                        │
│     └── CatalogProviderList                           │
│          └── CatalogProvider ("datafusion")           │
│               └── SchemaProvider ("public")           │
│                    └── TableProvider ("users")        │
│                         └── schema() -> Arrow Schema  │
└───────────────────────────────────────────────────────┘
                               │
                               ▼
                       (Plan Creation)
            Arrow Schema is wrapped in DFSchema
            (adding qualifiers) and embedded in the plan.
                               │
                               ▼
┌───────────────────────────────────────────────────────┐
│ 2. SCHEMA OWNER (LogicalPlan)                         │
│    Source of truth for the *current transformation*.  │
│                                                       │
│   LogicalPlan::TableScan                              │
│     ├── table_name: "users"                           │
│     └── projected_schema: DFSchema                    │
│              ├── inner: Arc<Schema>  (Arrow Schema)   │
│              ├── field_qualifiers    (TableReference) │
│              └── functional_dependencies              │
└───────────────────────────────────────────────────────┘
                               │
                               ▼
                       (API Wrapper)
            The DataFrame wraps the plan to provide
            a user-friendly API.
                               │
                               ▼
┌──────────────────────────────────────────────────────┐
│ 3. USER API (DataFrame)                              │
│                                                      │
│   DataFrame                                          │
│     ├── session_state: SessionState (Config Snapshot)│
│     └── plan: LogicalPlan (Holds the DFSchema)       │
│                                                      │
│   df.schema() ────delegates────► plan.schema()       │
└──────────────────────────────────────────────────────┘
```

---

### Types of Schemas: The Schema Dilemma

**In data systems, a schema is fundamentally a structural contract—a blueprint defining how data is organized. However, the scope of this contract changes drastically depending on the architectural layer.**

DataFusion uses the term "schema" for four distinct concepts. They fall into two layers:

**Layer 1 — DataFusion (Query Planning)**

| Type                                    | Purpose                                                                                                                                                                                    | Accessed via                                                       |
| :-------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------- |
| **Catalog Schema** ([`SchemaProvider`]) | A namespace in the catalog hierarchy (like `"public"` in Postgres). Contains registered tables.                                                                                            | `SessionState.catalog_list` → `CatalogProvider` → `SchemaProvider` |
| **DataFrame Schema** ([`DFSchema`])     | Wraps an Arrow `Schema` and adds **table qualifiers** so the planner can resolve ambiguous column references (e.g., `users.id` vs `orders.id` in a join). Embedded in the [`LogicalPlan`]. | [`df.schema()`] returns `&DFSchema`                                |

**Layer 2 — Apache Arrow (Data Description)**

| Type                                | Purpose                                                                                                                                            | Accessed via                                    |
| :---------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------- |
| **Arrow Schema** ([`Schema`])       | The generic columnar schema from Apache Arrow. Defines field names, data types, and nullability. Knows nothing about table names or query context. | [`TableProvider::schema()`] returns `SchemaRef` |
| **Arrow SchemaRef** ([`SchemaRef`]) | Simply `Arc<Schema>`—a reference-counted pointer for passing schemas cheaply between functions without cloning.                                    | `df.schema().inner()` returns `&SchemaRef`      |

#### Why `DFSchema` instead of Arrow's `Schema`?

Arrow's `Schema` describes _data_. `DFSchema` describes the _plan_—it adds table qualifiers for column resolution during query planning. When you need the underlying Arrow schema, use [`.inner()`] (returns `&SchemaRef`) or [`.as_arrow()`] (returns `&Schema`).

#### Logical vs Physical Schema:

[`df.schema()`][`.schema()`] returns the **logical** schema—what the plan _expects_ to produce. The actual physical memory layout during execution (e.g., dictionary encoding for strings, or nullable flags adjusted by optimizer passes) may differ. This is handled transparently by the physical plan; you rarely need to worry about it unless implementing a custom [`TableProvider`].

### How Schemas are Determined

DataFusion determines the initial schema in one of three ways, depending on your data source:

1.  **Self-Describing Formats ([Parquet], Avro, Arrow):**
    The schema is embedded in the file metadata. Types are known instantly at scan time.
2.  **Text Formats (CSV, JSON):**
    Types must be either **provided explicitly** (recommended) or **inferred** from a data sample (risk of **schema drift**—see below).
3.  **Custom Sources (TableProvider):**
    The source of truth is the [`TableProvider::schema()`]-method implemented by the provider. This contract must remain stable to ensure predictable query behavior.

For a deep dive into the underlying [Apache Arrow] type system, see the [Arrow Schema Specification][`arrow schema`].

> **Schema Drift:**
> Schema drift occurs when inferred types change silently across runs because the underlying data evolves. For example, a column inferred as `Int32` from the first 1000 rows may later contain values exceeding `Int32` range, or a previously all-numeric column may start containing strings. Because inference is sampling-based, these changes go undetected until they cause runtime errors or silent data corruption. Explicit schemas eliminate drift entirely—this is why they are recommended for production pipelines.

> **DataFrame vs SQL:**
> Both APIs produce the same [`DataFrame`] containing the same [`LogicalPlan`] with identical schemas. The DataFrame API provides compile-time visibility into schema changes—each method returns a new [`DataFrame`] whose schema you can inspect programmatically before execution.

---

## The Anatomy of a DataFusion DataFrame Schema

**Dissecting the schema reveals how DataFusion structures data: from the DataFrame down to each field's type, nullability, and meaning.**

Every DataFrame carries a [`DFSchema`] describing its columns and their properties. `DFSchema` wraps an Arrow [`Schema`] and adds query-planning context (table qualifiers and functional dependencies). Understanding the four field properties—name, type, nullability, and metadata—is key to diagnosing schema mismatch errors and handling data safely and performantly.

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

The example below creates a DataFrame using the `dataframe!` macro, casts a column, and inspects the resulting schema. To access the underlying Arrow Schema, use [`.inner()`] (returns [`&SchemaRef`]) or [`.as_arrow()`] (returns [`&Schema`]).

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

    // Print the schema structure
    println!("{:#?}", df.schema().inner());

    Ok(())
}
```

Output — each field shows its four properties (name, data_type, nullable, metadata):

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

> **Why types matter:**
> Correct types unlock query optimization. A `Timestamp` column enables date-range pruning, while the same bytes as `Int64` only support numeric comparisons.

---

### Schema Field Properties

**Understanding schema fields is essential for debugging mismatches and designing robust pipelines.**

In the hierarchy of the DataFrame Schema, we are now at the [`Field`] level.

> **Important:** > `Field` is an **Arrow type** ( [`arrow::datatypes::Field`]), not a DataFusion type. DFSchema _wraps_ an Arrow [`Schema`] and adds query-planning context on top.

```text
┌───────────────────────────────────────────────────────────┐
│ DataFrame                                                 │
│   └── LogicalPlan                                         │
│            └── DFSchema                                   │
│                 ├── inner: Arc<Schema>   ← Arrow Schema   │
│                 │        └── Field[]     ← CURRENT TOPIC  │
│                 │             ├── name    (YOU ARE HERE!) │
│                 │             ├── data_type               │
│                 │             ├── nullable                │
│                 │             └── metadata                │
│                 ├── field_qualifiers: Vec<TableReference> │
│                 └── functional_dependencies               │
└───────────────────────────────────────────────────────────┘
```

**What DFSchema adds to Arrow Schema:**

| Component                 | Access via                                                                                                            | Purpose                                                                  |
| :------------------------ | :-------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------- |
| `inner` (Arrow Schema)    | [`df.schema().inner()`][`.inner()`] returns `&SchemaRef`, [`df.schema().as_arrow()`][`.as_arrow()`] returns `&Schema` | Field definitions (name, type, nullable, metadata)                       |
| `field_qualifiers`        | [`df.schema().iter()`][`.iter()`] yields `(Option<&TableReference>, &Arc<Field>)` pairs                               | Track which table each field came from (e.g., `users.id` vs `orders.id`) |
| `functional_dependencies` | [`df.schema().with_functional_dependencies()`][`.with_functional_dependencies()`] to set; internal to optimizer       | Enable optimizations based on key relationships (e.g., primary keys)     |

**Where Arrow Schema originates:**

| Source                                                  | Returns                           | Example                             |
| :------------------------------------------------------ | :-------------------------------- | :---------------------------------- |
| [`TableProvider::schema()`]                             | `SchemaRef` (Arrow)               | Custom data sources, catalog tables |
| [`ctx.read_parquet(...)`][`.read_parquet()`]            | Arrow Schema from file metadata   | Self-describing formats             |
| [`ctx.read_csv(...).schema(...)`]                       | Explicit Arrow Schema you provide | Text formats requiring schema       |
| [`Schema::new(vec![Field::new(...)])`][`schema::new()`] | Constructed Arrow Schema          | Programmatic schema definition      |

> **Key insight:**<br> When you call [`df.schema()`], you get a `&DFSchema`. To access the underlying Arrow Schema, use [`.inner()`] (returns `&SchemaRef`) or [`.as_arrow()`] (returns `&Schema`). The Arrow Schema is what file writers (Parquet, IPC) and Arrow compute kernels expect.

Each column in a DataFrame is defined by four properties that control how data is stored, accessed, and validated. These properties determine how your data is interpreted—for example:
The same bytes (`1735689600_i64`) become a timestamp (`2025-01-01T00:00:00`) when the field declares `Timestamp` as its type.

| Property                     | Role                                     | Operations Affected                              |
| :--------------------------- | :--------------------------------------- | :----------------------------------------------- |
| **Primary**                  | Essential for performant data processing | Query engine                                     |
| [`field.name`][`field`]      | Column identity                          | joins, selects, filters, group by, union_by_name |
| [`field.data_type`][`field`] | Storage & compute                        | kernel selection, type coercion, optimization    |
| [`field.nullable`][`field`]  | Null handling                            | validity bitmaps, null-safe operations           |
| **Secondary**                | _Essential for giving data meaning_      | _Human understanding & tuning_                   |
| [`field.metadata`][`field`]  | Semantic context                         | descriptions, units, lineage, PII classification |

The query engine uses the **primary properties** to plan and execute queries efficiently. **Metadata** (secondary property), while preserved throughout processing, serves a different purpose: it gives your data _meaning_ so you (or downstream systems) can interpret results correctly and make informed decisions about schema evolution.

In the remainder of this section, we will focus on four practical aspects of schema management:

- [1. Column Names](#column-names)
- [2. Column Order](#column-order)
- [3. Column Count](#column-count)
- [4. Column Types](#column-types)

(column-names)=

#### 1. Column Names

The column [`field.name`][`field`] is the primary identifier for a column in the DataFrame API. Operations like [`.select()`], [`.with_column()`], and [`.union_by_name()`] all rely on the column name to perform their work.

> **The #1 Schema Mismatch Cause:**
> In the Rust DataFrame API, column names are **case-sensitive strings**. `col("Region")` and `col("region")` reference _different_ columns—this catches many users off guard.

> **Note:**
> This differs from DataFusion's SQL parser, where unquoted identifiers are normalized to lowercase by default. When mixing DataFrame API calls with SQL queries, be aware of this distinction.

> **Best Practice:**
> Enforce a consistent naming convention (e.g., all **snake_case** or **camelCase**) at your ingestion boundary.

(column-order)=

#### 2. Column Order

DataFusion's DataFrame API is **name-based, not positional**. For operations like [`.union_by_name()`], the physical column order doesn't matter—DataFusion aligns columns by name, making pipelines resilient to upstream ordering changes.

> **DataFrame API Advantage:**
> Unlike **traditional** [`UNION ALL`] which requires matching column positions, the DataFrame API's name-based approach is inherently safer. You don't need to worry about upstream schema reordering breaking your pipeline.

> **SQL equivalent:** `UNION BY NAME`
> DataFusion's SQL-API parser also supports `UNION BY NAME` syntax (inspired by [DuckDB]). Both produce the same `LogicalPlan`.
>
> ```sql
> SELECT *
> FROM table_a
> UNION BY NAME
> SELECT *
> FROM table_b
> ```

(column-count)=

#### 3. Column Count

When combining DataFrames with [`.union_by_name()`], differences in column count are handled gracefully: missing columns are filled with NULL values. This deliberate behavior supports schema evolution—new columns appear with NULL for historical rows, and dropped columns remain explicit rather than causing silent failures.

> **Note:**
> This flexibility applies to [`.union_by_name()`] only. The positional [`.union()`] requires **identical column counts** in both DataFrames—any mismatch will fail during planning.

> **Important:**
> While [`.union_by_name()`] handles _missing_ columns automatically, it does **not** silently handle _type mismatches_ for columns that exist in both DataFrames. When the same column name appears with different types (e.g., `Int32` vs `Int64`), DataFusion's [type coercion analyzer][`typecoercion`] attempts to find a common type. If no safe coercion path exists, the query will fail during analysis—forcing you to be explicit about how to resolve the ambiguity.

(column-types)=

#### 4. Column Types

Types drive planning-time validation, coercion, and operator selection—prefer widening over narrowing. In DataFusion's DataFrame API, every column must have a specific [Apache Arrow `DataType`][arrow dtype] that determines its storage format and computational behavior.

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

Beyond primitive types, DataFusion fully supports Arrow's nested types: [`List`], [`Struct`], [`Map`], and [`Union`]. These enable complex data structures like JSON objects, arrays of values, or key-value maps—common in Parquet files and semi-structured data.

Nested types follow the same schema rules but add complexity in coercion and comparison. For a comprehensive reference on nested type structures and memory layouts, see the [Apache Arrow Data Types documentation][arrow data types].

---

### Schema Field Features

Properties define _what_ a column is (name, type). Features define _how_ it behaves. Nullability directly affects query execution—validity bitmaps, null-safe operations, and schema merging rules. Metadata provides semantic meaning without affecting computation—it travels with the data, but the query engine doesn't use it for optimization.

(schema-field-nullability)=

#### Nullability

The [`field.nullable`][`field`] flag—the third property in our Schema Field Properties table—is a critical part of a field's type definition. When merging schemas (for example, via [`.union_by_name()`]), DataFusion follows a simple, safe rule:

> **The Golden Rule of Nullability:**
> If a column is nullable in any of the input schemas, it will be nullable in the output schema.

This is a widening conversion: a non‑nullable column can always be represented in a nullable one, but not the other way around.

> **How Arrow stores NULLs:** Arrow tracks nulls with a **validity bitmap**—one bit per row, separate from the data buffer. This makes null checks fast and memory-efficient (no per-null object overhead like Python's `NONE`). The data buffer still exists at null positions, but the validity bit marks it as "ignore this value."

For a deeper discussion of how NULL values behave in expressions, filters, and joins, see:

- [Concepts: Handling Null Values](./concepts.md#handling-null-values).

#### Metadata

The [`field.metadata`][`field`] property—the fourth in our Schema Field Properties table—provides semantic annotations beyond types: column descriptions, units of measure, data lineage, or custom tags. DataFusion **preserves** metadata when reading from formats like Parquet that embed it.

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

For the full metadata API, see the [`DFSchema` documentation][`dfschema`].

---

## Type Coercion: Auto-Alignment vs Explicit Casting

**Type coercion determines when DataFusion reconciles schema differences automatically and when you must cast explicitly.**

When you mix types in expressions or combine DataFrames, DataFusion must resolve type mismatches. The rules differ by context:

- **Expressions** (select, filter, with_column):
  Types widen automatically for convenience
- **Set operations** (union, except, intersect):
  Types must align explicitly for safety
- **Joins**:
  Keys auto-coerce, but result columns follow expression rules (see:[`TypeCoercion`])

This section covers the coercion hierarchy and when each mode applies.

**The Golden Rule of Type Casting**:
Always widen types (e.g., `Int32 → Int64`) rather than narrow them to prevent data loss. Narrowing (e.g., `Int64 → Int32`) risks silent data corruption unless you have explicitly proven that no values will be truncated.

### Mode 1: Automatic Coercion in Expressions

For convenience and intuitive use, DataFusion automatically promotes types to a common, wider type when they are mixed within an expression. This applies to functions like [`.select()`], [`.with_column()`], and [`.filter()`]. The promotion is always "widening" and follows the safe upcasting paths defined in the Type Coercion Hierarchy to prevent data loss.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a DataFrame with Int32 and Int64 columns
    let df = dataframe!(
        "int32_col" => [1_i32, 2_i32],
        "int64_col" => [100_i64, 200_i64]
    )?;

    // Adding Int32 + Int64 produces Int64 (automatic widening)
    let result = df.select(vec![
        (col("int32_col") + col("int64_col")).alias("sum")
    ])?;

    // Verify the result type is Int64 (widened from Int32)
    let field = result.schema().field_with_name(None, "sum")?;
    assert_eq!(field.data_type(), &DataType::Int64);

    // See the computed values
    let batches = result.collect().await?;
    assert_batches_eq!(
        &[
            "+-----+",
            "| sum |",
            "+-----+",
            "| 101 |",  // 1 + 100
            "| 202 |",  // 2 + 200
            "+-----+",
        ],
        &batches
    );

    Ok(())
}
```

### Mode 2: Strict Matching for Set Operations

For safety and to prevent silent data corruption, **set operations** like [`.union()`], [`.except()`], and [`.intersect()`] require columns in corresponding positions to have compatible types.

If the types do not match exactly, DataFusion's type coercion analyzer will attempt to find a common type. However, when no safe coercion path exists, you'll need to cast explicitly. This is a deliberate design choice—it forces you to be explicit about how to resolve type ambiguity.

> **Note on Joins:**
> Join keys are an exception—DataFusion automatically coerces join keys to a common type (e.g., `Int32 = Int64` becomes `Int64 = Int64`). This happens transparently via the [`TypeCoercion`] analyzer rule, so you rarely need to cast join keys manually.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {

    let df_i32 = dataframe!("id" => [1_i32, 2_i32])?;
    let df_i64 = dataframe!("id" => [3_i64, 4_i64])?;

    // This will FAIL because Int32 and Int64 are not an exact match.
    // let bad = df_i32.union(df_i64)?;

    // The Fix: explicitly cast one of the columns to match the other.
    // Using the cast() free function from the prelude - simple and no extra imports needed.
    let df_i32_fixed = df_i32.clone().with_column(
        "id",
        cast(col("id"), DataType::Int64)
    )?;

    // This now works because both `id` columns are Int64.
    let combined = df_i32_fixed.union(df_i64)?;

    // Verify: all four rows are present, all as Int64
    // Note: union does not guarantee row order, so we use sorted comparison
    let batches = combined.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+",
            "| id |",
            "+----+",
            "| 1  |",
            "| 2  |",
            "| 3  |",
            "| 4  |",
            "+----+",
        ],
        &batches
    );

    Ok(())
}
```

### Type Coercion Hierarchy

**Coercion** is the automatic conversion of one type to another to make an operation valid. When you write `Int32 + Int64`, DataFusion _coerces_ the `Int32` to `Int64` before adding—you don't have to cast explicitly.

The diagram below shows safe upcasting paths—conversions that preserve data without loss. Arrows indicate the direction of automatic widening.

```text
┌ Numeric Widening ───────────────────────────────────────────────┐
│                                                                 │
│ Signed:   Int8  ──► Int16  ──► Int32  ──► Int64 ───┐            │
│                                                    │            │
│ Unsigned: UInt8 ──► UInt16 ──► UInt32 ──► UInt64 ──┤            │
│                                                    │            │
│                                       (Precision)  ▼   (Range)  │
│                                       Decimal128 ──┬──► Float64 │
│                                                    │            │
│ Floats:             Float16 ──► Float32 ───────────┘            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌ Temporal Widening ──────────────────────────────────────────────┐
│                                                                 │
│ Dates:    Date32 ──► Date64 ──► Timestamp (ns) ──► +Timezone    │
│                                                                 │
│ Times:    Time32 ──► Time64                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌── Variable Width Compatibility (Views & Large Types) ───────────┐
│                                                                 │
│   Strings:  Utf8   ◄──► LargeUtf8   ◄──► Utf8View               │
│                                                                 │
│   Binary:   Binary ◄──► LargeBinary ◄──► BinaryView             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

> **Note:** <br>
> The `Decimal128 → Float64` path in this diagram widens **range** but can lose **precision**. `Decimal128` provides exact arithmetic (up to 38 digits), while `Float64` offers only ~15-17 significant digits. For financial or high-precision data, prefer keeping values as `Decimal128` and only convert to `Float64` when approximate results are acceptable (e.g., charting, statistical aggregates).

#### Key rules:

**Data Type interactions**

- **Numeric**:
  Integers widen to the largest container (`Int32 + Int64 → Int64`). Mixed with floats, the result is `Float64`. Decimals preserve precision when combined with integers.

- **Temporal**:
  Dates promote to `Timestamp` for comparisons and arithmetic. Timezones must match—cast explicitly to align them. `Date64` is rarely used; dates typically coerce directly to `Timestamp(ns)`.

- **Strings**:
  `Utf8`, `LargeUtf8`, and `Utf8View` auto-align via planner-inserted casts. No automatic coercion from string columns to numeric/temporal types (though string _literals_ may be coerced in some contexts).

- **Boolean**:
  Does not auto-coerce to numeric. Use explicit `CAST(bool_col AS Int32)` if needed.

- **NULL**:
  Adopts the other operand's type in expressions—this is safe widening. A standalone `NULL` remains untyped until context determines it.

**The core rules:**

- **Expressions** auto-widen for convenience (`Int32 + Int64 → Int64`)
- **Set operations** require explicit alignment for safety
- **Always widen, never narrow** — narrowing risks silent data loss

### Further Reading

Now that you understand schema structure and type coercion, you're ready to work with schemas in practice:

- **[DataFrame Methods That Change the Schema](#dataframe-methods-that-change-the-schema)** — Add, rename, project, drop columns
- **[Inspecting Schemas](#inspecting-schemas)** — Display and programmatically query schema properties
- **[Creating Schemas](#creating-schemas)** — Construct schemas explicitly for type safety
- **[Transforming Schemas](#transforming-schemas)** — Modify qualifiers, combine schemas, handle nullability
- **[Validating Schemas](#validating-schemas)** — Check existence, compare schemas, verify compatibility
- **[Concepts: Handling Null Values](./concepts.md#handling-null-values)** — Deep dive into NULL behavior in expressions, filters, and joins
- **Type coercion internals:** [`TypeCoercion`] analyzer behavior for nested and scalar types

---

## DataFrame Methods That Change the Schema

**Every DataFrame method that adds, removes, renames, or reshapes columns creates a new [`LogicalPlan`] node with its own [`DFSchema`]—the original DataFrame is never mutated.**

This is the main interface for schema changes at the DataFrame level. Most schema-modifying methods (`.select()`, `.select_columns()`, `.drop_columns()`, `.with_column()`, `.with_column_renamed()`) build a new projection plan. Methods like [`.unnest_columns()`] use dedicated logical plan nodes, but still produce a new DataFrame with a new output schema. You never need to construct `DFSchema` manually for these operations.

**SQL equivalent:** Most changes map to `SELECT expr AS name, ... FROM ...`; nested reshaping maps closer to `UNNEST`-style operations.

| Method                     | Signature                  | Schema Effect                            |
| :------------------------- | :------------------------- | :--------------------------------------- |
| [`.select()`]              | `(exprs: Vec<Expr>)`       | Keep, reorder, or compute columns        |
| [`.select_columns()`]      | `(columns: &[&str])`       | Keep columns by name (string shorthand)  |
| [`.drop_columns()`]        | `(columns: &[&str])`       | Remove columns by name                   |
| [`.with_column()`]         | `(name: &str, expr: Expr)` | Add a column, or replace if name exists  |
| [`.with_column_renamed()`] | `(old, new)`               | Rename a column (no-op if not found)     |
| [`.unnest_columns()`]      | `(columns: &[&str])`       | Expand `List`/`Struct` into flat columns |

---

### Adding and Replacing Columns

**Use [`.with_column()`] to add a computed column or replace an existing one by name.**

If a column with the given name already exists, it is replaced in place. Otherwise, the new column is appended. The method consumes `self` and returns a new DataFrame.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "id" => [1_i64, 2_i64, 3_i64],
        "price" => [10.0, 20.0, 30.0],
        "qty" => [2_i64, 1_i64, 4_i64]
    )?;

    // Add a computed column
    let df = df.with_column("total", col("price") * col("qty"))?;
    assert_eq!(df.schema().fields().len(), 4);

    // Replace an existing column (same name → in-place replacement)
    let df = df.with_column("price", col("price") * lit(1.1))?;
    assert_eq!(df.schema().fields().len(), 4); // still 4, not 5

    Ok(())
}
```

---

### Renaming Columns

**Use [`.with_column_renamed()`] to rename a column—it is a no-op if the column does not exist.**

Supports qualified names (`"table.column"`) and case-sensitive renames by wrapping the name in quotes, backticks, or single quotes.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "user_id" => [1_i64, 2_i64],
        "name" => ["Alice", "Bob"]
    )?;

    let df = df.with_column_renamed("user_id", "id")?;

    // Old name is gone, new name exists
    assert!(df.schema().field_with_unqualified_name("user_id").is_err());
    assert!(df.schema().field_with_unqualified_name("id").is_ok());

    // No-op for nonexistent columns (no error)
    let df = df.with_column_renamed("nonexistent", "x")?;
    assert_eq!(df.schema().fields().len(), 2);

    Ok(())
}
```

---

### Projecting and Removing Columns

**Use [`.select()`] to keep specific columns, or [`.drop_columns()`] to remove them—both create a new `Projection` node.**

- [`.select()`] takes `Vec<Expr>` — use when you know which columns to keep (safest, explicit)
- [`.select_columns()`] takes `&[&str]` — string shorthand for simple column selection
- [`.drop_columns()`] takes `&[&str]` — silently ignores nonexistent names (convenient but hides typos)

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "id" => [1_i64, 2_i64, 3_i64],
        "name" => ["Alice", "Bob", "Carol"],
        "email" => ["a@x.com", "b@x.com", "c@x.com"],
        "temp" => [true, false, true]
    )?;

    // select(): explicit inclusion list
    let projected = df.clone().select(vec![col("id"), col("name")])?;
    assert_eq!(projected.schema().fields().len(), 2);

    // select_columns(): string shorthand
    let projected = df.clone().select_columns(&["id", "name"])?;
    assert_eq!(projected.schema().fields().len(), 2);

    // drop_columns(): exclude by name
    let trimmed = df.drop_columns(&["temp"])?;
    assert_eq!(trimmed.schema().fields().len(), 3);

    Ok(())
}
```

> **Tip:** <br>
> Prefer [`.select()`] in production—the explicit column list serves as documentation and catches schema drift early. Use [`.drop_columns()`] for interactive exploration or when excluding a few columns from a wide schema.

---

### Reshaping Nested Columns

**Use [`.unnest_columns()`] to expand `List` or `Struct` columns into flat top-level columns.**

This changes the schema by replacing the nested column with its inner fields (for `Struct`) or repeating rows for each element (for `List`). See [Strategy 4: Nested Data](#strategy-nested-data) for schema modeling details.

---

## Inspecting Schemas

**Inspecting the schema is the first step before validating or transforming your data.**

When you call [`df.schema()`], you're reading the schema from the [`LogicalPlan`] that the DataFrame wraps—not accessing data. The schema is stored as a `DFSchemaRef` (`Arc<DFSchema>`), so you need methods to extract different representations depending on your goal.

### Display Methods (Human-Readable Output)

**Display methods format the schema as human-readable strings for debugging, logging, and quick inspection during development.**

When diagnosing schema mismatches or exploring unfamiliar data, you need to **_see_** the schema structure at a glance. These methods implement `Display` traits, so you can use them directly with `println!` or logging frameworks. Choose [`.to_string()`] for a quick field list, or [`.tree_string()`] for detailed type and nullability information—the latter is particularly useful when debugging type coercion errors.

| Method                                          | Returns        | Output                                                                |
| ----------------------------------------------- | -------------- | --------------------------------------------------------------------- |
| [`df.schema().to_string()`][`.to_string()`]     | `String`       | Compact field list: `"fields:[a, b, c], metadata:{}"`                 |
| [`df.schema().tree_string()`][`.tree_string()`] | `impl Display` | Tree format with types & nullability (like Spark's [`printSchema()`]) |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "user_id" => [1_i64, 2_i64],
        "email"   => [Some("alice@example.com"), None],
        "active"  => [true, false]
    )?;

    // Quick debug: field names only
    println!("{}", df.schema().to_string());
    // Output: "fields:[user_id, email, active], metadata:{}"

    // Detailed: types and nullability (best for debugging!)
    println!("{}", df.schema().tree_string());
    // Output:
    // root
    //  |-- user_id: int64 (nullable = true)
    //  |-- email: utf8 (nullable = true)
    //  |-- active: boolean (nullable = true)

    Ok(())
}
```

> **Tip:** <br>
> When debugging schema mismatches, use [`df.schema().tree_string()`][`.tree_string()`] first—it shows types and nullability, which are often the culprits.

---

### Programmatic Methods (Code-Based Inspection)

**Programmatic methods return schema information as Rust types (`bool`, `Result<>`, iterators), enabling your application logic to validate, branch, and handle errors based on schema properties.**

Production code needs more than display output—it needs to validate schemas before processing, handle missing columns gracefully, and make decisions based on field properties. Display methods show you the schema; programmatic methods let you _act_ on it. Most methods follow two patterns:

1. **Check methods** ([`has_column_*`]) return `bool` for guard clauses,
2. **Access methods** ([`field_with_*`]) return `Result<>` for explicit error handling when a column might not exist.

The most commonly used methods for both patterns:
| Method | Returns | Use Case |
| -------------------------------------------------------------------------------------------------------- | -------------------------- | -------------------------------------------------- |
| [`df.schema().fields()`][`df.schema().fields()`] | `&Fields` | Iterate over field definitions |
| [`df.schema().iter()`][`df.schema().iter()`] | `Iterator` | Get `(Option<&TableReference>, &Arc<Field>)` pairs |
| [`df.schema().metadata()`][`df.schema().metadata()`] | `&HashMap<String, String>` | Access schema-level metadata |
| [`df.schema().has_column_with_unqualified_name(name)`][`df.schema().has_column_with_unqualified_name()`] | `bool` | Check if column exists |
| [`df.schema().field_with_unqualified_name(name)`][`df.schema().field_with_unqualified_name()`] | `Result<&Arc<Field>>` | Get field by name (returns error if not found) |

#### Error Handling Patterns

Schema lookups can fail — a column may not exist, or a name may be ambiguous after a join. Pick the pattern that matches your goal:

1. **Guard clause**
   check with `has_column_*()` before accessing; use when you need to branch.
2. **Explicit match**
   `match` on `field_with_*()` result; use when you need informative error messages. This is the most common pattern for error handling.
3. **Propagate with [`?`]**
   `field_with_*().map_err(...)?`; use in pipeline functions that should fail fast.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "user_id" => [1_i64, 2_i64],
        "email"   => [Some("alice@example.com"), None]
    )?;

    let schema = df.schema();

    // Pattern 1: Check first (guard clause)
    // Use when you need to branch based on column existence
    if schema.has_column_with_unqualified_name("email") {
        let field = schema.field_with_unqualified_name("email")?;
        println!("Email type: {}", field.data_type());
    }

    // Pattern 2: Try and handle error explicitly
    // Use when you need informative error messages
    match schema.field_with_unqualified_name("nonexistent_column") {
        Ok(field) => println!("Found: {}", field.name()),
        Err(e) => eprintln!("Column lookup failed: {}", e),
        // Output: "Column lookup failed: Schema error: No field named nonexistent_column"
    }

    // Pattern 3: Propagate error with context
    // Use in functions that should fail if column is missing
    let _field = schema
        .field_with_unqualified_name("user_id")
        .map_err(|e| datafusion::error::DataFusionError::Plan(
            format!("Required column missing: {}", e)
        ))?;

    Ok(())
}
```

#### Iterating with Table Qualifiers

The [`.iter()`] method returns `(Option<&TableReference>, &Arc<Field>)` pairs. The qualifier is `Some` when fields come from named tables (e.g., after registering tables and joining them). This is how DataFusion disambiguates columns with the same name from different sources.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register two named tables — this gives fields their qualifiers
    ctx.sql("CREATE TABLE users    (id INT, name VARCHAR) AS VALUES (1, 'Alice'), (2, 'Bob')").await?;
    ctx.sql("CREATE TABLE orders   (id INT, user_id INT)  AS VALUES (10, 1), (20, 2)").await?;

    // Join produces qualified fields: users.id, users.name, orders.id, orders.user_id
    let joined_df = ctx.sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id").await?;

    // Iterate: qualifiers distinguish users.id from orders.id
    for (qualifier, field) in joined_df.schema().iter() {
        match qualifier {
            Some(table_ref) => {
                // Qualified: "users.id", "orders.id", etc.
                println!("{}.{}: {}", table_ref, field.name(), field.data_type());
            }
            None => {
                // Unqualified: from dataframe! macro or expressions
                println!("{}: {}", field.name(), field.data_type());
            }
        }
    }

    Ok(())
}
```

> **Note:** <br>
> Fields from the [`dataframe!`] macro have no qualifier (`None`). When you register tables with names (via `CREATE TABLE`, `register_table`, or file readers) and query them, fields carry their source table as a qualifier. This is essential for joins where both tables have columns with the same name.

---

### Arrow Interop Methods

Use these when you need to pass the schema to **Arrow ecosystem** functions (compute kernels, IPC writers, RecordBatch creation).

| Method                                    | Returns                       | Use Case                          |
| ----------------------------------------- | ----------------------------- | --------------------------------- |
| [`df.schema().inner()`][`.inner()`]       | `&SchemaRef` (`&Arc<Schema>`) | Cheap cloning for Arrow functions |
| [`df.schema().as_arrow()`][`.as_arrow()`] | `&Schema`                     | Direct reference for field access |

> **Note:**
> Table qualifiers (e.g., `users.id` vs `orders.id`) are **lost** when converting to Arrow [`Schema`]. If you need qualified names for join disambiguation, stay with [`DFSchema`].

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!("id" => [1_i64, 2_i64])?;

    // Get Arc<Schema> for Arrow functions (cheap to clone)
    let schema_ref = df.schema().inner().clone();

    // Get &Schema for direct field access
    let arrow_schema = df.schema().as_arrow();
    println!("Arrow schema has {} fields", arrow_schema.fields().len());

    Ok(())
}
```

> **Note:**
> The [`dataframe!`] macro sets all columns to `nullable = true` by default. In production, use [`ctx.read_parquet(...)`][`.read_parquet()`], [`ctx.read_csv(...)`][`.read_csv()`], or [`ctx.read_table(...)`][`.read_table()`] to load data with their native nullability settings.

### Additional DFSchema Methods

For a complete reference of all [`DFSchema`] methods, see the [API documentation][`dfschema`]. Beyond the core methods shown above, these are useful for field access and column-level inspection:

| Method                                          | Returns                                  | Purpose                                          |
| ----------------------------------------------- | ---------------------------------------- | ------------------------------------------------ |
| [`.field(i)`]                                   | `&Arc<Field>`                            | Get field by index                               |
| [`.qualified_field(i)`]                         | `(Option<&TableReference>, &Arc<Field>)` | Get field + qualifier by index                   |
| [`.field_with_name(qualifier, name)`]           | `Result<&Arc<Field>>`                    | Find field by optional qualifier + name          |
| [`.field_with_qualified_name(qualifier, name)`] | `Result<&Arc<Field>>`                    | Find field by required qualifier + name          |
| [`.field_names()`]                              | `Vec<String>`                            | Quick list of all field names                    |
| [`.columns()`]                                  | `Vec<Column>`                            | All columns as `Column` structs                  |
| [`.data_type(&column)`]                         | `Result<&DataType>`                      | Get column's type (via [`ExprSchema`])           |
| [`.nullable(&column)`]                          | `Result<bool>`                           | Check if column is nullable (via [`ExprSchema`]) |
| [`.functional_dependencies()`]                  | `&FunctionalDependencies`                | Access functional dependency constraints         |

> **Note:**
> Methods taking `&column` expect a [`Column`] struct (e.g., `Column::from("name")` or `Column::new_unqualified("name")`), not a plain `&str`. The `data_type` and `nullable` methods come from the [`ExprSchema`] trait, which `DFSchema` implements.

For column existence checks and index lookups (`.has_column()`, `.index_of_column()`, `.maybe_index_of_column()`), see [Validating Schemas](#validating-schemas).

#### Qualifier-Aware Lookup Methods

When working with qualified schemas—typically after joins or when implementing custom plan nodes—[`DFSchema`] provides specialized methods to look up fields by qualifier, by unqualified name across qualifiers, or by [`Column`] reference. These are rarely needed in typical DataFrame workflows but essential for disambiguation in multi-table contexts.

| Method                                          | Returns                                          | Purpose                                               |
| ----------------------------------------------- | ------------------------------------------------ | ----------------------------------------------------- |
| `.fields_with_qualified(qualifier)`             | `Vec<&Arc<Field>>`                               | All fields belonging to a specific table qualifier    |
| `.fields_indices_with_qualified(qualifier)`     | `Vec<usize>`                                     | Field indices for a specific table qualifier          |
| `.fields_with_unqualified_name(name)`           | `Vec<&Arc<Field>>`                               | All fields matching a name (ignoring qualifiers)      |
| `.qualified_fields_with_unqualified_name(name)` | `Vec<(Option<&TableReference>, &Arc<Field>)>`    | Fields + qualifiers matching a name                   |
| `.qualified_field_with_unqualified_name(name)`  | `Result<(Option<&TableReference>, &Arc<Field>)>` | Single field by name (errors if ambiguous)            |
| `.qualified_field_from_column(column)`          | `Result<(Option<&TableReference>, &Arc<Field>)>` | Resolve a [`Column`] to its field + qualifier         |
| `.columns_with_unqualified_name(name)`          | `Vec<Column>`                                    | All [`Column`] refs matching a name                   |
| `.index_of_column_by_name(qualifier, name)`     | `Option<usize>`                                  | Find index by optional qualifier + name               |
| `.is_column_from_schema(col)`                   | `bool`                                           | Check if a [`Column`] reference exists in this schema |

> **Tip:**
> Most of these methods are wrappers around [`.iter()`] with different filter/return semantics. If you need a custom lookup pattern, iterating directly with `.iter()` is often simpler than finding the right method name.

---

## Creating Schemas

**Define schemas explicitly in code to enforce types, nullability, and structure at planning time.**

Use Arrow's [`Schema`], [`Field`], and [`DataType`] to build schemas that readers, writers, and the optimizer all share. Explicit schemas prevent inference drift in text formats and give the optimizer the type information it needs for efficient execution. See [The Anatomy of a DataFusion DataFrame Schema](#the-anatomy-of-a-datafusion-dataframe-schema) for the architectural background.

> **Note:**
> In most DataFrame workflows, you work with Arrow's `Schema` type directly. [`DFSchema`] wraps it with table qualifiers and is created automatically when you register tables or read files. You typically create [`DFSchema`] directly only when implementing custom [`TableProvider`]s.

### Basic Schema Construction

Build schemas with [`Schema`], [`Field`], and [`DataType`]; then apply them to readers so DataFusion uses your types instead of inference.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # // Hidden: create a temporary CSV file for the doctest
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("users.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "id,name,active")?;
    # writeln!(file, "1,Alice,true")?;
    # writeln!(file, "2,,false")?;

    // 1. Define the schema — this is the contract for your pipeline
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),        // not nullable
        Field::new("name", DataType::Utf8, true),        // nullable
        Field::new("active", DataType::Boolean, false),
    ]);

    // 2. Apply the schema to a CSV reader — overrides inference
    let path = "users.csv";
    # let path = csv_path.to_str().unwrap();
    let df = ctx.read_csv(path, CsvReadOptions::new().schema(&schema)).await?;

    // 3. Verify: types match the schema, not what inference might have guessed
    assert_batches_eq!(
        &[
            "+----+-------+--------+",
            "| id | name  | active |",
            "+----+-------+--------+",
            "| 1  | Alice | true   |",
            "| 2  |       | false  |",
            "+----+-------+--------+",
        ],
        &df.collect().await?
    );

    Ok(())
}
```

Each `Field` in the schema specifies:

- **Name**: The column identifier (case-sensitive)
- **DataType**: The type of values the column holds
- **Nullable**: Whether `NULL` values are permitted

> **Note:**
> Always use [`SchemaRef`] (`Arc<Schema>`) for efficient sharing. Cloning an `Arc` is O(1) and avoids deep copies of the schema structure.

#### DFSchema Construction

In most workflows, [`DFSchema`] is created automatically when you register tables or read files. When you need to create one directly—typically for custom [`TableProvider`] implementations or plan nodes—use these constructors:

| Constructor                                                           | Input                                              | Qualifiers         | Purpose                                    |
| --------------------------------------------------------------------- | -------------------------------------------------- | ------------------ | ------------------------------------------ |
| `DFSchema::try_from(schema)`                                          | `Schema` or `SchemaRef`                            | All `None`         | Convert an Arrow schema (no qualifiers)    |
| `DFSchema::empty()`                                                   | —                                                  | —                  | Create an empty schema (zero fields)       |
| `DFSchema::from_unqualified_fields(fields, metadata)`                 | `Fields` + `HashMap<String, String>`               | All `None`         | Build from Arrow fields with metadata      |
| `DFSchema::new_with_metadata(qualified_fields, metadata)`             | `Vec<(Option<TableReference>, Arc<Field>)>` + meta | Per-field          | Full control: explicit qualifier per field |
| `DFSchema::try_from_qualified_schema(qualifier, &schema)`             | `impl Into<TableReference>` + `&Schema`            | All same qualifier | Qualify every field with one table name    |
| `DFSchema::from_field_specific_qualified_schema(qualifiers, &schema)` | `Vec<Option<TableReference>>` + `&SchemaRef`       | Per-field          | Different qualifier per field              |

> **Note:** > `try_from`, `from_unqualified_fields`, and `try_from_qualified_schema` call [`.check_names()`][`check_names()`] and return `Result`—they will error on duplicate field names. `empty()` always succeeds. For qualifier transformations on an existing `DFSchema`, see [Aligning Qualifiers](#aligning-qualifiers).

### Default Values

Schemas define structure only, not default values -- the schema is the contract, defaults are a transformation concern. To provide defaults for `NULL` values, apply transformations after reading:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "name" => [Some("Alice"), None,        Some("Carol")],
        "age"  => [Some(25),      Some(35),    Some(30)]
    )?;

    // Replace NULL names with a default — schema defines structure, not defaults
    let df = df.with_column(
        "name",
        coalesce(vec![col("name"), lit("Unknown")])
    )?;

    assert_batches_eq!(
        &[
            "+---------+-----+",
            "| name    | age |",
            "+---------+-----+",
            "| Alice   | 25  |",
            "| Unknown | 35  |",
            "| Carol   | 30  |",
            "+---------+-----+",
        ],
        &df.collect().await?
    );

    Ok(())
}
```

See [Handling Nullability in Transformations](#handling-nullability-in-transformations) for more patterns.

> **Best practice:**
> In production, always prefer **explicit schemas** over inference to prevent drift and ensure consistency.

### Configuring Common Field Types

Certain data types require specific configuration to ensure correctness and prevent data loss. This section covers the most common cases.

#### Decimal Types: Precision and Scale

**Why decimals matter**:
Floating-point types (Float32/Float64) can introduce rounding errors for financial calculations. Decimals provide exact arithmetic for monetary values.

**What you need to specify**:

- **Precision**:
  Total number of digits (maximum 38 for Decimal128)
- **Scale**:
  Digits after the decimal point

**Example**: `Decimal128(10, 2)`

- Can store: `12345678.99` (8 digits + 2 decimals = 10 total)
- Cannot store: `123456789.99` (11 digits, exceeds precision)
- Cannot store: `1234567.999` (3 decimals, exceeds scale)

```rust
use datafusion::arrow::datatypes::{DataType, Field};

fn main() {
    // For currency: typically 2 decimal places
    let _price = Field::new("price", DataType::Decimal128(19, 2), false);

    // For percentages: more decimal places
    let _rate = Field::new("rate", DataType::Decimal128(10, 6), false);  // e.g., 0.123456
}
```

> **Tip:**
> When casting between decimals, ensure the target has enough precision **AND** scale. Casting `Decimal128(10, 2)` to `Decimal128(8, 2)` will fail if values exceed 6 integer digits.

#### Timestamp Types: Timezone Handling

**Why timezone matters**:
A timestamp can represent either an absolute moment in time (with timezone) or a local time (without timezone). Mixing them causes errors.

**Your two choices**:

| Type                 |             Code Example              | What it stores                                    | When to use                                                            |
| :------------------- | :-----------------------------------: | :------------------------------------------------ | :--------------------------------------------------------------------- |
| **With timezone**    | `Timestamp(Microsecond, Some("UTC"))` | A specific instant (e.g., "2024-01-15 10:00 UTC") | Server logs, transactions, anything that happened at a specific moment |
| **Without timezone** |    `Timestamp(Microsecond, None)`     | A local time (e.g., "2024-01-15 10:00")           | Scheduled events, opening hours, anything relative to local time       |

At the Arrow level, timestamps with a non-empty timezone are always stored as UTC instants; the timezone string is display/interpretation metadata. Timestamps without a timezone are "wall clock" values with no absolute reference and cannot be compared to timestamped instants without explicit conversion. Changing between two non-empty timezones (e.g., `"UTC"` to `"America/New_York"`) is a metadata-only change at the type level.

**Common mistake**: Mixing the two types in operations

```rust
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

fn main() {
    // Absolute instant — stores a specific moment (e.g., server logs, transactions)
    let event_time = Field::new(
        "event_time",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    );

    // Local time — no timezone, relative to the user's location (e.g., opening hours)
    let scheduled_at = Field::new(
        "scheduled_at",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    );

    let schema = Arc::new(Schema::new(vec![event_time, scheduled_at]));

    // Verify the types are distinct
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

> **Best practice:**
> Pick one strategy for your entire pipeline. Most systems use UTC timestamps throughout. When you need to compare or join columns with different timezone settings, cast them to the same type first using `cast(col("ts")`, [`DataType::Timestamp(...)`] (available via the prelude).

#### Advanced: Field Metadata

Field metadata is used to embed rich, contextual information—such as column descriptions, data lineage, or security classifications—directly into the schema as key-value pairs. This information is not used by the DataFusion query engine and its preservation across I/O is format-dependent and best-effort, but it is a powerful tool for external systems, documentation, and compliance.

Common Use Cases:

- **Constraints (documentation only):**
  `primary_key`, `unique`, `foreign_key`
- **Data Lineage:**
  `source_system`, `ingest_time`, `source_column`
- **Compliance & Security:**
  `pii` (Personally Identifiable Information), `encryption_required`
- **Documentation:**
  `description`, `owner`, `version`

> **Warning:**
> Storing `primary_key=true` in Arrow metadata is for documentation and external systems only—the DataFusion optimizer does not read it. For optimizer-level benefits (e.g., functional dependencies, join elimination), express constraints through DataFusion's dedicated [`Constraints`] API on the table or plan.

```rust
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() {
    // --- Attaching Metadata ---

    // Field-level metadata: annotate columns with lineage and constraints
    let id_field = Field::new("user_id", DataType::Int64, false).with_metadata(HashMap::from([
        ("primary_key".to_string(), "true".to_string()),
        ("source_system".to_string(), "crm".to_string()),
    ]));

    // Schema-level metadata: annotate the entire dataset
    let schema_meta = HashMap::from([
        ("schema_version".to_string(), "v2.1".to_string()),
        ("owner".to_string(), "Analytics Team".to_string()),
    ]);

    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            id_field,
            Field::new("email", DataType::Utf8, true)
                .with_metadata(HashMap::from([("pii".to_string(), "true".to_string())])),
        ],
        schema_meta,
    ));

    // --- Reading Metadata Back ---

    // From a field (index-based access avoids the ArrowError return type)
    let field = schema.field(0);
    assert_eq!(field.name(), "user_id");
    let is_pk = field.metadata().get("primary_key") == Some(&"true".to_string());
    assert!(is_pk);

    // From the schema
    let version = schema.metadata().get("schema_version");
    assert_eq!(version, Some(&"v2.1".to_string()));
}
```

**Best Practices and Considerations**

- **Standardize your format**:
  use lowercase snake_case keys and parseable values (e.g., `"true"`, ISO 8601 timestamps/durations).
- **Re‑attach intentionally**:
  derived/aggregated columns don't inherit metadata—add it on the final output schema if needed.
- **Verify format support**:
  Arrow IPC preserves metadata; Parquet can embed it, but DataFusion skips file-level schema metadata by default (`skip_metadata = true`)—set `skip_metadata(false)` in Parquet options if you rely on it; CSV/NDJSON do not carry metadata at all.
- **Reconcile on merge**:
  when sources disagree, prefer a canonical schema and explicitly resolve conflicts.
- **Keep it small**:
  avoid large blobs; store long docs externally and reference via a short key (e.g., `doc_url`).
- **Validate early**:
  add lightweight checks in tests/pipeline (e.g., require `owner`, `schema_version`, `pii` flags where applicable).

---

## Schema Inference

**Schema inference derives column names and types from data samples—useful for exploration, but unreliable for production.**

When reading text formats (CSV, NDJSON) without an explicit schema, DataFusion samples the first N records to determine column structure. The sampling depth is controlled by [`schema_infer_max_records`] (default: 1,000). Fields not encountered within that window are excluded entirely—no new columns are added after inference completes.

### How Inference Works

Inference behavior varies by format. CSV uses **positional** alignment (column index determines mapping), while NDJSON uses **name-based** alignment (JSON keys map to fields by name).

| Aspect                  | CSV                                                                  | NDJSON                                            |
| :---------------------- | :------------------------------------------------------------------- | :------------------------------------------------ |
| **Field alignment**     | Positional (column index)                                            | Name-based (JSON key)                             |
| **Missing fields**      | Row-length mismatch errors by default                                | NULL if field exists in schema                    |
| **Short rows**          | Error; use [`.truncated_rows(true)`][`truncated_rows`] to fill NULLs | N/A (each line is a self-contained object)        |
| **Sampling window**     | First N records ([`schema_infer_max_records`])                       | First N records ([`schema_infer_max_records`])    |
| **Default sample size** | 1,000                                                                | 1,000                                             |
| **Type inference**      | Attempts numeric/boolean detection; falls back to `Utf8`             | Infers from JSON value types (`number`, `string`) |

> **Tip:**
> For detailed format behavior with explicit schemas, see [Strategy 1: Text Formats](#strategy-text-formats).

If inference is necessary, increase the sample size to reduce the risk of missing columns or mistyped fields:

```rust
use datafusion::prelude::*;

fn main() {
    // CSV: increase from default 1,000 to 10,000 rows
    let csv_opts = CsvReadOptions::new()
        .schema_infer_max_records(10_000);
    # assert_eq!(csv_opts.schema_infer_max_records, 10_000);

    // NDJSON: same configuration pattern
    let json_opts = NdJsonReadOptions::default()
        .schema_infer_max_records(10_000);
    # assert_eq!(json_opts.schema_infer_max_records, 10_000);
}
```

### When to Use Explicit Schemas

| Scenario                                   | Recommendation                                        |
| :----------------------------------------- | :---------------------------------------------------- |
| Production pipelines                       | **Explicit** — prevents drift, ensures data quality   |
| Specific types needed (e.g., `Decimal128`) | **Explicit** — inference may choose `Float64`         |
| Multi-file reads with varying structure    | **Explicit** — guarantees consistency across files    |
| Interactive exploration / prototyping      | **Inference OK** — validate before relying on results |
| Single-file reads with uniform structure   | **Inference OK** — lower risk of missing fields       |

> **Warning:**
> Inference can drift as data evolves. A column that appears as `Int64` in the first 1,000 rows may contain floats later, causing runtime parse errors. Validate inferred schemas before deploying to production.

**See also:**

- [Creating Schemas](#creating-schemas) for constructing explicit schemas.
- [Applying Schemas and Modeling Data](#applying-schemas-and-modeling-data) for format-specific configuration.
- [Validating Schemas](#validating-schemas) for checking inferred schemas before use.

---

## Applying Schemas and Modeling Data

A schema defines the structure of your data—column names, types, nullability, and nested structures. Applying schemas when reading files enables planning-time validation, improves query performance, and ensures data quality. This section covers schema strategies for different file formats, handling schema evolution, partition pruning, and modeling nested data.

**See also:**

- [Data Model & Schema](./concepts.md#data-model--schema) for fundamentals and
- [Creating DataFrames](./creating-dataframes.md) for file reading basics.

**Jump to:**

- [CSV](#strategy-text-formats)
- [NDJSON](#strategy-text-formats)
- [Parquet](#strategy-self-describing-formats)
- [Partitions](#strategy-partitioned-datasets)
- [Nested Data](#strategy-nested-data)

(strategy-text-formats)=

### Strategy 1: Text Formats (CSV & NDJSON) — Enforce Schemas

**Text formats don't embed type information—provide an explicit schema for production workloads to prevent inference drift.**

Without a schema, DataFusion infers types from a sample of rows ([`schema_infer_max_records`], default 1,000). Providing a schema enforces a contract on the raw data.

| Format     |   Alignment    | Key Behaviors                                                                                  |
| :--------- | :------------: | :--------------------------------------------------------------------------------------------- |
| **CSV**    | **Positional** | Fields map to schema columns by order. Header names are read but position determines mapping   |
| **NDJSON** | **Name-based** | JSON keys match schema field names. Order doesn't matter. Missing keys → NULL, extra → ignored |

#### CSV — Positional Alignment

CSV uses **positional** mapping: the first column maps to the first field in your schema, the second to the second, and so on. Header names (if present with [`has_header(true)`][`has_header`]) are read but field order determines the mapping.

Key behaviors:

- **Types**: values are parsed into the declared Arrow types (e.g., `Decimal128(19,2)` for currency).
- **Missing/extra columns**: row-length mismatches error by default; use [`truncated_rows(true)`][`truncated_rows`] to fill missing nullable columns with NULLs.
- **Format details**: single-byte `delimiter`, `quote`, optional `escape`, `comment`, and `terminator`.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("sales.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "order_id,customer_id,amount")?;
    # writeln!(file, "1001,CUST-001,99.99")?;
    # writeln!(file, "1002,CUST-002,150.50")?;

    // Define the canonical schema — explicit types prevent inference drift
    let sales_schema = Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(19, 2), true),
    ]);

    // Apply schema to CSV reader — overrides inference
    let path = "sales.csv";
    # let path = csv_path.to_str().unwrap();
    let df = ctx.read_csv(path, CsvReadOptions::new()
        .schema(&sales_schema)
        .has_header(true)
    ).await?;

    assert_batches_eq!(
        &[
            "+----------+-------------+--------+",
            "| order_id | customer_id | amount |",
            "+----------+-------------+--------+",
            "| 1001     | CUST-001    | 99.99  |",
            "| 1002     | CUST-002    | 150.50 |",
            "+----------+-------------+--------+",
        ],
        &df.collect().await?
    );

    Ok(())
}
```

> **Warning:**
> Schema inference samples only the first 1,000 rows by default ([`schema_infer_max_records`]). Common pitfalls: IDs inferred as `Int32` then overflow, currency inferred as `Float64` (rounding errors), sparse columns inferred as `Utf8`. Always provide explicit schemas for CSV in production.

#### NDJSON — Name-Based Alignment with Flexible Structure

NDJSON uses **name-based** mapping: JSON keys match schema field names, so field order does not matter. Missing keys become NULL; extra keys are silently ignored.

Key behaviors:

- **Missing keys**: with an explicit schema, missing fields become NULL.
- **Extra keys**: keys not in the schema are ignored (no error, no column).
- **Types**: JSON values are cast to declared Arrow types; invalid casts raise errors.
- **Nested data**: supports `Struct`, `List`, `Map` (CSV cannot express nested types).

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let dir = tempdir()?;
    # let json_path = dir.path().join("data.json");
    # let mut file = File::create(&json_path)?;
    # writeln!(file, r#"{{"id": 1, "name": "Alice"}}"#)?;
    # writeln!(file, r#"{{"id": 2}}"#)?;

    // Explicit schema: "name" is nullable to handle missing keys
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let path = "data.json";
    # let path = json_path.to_str().unwrap();
    let df = ctx.read_json(path, NdJsonReadOptions::default()
        .schema(&schema)
    ).await?;

    // Row 2 has no "name" key — becomes NULL
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  |       |",
            "+----+-------+",
        ],
        &df.collect().await?
    );

    Ok(())
}
```

(strategy-self-describing-formats)=

### Strategy 2: Self-Describing Formats (Parquet/Avro/Arrow) — Merge & Normalize

**Self-describing formats embed schemas, but schemas evolve—DataFusion auto-merges them and you should normalize to canonical types.**

When reading multiple files with evolved schemas, DataFusion merges them automatically:

```text
v1_sales.parquet:  id (Int32),  amount (Decimal128(19,2))
v2_sales.parquet:  id (Int64),  amount (Decimal128(38,9)),  region (Utf8)

Merged result:     id (Int64),  amount (Decimal128(38,9)),  region (Utf8, nullable)
```

- Types are **widened**: `Int32` + `Int64` → `Int64`
- Columns are **added**: missing columns appear as nullable
- Use [`.read_parquet()`] with multiple file paths to trigger auto-merge

#### Defensive pattern: enforce a canonical schema

**After reading, normalize to your canonical schema using [`.select()`] and [`.cast_to()`]—decouple your pipeline from upstream schema changes.**

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::assert_batches_eq;
use datafusion::logical_expr::ExprSchemable;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Simulate data that arrived as Int32 (might be widened Int32 from Parquet)
    let df = dataframe!(
        "id" => [1_i32, 2_i32, 3_i32, 4_i32],
        "amount" => [99.99, 150.50, 75.25, 200.00]
    )?;

    // Normalize to canonical types: Int32 → Int64, Float64 stays Float64
    let canonical_df = df.clone().select(vec![
        col("id").cast_to(&DataType::Int64, df.schema())?.alias("id"),
        col("amount").cast_to(&DataType::Float64, df.schema())?.alias("amount"),
    ])?;

    // Verify the canonical schema has the expected types
    let schema = canonical_df.schema();
    assert_eq!(
        schema.field_with_unqualified_name("id")?.data_type(),
        &DataType::Int64
    );

    assert_batches_eq!(
        &[
            "+----+--------+",
            "| id | amount |",
            "+----+--------+",
            "| 1  | 99.99  |",
            "| 2  | 150.5  |",
            "| 3  | 75.25  |",
            "| 4  | 200.0  |",
            "+----+--------+",
        ],
        &canonical_df.collect().await?
    );

    Ok(())
}
```

(strategy-partitioned-datasets)=

### Strategy 3: Partitioned Datasets — Pruning with ListingTable

**Hive-style partitioning lets DataFusion skip entire directories based on query filters.**

[`ListingTable`] reads directories like `/data/events/year=2024/month=01/...` and uses query filters to skip non-matching partitions entirely. Use [`.explain()`] to verify pruning in your plan. (See also: [Reading Explain Plans](../../user-guide/explain-usage.md))

```rust,no_run
// no_run: requires a filesystem with Hive-style partition layout
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // 1. Schema of the DATA INSIDE the Parquet files
    //    Do NOT include partition columns here
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, true),
    ]));

    // 2. Describe directory structure — partition column order must match nesting
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("parquet")
        .with_table_partition_cols(vec![
            ("year".into(),  DataType::Int32),
            ("month".into(), DataType::Int8),
        ]);

    // 3. Build the ListingTable and register it
    let config = ListingTableConfig::new(ListingTableUrl::parse("/data/events")?)
        .with_listing_options(listing_options)
        .with_schema(file_schema);
    let table = Arc::new(ListingTable::try_new(config)?);
    ctx.register_table("events", table)?;

    // 4. Query with a filter — DataFusion prunes partitions automatically
    let df = ctx.sql("SELECT * FROM events WHERE year = 2024").await?;

    // Verify pruning with EXPLAIN
    df.explain(false, false)?.show().await?;

    Ok(())
}
```

(strategy-nested-data)=

### Strategy 4: Nested Data — Struct/List/Map Modeling

**Use Arrow's `Struct`, `List`, and `Map` types to model hierarchical data—avoid lossy flattening of JSON or Parquet sources.**

Define the schema with nested types and query nested fields using `get_field()` (structs) or `array_element()` (lists):

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, Fields};

fn main() {
    // Struct: a nested object
    let metadata_type = DataType::Struct(Fields::from(vec![
        Field::new("source", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
    ]));

    // List: variable-length array
    let tags_type = DataType::List(
        Arc::new(Field::new("tag", DataType::Utf8, true))
    );

    // Map: key-value pairs (List<Struct<key, value>> internally)
    let attributes_type = DataType::Map(
        Arc::new(Field::new("entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false
        )),
        false,
    );

    let schema = Schema::new(vec![
        Field::new("metadata", metadata_type, true),
        Field::new("tags", tags_type, true),
        Field::new("attributes", attributes_type, true),
    ]);
    assert_eq!(schema.fields().len(), 3);

    // Querying nested fields:
    let _source = get_field(col("metadata"), "source");          // Struct access
    let _filter = get_field(col("metadata"), "version").gt_eq(lit(2));
    let _first  = array_element(col("tags"), lit(1));            // List: 1-based
}
```

> **Tip:** <br>
> Use `LargeUtf8` or `LargeList` only when a single value might exceed 2 GB. DataFusion does not enforce key uniqueness in maps—handle duplicate keys in query logic if needed.

---

## Transforming Schemas

**Modify existing schemas by changing qualifiers, combining schemas, or handling nullability.**

While DataFusion schemas are conceptually immutable (each operation creates a new schema), [`DFSchema`] provides methods to transform schemas in common ways. These transformations are essential for aligning data from different sources and evolving pipelines.

### DFSchema Transform Methods

| Category     | Method                                                  | Ownership           | Purpose                                               |
| ------------ | ------------------------------------------------------- | ------------------- | ----------------------------------------------------- |
| **Create**   | `DFSchema::try_from_qualified_schema(q, s)`             | Associated fn       | Create a qualified [`DFSchema`] from an Arrow schema  |
| **Create**   | `DFSchema::from_field_specific_qualified_schema(qs, s)` | Associated fn       | Create a [`DFSchema`] with per-field qualifiers       |
| **Align**    | `.strip_qualifiers()`                                   | Consumes self       | Remove all table qualifiers from fields               |
| **Align**    | `.replace_qualifier(qualifier)`                         | Consumes self       | Replace all qualifiers with a new table name          |
| **Align**    | `.with_field_specific_qualified_schema(qs)`             | Borrows `&self`     | Replace qualifiers with per-field values              |
| **Combine**  | `.join(&other)`                                         | Borrows `&self`     | Combine two schemas (errors on duplicate field names) |
| **Combine**  | `.merge(&other)`                                        | Mutates `&mut self` | Append fields, silently skipping duplicates           |
| **Annotate** | `.with_functional_dependencies(deps)`                   | Consumes self       | Set functional dependencies for optimization          |

> **Note:**
> Methods that **consume self** (`.strip_qualifiers()`, `.replace_qualifier()`) cannot be called directly on `df.schema()`, which returns `&DFSchema`. Clone first: `df.schema().clone().strip_qualifiers()`. For per-field qualifier control, see [`with_field_specific_qualified_schema()`].

---

### Aligning Qualifiers

**Table qualifiers disambiguate columns from different sources—essential after joins where multiple tables share column names.**

When DataFusion joins tables, each field retains its source qualifier (e.g., `users.id`, `orders.id`). The qualifier methods let you normalize these for downstream processing: strip them for simplicity, or replace them with a uniform name.

#### try_from_qualified_schema

Create a [`DFSchema`] where every field carries the same table qualifier. This is the primary way to build a qualified schema from an Arrow [`Schema`]:

```rust
use datafusion::common::{DFSchema, TableReference};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() -> datafusion::error::Result<()> {
    let arrow_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    // Qualify all fields with "users"
    let qualified = DFSchema::try_from_qualified_schema("users", &arrow_schema)?;

    // Verify: each field now carries the "users" qualifier
    for (qualifier, field) in qualified.iter() {
        assert_eq!(qualifier, Some(&TableReference::bare("users")));
        assert!(field.name() == "id" || field.name() == "name");
    }

    Ok(())
}
```

#### strip_qualifiers

Remove all table qualifiers, reducing `users.id` to just `id`. Consumes `self` and returns a new [`DFSchema`]:

```rust
use datafusion::common::{DFSchema, TableReference};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() -> datafusion::error::Result<()> {
    let arrow_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let qualified = DFSchema::try_from_qualified_schema("users", &arrow_schema)?;

    // Strip all qualifiers
    let stripped = qualified.strip_qualifiers();

    // Verify: no qualifiers remain
    for (qualifier, _field) in stripped.iter() {
        assert_eq!(qualifier, None);
    }

    Ok(())
}
```

> **Warning:**
> Stripping qualifiers after a join can create duplicate unqualified names (e.g., two `id` columns). Use `.replace_qualifier()` or rename columns first if ambiguity is possible.

#### replace_qualifier

Replace all qualifiers with a new table name. Useful for normalizing a schema after a join to a single logical name:

```rust
use datafusion::common::{DFSchema, TableReference};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() -> datafusion::error::Result<()> {
    let arrow_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let qualified = DFSchema::try_from_qualified_schema("users", &arrow_schema)?;

    // Replace "users" qualifier with "result"
    let renamed = qualified.replace_qualifier("result");

    // Verify: all fields now have "result" qualifier
    for (qualifier, _field) in renamed.iter() {
        assert_eq!(qualifier, Some(&TableReference::bare("result")));
    }

    Ok(())
}
```

#### from_field_specific_qualified_schema

Create a [`DFSchema`] from an Arrow [`SchemaRef`] with a **different qualifier per field**. Unlike `try_from_qualified_schema` (which applies one qualifier to all fields), this lets you assign qualifiers individually—useful when constructing schemas that represent joined results:

```rust
use std::sync::Arc;
use datafusion::common::{DFSchema, TableReference};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() -> datafusion::error::Result<()> {
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
    ]));

    // First field from "users", second from "orders"
    let qualifiers = vec![
        Some(TableReference::bare("users")),
        Some(TableReference::bare("orders")),
    ];

    let schema = DFSchema::from_field_specific_qualified_schema(qualifiers, &arrow_schema)?;

    // Verify: each field has its own qualifier
    let (q0, f0) = schema.qualified_field(0);
    assert_eq!(q0, Some(&TableReference::bare("users")));
    assert_eq!(f0.name(), "id");

    let (q1, f1) = schema.qualified_field(1);
    assert_eq!(q1, Some(&TableReference::bare("orders")));
    assert_eq!(f1.name(), "order_id");

    Ok(())
}
```

#### Re-qualify Fields with .with_field_specific_qualified_schema()

Re-qualify an **existing** [`DFSchema`] with per-field qualifiers. Borrows `&self` and returns a new [`DFSchema`] with the same fields but different qualifiers. Errors if the number of qualifiers does not match the number of fields:

```rust
use datafusion::common::{DFSchema, TableReference};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() -> datafusion::error::Result<()> {
    let arrow_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
    ]);

    // Start with a uniformly qualified schema
    let original = DFSchema::try_from_qualified_schema("source", &arrow_schema)?;

    // Re-qualify: move "id" to "users", "amount" to "transactions"
    let requalified = original.with_field_specific_qualified_schema(vec![
        Some(TableReference::bare("users")),
        Some(TableReference::bare("transactions")),
    ])?;

    let (q0, _) = requalified.qualified_field(0);
    assert_eq!(q0, Some(&TableReference::bare("users")));

    let (q1, _) = requalified.qualified_field(1);
    assert_eq!(q1, Some(&TableReference::bare("transactions")));

    // Mismatched qualifier count returns an error
    let result = original.with_field_specific_qualified_schema(vec![None]);
    assert!(result.is_err());

    Ok(())
}
```

> **Note:**
> Unlike `.strip_qualifiers()` and `.replace_qualifier()` which consume `self`, `.with_field_specific_qualified_schema()` borrows `&self`—so you can call it directly without cloning.

---

### Combining Schemas

**Combine fields from multiple schemas into one—either strictly (rejecting duplicates) or permissively (ignoring them).**

Use [`users_schema.join(&contact_schema)`][dfschema::join] when schemas must have entirely distinct fields (e.g., after a SQL JOIN), and [`base_schema.merge(&overlapping_schema)`][dfschema::merge] when you want to accumulate fields while silently skipping duplicates (e.g., building a union schema from overlapping sources).

**SQL equivalent:**
`.join()` mirrors the schema produced by `SELECT * FROM a JOIN b`; `.merge()` is closer to `UNION BY NAME` schema resolution.

#### Combine Strictly with .join()

Combine two schemas into one, appending all fields from `other` after the fields from `self`. Borrows `&self` and returns a new [`DFSchema`].

`.join()` enforces **uniqueness**: it calls [`check_names()`] on the result and returns an error if any field names collide. Duplicate detection follows qualifier scope:

- **Qualified fields:** both qualifier _and_ name must match to be a duplicate (`users.id` and `orders.id` are distinct).
- **Unqualified fields:** name alone must be unique (two bare `id` fields error).
- **Cross-scope:** an unqualified `id` also conflicts with any qualified `*.id`, since unqualified names must be unambiguous.

Metadata from both schemas is merged (keys from `other` overwrite matching keys from `self`). Functional dependencies are reset to empty.

```rust
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() -> datafusion::error::Result<()> {
    let users_schema = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))?;

    let contact_schema = DFSchema::try_from(Schema::new(vec![
        Field::new("email", DataType::Utf8, true),
    ]))?;

    // join: appends fields from contact_schema, errors on duplicates
    let combined = users_schema.join(&contact_schema)?;

    assert_eq!(combined.fields().len(), 3);
    assert_eq!(combined.field_names(), vec!["id", "name", "email"]);

    // Joining schemas with overlapping unqualified names would error:
    // users_schema.join(&users_schema) -> Err(DuplicateUnqualifiedField)

    Ok(())
}
```

#### Combine Permissively with .merge()

Append fields from another schema, silently skipping duplicates. Unlike `.join()`, `.merge()` mutates `&mut self` in place and never errors—it is a permissive accumulation operation, designed for building union-compatible schemas.

**Merge precedence** (important—fields and metadata follow _opposite_ rules):

| Aspect                    | Precedence                              | Rationale                                         |
| :------------------------ | :-------------------------------------- | :------------------------------------------------ |
| **Fields**                | `self` wins — duplicates skipped        | Preserves the original schema's field definitions |
| **Schema-level metadata** | `other` wins — overwrites matching keys | Allows newer metadata to propagate                |

Duplicate detection mirrors `.join()`:

- **Qualified fields:** both qualifier and field name must match.
- **Unqualified fields:** field name alone is sufficient.

```rust
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() -> datafusion::error::Result<()> {
    let mut base_schema = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))?;

    let overlapping_schema = DFSchema::try_from(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),   // duplicate — skipped
        Field::new("email", DataType::Utf8, true),   // new — appended
    ]))?;

    // merge: appends non-duplicate fields, ignores "name" (already in base)
    base_schema.merge(&overlapping_schema);

    assert_eq!(base_schema.fields().len(), 3);
    assert_eq!(base_schema.field_names(), vec!["id", "name", "email"]);

    Ok(())
}
```

---

### Handling Nullability in Transformations

**After combining schemas via [`users_schema.join(&contact_schema)`][dfschema::join] or [`base_schema.merge(&overlapping_schema)`][dfschema::merge], nullable fields often appear—requiring strategies to fill, filter, or preserve NULL values.**

As described in [Nullability](#schema-field-nullability), the widening rule applies: if a column is nullable in **any** input schema, it remains nullable in the combined result. The patterns below address what to do with the resulting NULLs.

```rust
use datafusion::prelude::*;
use datafusion::functions::expr_fn::coalesce;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "email" => [Some("a@some.com"), None, Some("c@some.com")],
        "status" => [Some("active"), None, Some("inactive")]
    )?;

    // Pattern 1: Fill NULLs with a default using coalesce
    let df = df.with_column("status",
        coalesce(vec![col("status"), lit("pending")])
    )?;

    // Pattern 2: Conditional fill with CASE/WHEN
    let df = df.with_column(
        "email",
        when(col("email").is_null(), lit("unknown@example.com"))
            .otherwise(col("email"))?
    )?;

    let results = df.clone().collect().await?;
    assert_batches_eq!(
        &[
            "+---------------------+----------+",
            "| email               | status   |",
            "+---------------------+----------+",
            "| a@some.com          | active   |",
            "| unknown@example.com | pending  |",
            "| c@some.com          | inactive |",
            "+---------------------+----------+",
        ],
        &results
    );

    // Pattern 3: Filter out incomplete records
    let complete_df = df.filter(col("email").is_not_null())?;
    assert_eq!(complete_df.collect().await?.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    Ok(())
}
```

| Strategy              | When to Use                                   | Example                                 |
| :-------------------- | :-------------------------------------------- | :-------------------------------------- |
| **Fill with default** | Reasonable default exists, row still valuable | Missing status → "pending"              |
| **Fill with logic**   | Value derivable from other columns            | Missing full_name → concat(first, last) |
| **Drop row**          | Required field missing or would skew analysis | Missing primary key                     |
| **Keep NULL**         | NULL is meaningful (unknown ≠ default)        | Missing survey response                 |

**See also:**

- [Concepts: Handling Null Values](./concepts.md#handling-null-values) for SQL NULL semantics and three-valued logic.
- [Nullability](#schema-field-nullability) for the widening rule when schemas are merged.
- [Default Values](#default-values) for applying defaults during schema creation.

---

## Validating Schemas

**Validate column existence, resolve field positions, and compare schemas against expected contracts—before your pipeline runs into runtime surprises.**

Schema validation sits between inspection and transformation. After you [inspect](#inspecting-schemas) what you have, validation answers: "Is this what I expected?" Use column-level checks as guard clauses, index lookups for positional access, and schema-level comparisons to enforce contracts between pipeline stages.

### Does This Column Exist?

**Check column presence before accessing it—use `has_column_*` methods as guard clauses to branch safely.**

These methods return `bool` and never error. Use them when the column might legitimately be absent (optional fields, schema evolution) and your code needs to branch:

- `.has_column_with_unqualified_name(name)` — check by name only (most common)
- `.has_column_with_qualified_name(qualifier, name)` — check by table-qualified name (after joins)
- `.has_column(&column)` — check using a [`Column`] struct (qualifier-aware)
- `.is_column_from_schema(col)` — equivalent to `.has_column()`, returns `bool`

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "id" => [1_i64, 2_i64],
        "amount" => [100.0, 200.0]
    )?;

    let schema = df.schema();

    // Guard clause: validate required columns before processing
    let required = ["id", "amount", "timestamp"];
    let missing: Vec<_> = required.iter()
        .filter(|name| !schema.has_column_with_unqualified_name(name))
        .collect();

    if !missing.is_empty() {
        // "timestamp" is missing — handle gracefully
        assert_eq!(missing, vec![&"timestamp"]);
    }

    Ok(())
}
```

### Can I Safely Index a Column?

**Use index lookups when you need a column's position—choose between fail-fast (`Result`) and optional (`Option`) semantics.**

Two methods, one design choice:

- `.index_of_column(col)` returns `Result<usize>` — use when absence is a **hard error** (pipeline should fail)
- `.maybe_index_of_column(col)` returns `Option<usize>` — use when absence is **expected** (optional columns, defensive code)

Both take a [`Column`] struct. Use `Column::from("name")` for unqualified lookups.

```rust
use datafusion::prelude::*;
use datafusion::common::Column;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "id" => [1_i64, 2_i64],
        "name" => ["Alice", "Bob"]
    )?;

    let schema = df.schema();

    // Option path: column might not exist
    let name_idx = schema.maybe_index_of_column(&Column::from("name"));
    assert_eq!(name_idx, Some(1));

    let missing_idx = schema.maybe_index_of_column(&Column::from("email"));
    assert_eq!(missing_idx, None);

    // Result path: column MUST exist, or fail with a descriptive error
    let id_idx = schema.index_of_column(&Column::from("id"))?;
    assert_eq!(id_idx, 0);

    Ok(())
}
```

### Do These Schemas Match My Contract?

**Compare schemas for compatibility using either loose checks (gating, tolerant) or strict checks (enforcing invariants).**

DataFusion provides two levels of schema comparison. Choose based on how strict you need to be:

- **Loose** — `.logically_equivalent_names_and_types(&other)` returns `bool`. Ignores nullability, metadata, and encoding differences (e.g., `Dict<Utf8>` equals `Utf8`). Use for gating checks and tolerant compatibility.
- **Strict** — `.has_equivalent_names_and_types(&other)` returns `Result<()>`. Compares field names and types semantically (ignores nullability and metadata, but requires same encoding). Returns a descriptive error on mismatch. Use for enforcing invariants and debugging.

For type-level comparisons (useful in custom plan nodes):

- `DFSchema::datatype_is_logically_equal(dt1, dt2)` — loose: `Dict<K, Utf8>` equals `Utf8`, `Utf8View` equals `Utf8`
- `DFSchema::datatype_is_semantically_equal(dt1, dt2)` — strict: same representation required

```rust
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() -> datafusion::error::Result<()> {
    let expected = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))?;

    let actual = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Int64, true),   // different nullability
        Field::new("name", DataType::Utf8, false),  // different nullability
    ]))?;

    // Loose: passes — nullability differences are ignored
    assert!(expected.logically_equivalent_names_and_types(&actual));

    // Strict: also passes — semantic equality ignores nullability too
    assert!(expected.has_equivalent_names_and_types(&actual).is_ok());

    // Where they diverge: type mismatches
    let wrong_type = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),  // Utf8 instead of Int64
        Field::new("name", DataType::Utf8, true),
    ]))?;

    // Loose: fails on type mismatch
    assert!(!expected.logically_equivalent_names_and_types(&wrong_type));

    // Strict: returns descriptive error
    let err = expected.has_equivalent_names_and_types(&wrong_type).unwrap_err();
    assert!(err.to_string().contains("Schema mismatch"));

    Ok(())
}
```

> **Tip:**
> Use `.has_equivalent_names_and_types()` in tests and pipeline entry points—its error messages pinpoint exactly which field mismatches, saving debugging time.

---

### References

See also:

- [Handling Nullability in Transformations](#handling-nullability-in-transformations)
- [Strategy 2: Self-Describing Formats](#strategy-self-describing-formats)
- [Strategy 3: Partitioned Datasets](#strategy-partitioned-datasets)

Further reading:

- [Parquet schema evolution][parquet-evolution]
- [Medium-article: All About Parquet Part 04][parquet-dremio]
- [Avro schema resolution][avro-evolution]
- [Designing Data-Intensive Applications][kleppmann]

---

<!-- TODO: MERGE into "### Reference" subsection within consolidated "## Schema Evolution & Versioning" -->

## Further Reading

Resources for understanding Arrow’s type system, schema metadata, and DataFusion’s coercion rules—useful when debugging schema mismatches, unexpected casts, or expensive conversions.

### Arrow & Memory Model (Essential)

| Resource                                                                                      | Description                                                                                                                           |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| [Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)            | Physical memory layout, validity bitmaps, and variable-size views (for example, `StringView`) — explains why some casts are expensive |
| [Arrow Schema IPC Message](https://arrow.apache.org/docs/format/Columnar.html#schema-message) | How fields, metadata, and nullability are serialized — helpful when diagnosing “schema mismatch” errors                               |

### Storage ↔ Memory Type Mapping

| Resource                                                                                                             | Description                                                                                                           |
| -------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| [Parquet Logical Types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)                        | How Parquet logical types (`DECIMAL`, timestamps, etc.) map into Arrow types                                          |
| [DataFusion Type Coercion Rules](https://docs.rs/datafusion/latest/datafusion/logical_expr/type_coercion/index.html) | The exact rules DataFusion uses to reconcile type differences (for example, joining or unioning `Int32` with `Int64`) |

### Execution & Optimization

| Resource                                                                                        | Description                                                                                                              |
| ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| [DataFusion Optimizer Rules](https://docs.rs/datafusion/latest/datafusion/optimizer/index.html) | How the optimizer rewrites plans (it may insert implicit `CAST`s); start with `type_coercion` and `simplify_expressions` |

### Books (Foundational)

| Resource                                                 | Description                                                                                                                                                                                                                                                              |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| The Data Model Resource Book (Vol 1–3) — Len Silverston  | Universal data models for common domains (Vol [1](https://www.oreilly.com/library/view/the-data-model/9780471380238/), [2](https://www.oreilly.com/library/view/the-data-model/9780471353485/), [3](https://www.oreilly.com/library/view/the-data-model/9780470178454/)) |
| Patterns of Data Modeling — David Hay                    | Conceptual modeling patterns that translate well to analytical schemas ([O’Reilly](https://www.oreilly.com/library/view/patterns-of-data/9781439819906/))                                                                                                                |
| The Data Warehouse Toolkit — Kimball & Ross              | Dimensional modeling (star schemas) for analytics ([O’Reilly](https://www.oreilly.com/library/view/the-data-warehouse/9781118530801/))                                                                                                                                   |
| Designing Data-Intensive Applications — Martin Kleppmann | Schema evolution and encoding trade-offs ([O’Reilly](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/))                                                                                                                         |
| How Query Engines Work — Andy Grove                      | Query engine internals (DataFusion’s creator) ([Leanpub](https://leanpub.com/how-query-engines-work))                                                                                                                                                                    |

---

<!-- ==========================================================================
     REFERENCE List as Limiter for Focusing on the Above sections

     ========================================================================== -->

<!-- ADD NEW REFERENCES BELOW  THEY WILL BE SORTET TOMOOROW !--->

[`.with_functional_dependencies()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.with_functional_dependencies
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`has_column_*`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column
[`field_with_*`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_unqualified_name
[`df.schema().fields()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.fields
[`df.schema().iter()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.iter
[`df.schema().metadata()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.metadata
[`df.schema().has_column_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column_with_unqualified_name
[`df.schema().field_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_unqualified_name

<!-- DataFusion: DFSchema & Schema Methods -->

[`&schemaref`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/type.SchemaRef.html
[`.as_arrow()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.as_arrow
[`.columns()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.columns
[`.data_type(&column)`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.data_type
[`.datatype_is_logically_equal()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.datatype_is_logically_equal
[`.datatype_is_semantically_equal()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.datatype_is_semantically_equal
[`.field(i)`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field
[`.functional_dependencies()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.functional_dependencies
[`.field_names()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_names
[`.field_with_name(qualifier, name)`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`.field_with_qualified_name(qualifier, name)`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_qualified_name
[`.field_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_unqualified_name
[`.fields()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.fields
[`.has_column(&column)`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column
[`.has_column_with_qualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column_with_qualified_name
[`.has_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_equivalent_names_and_types
[`.index_of_column(&column)`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.index_of_column
[`.inner()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`.iter()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.iter
[`.logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`.matches_arrow_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.matches_arrow_schema
[`.maybe_index_of_column(&column)`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.maybe_index_of_column
[`.nullable(&column)`]: https://docs.rs/datafusion/latest/datafusion/common/trait.ExprSchema.html#method.nullable
[`.qualified_field(i)`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.qualified_field
[`.qualified_field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.qualified_field_with_name
[`.to_string()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.to_string
[`.tree_string()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.tree_string
[`df.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`dfschema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`dfschema::field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`check_names()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.check_names
[dfschema::join]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.join
[`dfschema::logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[dfschema::merge]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.merge
[`dfschema::metadata`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.metadata
[`exprschema`]: https://docs.rs/datafusion/latest/datafusion/common/trait.ExprSchema.html
[`schema::new()`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Schema.html#method.new
[`schemaref`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/type.SchemaRef.html
[`with_field_specific_qualified_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.with_field_specific_qualified_schema

<!-- DataFusion: DataFrame Methods -->

[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.drop_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns
[`.distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[`.except()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.union_by_name_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name_distinct
[`.select_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_columns
[`.unnest_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.unnest_columns
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.with_column_renamed()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html

<!-- DataFusion: Expressions & Functions -->

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.alias
[`.cast_to()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.cast_to
[`avg()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/average/index.html
[`coalesce`]: https://docs.rs/datafusion-functions/latest/datafusion_functions/core/expr_fn/fn.coalesce.html
[`col()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.col.html
[`column`]: https://docs.rs/datafusion/latest/datafusion/common/struct.Column.html
[`constraints`]: https://docs.rs/datafusion/latest/datafusion/common/struct.Constraints.html
[`count()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/count/index.html
[`max()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/min_max/index.html
[`median()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/median/index.html
[`min()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/min_max/index.html
[`stddev()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/stddev/index.html
[`typecoercion`]: https://docs.rs/datafusion/latest/datafusion/expr/type_coercion/struct.TypeCoercion.html
[`typesignature`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.TypeSignature.html

<!-- DataFusion: Context, IO & Configuration -->

[`.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.read_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_table
[`csvreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`ctx.read_csv(...).schema(...)`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/csv/reader/struct.BufReader.html#method.schema
[`has_header`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#structfield.has_header
[`infer_schema_max_records`]: https://docs.rs/deltalake/latest/deltalake/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`listingtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`ndjsonreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`parquetreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`schema_infer_max_records`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`schemaprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`sessionstate.catalog_list`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html#method.catalog_list
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[`tableprovider::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.schema
[`tablescan.projected_schema`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.TableScan.html#structfield.projected_schema
[`truncated_rows`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.truncated_rows

<!-- DataFusion: Errors & Logical Plans -->

[`?`]: https://doc.rust-lang.org/stable/std/ops/trait.Try.html
[`datafusionerror`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html
[`datafusionerror::plan`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.Plan
[`datafusionerror::schemaerror`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.SchemaError
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`logicalplan.schema()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html#method.schema
[`logicalplanbuilder`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html

<!-- Arrow & Data Types -->

[`arrow` crate]: https://docs.rs/arrow/latest/arrow/
[`arrow schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`arrow::compute::can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`arrow::datatypes::field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html
[`can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`datatype::timestamp(...)`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/enum.DataType.html#variant.Timestamp
[`datatype`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/enum.DataType.html
[`field.metadata()`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html#method.metadata
[`field`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Field.html
[`list`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.List.html
[`map`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Map.html
[`null`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/dialect/keywords/constant.NULL.html
[`schema`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Schema.html
[`struct`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Struct.html
[`union`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Union.html

<!-- External References & Standards -->

[apache arrow]: https://arrow.apache.org/
[arrow data types]: https://arrow.apache.org/docs/python/data.html
[arrow dtype]: https://arrow.apache.org/docs/python/api/datatypes.html
[arrow schema docs]: https://arrow.apache.org/cookbook/py/schema.html
[arrow schema rust]: https://github.com/apache/arrow-rs/tree/main/arrow/examples
[avro-evolution]: https://avro.apache.org/docs/current/specification/#schema-resolution
[duckdb]: https://duckdb.org/docs/sql/query_syntax/setops.html#union-by-name
[kleppmann]: https://dataintensive.net/
[parquet]: https://parquet.apache.org/docs/file-format/
[parquet-dremio]: https://medium.com/data-engineering-with-dremio/all-about-parquet-part-04-schema-evolution-in-parquet-c2c2b1aa6141
[parquet-evolution]: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#schema-merging
[`printschema()`]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.printSchema.html
[project nessie]: https://projectnessie.org/
[schema mismatch medium]: https://medium.com/data-engineering-with-dremio/schema-mismatch-error-understanding-and-resolving-8d6c1e1a7e1a
[unity catalog]: https://www.unitycatalog.io/
[`.to_string_pretty()`]: https://docs.rs/serde_json/latest/serde_json/fn.to_string_pretty.html

<!-- =========================================================================
     TODO SECTION - Future Work for Schema Management Documentation
     =========================================================================

DONE: Schema drift is now defined explicitly in "How Schemas are Determined" section.


DOCUMENT STRUCTURE (Action Part - DFSchema Methods)
====================================================

This section follows the data lifecycle through the query engine:
Schema lives IN the LogicalPlan → df.schema() reads from plan.schema()
→ Transformations create NEW plans with NEW schemas (immutable)

| Section          | Data Lifecycle         | Key DFSchema Methods                                              | Edge Cases / Links                              |
| ---------------- | ---------------------- | ----------------------------------------------------------------- | ----------------------------------------------- |
| **1. Inspect**   | "What do I have?"      | `fields()`, `tree_string()`, `has_column_*()`, `iter()`           | Link: LogicalPlan schema relationship           |
| **2. Create**    | "Define the contract"  | `empty()`, `from_unqualified_fields()`, `try_from()`              | Arrow Schema ↔ DFSchema conversion              |
| **3. Transform** | "Adapt and combine"    | `join()`, `merge()`, `strip_qualifiers()`, `replace_qualifier()`  | TableReference variants (Bare/Partial/Full)     |
| **4. Apply**     | "Use in production"    | `TableProvider::schema()`, reader `.schema()` options             | When SQL/external tools are better              |
| **5. Additional** | Edge cases            | Functional dependencies, metadata, type coercion                  | Decision matrix: DataFrame vs SQL vs External   |

ARROW SCHEMA vs DFSCHEMA - Decision Points
==========================================

When to stay with DFSchema:
- You need qualified column names (joins, subqueries)
- You're building LogicalPlan nodes
- You're using DataFusion's validation methods

When to drop to Arrow Schema (.inner() or .as_arrow()):
- Passing to Arrow compute kernels
- Writing to Parquet/IPC (they only understand Arrow)
- Interop with other Arrow-based tools

TABLEFERENCE - Critical for Transform Section
=============================================

TableReference variants affect qualifier methods:
- `TableReference::Bare("id")` - just column name
- `TableReference::Partial { schema, table }` - schema.table
- `TableReference::Full { catalog, schema, table }` - catalog.schema.table

Edge case example:
  After join: schema has fields like ("users", "id") and ("orders", "id")
  strip_qualifiers() → ("id"), ("id") — now ambiguous!
  replace_qualifier("result") → ("result", "id"), ("result", "id") — still duplicates

WHEN TO USE OTHER TOOLS (for Apply/Additional sections)
=======================================================

| Scenario                          | Better Tool                   | Why                                               |
| --------------------------------- | ----------------------------- | ------------------------------------------------- |
| Introspecting many tables         | SQL `INFORMATION_SCHEMA`      | Single query vs. N `df.schema()` calls            |
| Schema from external catalog      | `TableProvider` impl          | Schema comes from Hive/Iceberg/Delta metadata     |
| Complex qualification resolution  | `TableReference` directly     | Fine control over Bare/Partial/Full               |
| Bulk schema validation            | Arrow's `Schema::equals()`    | Faster for simple field-by-field comparison       |
| Schema stored in external system  | External tool (Postgres, etc) | Let the source of truth manage it                 |

TASK LIST (Priority Order)
==========================

1. [IN PROGRESS] restructure-action-sections
   Reorganize existing content into the 5-section structure above
   - Move/consolidate duplicate content (lines ~1454-1727 duplicate ~813-1047)
   - Ensure each section has: narrative → methods table → code example → edge cases

2. [MEDIUM] add-tablereference-coverage
   Document TableReference interaction with DFSchema
   - Explain Bare/Partial/Full variants
   - Show strip_qualifiers() and replace_qualifier() edge cases
   - Link to query planning docs for deeper context

3. [MEDIUM] add-decision-matrix
   Create summary section: "Choosing the Right Tool"
   - DataFrame API vs SQL API comparison
   - When to use TableProvider directly
   - When external tools (Postgres, etc.) are appropriate

4. [LATER] fix-code-tests
   Enable cargo test for code examples (currently ignored)
   Step by step as we iterate through sections

-->
