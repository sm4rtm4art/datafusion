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

# Inspecting and Validating Schemas

**Display, query, compare, and extract a DataFrame's structural contract from the plan before execution reads rows.**

The schema attached to every [`DataFrame`] is a rich, queryable object — not just static metadata. This document shows how to work with that object across four progressively deeper levels: **displaying** schemas for quick debugging and logging, **accessing** individual field properties — names, types, nullability, qualifiers — for programmatic pipeline logic, **comparing and validating** schemas against expected contracts, and **extracting** the underlying Arrow `Schema` for ecosystem interop. Each section builds on the previous, moving from human-readable inspection to type-safe API access and `Result`-based validation.

**Key methods:**

| Method                                      | Purpose                                        | Section                                                                   |
| ------------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------- |
| [`.schema()`]                               | Access `&DFSchema` from a `DataFrame`          | [The Schema as a Queryable Contract](#the-schema-as-a-queryable-contract) |
| [`.tree_string()`]                          | Human-readable schema with types & nullability | [Displaying Schemas](#displaying-schemas)                                 |
| [`.fields()`]                               | Iterate over field definitions                 | [Accessing Fields and Properties](#accessing-fields-and-properties)       |
| [`.field_with_unqualified_name()`]          | Get field definition by name                   | [Field Lookup by Name](#field-lookup-by-name)                             |
| [`.has_column_with_unqualified_name()`]     | Check column existence (returns `bool`)        | [Validating Column Existence](#validating-column-existence)               |
| `.data_type(&col)` ([`ExprSchema`])         | Get a column's Arrow data type                 | [Per-Column Type and Nullability](#per-column-type-and-nullability)       |
| [`.iter()`]                                 | Field + qualifier pairs                        | [Qualified Field Access](#qualified-field-access)                         |
| [`.index_of_column()`]                      | Get column's positional index                  | [Index-Based Lookup](#index-based-lookup)                                 |
| [`.has_equivalent_names_and_types()`]       | Compare schemas with error detail              | [Schema Equivalence](#schema-equivalence)                                 |
| [`DFSchema::datatype_is_logically_equal()`] | Compare two data types tolerantly              | [Type-Level Comparison](#type-level-comparison)                           |
| [`.check_names()`]                          | Detect duplicate or ambiguous field names      | [Validating Schema Integrity](#validating-schema-integrity)               |
| [`.matches_arrow_schema()`]                 | Positional name-alignment with Arrow `Schema`  | [Validating Against Arrow Schemas](#validating-against-arrow-schemas)     |
| [`.inner()`] / [`.as_arrow()`]              | Extract Arrow `Schema` for ecosystem interop   | [Arrow Interop](#arrow-interop)                                           |

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

```{contents} Inspecting and Validating Schemas
:local:
:depth: 2
```

## The Schema as a Queryable Contract

**Schema inspection validates the `DFSchema` contract that DataFusion has planned; source-data parse errors and value-level mismatches may still appear when execution reads rows.**

Data arrives from heterogeneous sources — Parquet files with evolving schemas, CSV feeds from external partners, programmatic `RecordBatch` construction — and every source carries its own structural assumptions. Schemas drift between releases, upstream changes silently add or drop fields, and type mismatches hide until they break a downstream join or aggregation. DataFusion exposes the schema as a first-class, queryable object through [`df.schema()`][`.schema()`], which returns a `&DFSchema` containing field names, Arrow data types, nullability flags, table qualifiers, and metadata — the full structural contract of the [`DataFrame`].

This document covers two complementary paths for working with that contract:

- **Human-readable inspection** — displaying the schema for debugging, logging, and quick verification ([Displaying Schemas](#displaying-schemas)).
- **Programmatic validation** — accessing field properties, checking column existence, comparing schemas against expectations, and extracting Arrow schemas for ecosystem interop ([Accessing Fields and Properties](#accessing-fields-and-properties) through [Arrow Interop](#arrow-interop)).

Consider a pipeline that joins customer data from Parquet files with transaction records from a partner's CSV feed. Before the join, you need to verify that both sources expose the expected key columns, compatible planned types, and stable field names. These checks happen through [`df.schema()`][`.schema()`] — a plan-time operation over the `DFSchema`. Source values still need to parse into that schema when an action runs.

The schema you inspect originates from the data source. How it arrives depends on how the [`DataFrame`] was created:

| Source                                                  | Returns                           | Example                                            |
| :------------------------------------------------------ | :-------------------------------- | :------------------------------------------------- |
| [`TableProvider::schema()`]                             | `SchemaRef` (Arrow)               | Custom data sources, catalog tables                |
| [`ctx.read_parquet(...)`][`.read_parquet()`]            | Arrow Schema from file metadata   | Self-describing formats (Parquet, Arrow IPC, Avro) |
| `CsvReadOptions::new().schema(&schema)`                 | Explicit Arrow Schema you provide | Text formats requiring schema                      |
| [`Schema::new(vec![Field::new(...)])`][`Schema::new()`] | Constructed Arrow Schema          | Programmatic schema definition                     |

For a deeper treatment of schema origins and ownership, see [Schema Concepts](schema-concepts.md). For the internal structure of [`DFSchema`], see [Anatomy of a Schema](schema-anatomy.md).

:::{admonition} Pre-analysis vs post-analysis schema
:class: tip
The schema returned by [`.schema()`] reflects the **pre-analysis** state of the [`LogicalPlan`]. After the [`TypeCoercion`] analyzer runs (triggered by [`.collect()`] or [`.show()`]), types may change due to implicit widening. To see the post-analysis schema with inserted `CAST` nodes, use [`.explain(false, false)`] — see [Type Coercion](type-coercion.md) for details.
:::

## Displaying Schemas

**A compact field list or a full type-and-nullability tree — two display methods cover every human-readable schema output need.**

`DFSchema` implements `Display` directly, so `println!("{}", df.schema())` works — but the output is a compact field list with names only, no types or nullability. For debugging type mismatches or coercion errors, you almost always want [`.tree_string()`], which reveals the type information that the `Display` output omits. Both work directly with `println!`, `format!`, or logging frameworks. For programmatic field access, see [Accessing Fields and Properties](#accessing-fields-and-properties).

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

    // Detailed: types and nullability (best for debugging)
    println!("{}", df.schema().tree_string());
    // Output:
    // root
    //  |-- user_id: int64 (nullable = true)
    //  |-- email: utf8 (nullable = true)
    //  |-- active: boolean (nullable = true)

    # // Verify display output
    # let tree_output = format!("{}", df.schema().tree_string());
    # assert!(tree_output.contains("user_id: int64"));
    # assert!(tree_output.contains("email: utf8"));
    # assert!(tree_output.contains("active: boolean"));
    # let string_output = df.schema().to_string();
    # assert!(string_output.contains("user_id"));
    # assert!(string_output.contains("email"));
    # assert!(string_output.contains("active"));

    Ok(())
}
```

:::{admonition} Debugging schema mismatches
:class: tip
When debugging schema mismatches, use [`df.schema().tree_string()`][`.tree_string()`] first — it shows types and nullability, which are often the culprits.
:::

---

## Accessing Fields and Properties

**Programmatic access to every field property — names, types, nullability, qualifiers — flows through a single `&DFSchema` reference, giving you type-safe API access and `Result`-based error handling over the schema contract.**

All access goes through [`df.schema()`][`.schema()`], which returns a `&DFSchema`. For a detailed breakdown of what each field contains — name, data type, nullability, and metadata — see [Anatomy of a Schema](schema-anatomy.md). The methods below fall into two categories: **collection methods** that return the full set of fields or metadata, and **lookup methods** that target specific columns by name, qualifier, or index. Collection methods are useful for iteration, counting, or bulk validation. Lookup methods are useful for guard clauses, type checks, and error handling. The subsections below cover lookups by name, per-column type inspection via [`ExprSchema`], qualifier-aware access, existence checks, and index-based lookups.

| Method                                          | Returns                                            | Use Case                        |
| ----------------------------------------------- | -------------------------------------------------- | ------------------------------- |
| [`df.schema().fields()`][`.fields()`]           | `&Fields`                                          | Iterate over field definitions  |
| [`df.schema().field(i)`][`.field()`]            | `&Arc<Field>`                                      | Get field by positional index   |
| [`df.schema().iter()`][`.iter()`]               | `Iterator<(Option<&TableReference>, &Arc<Field>)>` | Field + qualifier pairs         |
| [`df.schema().field_names()`][`.field_names()`] | `Vec<String>`                                      | Quick list of all field names   |
| [`df.schema().columns()`][`.columns()`]         | `Vec<Column>`                                      | All columns as `Column` structs |
| [`df.schema().metadata()`][`.metadata()`]       | `&HashMap<String, String>`                         | Schema-level metadata           |

:::{admonition} Schema-level vs field-level metadata
:class: caution
[`df.schema().metadata()`][`.metadata()`] returns **schema-level** metadata — key-value pairs attached to the schema as a whole (e.g., file origin, creation timestamp). For **field-level** metadata (attached to individual columns), use the [`ExprSchema`] trait: `df.schema().metadata(&Column::from("col_name"))`. Both return `&HashMap<String, String>`, but they serve different purposes.
:::

### Field Lookup by Name

**Two method families handle field lookup: `has_column_*` returns `bool` for guard clauses, `field_with_*` returns `Result<>` for explicit error handling.**

Name-based lookups are the most common access pattern — you know the column name from your domain logic and need to verify it exists or retrieve its definition. Two method families serve this need: `has_column_*` methods return `bool` for branching (is this optional field present?), while `field_with_*` methods return `Result<&Arc<Field>>` for access (retrieve the field or fail with a descriptive error). The choice between them depends on whether absence is expected or exceptional:

| Pattern        | Method                     | Returns               | Use When                                |
| -------------- | -------------------------- | --------------------- | --------------------------------------- |
| Guard clause   | `has_column_*()` → `if`    | `bool`                | Branch based on column existence        |
| Explicit match | `field_with_*()` → `match` | `Result<&Arc<Field>>` | Need informative error messages         |
| Fail fast      | `field_with_*()` → `?`     | `Result<&Arc<Field>>` | Pipeline should abort if column missing |

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
    if schema.has_column_with_unqualified_name("email") {
        let field = schema.field_with_unqualified_name("email")?;
        println!("Email type: {}", field.data_type());
    }

    // Pattern 2: Try and handle error explicitly
    match schema.field_with_unqualified_name("nonexistent_column") {
        Ok(field) => println!("Found: {}", field.name()),
        Err(e) => eprintln!("Column lookup failed: {}", e),
        // Output: "Column lookup failed: Schema error: No field named nonexistent_column"
    }

    // Pattern 3: Propagate error with context
    let _field = schema
        .field_with_unqualified_name("user_id")
        .map_err(|e| datafusion::error::DataFusionError::Plan(
            format!("Required column missing: {}", e)
        ))?;

    # // Verify guard clause
    # assert!(schema.has_column_with_unqualified_name("email"));
    # assert!(!schema.has_column_with_unqualified_name("nonexistent_column"));
    # // Verify error path
    # assert!(schema.field_with_unqualified_name("nonexistent_column").is_err());
    # // Verify success path
    # let field = schema.field_with_unqualified_name("user_id")?;
    # assert_eq!(field.name(), "user_id");

    Ok(())
}
```

### Validating Column Existence

**Check column presence before accessing — use `has_column_*` methods as guard clauses to branch safely.**

The [Field Lookup by Name](#field-lookup-by-name) section introduced `has_column_*` alongside `field_with_*`. This section focuses on the `has_column_*` family for guard clauses — when you need to branch based on column presence rather than access the field definition. All three methods return `bool` and never error:

| Method                                             | Takes                   | Use When                               |
| -------------------------------------------------- | ----------------------- | -------------------------------------- |
| `.has_column_with_unqualified_name(name)`          | `&str`                  | Check by name only (most common)       |
| `.has_column_with_qualified_name(qualifier, name)` | `&TableReference, &str` | Check after joins (table-qualified)    |
| `.has_column(&column)`                             | `&Column`               | Dispatches based on qualifier presence |

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
        println!("Missing columns: {:?}", missing);
        // Output: Missing columns: ["timestamp"]
        assert_eq!(missing, vec![&"timestamp"]);
    }

    Ok(())
}
```

### Per-Column Type and Nullability

**Per-column type and nullability lookups let you build conditional logic, validation gates, and dynamic expressions based on a column's Arrow [`DataType`] — accessed through the [`ExprSchema`] trait that [`DFSchema`] implements.**

These methods take a [`Column`] reference (constructed via `Column::from("name")` for unqualified lookups) and return the property for that specific column. Unlike the collection methods in the parent section which return all fields at once, [`ExprSchema`] methods target a single column — useful when your logic needs to branch based on whether a column is `Float64` vs `Decimal128`, or whether NULLs are possible:

| Method                                     | Returns                     | Use Case                       |
| ------------------------------------------ | --------------------------- | ------------------------------ |
| `df.schema().data_type(&col)`              | `Result<&DataType>`         | Get a column's Arrow data type |
| `df.schema().nullable(&col)`               | `Result<bool>`              | Check if a column allows NULLs |
| `df.schema().data_type_and_nullable(&col)` | `Result<(&DataType, bool)>` | Both in one call               |

```rust
use datafusion::prelude::*;
use datafusion::common::ExprSchema;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "price"    => [10.5_f64, 20.0],
        "quantity" => [1_i32, 5]
    )?;

    let schema = df.schema();
    let price_col = Column::from("price");

    // Check column type before processing
    let dt = schema.data_type(&price_col)?;
    assert_eq!(dt, &DataType::Float64);

    // Check nullability for validation gates
    let is_nullable = schema.nullable(&price_col)?;
    assert!(is_nullable);

    Ok(())
}
```

:::{admonition} ExprSchema is a trait — import required
:class: note
The `data_type()`, `nullable()`, and per-column `metadata()` methods come from the [`ExprSchema`] trait, not from [`DFSchema`] directly. Import it with `use datafusion::common::ExprSchema;` to bring the methods into scope. For the full trait definition, see the [API documentation][`ExprSchema`].
:::

### Qualified Field Access

**The [`.iter()`] method returns `(Option<&TableReference>, &Arc<Field>)` pairs — the qualifier distinguishes columns with the same name from different tables.**

Fields from the [`dataframe!`] macro or single-table queries have no qualifier (`None`). When you register tables with names (via `CREATE TABLE`, `register_table`, or file readers) and join them, fields carry their source table as a qualifier. This is how DataFusion disambiguates columns with the same name from different sources — without qualifiers, accessing `id` after joining `users` and `orders` would be ambiguous.

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
                println!("{}.{}: {}", table_ref, field.name(), field.data_type());
            }
            None => {
                println!("{}: {}", field.name(), field.data_type());
            }
        }
    }

    Ok(())
}
```

For qualifier-specific lookups, [`DFSchema`] provides methods that filter by qualifier:

| Method                                                                                       | Returns                                          | Purpose                                    |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------ | ------------------------------------------ |
| [`.fields_with_qualified(qualifier)`][`.fields_with_qualified()`]                            | `Vec<&Arc<Field>>`                               | All fields belonging to a specific table   |
| [`.has_column_with_qualified_name(qualifier, name)`][`.has_column_with_qualified_name()`]    | `bool`                                           | Check existence with qualifier             |
| [`.field_with_qualified_name(qualifier, name)`][`.field_with_qualified_name()`]              | `Result<&Arc<Field>>`                            | Lookup by required qualifier + name        |
| [`.qualified_field_with_unqualified_name(name)`][`.qualified_field_with_unqualified_name()`] | `Result<(Option<&TableReference>, &Arc<Field>)>` | Single field by name (errors if ambiguous) |

:::{admonition} When qualifiers appear
:class: note
Fields from the [`dataframe!`] macro have no qualifier (`None`). When you register tables with names (via `CREATE TABLE`, `register_table`, or file readers) and query them, fields carry their source table as a qualifier. This is essential for joins where both tables have columns with the same name.
:::

### Index-Based Lookup

**Use index lookups when you need a column's position — choose between fail-fast (`Result`) and optional (`Option`) semantics.**

Index-based lookups return a column's ordinal position in the schema — the `usize` you need when accessing columns from a `RecordBatch` via `batch.column(idx)` or when building custom operators that reference columns by position. Three methods offer different failure semantics:

| Method                                                                      | Returns         | Use When                                           |
| --------------------------------------------------------------------------- | --------------- | -------------------------------------------------- |
| [`.index_of_column(col)`][`.index_of_column()`]                             | `Result<usize>` | Absence is a **hard error** (pipeline should fail) |
| [`.maybe_index_of_column(col)`][`.maybe_index_of_column()`]                 | `Option<usize>` | Absence is **expected** (optional columns)         |
| [`.index_of_column_by_name(qualifier, name)`][`.index_of_column_by_name()`] | `Option<usize>` | Name-based lookup without constructing a `Column`  |

The first two take a [`Column`] struct — use `Column::from("name")` for unqualified lookups. The third takes `Option<&TableReference>` and `&str` directly, avoiding `Column` construction.

```rust
use datafusion::prelude::*;

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

    // Name-based path: no Column struct needed
    let idx = schema.index_of_column_by_name(None, "name");
    assert_eq!(idx, Some(1));

    Ok(())
}
```

---

## Comparing and Validating Schemas

**Validate the planned schema contract before execution, then keep value-level checks for the read path.**

Schema comparison sits between inspection and transformation. After you inspect what you have, validation answers: "Is this what I expected?" The built-in equivalence helpers cover field names and data types. If your contract also depends on nullability or metadata, validate those properties explicitly with field accessors before trusting the schema downstream.

| Contract property | Primary API                                       | Notes                                                                 |
| ----------------- | ------------------------------------------------- | --------------------------------------------------------------------- |
| Names and types   | [`.has_equivalent_names_and_types()`]             | Ignores nullability and metadata                                      |
| Nullability       | `field.is_nullable()` or [`ExprSchema`] methods   | Compare manually when required fields must reject `NULL`              |
| Field metadata    | `field.metadata()` or [`ExprSchema`] `metadata()` | Compare manually for semantic tags such as PII or schema version data |
| Schema metadata   | [`.metadata()`]                                   | Compare manually for schema-level contract attributes                 |
| Name uniqueness   | [`.check_names()`]                                | Detects duplicate and ambiguous field names                           |

The subsections below cover progressively narrower scopes: whole-schema equivalence checks for enforcing contracts between pipeline stages, individual type comparisons for custom plan nodes or UDFs, name-uniqueness validation for schema integrity after construction, and name-alignment checks for `DFSchema`-to-Arrow interop.

### Schema Equivalence

**Two comparison methods differ on field matching and type compatibility: one is qualifier-aware and more permissive, the other is positional and more specific.**

Both methods ignore nullability and metadata — they focus purely on field names and data types. Where they differ is on two independent axes:

- **Field matching:** [`.logically_equivalent_names_and_types()`] pairs fields using [`.iter()`], which includes qualifiers — so `users.id` and `orders.id` are distinct fields. [`.has_equivalent_names_and_types()`] pairs fields using [`.fields()`] by position only, ignoring qualifiers entirely.
- **Type compatibility:** `logically_equivalent` treats encoding variants as equal (`Dict<K, Utf8>` = `Utf8`, `Utf8View` = `Utf8`). `has_equivalent` uses `datatype_is_semantically_equal()`, which is more specific about representation, but still ignores decimal precision/scale and timestamp unit/timezone.

| Method                                      | Field Matching              | Type Compatibility                               | Returns      |
| ------------------------------------------- | --------------------------- | ------------------------------------------------ | ------------ |
| [`.logically_equivalent_names_and_types()`] | Qualifier-aware (`.iter()`) | Most permissive (`datatype_is_logically_equal`)  | `bool`       |
| [`.has_equivalent_names_and_types()`]       | Positional (`.fields()`)    | More specific (`datatype_is_semantically_equal`) | `Result<()>` |

:::{admonition} In practice
:class: tip

Use `logically_equivalent` for compatibility gates where encoding differences are acceptable. Use `has_equivalent` when field positions, names, and semantic type mismatches should produce a descriptive error. If precision, timezone, nullability, or metadata are part of the contract, add explicit checks for those properties.
:::

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

    // Both pass — nullability differences are ignored by both methods
    assert!(expected.logically_equivalent_names_and_types(&actual));
    assert!(expected.has_equivalent_names_and_types(&actual).is_ok());

    // Where they diverge: type mismatches
    let wrong_type = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),  // Utf8 instead of Int64
        Field::new("name", DataType::Utf8, true),
    ]))?;

    // logically_equivalent returns bool
    assert!(!expected.logically_equivalent_names_and_types(&wrong_type));

    // has_equivalent returns a descriptive error pinpointing the mismatch
    let err = expected.has_equivalent_names_and_types(&wrong_type).unwrap_err();
    println!("{}", err);
    // Output: "Schema mismatch: Expected field 'id' with type Int64, but got 'id' with type Utf8."
    assert!(err.to_string().contains("Schema mismatch"));

    Ok(())
}
```

:::{admonition} Use `has_equivalent_names_and_types()` for debugging
:class: tip
Use [`.has_equivalent_names_and_types()`] in tests and pipeline entry points — its error messages pinpoint exactly which field mismatches, saving debugging time.
:::

### Type-Level Comparison

**For comparing individual data types — useful in custom plan nodes, UDFs, or when building dynamic expressions.**

The schema equivalence methods above use these functions internally. When you need to compare individual types — for example, validating a UDF's input type matches the column, or building a dynamic expression that depends on the column's encoding — call them directly. These are **associated functions** on [`DFSchema`], not instance methods. Neither function compares metadata or nullability; `datatype_is_semantically_equal()` also treats decimal precision/scale and timestamp unit/timezone as equal.

| Function                                             | Treats as Equal                                         | Use Case                                 |
| ---------------------------------------------------- | ------------------------------------------------------- | ---------------------------------------- |
| `DFSchema::datatype_is_logically_equal(dt1, dt2)`    | `Dict<K, Utf8>` = `Utf8`, `Utf8View` = `Utf8`           | Broad compatibility checks               |
| `DFSchema::datatype_is_semantically_equal(dt1, dt2)` | Same logical type family, with selected details ignored | More specific field-type contract checks |

```rust
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::DataType;

fn main() {
    // Logical equality: Utf8View and Utf8 are logically the same data
    assert!(DFSchema::datatype_is_logically_equal(
        &DataType::Utf8View, &DataType::Utf8
    ));

    // Semantic equality: different representations are NOT semantically equal
    assert!(!DFSchema::datatype_is_semantically_equal(
        &DataType::Utf8View, &DataType::Utf8
    ));

    // Both agree when representations match
    assert!(DFSchema::datatype_is_logically_equal(
        &DataType::Int64, &DataType::Int64
    ));
    assert!(DFSchema::datatype_is_semantically_equal(
        &DataType::Int64, &DataType::Int64
    ));
}
```

### Validating Schema Integrity

**Duplicate field names cause ambiguous column resolution — [`.check_names()`] detects them before they surface as runtime errors.**

Duplicates can appear in a `DFSchema` because `DFSchema::try_from()` skips this check by design (internal operations like partial aggregates can produce duplicate state fields). When you construct or manipulate schemas programmatically, call [`.check_names()`] explicitly to catch conflicts. The method performs three independent checks:

| Check                       | Detects                                       | Example                                 |
| --------------------------- | --------------------------------------------- | --------------------------------------- |
| Duplicate qualified names   | Two fields with the same qualifier + name     | Two `users.id` fields                   |
| Duplicate unqualified names | Two bare fields with the same name            | Two `id` fields without qualifiers      |
| Ambiguous references        | A qualified name collides with an unqualified | `users.id` exists alongside a bare `id` |

```rust
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::{Field, DataType, Schema};

fn main() -> datafusion::error::Result<()> {
    // Valid schema — unique field names
    let valid = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))?;
    assert!(valid.check_names().is_ok());

    // Duplicate unqualified names — check_names catches the conflict
    let duplicate = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("id", DataType::Utf8, true),
    ]))?;
    let err = duplicate.check_names().unwrap_err();
    assert!(err.to_string().contains("id"), "Expected duplicate field error");

    Ok(())
}
```

### Validating Against Arrow Schemas

**Verify that a `DFSchema` and an Arrow [`Schema`] share the same field names at each position — a lightweight check for cross-ecosystem handoffs.**

[`.matches_arrow_schema()`] pairs field names by position — the first field in the `DFSchema` against the first in the Arrow [`Schema`], the second against the second, and so on (using Rust's [`Iterator::zip()`], which works like Python's `zip()`). Only names are compared; types, nullability, and metadata are ignored. If the schemas have different lengths, only the overlapping positions are checked — extra fields in the longer schema are silently skipped.

```rust
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::{Field, DataType, Schema};

fn main() -> datafusion::error::Result<()> {
    let df_schema = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))?;

    // Same field names — matches regardless of types
    let arrow_schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("name", DataType::Int32, false),
    ]);
    assert!(df_schema.matches_arrow_schema(&arrow_schema));

    // Different field names — does not match
    let wrong_names = Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    assert!(!df_schema.matches_arrow_schema(&wrong_names));

    // Different lengths — only overlapping positions are compared,
    // extra fields in the longer schema are silently skipped
    let longer_arrow = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("extra_col", DataType::Boolean, false),
    ]);
    assert!(df_schema.matches_arrow_schema(&longer_arrow));

    Ok(())
}
```

:::{admonition} Field count is not checked
:class: warning
When field count matters, compare `.fields().len()` explicitly before calling [`.matches_arrow_schema()`] — the method only validates that overlapping positions have matching names.
:::

[`Iterator::zip()`]: https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.zip

---

## Arrow Interop

**Use Arrow interop methods when you need to pass the schema to Arrow ecosystem functions — compute kernels, IPC writers, `RecordBatch` creation.**

After inspecting and validating your schema within the DataFusion planning layer, you often need to hand it to Arrow ecosystem code — compute kernels that operate on `RecordBatch` columns, IPC writers that serialize data, or third-party libraries that expect an Arrow [`Schema`]. [`DFSchema`] wraps an Arrow [`Schema`] with query-planning context (qualifiers, functional dependencies), so extraction is a cheap reference operation rather than a copy. Two methods provide different ownership semantics: [`.inner()`] returns `&SchemaRef` (`&Arc<Schema>`) for cheap cloning, [`.as_arrow()`] returns `&Schema` for direct field access. The trade-off: table qualifiers and functional dependencies do not survive the conversion — if you need qualified names for join disambiguation, stay with [`DFSchema`].

| Method                                    | Returns                       | Use Case                          |
| ----------------------------------------- | ----------------------------- | --------------------------------- |
| [`df.schema().inner()`][`.inner()`]       | `&SchemaRef` (`&Arc<Schema>`) | Cheap cloning for Arrow functions |
| [`df.schema().as_arrow()`][`.as_arrow()`] | `&Schema`                     | Direct reference for field access |

:::{admonition} Table qualifiers are lost
:class: warning
Table qualifiers (e.g., `users.id` vs `orders.id`) are **lost** when converting to Arrow [`Schema`]. If you need qualified names for join disambiguation, stay with [`DFSchema`]. The Arrow [`Schema`] only carries unqualified field names.
:::

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

[`DFSchema`] also implements `AsRef<Schema>` and `TryFrom<Schema>`, allowing conversion in both directions:

```rust
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::{Field, DataType, Schema};

fn main() -> datafusion::error::Result<()> {
    // Arrow Schema → DFSchema (qualifiers default to None)
    let arrow_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
    ]);
    let df_schema = DFSchema::try_from(arrow_schema)?;

    // DFSchema → &Schema (via AsRef)
    let back: &Schema = df_schema.as_ref();
    assert_eq!(back.fields().len(), 1);

    Ok(())
}
```

:::{admonition} Functional dependencies
:class: note
[`DFSchema`] carries [`FunctionalDependencies`] — constraints like primary keys and unique columns that the optimizer uses for plan optimization (e.g., eliminating redundant sorts). Access them via [`df.schema().functional_dependencies()`][`.functional_dependencies()`]. These constraints are lost when converting to Arrow [`Schema`]. For details on how functional dependencies participate in query planning, see [Schema Concepts](schema-concepts.md).
:::

:::{admonition} `dataframe!` macro nullability
:class: caution
The [`dataframe!`] macro sets all columns to `nullable = true` by default. In production, use [`ctx.read_parquet(...)`][`.read_parquet()`], [`ctx.read_csv(...)`][`.read_csv()`], or [`ctx.read_table(...)`][`.read_table()`] to load data with their native nullability settings.
:::

---

## Conclusion & Further Reading

**Schema inspection and validation form the defensive layer between planned data sources and pipeline logic — checking the available contract before execution reads values.**

[`.schema()`] gives you the planned structural contract: [`.tree_string()`] and [`.to_string()`] for human-readable display, [`.fields()`] and [`.iter()`] for programmatic access, `has_column_*` and `field_with_*` for existence checks, and equivalence methods for names and types. When nullability, metadata, precision, or timezone details matter, compare those fields explicitly. When you cross into the Arrow ecosystem, [`.inner()`] and [`.as_arrow()`] extract the inner Arrow [`Schema`] — but table qualifiers and functional dependencies are lost in the conversion.

:::{admonition} Related documents
:class: seealso

- [Schema Concepts](schema-concepts.md) — ownership flow, schema types, `DFSchema` vs Arrow `Schema`
- [Anatomy of a Schema](schema-anatomy.md) — per-column field properties (name, data type, nullable, metadata)
- [Schema Inference](schema-inference.md) — why inferred schemas should be checked before production use
- [Type Coercion](type-coercion.md) — automatic type alignment and explicit casting
- [Schema Transformation](schema-transformation.md) — qualifiers, combining schemas, nullability handling
- [DataFrame Methods](schema-dataframe-methods.md) — methods that change the schema (`.with_column()`, `.with_column_renamed()`)
  ::::

---

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`Schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`Schema::new()`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html#method.new
[`Field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html
[`Column`]: https://docs.rs/datafusion/latest/datafusion/common/struct.Column.html
[`DataType`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
[`ExprSchema`]: https://docs.rs/datafusion/latest/datafusion/common/trait.ExprSchema.html
[`FunctionalDependencies`]: https://docs.rs/datafusion/latest/datafusion/common/struct.FunctionalDependencies.html
[`TableProvider::schema()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html#tymethod.schema
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.to_string()`]: https://doc.rust-lang.org/std/string/trait.ToString.html
[`.tree_string()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.tree_string
[`.fields()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.fields
[`.field()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field
[`.iter()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.iter
[`.field_names()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_names
[`.columns()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.columns
[`.metadata()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.metadata
[`.has_column_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column_with_unqualified_name
[`.has_column_with_qualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column_with_qualified_name
[`.field_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_unqualified_name
[`.field_with_qualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_qualified_name
[`.fields_with_qualified()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.fields_with_qualified
[`.qualified_field_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.qualified_field_with_unqualified_name
[`.index_of_column()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.index_of_column
[`.maybe_index_of_column()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.maybe_index_of_column
[`.index_of_column_by_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.index_of_column_by_name
[`.logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`.has_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_equivalent_names_and_types
[`DFSchema::datatype_is_logically_equal()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.datatype_is_logically_equal
[`.check_names()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.check_names
[`.matches_arrow_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.matches_arrow_schema
[`.inner()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`.as_arrow()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.as_arrow
[`.functional_dependencies()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.functional_dependencies
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`.read_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_table
[`TypeCoercion`]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/type_coercion/struct.TypeCoercion.html
[`.explain(false, false)`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`printSchema()`]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.printSchema.html
