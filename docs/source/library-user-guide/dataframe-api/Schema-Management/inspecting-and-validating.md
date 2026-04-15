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

<!--TODO

1. ABSTRACT (write last, after content stabilizes)
2. Sibling cross-references: add links TO this file from creating-schemas.md,
   applying-schemas-modeling-data.md, schema-transformation.md, dataframe-methods.md
3. Test all new code examples with cargo test --doc

-->

# Inspecting and Validating Schemas

[TODO: Abstract — universe → needle tip. Written last after content stabilizes.]

**Key methods:**

| Method                                  | Purpose                                       |
| --------------------------------------- | --------------------------------------------- |
| [`.schema()`]                           | Access the `DFSchema` from a `DataFrame`      |
| [`.tree_string()`]                      | Human-readable schema with types & nullability |
| [`.fields()`]                           | Iterate over field definitions                 |
| `.data_type(&col)`                      | Get a column's Arrow data type (`ExprSchema`)  |
| [`.has_column_with_unqualified_name()`] | Check column existence                         |
| [`.has_equivalent_names_and_types()`]   | Compare schemas with error detail              |
| [`.inner()`]                            | Extract Arrow `Schema` for interop             |

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

## Schema Inspection and Validation

**[`df.schema()`][`.schema()`] returns a `&DFSchema` from the [`LogicalPlan`] — the single entry point for reading field names, types, nullability, qualifiers, and metadata, all at plan time before execution.**

The schema carries field names, Arrow data types, nullability flags, table qualifiers, and metadata — the full structural contract of the [`DataFrame`]. Inspection methods read these properties: display for debugging, iteration for programmatic access, lookups for specific fields. Validation methods check them against expectations: existence checks for guard clauses, equivalence comparisons for contract enforcement, integrity checks for consistency. Both categories operate at plan time — type mismatches, missing columns, and contract violations surface before execution begins.

The schema you inspect originates from the data source. How it arrives depends on how the [`DataFrame`] was created:

| Source                                                   | Returns                           | Example                             |
| :------------------------------------------------------- | :-------------------------------- | :---------------------------------- |
| [`TableProvider::schema()`]                              | `SchemaRef` (Arrow)               | Custom data sources, catalog tables |
| [`ctx.read_parquet(...)`][`.read_parquet()`]             | Arrow Schema from file metadata   | Self-describing formats (Parquet, Arrow IPC, Avro) |
| `CsvReadOptions::new().schema(&schema)`                  | Explicit Arrow Schema you provide | Text formats requiring schema       |
| [`Schema::new(vec![Field::new(...)])`][`Schema::new()`]  | Constructed Arrow Schema          | Programmatic schema definition      |

For a deeper treatment of schema origins and ownership, see [Schema Concepts](schema-concepts.md). For the internal structure of [`DFSchema`], see [Anatomy of a Schema](anatomy-schema.md).

## Displaying Schemas

**A compact field list or a full type-and-nullability tree — two `Display` methods cover every human-readable schema output need.**

Two methods cover every display need: [`.to_string()`] produces a compact field list for quick logging, and [`.tree_string()`] produces a tree with types and nullability — the format you want when debugging coercion errors or null mismatches. Both implement `Display`, so they work directly with `println!`, `format!`, or logging frameworks.

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

**Field definitions, column names, qualifiers, and metadata are all accessible as standard Rust types — enabling guard clauses, iteration, and error handling before any data is processed.**

All access goes through [`df.schema()`][`.schema()`], which returns a `&DFSchema`. The methods below fall into two categories: **collection methods** that return the full set of fields or metadata, and **lookup methods** (covered in the subsections) that target specific columns by name, qualifier, or index.

| Method                                                      | Returns                                          | Use Case                           |
| ----------------------------------------------------------- | ------------------------------------------------ | ---------------------------------- |
| [`df.schema().fields()`][`.fields()`]                       | `&Fields`                                        | Iterate over field definitions     |
| [`df.schema().field(i)`][`.field()`]                        | `&Arc<Field>`                                    | Get field by positional index      |
| [`df.schema().iter()`][`.iter()`]                           | `Iterator<(Option<&TableReference>, &Arc<Field>)>` | Field + qualifier pairs          |
| [`df.schema().field_names()`][`.field_names()`]             | `Vec<String>`                                    | Quick list of all field names      |
| [`df.schema().columns()`][`.columns()`]                     | `Vec<Column>`                                    | All columns as `Column` structs    |
| [`df.schema().metadata()`][`.metadata()`]                   | `&HashMap<String, String>`                       | Schema-level metadata              |

:::{admonition} Schema-level vs field-level metadata
:class: caution
[`df.schema().metadata()`][`.metadata()`] returns **schema-level** metadata — key-value pairs attached to the schema as a whole (e.g., file origin, creation timestamp). For **field-level** metadata (attached to individual columns), use the [`ExprSchema`] trait: `df.schema().metadata(&Column::from("col_name"))`. Both return `&HashMap<String, String>`, but they serve different purposes.
:::

### Field Lookup by Name

**Two method families handle field lookup: `has_column_*` returns `bool` for guard clauses, `field_with_*` returns `Result<>` for explicit error handling.**

Schema lookups can fail — a column may not exist, or a name may be ambiguous after a join. Pick the error handling pattern that matches your goal:

| Pattern            | Method                            | Returns              | Use When                                |
| ------------------ | --------------------------------- | -------------------- | --------------------------------------- |
| Guard clause       | `has_column_*()` → `if`           | `bool`               | Branch based on column existence        |
| Explicit match     | `field_with_*()` → `match`        | `Result<&Arc<Field>>` | Need informative error messages        |
| Fail fast          | `field_with_*()` → `?`            | `Result<&Arc<Field>>` | Pipeline should abort if column missing |

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

### Per-Column Type and Nullability

**[`DFSchema`] implements the [`ExprSchema`] trait — giving you per-column type, nullability, and metadata lookups via [`Column`] references.**

When you need the data type or nullability of a specific column — for conditional logic, validation gates, or building dynamic expressions — use the [`ExprSchema`] methods:

| Method                              | Returns                   | Use Case                          |
| ----------------------------------- | ------------------------- | --------------------------------- |
| `df.schema().data_type(&col)`       | `Result<&DataType>`       | Get a column's Arrow data type    |
| `df.schema().nullable(&col)`        | `Result<bool>`            | Check if a column allows NULLs    |
| `df.schema().data_type_and_nullable(&col)` | `Result<(&DataType, bool)>` | Both in one call            |

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

The qualifier is `Some` when fields come from named tables (e.g., after registering tables and joining them). This is how DataFusion disambiguates columns with the same name from different sources.

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

| Method                                                                                          | Returns                                          | Purpose                                         |
| ----------------------------------------------------------------------------------------------- | ------------------------------------------------ | ----------------------------------------------- |
| [`.fields_with_qualified(qualifier)`][`.fields_with_qualified()`]                               | `Vec<&Arc<Field>>`                               | All fields belonging to a specific table         |
| [`.has_column_with_qualified_name(qualifier, name)`][`.has_column_with_qualified_name()`]        | `bool`                                           | Check existence with qualifier                   |
| [`.field_with_qualified_name(qualifier, name)`][`.field_with_qualified_name()`]                  | `Result<&Arc<Field>>`                            | Lookup by required qualifier + name              |
| [`.qualified_field_with_unqualified_name(name)`][`.qualified_field_with_unqualified_name()`]     | `Result<(Option<&TableReference>, &Arc<Field>)>` | Single field by name (errors if ambiguous)       |

:::{admonition} When qualifiers appear
:class: note
Fields from the [`dataframe!`] macro have no qualifier (`None`). When you register tables with names (via `CREATE TABLE`, `register_table`, or file readers) and query them, fields carry their source table as a qualifier. This is essential for joins where both tables have columns with the same name.
:::

---

## Validating Column Existence

**Check column presence before accessing — use `has_column_*` methods as guard clauses to branch safely.**

These methods return `bool` and never error. Use them when the column might legitimately be absent (optional fields, schema evolution) and your code needs to branch:

- `.has_column_with_unqualified_name(name)` — check by name only (most common)
- `.has_column_with_qualified_name(qualifier, name)` — check by table-qualified name (after joins)
- `.has_column(&column)` — check using a [`Column`] struct (qualifier-aware)

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

### Index-Based Lookup

**Use index lookups when you need a column's position — choose between fail-fast (`Result`) and optional (`Option`) semantics.**

Three methods, each with different failure semantics:

| Method                                                                                  | Returns         | Use When                                           |
| --------------------------------------------------------------------------------------- | --------------- | -------------------------------------------------- |
| [`.index_of_column(col)`][`.index_of_column()`]                                        | `Result<usize>` | Absence is a **hard error** (pipeline should fail) |
| [`.maybe_index_of_column(col)`][`.maybe_index_of_column()`]                            | `Option<usize>` | Absence is **expected** (optional columns)         |
| [`.index_of_column_by_name(qualifier, name)`][`.index_of_column_by_name()`]            | `Option<usize>` | Name-based lookup without constructing a `Column`  |

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

**Validate schemas against expected contracts — before your pipeline runs into runtime surprises.**

Schema comparison sits between inspection and transformation. After you inspect what you have, validation answers: "Is this what I expected?" Use schema-level comparisons to enforce contracts between pipeline stages, type-level comparisons for custom plan nodes, and Arrow validation for ecosystem integration.

### Schema Equivalence

**Two comparison methods with different semantics: qualifier-aware (logical) vs positional (semantic).**

| Method                                                                     | Qualifier-Aware?          | Type Comparison                 | Returns    |
| -------------------------------------------------------------------------- | ------------------------- | ------------------------------- | ---------- |
| [`.logically_equivalent_names_and_types()`]                                | Yes (uses `.iter()`)      | `datatype_is_logically_equal`   | `bool`     |
| [`.has_equivalent_names_and_types()`]                                      | No (positional `.fields()`) | `datatype_is_semantically_equal` | `Result<()>` |

Both methods ignore nullability and metadata. The real distinction:

- **`logically_equivalent`** compares fields **with qualifiers** (via `.iter()`), and treats encoding variants as equal (`Dict<Utf8>` = `Utf8`, `Utf8View` = `Utf8`). Returns `bool` — use for gating checks and tolerant compatibility.
- **`has_equivalent`** compares fields **by position** (via `.fields()`), requires the same encoding representation, and returns a descriptive error on mismatch pinpointing which field differs. Use for enforcing invariants and debugging.

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

    // Both fail on type mismatch
    assert!(!expected.logically_equivalent_names_and_types(&wrong_type));

    // has_equivalent returns a descriptive error
    let err = expected.has_equivalent_names_and_types(&wrong_type).unwrap_err();
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

| Function                                          | Treats as Equal                                   | Use Case                          |
| ------------------------------------------------- | ------------------------------------------------- | --------------------------------- |
| `DFSchema::datatype_is_logically_equal(dt1, dt2)` | `Dict<K, Utf8>` = `Utf8`, `Utf8View` = `Utf8`    | Tolerant (ignores encoding)       |
| `DFSchema::datatype_is_semantically_equal(dt1, dt2)` | Same representation required                   | Strict (encoding matters)         |

These are **associated functions** on [`DFSchema`], not instance methods — call them as `DFSchema::datatype_is_logically_equal(dt1, dt2)`.

### Validating Schema Integrity

**[`.check_names()`] detects duplicate field names — useful after programmatic schema construction or joins.**

[`.check_names()`] validates that a [`DFSchema`] has no duplicate field names, considering both qualified and unqualified names. Duplicate names can arise from programmatic schema construction or plan manipulation:

```rust
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::{Field, DataType, Schema};

fn main() -> datafusion::error::Result<()> {
    let valid = DFSchema::try_from(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))?;
    assert!(valid.check_names().is_ok());

    Ok(())
}
```

### Validating Against Arrow Schemas

**[`.matches_arrow_schema()`] checks field-by-field name alignment between a [`DFSchema`] and an Arrow [`Schema`].**

This method compares field names only — ignoring types, nullability, and metadata. Use it when verifying that the logical plan's schema aligns with physical data before execution:

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

    Ok(())
}
```

---

## Arrow Interop

**Use Arrow interop methods when you need to pass the schema to Arrow ecosystem functions — compute kernels, IPC writers, `RecordBatch` creation.**

[`DFSchema`] wraps an Arrow [`Schema`] with additional query-planning context (qualifiers, functional dependencies). When you cross into the Arrow ecosystem, you extract the inner Arrow schema:

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

:::{admonition} Related documents
:class: seealso

- [Schema Concepts](schema-concepts.md) — ownership flow, schema types, `DFSchema` vs Arrow `Schema`
- [Anatomy of a Schema](anatomy-schema.md) — per-column field properties (name, data type, nullable, metadata)
- [Type Coercion](type-coercion.md) — automatic type alignment and explicit casting
- [Schema Transformation](schema-transformation.md) — qualifiers, combining schemas, nullability handling
- [DataFrame Methods](dataframe-methods.md) — methods that change the schema (`.with_column()`, `.with_column_renamed()`)
:::

Further reading:

- [Parquet schema evolution][parquet-evolution]
- [Designing Data-Intensive Applications][kleppmann]

---

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`Schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`Schema::new()`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html#method.new
[`Field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html
[`Column`]: https://docs.rs/datafusion/latest/datafusion/common/struct.Column.html
[`DataType`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
[`ExprSchema`]: https://docs.rs/datafusion/latest/datafusion/common/trait.ExprSchema.html
[`FunctionalDependencies`]: https://docs.rs/datafusion/latest/datafusion/common/functional_dependencies/struct.FunctionalDependencies.html
[`TableProvider::schema()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html#tymethod.schema
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html

[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.to_string()`]: https://doc.rust-lang.org/std/string/trait.ToString.html
[`.tree_string()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.tree_string
[`.fields()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.fields
[`.field()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.field
[`.iter()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.iter
[`.field_names()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.field_names
[`.columns()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.columns
[`.metadata()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.metadata
[`.has_column_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.has_column_with_unqualified_name
[`.has_column_with_qualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.has_column_with_qualified_name
[`.field_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.field_with_unqualified_name
[`.field_with_qualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.field_with_qualified_name
[`.fields_with_qualified()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.fields_with_qualified
[`.qualified_field_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.qualified_field_with_unqualified_name
[`.index_of_column()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.index_of_column
[`.maybe_index_of_column()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.maybe_index_of_column
[`.index_of_column_by_name()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.index_of_column_by_name
[`.logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`.has_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.has_equivalent_names_and_types
[`.check_names()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.check_names
[`.matches_arrow_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.matches_arrow_schema
[`.inner()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.inner
[`.as_arrow()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.as_arrow
[`.functional_dependencies()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.functional_dependencies
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`.read_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_table

[parquet-evolution]: https://parquet.apache.org/docs/file-format/metadata/#schema-evolution
[kleppmann]: https://dataintensive.net/
