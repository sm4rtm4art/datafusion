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

1. ABSTRACT
2. Unify introduction (merge the two intro paragraphs into one coherent narrative)
3. Fix cross-references to other files

-->

# Inspecting and Validating Schemas

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

1. **Guard clause** <br> check with `has_column_*()` before accessing; use when you need to branch.
2. **Explicit match**<br> `match` on `field_with_*()` result; use when you need informative error messages. This is the most common pattern for error handling.
3. **Propagate with [`?`]**<br> `field_with_*().map_err(...)?`; use in pipeline functions that should fail fast.

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

> **Note:**<br>
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

> **Note:** <br>
> The [`dataframe!`] macro sets all columns to `nullable = true` by default. In production, use [`ctx.read_parquet(...)`][`.read_parquet()`], [`ctx.read_csv(...)`][`.read_csv()`], or [`ctx.read_table(...)`][`.read_table()`] to load data with their native nullability settings.

### Additional DFSchema Methods

For a complete reference of all [`DFSchema`] methods, see the [API documentation][`DFSchema`]. Beyond the core methods shown above, these are useful for field access and column-level inspection:

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

> **Note:** <br>
> Methods taking `&column` expect a [`Column`] struct (e.g., `Column::from("name")` or `Column::new_unqualified("name")`), not a plain `&str`. The `data_type` and `nullable` methods come from the [`ExprSchema`] trait, which `DFSchema` implements.

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

> **Tip:** <br>
> Most of these methods are wrappers around [`.iter()`] with different filter/return semantics. If you need a custom lookup pattern, iterating directly with `.iter()` is often simpler than finding the right method name.

---

## Validating Schemas

**Validate column existence, resolve field positions, and compare schemas against expected contracts—before your pipeline runs into runtime surprises.**

Schema validation sits between inspection and transformation. After you inspect what you have, validation answers: "Is this what I expected?" Use column-level checks as guard clauses, index lookups for positional access, and schema-level comparisons to enforce contracts between pipeline stages.

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

> **Tip:** <br>
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
