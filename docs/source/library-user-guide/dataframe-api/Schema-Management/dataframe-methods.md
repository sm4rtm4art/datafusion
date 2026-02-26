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

# Schema Management with DataFrame methods

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents} Table of Contents for Schema Management with DataFrame methods
:local:
:depth: 2
:caption:
```

## Introduction (placeholder)

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

> **Tip:** <br>
> Most of these methods are wrappers around [`.iter()`] with different filter/return semantics. If you need a custom lookup pattern, iterating directly with `.iter()` is often simpler than finding the right method name.
