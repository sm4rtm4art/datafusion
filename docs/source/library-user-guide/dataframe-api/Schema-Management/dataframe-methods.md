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
2. Introduction
3. Expand unnest_columns section

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
