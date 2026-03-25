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

# Transforming Schemas

<!--TODO

1. ABSTRACT
2. Fix cross-references to anatomy-schema.md and creating-schemas.md (anchors moved across files)
3. Add cross-ref to schema-concepts.md "Schema Propagation Through Transformations" for the conceptual overview

-->

```{contents} Transforming Schemas
:local:
:depth: 2
```

## Introduction (placeholder)

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

> **Note:** <br>
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

> **Warning:** <br>
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

> **Note:** <br>
> Unlike `.strip_qualifiers()` and `.replace_qualifier()` which consume `self`, `.with_field_specific_qualified_schema()` borrows `&self`—so you can call it directly without cloning.

---

### Combining Schemas

**Combine fields from multiple schemas into one—either strictly (rejecting duplicates) or permissively (ignoring them).**

Use [`users_schema.join(&contact_schema)`][dfschema::join] when schemas must have entirely distinct fields (e.g., after a SQL JOIN), and [`base_schema.merge(&overlapping_schema)`][dfschema::merge] when you want to accumulate fields while silently skipping duplicates (e.g., building a union schema from overlapping sources).

**SQL equivalent:**<br>
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

**See also:**<br>

- [Concepts: Handling Null Values](./concepts.md#handling-null-values) for SQL NULL semantics and three-valued logic.
- [Nullability](#schema-field-nullability) for the widening rule when schemas are merged.
- [Default Values](#default-values) for applying defaults during schema creation.

---
