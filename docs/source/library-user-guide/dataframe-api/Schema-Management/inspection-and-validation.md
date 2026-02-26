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

# Validating Schemas

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents} Table of Contents for Validating Schemas
:local:
:depth: 2
```

## Introduction (placeholder)

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
