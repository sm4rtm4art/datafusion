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

<!--TODO

1. ABSTRACT
2. Fix cross-references to other files in this directory

-->

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

```{contents} Creating Schemas
:local:
:depth: 2
```

## Introduction (placeholder)

**Define schemas explicitly in code to enforce types, nullability, and structure at planning time.**

Use Arrow's [`Schema`], [`Field`], and [`DataType`] to build schemas that readers, writers, and the optimizer all share. Explicit schemas prevent inference drift in text formats and give the optimizer the type information it needs for efficient execution. See [The Anatomy of a DataFusion DataFrame Schema](#the-anatomy-of-a-datafusion-dataframe-schema) for the architectural background.

> **Note:** <br>
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

> **Note:** <br> Always use [`SchemaRef`] (`Arc<Schema>`) for efficient sharing. Cloning an `Arc` is O(1) and avoids deep copies of the schema structure.

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

> **Note:** <br>
> `try_from`, `from_unqualified_fields`, and `try_from_qualified_schema` call [`.check_names()`][`check_names()`] and return `Result`—they will error on duplicate field names. `empty()` always succeeds. For qualifier transformations on an existing `DFSchema`, see [Aligning Qualifiers](#aligning-qualifiers).

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

> **Best practice:** <br> In production, always prefer **explicit schemas** over inference to prevent drift and ensure consistency.

### Configuring Common Field Types

Certain data types require specific configuration to ensure correctness and prevent data loss. This section covers the most common cases.

#### Decimal Types: Precision and Scale

**Why decimals matter**: <br>
Floating-point types (Float32/Float64) can introduce rounding errors for financial calculations. Decimals provide exact arithmetic for monetary values.

**What you need to specify**:

- **Precision**:<br>
  Total number of digits (maximum 38 for Decimal128)
- **Scale**:<br>
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

> **Tip:** <br> When casting between decimals, ensure the target has enough precision **AND** scale. Casting `Decimal128(10, 2)` to `Decimal128(8, 2)` will fail if values exceed 6 integer digits.

#### Timestamp Types: Timezone Handling

**Why timezone matters**:<br>
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

> **Best practice:** <br> Pick one strategy for your entire pipeline. Most systems use UTC timestamps throughout. When you need to compare or join columns with different timezone settings, cast them to the same type first using `cast(col("ts")`, [`DataType::Timestamp(...)`] (available via the prelude).

#### Advanced: Field Metadata

Field metadata is used to embed rich, contextual information—such as column descriptions, data lineage, or security classifications—directly into the schema as key-value pairs. This information is not used by the DataFusion query engine and its preservation across I/O is format-dependent and best-effort, but it is a powerful tool for external systems, documentation, and compliance.

Common Use Cases:

- **Constraints (documentation only):**<br>
  `primary_key`, `unique`, `foreign_key`
- **Data Lineage:**<br>
  `source_system`, `ingest_time`, `source_column`
- **Compliance & Security:**
  `pii` (Personally Identifiable Information), `encryption_required`
- **Documentation:**
  `description`, `owner`, `version`

> **Warning:** <br>
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

- **Standardize your format**:<br>
  use lowercase snake_case keys and parseable values (e.g., `"true"`, ISO 8601 timestamps/durations).
- **Re‑attach intentionally**:<br>
  derived/aggregated columns don't inherit metadata—add it on the final output schema if needed.
- **Verify format support**:<br>
  Arrow IPC preserves metadata; Parquet can embed it, but DataFusion skips file-level schema metadata by default (`skip_metadata = true`)—set `skip_metadata(false)` in Parquet options if you rely on it; CSV/NDJSON do not carry metadata at all.
- **Reconcile on merge**:<br>
  when sources disagree, prefer a canonical schema and explicitly resolve conflicts.
- **Keep it small**:<br>
  avoid large blobs; store long docs externally and reference via a short key (e.g., `doc_url`).
- **Validate early**:<br> add lightweight checks in tests/pipeline (e.g., require `owner`, `schema_version`, `pii` flags where applicable).

---
