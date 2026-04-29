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

# Schema Inference

**Inference derives column names and types from data samples — useful for exploration, but a source of silent failures in production.**

DataFusion can derive an Arrow `Schema` — column names, data types, and nullability — automatically when reading CSV or JSON files, sampling the first N records and guessing types from the values it finds. This document covers when to prefer explicit schemas over inference, how the sampling mechanism works, how multi-file reads merge schemas, and the failure modes that make inferred schemas risky in production pipelines. For the query-planning schema (`DFSchema`) that wraps the inferred result, see [Schema Concepts](schema-concepts.md).

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

```{contents} Table of Contents for Schema Inference
:local:
:depth: 2
```

## Explicit Schemas vs. Inference

**Use explicit schemas when type stability, precision, or multi-file consistency matter — reserve inference for exploration and prototyping.**

Schema inference derives column names and types from a data sample automatically, without the user providing a schema definition. It provides a zero-configuration on-ramp for exploration: read a file and start querying immediately, without knowing its structure upfront. The trade-off is correctness — inference examines a limited sample and guesses types, so the result can diverge from the actual data. Explicit schemas eliminate that risk by defining the contract upfront.

| Scenario                                   | Recommendation                                        |
| :----------------------------------------- | :---------------------------------------------------- |
| Production pipelines                       | **Explicit** — prevents drift, ensures data quality   |
| Specific types needed (e.g., `Decimal128`) | **Explicit** — inference may choose `Float64`         |
| Multi-file reads with varying structure    | **Explicit** — guarantees consistency across files    |
| Interactive exploration / prototyping      | **Inference OK** — validate before relying on results |
| Single-file reads with uniform structure   | **Inference OK** — lower risk, but not zero risk      |

:::{admonition} Constructing and applying explicit schemas
:class: seealso

- [Creating Schemas](schema-creation.md) for constructing Arrow `Schema` and `DFSchema` programmatically.
- [Applying Schemas and Modeling Data](schema-application.md) for passing schemas to [`CsvReadOptions`], [`NdJsonReadOptions`], and other format-specific readers.
  :::

For cases where inference is appropriate, the following sections explain how it works, how to configure it, and what can go wrong.

---

## How Schema Inference Works

**Schema inference determines column names and types automatically — but the result is only as reliable as the data sample it examined.**

Inference triggers automatically when you read CSV or JSON files without providing a schema via [`CsvReadOptions::schema()`] or [`NdJsonReadOptions::schema()`]. DataFusion reads up to the configured sample size ([`CsvReadOptions::schema_infer_max_records()`] or [`NdJsonReadOptions::schema_infer_max_records()`], default: 1,000), examines the values it finds, and assigns Arrow data types. CSV inference is **positional** — column index determines mapping. JSON inference is **name-based** — JSON keys map to field names by name, regardless of order. Fields that do not appear within the sampling window are excluded from the resulting schema entirely.

| Aspect                  | CSV                                                                                 | JSON                                                                |
| :---------------------- | :---------------------------------------------------------------------------------- | :------------------------------------------------------------------ |
| **Field alignment**     | Positional (column index)                                                           | Name-based (JSON key)                                               |
| **Missing fields**      | Row-length mismatch errors by default                                               | `NULL` if field exists in schema                                    |
| **Short rows**          | Error; use [`.truncated_rows(true)`][`truncated_rows`] to fill with `NULL`s         | N/A (each line is a self-contained object)                          |
| **Sampling window**     | First N records ([`CsvReadOptions::schema_infer_max_records()`])                    | First N records ([`NdJsonReadOptions::schema_infer_max_records()`]) |
| **Default sample size** | 1,000                                                                               | 1,000                                                               |
| **Type fallback order** | Booleans, numbers, temporal regexes, then `Utf8` for conflicts or unmatched strings | Infers from JSON value types (`number`, `string`, `boolean`)        |

:::{admonition} Temporal inference is format-specific
:class: caution
CSV inference recognizes ISO-like date and timestamp strings and can infer `Date32` or `Timestamp` types for matching values. JSON inference does not parse string contents as temporal values: ISO-8601 strings stay `Utf8`, and epoch values stay numeric. When temporal semantics matter, provide an explicit schema via [`CsvReadOptions::schema()`] or [`NdJsonReadOptions::schema()`], or cast the columns after reading.
:::

:::{admonition} Format-specific details
:class: seealso
For detailed reading options, error handling, and compression support beyond inference, see [Reading CSV Files](../Creating-DataFrames/from-files/csv.md) and [Reading JSON Files](../Creating-DataFrames/from-files/json.md).
:::

The following example reads a CSV file with inference, shows the resulting data, and inspects the inferred schema:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::assert_batches_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("sensor_readings.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "id,name,score")?;
    # writeln!(file, "1,Alice,95.5")?;
    # writeln!(file, "2,Bob,87.0")?;
    # writeln!(file, "3,Carol,91.2")?;
    let path = "sensor_readings.csv";
    # let path = csv_path.to_str().unwrap();

    let ctx = SessionContext::new();

    // Read CSV without an explicit schema — DataFusion infers types
    let df = ctx.read_csv(path, CsvReadOptions::new()).await?;

    // The data DataFusion read and typed automatically
    let results = df.clone().collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+-------+",
            "| id | name  | score |",
            "+----+-------+-------+",
            "| 1  | Alice | 95.5  |",
            "| 2  | Bob   | 87.0  |",
            "| 3  | Carol | 91.2  |",
            "+----+-------+-------+",
        ],
        &results
    );

    // Inspect the inferred schema — tree_string() shows types and nullability
    let schema = df.schema();
    println!("{}", schema.tree_string());
    // Output:
    // root
    //  |-- id: int64 (nullable = true)
    //  |-- name: utf8 (nullable = true)
    //  |-- score: float64 (nullable = true)

    // Verify the inferred types programmatically
    assert_eq!(
        schema.field_with_unqualified_name("id")?.data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_unqualified_name("score")?.data_type(),
        &DataType::Float64
    );
    assert_eq!(
        schema.field_with_unqualified_name("name")?.data_type(),
        &DataType::Utf8
    );

    Ok(())
}
```

### Configuring the Sample Size

**Increase the sample window to cover heterogeneous data — but no finite sample eliminates drift entirely.**

Both [`CsvReadOptions`] and [`NdJsonReadOptions`] expose the same builder method to control how many records DataFusion reads before assigning types:

```rust
use datafusion::prelude::*;

fn main() {
    // CSV: increase from default 1,000 to 10,000 rows
    let csv_opts = CsvReadOptions::new()
        .schema_infer_max_records(10_000);
    # assert_eq!(csv_opts.schema_infer_max_records, 10_000);

    // JSON: same configuration pattern
    let json_opts = NdJsonReadOptions::default()
        .schema_infer_max_records(10_000);
    # assert_eq!(json_opts.schema_infer_max_records, 10_000);
}
```

:::{admonition} Limitations of increasing the sample size
:class: caution

1. **Inference cannot see beyond the sample.** Values that appear only after the sampling window — different types, new columns, overflow ranges — remain invisible regardless of sample size.
2. **Setting the sample to zero is format-specific.** CSV keeps the header fields and assigns `Utf8` because type detection is disabled. NDJSON samples no objects, so the inferred schema can be empty.
3. **Startup cost scales linearly.** Larger samples delay `DataFrame` creation because DataFusion must read and parse more records before the plan is built.
4. **The budget is shared across files.** In multi-file reads, increasing the budget benefits only the files that are sampled — see [Multi-File Inference](#multi-file-inference-and-schema-merging).
   :::

### Multi-File Inference and Schema Merging

**The sampling budget is shared across files — in a multi-file read, early files can exhaust the budget before later files contribute to the schema.**

When DataFusion reads multiple files (e.g., a directory of CSV files via [`ListingTable`]), inference loops over the file list and decrements the sampling budget as records are read from each file. If the first file contains more records than the budget, **only that file determines the inferred schema**. Remaining files contribute nothing — and if they have different structures, the mismatch goes undetected until runtime.

After sampling, DataFusion merges the per-file schemas using Arrow's [`Schema::try_merge()`]. The merge:

- **Unions fields** — a field appearing in any sampled file appears in the merged schema.
- **Fails on type conflicts** — if file A has `amount: Int64` and file B has `amount: Utf8`, the merge returns an error. Arrow does not widen types during merge (e.g., `Int32` and `Int64` for the same field is a conflict, not a promotion). DataFusion's [type coercion](type-coercion.md) rules apply later, inside the `LogicalPlan` — not at the merge step.

:::{admonition} Self-describing formats
:class: note

Parquet, Avro, and Arrow IPC also go through the [`FileFormat::infer_schema()`] trait method, but they extract schemas from embedded metadata rather than sampling data rows. No row-sampling budget applies. However, the multi-file **merge** step still runs — [`Schema::try_merge()`] reconciles schemas across all files. If different Parquet files have different schemas (common during schema evolution), the merge must reconcile them or it will fail on type conflicts.

Parquet's implementation sorts files by path before merging to ensure **deterministic** field ordering, regardless of the storage backend's file listing order.
:::

---

## Inference Risks and Failure Modes

**Wrong results, runtime errors, and pipeline failures — all traceable to an inferred schema that didn't match the actual data.**

Schema inference can fail in three distinct ways, each with different symptoms and blast radius. **Schema drift** produces wrong results silently — the inferred type is technically valid but semantically wrong. **Type mismatches across files** surface as merge errors at plan-build time. **Unchecked inferred schemas** propagate through the pipeline, turning a local guess into a systemic assumption. The subsections below cover each failure mode and how to detect it.

### Schema Drift

A column inferred as `Int64` from the first 1,000 rows may encounter float values, strings, or nulls further into the file. The inferred schema is fixed at inference time and does not adapt. At execution time, values that don't parse into the inferred type cause errors or are silently coerced to null (depending on the format reader's error handling). For how DataFusion reconciles types _within_ a plan, see [Type Coercion](type-coercion.md) — but coercion cannot fix a fundamentally wrong inferred type.

| Scenario                         | Inferred Type | Actual Data                  | Consequence                                            |
| :------------------------------- | :------------ | :--------------------------- | :----------------------------------------------------- |
| Large IDs or timestamps          | `Int64`       | Values exceeding `i64::MAX`  | Overflow errors at execution time                      |
| Currency / financial amounts     | `Float64`     | Precision-sensitive decimals | Rounding errors; `Decimal128` needed                   |
| Sparse or late-appearing columns | `Utf8`        | Numeric or boolean values    | Wrong type; values appear only after the sample window |

### Type Mismatch Across Files

When reading multiple files, files sampled later in the loop may have different types for the same field. [`Schema::try_merge()`] returns an error on conflicting types — but only for files that were actually sampled within the budget. Files beyond the budget are never checked.

### Validating an Inferred Schema

Use [`has_equivalent_names_and_types()`] from [Inspecting and Validating Schemas](schema-inspection.md) to compare the inferred schema against an expected contract before executing the pipeline. The method returns `Ok(())` on match and a descriptive error on mismatch — making it a natural guard clause.

The following example uses the same `sensor_readings.csv` data, constructs an expected schema, and validates the inferred result:

```rust
use datafusion::prelude::*;
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("sensor_readings.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "id,name,score")?;
    # writeln!(file, "1,Alice,95.5")?;
    # writeln!(file, "2,Bob,87.0")?;
    # writeln!(file, "3,Carol,91.2")?;
    let path = "sensor_readings.csv";
    # let path = csv_path.to_str().unwrap();

    let ctx = SessionContext::new();
    let df = ctx.read_csv(path, CsvReadOptions::new()).await?;

    // Define the schema the pipeline expects
    let expected_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]);
    let expected = DFSchema::try_from(expected_schema)?;

    // Compare — returns Ok(()) on match, detailed error on mismatch
    expected.has_equivalent_names_and_types(df.schema())?;

    Ok(())
}
```

:::{admonition} When inference picks the wrong type
:class: tip
If validation fails — for example, inference chose `Float64` for a currency column that needs `Decimal128(19, 2)` — the fix is to provide an explicit schema via [`CsvReadOptions::schema()`] instead of relying on inference. See [Creating Schemas](schema-creation.md) for how to construct one.
:::

## Conclusion & Further Reading

**Inference is a convenience — not a contract. Validate inferred schemas before production use, or provide explicit schemas from the start.**

Schema inference provides a fast on-ramp for exploration, but the guess is based on a limited sample and the sampling budget is shared across files. Schema management begins where inference ends: define a target schema, apply it explicitly, and use the validation methods to enforce it as a contract.

:::{admonition} Related documents
:class: seealso

- [Creating Schemas](schema-creation.md) — constructing explicit schemas programmatically
- [Applying Schemas and Modeling Data](schema-application.md) — format-specific schema strategies
- [Inspecting and Validating Schemas](schema-inspection.md) — checking inferred schemas before use
  :::

<!-- Link references -->

[`Schema`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html
[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`CsvReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`CsvReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema
[`NdJsonReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`NdJsonReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.schema
[`CsvReadOptions::schema_infer_max_records()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`NdJsonReadOptions::schema_infer_max_records()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.schema_infer_max_records
[`truncated_rows`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.truncated_rows
[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`Schema::try_merge()`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html#method.try_merge
[`FileFormat::infer_schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/trait.FileFormat.html#tymethod.infer_schema
[`has_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_equivalent_names_and_types
