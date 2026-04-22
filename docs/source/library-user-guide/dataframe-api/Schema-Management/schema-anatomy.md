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

# Anatomy of a Schema

**[`DFSchema`] is the structural contract at every [`LogicalPlan`] node — bridging Arrow's physical data definition with DataFusion's query-planning context.**


Every [`DataFrame`] in DataFusion carries a [`DFSchema`] — the structural contract that the query engine validates at every [`LogicalPlan`] node. This document dissects [`DFSchema`] from the outside in: its query-planning extensions (table qualifiers and functional dependencies), the Arrow [`Schema`] underneath (field order, field count), and the four per-column properties (`name`, `data_type`, `nullable`, `metadata`) that define how each column is stored, processed, and interpreted.

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


```{contents} Table of Contents for the Anatomy of a Schema
:local:
:depth: 3
```

## DFSchema: Structure and Components

**[`DFSchema`] bridges the Arrow [`Schema`] and the [`LogicalPlan`] — connecting physical data definitions with DataFusion's query planning and optimization.**

Every [`DataFrame`] carries a [`DFSchema`] at the core of its [`LogicalPlan`], accessible via [`df.schema()`][`.schema()`]. The [`DFSchema`] provides the relational context that the query engine needs for column resolution, plan optimization, and schema validation — context that the Arrow [`Schema`] alone cannot supply.

[`DFSchema`] achieves this by wrapping the physical Arrow data definition with relational context. Here is where [`DFSchema`] sits within the [`DataFrame`]:

```text
┌───────────────────────────────────────────────────────────┐
│ DataFrame                                                 │
│   └── LogicalPlan         CURRENT TOPIC (YOU ARE HERE!)   │
│            └── DFSchema  <------------┘                   │
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

[`DFSchema`] connects the physical data layout to the [`LogicalPlan`] through three components:

- **`inner` (Arrow Schema):** The physical column contract — field names, data types, nullability, and metadata — shared with Arrow compute kernels and file writers.
- **`field_qualifiers`:** Maps each field to its source table or relation ([`TableReference`]), preventing column ambiguity after joins.
- **`functional_dependencies`:** Captures key constraints (primary keys, unique constraints) that the optimizer uses to simplify query plans.

:::{admonition} Schema Concepts
:class: seealso
For a broader conceptual overview — ownership chain, immutability guarantees, and schema propagation — see [Schema Concepts](schema-concepts.md).
:::


### Schema in Practice

**Inspecting a schema at both levels — [`DFSchema`] and its inner Arrow [`Schema`] — makes the structural concepts concrete.**

The [`DFSchema`] is accessed via [`df.schema()`][`.schema()`], which returns `&DFSchema`. Its `Display` format shows qualified field names — or bare names when no table is registered. To reach the underlying Arrow [`Schema`] with full field details, use [`.inner()`] (returns `&SchemaRef`) or [`.as_arrow()`] (returns `&Schema`). The example below creates a [`DataFrame`], casts a column, and inspects the schema at both levels:

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

    // DFSchema: logical view with qualified field names
    println!("DFSchema:  {}", df.schema());

    // Arrow Schema: physical field details
    println!("Arrow Schema:\n{:#?}", df.schema().inner());

    // Verify the schema structure
    assert_eq!(df.schema().fields().len(), 4);
    assert_eq!(
        df.schema().inner().field(2).data_type(),
        &DataType::Timestamp(TimeUnit::Second, None)
    );

    Ok(())
}
```

Correct types unlock query optimization. A `Timestamp` column enables date-range pruning, while the same bytes as `Int64` only support numeric comparisons.

:::{admonition} DFSchema output
:class: information
The DFSchema output shows the field names without qualifiers (no table registration in this example):

```text
DFSchema:  fields:[user_id, email, created_at, active], metadata:{}
```
:::

:::{admonition} Arrow Schema output
:class: information
The Arrow Schema output shows the field names with their properties (name, data_type, nullable, metadata):

```text
Arrow Schema:
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
:::


For comprehensive inspection patterns — human-readable display, programmatic field access, existence checks, and schema comparison — see [Inspecting and Validating Schemas](schema-inspection.md).

### DFSchema Components

**[`DFSchema`] translates the Arrow [`Schema`] into a query-planning resource — adding `field_qualifiers` and `functional_dependencies` on top of the physical data contract.**

[`DFSchema`] carries three components: the `inner` Arrow [`Schema`] defining the physical column contract (names, types, nullability, metadata), `field_qualifiers` mapping each field to its source table, and `functional_dependencies` encoding key constraints for optimizer reductions. Together, these form the complete schema at every [`LogicalPlan`] node.

| Component                 | Purpose                                                            |
| :------------------------ | :----------------------------------------------------------------- |
| `inner` (Arrow Schema)    | Field definitions (name, type, nullable, metadata)                 |
| `field_qualifiers`        | Maps each field to its source table (e.g., `users.id` vs `orders.id`) |
| `functional_dependencies` | Captures key relationships within a table for optimizer reductions |

:::{admonition} Accessing and inspecting schemas
:class: seealso
For access patterns, source-type differences, and programmatic field inspection — see [Inspecting and Validating Schemas](schema-inspection.md).
:::

### Table Qualifiers

**Table qualifiers resolve column ambiguity by mapping each field to its source table — enabling unambiguous column resolution, predictable query optimization, and reliable results across the entire plan.**

Every column reference in a [`LogicalPlan`] resolves against the [`DFSchema`] using qualifiers. Each field carries a [`TableReference`] connecting the column to its origin, enabling safe matching even when multiple tables share the same name. Joins are the most prominent example, but qualifiers also drive wildcard expansion, duplicate detection, and correlated subquery resolution. Under the hood, [`DFSchema`] stores qualifiers as a `Vec<Option<TableReference>>` parallel to the field list — `Some(TableReference)` identifies a registered table, `None` marks a computed expression with no table origin. After joining a `users` and `orders` table, the [`DFSchema`] display shows each field qualified by its source:

```text
fields:[users.id, users.name, orders.order_id, orders.user_id, orders.amount], metadata:{}
```

For qualifier manipulation methods (`.strip_qualifiers()`, `.replace_qualifier()`, per-field qualifiers), see [Transforming Schemas — Aligning Qualifiers](schema-transformation.md#aligning-qualifiers).

### Functional Dependencies

**Functional dependencies describe key relationships within a table — fusing the data with its origin constraints for reliable and performant optimization and data processing.**

Functional dependencies encode a "determines" relationship: when `order_id` is a primary key, it uniquely determines `region` and `amount` — expressed as `{order_id} → {region, amount}`. Unlike table qualifiers (cross-table), functional dependencies describe relationships *within* a single table. [`DFSchema`] stores them as [`FunctionalDependencies`], derived automatically from primary key and unique constraints on the [`TableProvider`] when the `TableScan` node is built, and propagated through each plan node. For a table `sales(order_id PK, region, amount)`, the functional dependencies record that column 0 determines columns 1 and 2:

```text
FunctionalDependencies { deps: [
    FunctionalDependence { source_indices: [0], target_indices: [1, 2], nullable: false, mode: Single }
]}
```

:::{admonition} Functional dependencies are structural, not statistical
:class: note
Functional dependencies express **logical guarantees** (primary key, unique constraint) — not statistical correlations. They are set via [`TableProvider::constraints()`] and converted to [`FunctionalDependencies`] during plan construction. For setting functional dependencies on a [`DFSchema`] directly, see [Transforming Schemas](schema-transformation.md).
:::

---

## Arrow Schema

**The Arrow [`Schema`] is the `inner` component of [`DFSchema`] — an ordered list of [`Field`] entries that defines the physical columnar contract for compute kernels and file writers.**

The previous section covered [`DFSchema`] and its query-planning extensions — qualifiers and functional dependencies. Those additions enable column resolution and optimizer reductions. The Arrow [`Schema`] underneath carries a different responsibility: defining the physical data contract that compute kernels and file writers depend on.

```text
┌─ Arrow Schema (inner) ────────────────────────────────────┐
│                                                           │
│  Schema-level:  field order · field count · metadata      │
│                                                           │
│  Field[]:                                                 │
│       ├── name        ← column identity                   │
│       ├── data_type   ← storage & compute format          │
│       ├── nullable    ← null handling (validity bitmap)   │
│       └── metadata    ← semantic context (free-form)      │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

:::{admonition} Field is an Arrow type
:class: note
[`Field`] is defined in [`arrow::datatypes::Field`], not in DataFusion. [`DFSchema`] _wraps_ an Arrow [`Schema`] (which contains `Field[]`) and adds query-planning context on top. For how [`DFSchema`] adds qualifiers and functional dependencies, see [Schema Concepts — DFSchema](schema-concepts.md#dfschema-the-query-planning-layer).
:::

Each [`Field`] carries four properties that determine how its column is stored, processed, and interpreted.

### Schema-Level Characteristics

**The Arrow [`Schema`] is an ordered collection — field position and field count carry behavioral implications for DataFrame operations that combine or compare schemas.**

Before examining individual field properties, two characteristics of the schema as a whole — field order and field count — affect how DataFrames combine and compare.

#### Field Order

**Column order in the schema defines the physical position of fields — but DataFusion's DataFrame API resolves columns by name, making most operations resilient to upstream reordering.**

The Arrow [`Schema`] stores fields in an ordered list, and `RecordBatch` columns follow that order. However, the DataFrame API uses column names — not positions — for resolution. Operations like [`.select()`], [`.filter()`], and [`.join()`] reference columns by name via `col("...")`, so reordering fields in the source schema does not break downstream transformations.

Where column order *does* matter: positional operations like [`.union()`] match columns by index, not name. If the source schema reorders, a positional union produces silently wrong results. Name-based [`.union_by_name()`] eliminates this risk.

:::{admonition} Best practice
:class: tip
Prefer name-based operations ([`.union_by_name()`], `col("...")`) over positional access. This makes pipelines resilient to upstream schema evolution — new columns, reordered columns, and renamed sources are handled gracefully.
:::

:::{admonition} SQL equivalent: UNION BY NAME
:class: note
DataFusion's SQL parser supports `UNION BY NAME` syntax (inspired by [DuckDB]), which matches columns by name rather than position. Both the DataFrame API and SQL produce the same `LogicalPlan`.
:::

#### Field Count

**The number of fields in a schema determines compatibility between DataFrames — strict operations require identical counts, while flexible operations handle differences by filling missing columns with NULL.**

Schema field count is a compatibility constraint when combining DataFrames. Positional operations like [`.union()`] require identical column counts — any mismatch fails at plan-build time. Name-based operations like [`.union_by_name()`] are more flexible: missing columns are filled with NULL, supporting schema evolution where new columns appear over time or sources have different field sets.

This count flexibility extends to joins as well: the output schema's field count is the sum of both input schemas (minus join keys in some join types), and each field retains its source qualifier for disambiguation.

:::{admonition} Schema evolution pattern
:class: tip
When combining DataFrames from sources that evolve independently (e.g., daily exports with new columns), prefer [`.union_by_name()`]. Missing columns are filled with NULL — new columns appear for historical rows without breaking the pipeline. For the operational details of `.union_by_name()`, see [Transforming Schemas](schema-transformation.md).
:::

:::{admonition} Type mismatches require explicit resolution
:class: important
Count differences are handled automatically, but type mismatches are not. When the same column name appears with different types (e.g., `Int32` vs `Int64`), DataFusion's [`TypeCoercion`] analyzer attempts to find a common type. If no safe coercion path exists, the query fails during analysis. See [Type Coercion](type-coercion.md).
:::

### Arrow Field Properties

**Every column in a [`DataFrame`] is defined by four Arrow [`Field`] properties — three primary (`name`, `data_type`, `nullable`) and one secondary (`metadata`).**

| Property                     | Role                                     | Operations Affected                              |
| :--------------------------- | :--------------------------------------- | :----------------------------------------------- |
| **Primary**                  | Essential for performant data processing | Query engine                                     |
| [`field.name`][`field`]      | Column identity                          | joins, selects, filters, group by, union_by_name |
| [`field.data_type`][`field`] | Storage & compute                        | kernel selection, type coercion, optimization    |
| [`field.nullable`][`field`]  | Null handling                            | validity bitmaps, null-safe operations           |
| **Secondary**                | _Essential for giving data meaning_      | _Human understanding & tuning_                   |
| [`field.metadata`][`field`]  | Semantic context                         | descriptions, units, lineage, PII classification |


:::{admonition} Implementation detail
:class: note
Primary properties (`name`, `data_type`, `nullable`) are first-class typed fields on the Arrow `Field` struct. Secondary metadata is physically stored as a `HashMap<String, String>` — a free-form key-value map with no schema enforcement or validation from Arrow. DataFusion preserves this metadata through transformations like projections and aggregations.
:::

#### Name

**Column names are the primary identifier for every DataFrame operation — case-sensitive matching in the Rust API catches most developers off guard.**

The column [`field.name`][`field`] is the primary identifier for a column in the DataFrame API. Operations like [`.select()`], [`.with_column()`], and [`.union_by_name()`] all resolve columns by name. Case-sensitivity is the most common source of schema mismatch errors — `col("Region")` and `col("region")` reference different columns because DataFusion's DataFrame API performs exact string matching with no normalization. A mismatched case fails during query analysis, not at runtime.

:::{admonition} SQL parser behavior differs
:class: note
DataFusion's SQL parser normalizes unquoted identifiers to lowercase by default. When mixing DataFrame API calls with SQL queries, be aware of this distinction.
:::

:::{admonition} Best practice
:class: tip
Enforce a consistent naming convention (e.g., all **snake_case** or **camelCase**) at your ingestion boundary.
:::

#### Data Type

**The `data_type` determines how a column is stored, which compute kernels operate on it, and how DataFusion coerces types when expressions mix them.**

Every column carries an [Apache Arrow `DataType`][arrow dtype] that defines its storage format and computational behavior. The same raw bytes (`1735689600_i64`) become a timestamp (`2025-01-01T00:00:00`) when the field declares `Timestamp` as its type — choosing the correct type unlocks date-range pruning, temporal arithmetic, and timezone-aware comparisons that `Int64` cannot provide.

DataFusion selects compute kernels based on the declared type, validates type compatibility during plan analysis, and applies automatic widening (e.g., `Int32 + Int64 → Int64`) when expressions combine columns of different types. When no safe coercion path exists, the query fails during analysis rather than producing wrong results at runtime.

:::{admonition} Type reference and coercion rules
:class: seealso
For the full type reference table, coercion hierarchy, and widening rules — see [Type Coercion](type-coercion.md).
:::

##### Nested Types

Beyond primitive types, DataFusion fully supports Arrow's nested types: [`List`], [`Struct`], [`Map`], and [`Union`]. These enable complex data structures like JSON objects, arrays of values, or key-value maps — common in Parquet files and semi-structured data.

Nested types follow the same schema rules but add complexity in coercion and comparison. For a comprehensive reference on nested type structures and memory layouts, see the [Apache Arrow Data Types documentation][arrow data types].

#### Nullability

**The nullable flag controls validity bitmaps and schema merging — when schemas combine, nullability always widens to the most permissive side.**

The [`field.nullable`][`field`] flag declares whether a column may contain NULL values. Arrow uses a **validity bitmap** — one bit per row, separate from the data buffer — to track NULLs efficiently (no per-null object overhead like Python's `None`).

When schemas combine (for example, via [`.union_by_name()`] or [`.join()`]), DataFusion follows a widening rule:

:::{admonition} The Golden Rule of Nullability
:class: important
If a column is nullable in any of the input schemas, it will be nullable in the output schema.
:::

A non-nullable column can always be represented in a nullable one, but not the other way around. When [`.union_by_name()`] fills a missing column with NULL, that column becomes nullable in the output schema — regardless of its original nullability.

For a deeper discussion of how NULL values behave in expressions, filters, and joins, see [Handling Null Values](../Concepts/null-handling.md).

#### Metadata

**Metadata preserves semantic context — column descriptions, units, lineage, and PII classifications — that the query engine does not use for optimization but that is essential for data governance.**

The [`field.metadata`][`field`] property provides semantic annotations beyond types. DataFusion **preserves** metadata when reading from formats like Parquet that embed it, and through transformations like projections and aggregations.

Unlike the three primary properties (`name`, `data_type`, `nullable`) which are first-class typed fields on the Arrow `Field` struct, metadata is physically stored as a `HashMap<String, String>` — a free-form key-value map with no schema enforcement or validation from Arrow. Any string key and value are accepted without constraints.

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

A schema with field-level metadata looks like this when inspected via the Arrow Schema debug output:

```text
Schema {
    fields: [
        Field { name: "sensor_id", data_type: Utf8, nullable: false, metadata: {} },
        Field { name: "temperature", data_type: Float64, nullable: true,
                metadata: {"unit": "celsius", "pii_level": "none"} },
    ],
    metadata: {"source_system": "iot_pipeline"},
}
```

The `DFSchema` display does not show metadata — use [`.inner()`] or [`.as_arrow()`] to reach the Arrow [`Schema`] with full metadata details.

For the full metadata API and inspection patterns, see [Inspecting and Validating Schemas](schema-inspection.md).

---

## Conclusion

**[`DFSchema`] combines Arrow's physical per-column contract with relational query-planning context — forming the complete structural foundation that every [`LogicalPlan`] node validates against.**

The Arrow [`Schema`] and its individual [`Field`] properties (`name`, `data_type`, `nullable`, `metadata`) define the exact physical layout of your data. By wrapping this physical contract and injecting `field_qualifiers` (for cross-table disambiguation) and `functional_dependencies` (for optimizer reductions), [`DFSchema`] bridges the gap between raw bytes and DataFusion's relational query engine. Together, these layers form the complete schema accessible via [`df.schema()`][`.schema()`].

:::{admonition} Next steps
:class: seealso
- **[Schema Concepts](schema-concepts.md):** Ownership chains, memory management, and the big-picture schema lifecycle.
- **[Inspecting and Validating Schemas](schema-inspection.md):** Hands-on patterns for programmatic field access and schema comparison.
- **[Transforming Schemas](schema-transformation.md):** Qualifier manipulation, schema combining, and functional dependency methods.
- **[Type Coercion](type-coercion.md):** How DataFusion automatically reconciles types when they do not match.
:::

<!-- Literature references -->