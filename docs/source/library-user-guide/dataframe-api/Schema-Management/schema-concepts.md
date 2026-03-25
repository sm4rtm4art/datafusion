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

# Schema Concepts

**The schema — the data contract with the query engine — guarantees reliable data handling and enables DataFusion to validate, optimize, and execute [`DataFrame`] operations before any data is touched.**

The DataFrame schema is essential for transforming raw information into structured, processable data. [`DFSchema`] — DataFusion's schema type — wraps an Arrow [`Schema`] and adds relational context such as table qualifiers, enabling the query engine to validate operations, optimize execution, and catch errors at plan-build time rather than at runtime.
DataFusion uses the term "schema" for [four distinct concepts](#schema-terminology-in-datafusion) — this page disambiguates them and covers the foundational concepts and patterns for schema management, and how the DataFrame API interacts with the [`DFSchema`] type.


**Concepts covered on this page:**

| Concept                                                                                   | Description                                                                        |
| :---------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------- |
| [The Schema Contract](#the-schema-a-data-contract-with-the-query-engine)                  | The schema as data contract with the query engine, defined by metadata              |
| [Schema Terminology in DataFusion](#schema-terminology-in-datafusion)                     | **Schema** an overlapping terminology with different semantics                      |
| [DFSchema — The Query-Planning Layer](#dfschema-the-query-planning-layer)                 | DFSchema as query-planning wrapper, immutability, Expr validation                  |
| [Type Coercion at a Glance](#type-coercion-at-a-glance)                                   | The widening principle, two coercion modes                                         |
| [How the Initial Schema is Determined](#how-the-initial-schema-is-determined)             | Self-describing formats, text formats, custom sources                              |
| [Schema Ownership — From Source to DataFrame](#schema-ownership-from-source-to-dataframe) | Schema origin, resolution paths, ownership delegation from source to DataFrame     |
| [Schema Propagation Through Transformations](#schema-propagation-through-transformations) | Schema evolution through transformations, fail-fast validation at plan-build time   |
| [Logical vs Physical Schema](#logical-vs-physical-schema)                                 | Schema representation and the resulting plan and physical execution layout          |

---


## The Schema: A data contract with the query engine

**A DataFrame schema turns raw information into processable data by defining column names, data types, and constraints that enable reliable, performant query execution.**

Defined data structures are essential for DataFusion's query engine to plan and execute transformations. The column name, data type, and nullability of each column must be consistent across all data provided to a [`DataFrame`]. For humans, additional context — timestamps, units, source identifiers — adds interpretive value and supports downstream data validation.


In DataFusion, every [`DataFrame`], every table, and every [`LogicalPlan`] node carries a schema that answers: _"What columns exist, what types do they hold, and which values may be null?"_ This contract is captured by [`DFSchema`], which wraps Apache Arrow's type system with query-planning context and is accessed via [`df.schema()`][`.schema()`].


:::{admonition} An Example
:class: tip
Raw information like _`22.5`_ has no meaning without metadata like:
- `column named: "temperature"`
- `type: "Float16"`
- `nullable: "false"`
- and additional (secondary) metadata:
  - `sensor: A`
  - `unit: "celsius"`

The query engine uses the primary metadata for processing and optimizing. The human uses the secondary metadata for interpretation and data quality validation.
:::

### What the Contract Contains

The schema contract carries two categories of metadata serving different audiences. For what [`DFSchema`] structurally adds to Arrow's [`Schema`] — table qualifiers, functional dependencies — see [DFSchema — The Query-Planning Layer](#dfschema-the-query-planning-layer). For the full field-level breakdown, see [Anatomy of a Schema](anatomy-schema.md).

1. **Primary metadata:** structural properties that the query engine uses for planning and execution.

2. **Secondary metadata:** semantic annotations that add interpretive value for humans.

This distinction clarifies which parts of a schema affect query behavior and which are preserved but not acted upon.

**Primary metadata in an overview:**

| Property                  | Level       | Purpose                                                             |
| :------------------------ | :---------- | :------------------------------------------------------------------ |
| `name`                    | Arrow Field | Column identity — used in selects, joins, filters, group-by         |
| `data_type`               | Arrow Field | Storage format and compute kernel selection                         |
| `nullable`                | Arrow Field | Validity bitmap, null-safe operations, schema merging               |
| `field_qualifiers`        | DFSchema    | Table provenance — disambiguates `users.id` vs `orders.id` in joins |
| `functional_dependencies` | DFSchema    | Key relationships the optimizer uses for deduplication and ordering  |


**Secondary metadata:**

| Property                     | Level                | Purpose                                                                     |
| :--------------------------- | :------------------- | :-------------------------------------------------------------------------- |
| `metadata` (key-value pairs) | Arrow Field / Schema | Units (`"unit": "celsius"`), descriptions, PII classification, data lineage |

DataFusion preserves secondary metadata throughout processing but does not use it for optimization. The query engine operates exclusively on primary metadata.

:::{admonition} File-format statistics are not schema
:class: note
Parquet file statistics (min/max values, row counts, null counts) are **not** part of the schema. They reside in the Parquet file footer and are cached at the physical execution layer — by `ListingTable`'s `FileStatisticsCache` for aggregated file-level statistics, and by `RuntimeEnv`'s `FileMetadataCache` for raw footer metadata. The [`LogicalPlan`] and [`DFSchema`] never carry them. These statistics are consumed by `PruningPredicate` during physical execution for row group and page pruning.
:::

### Contract Violations — Fail-Fast at Plan-Build Time

**Schema violations — structural mismatches in column names or types — are caught at plan-build time, before any data is scanned.**

[`DFSchema`] validates the structural contract each time a new [`LogicalPlan`] node is constructed. When you call a lazy method, without execution (`.filter()`, `.select()`, `.union()`, etc.), DataFusion checks the referenced columns and types against the current schema. Errors surface immediately at the point of construction — not during execution. This fail-fast behavior saves compute and debugging time by catching structural mistakes early.

Two common violations:

- **"Column not found"**: The referenced column does not exist in the current schema. Column names are case-sensitive — `col("Temperature")` and `col("temperature")` are distinct. This error surfaces when calling `.filter()`, `.select()`, or any method that references columns by name.

- **"Type mismatch"**: Column types are incompatible between schemas — for example, unioning a `Float32` column with a `Utf8` column of the same name. The [`DFSchema`] rejects the combination during plan construction.

These errors come from [`DFSchema`] validation — the contract doing its job.

For the detailed breakdown of each field property, see [Anatomy of a Schema](anatomy-schema.md). For how types are reconciled when they don't match, see [Type Coercion](type-coercion.md).

---

## Schema Terminology in DataFusion

**"Schema" carries different semantics at each abstraction layer in DataFusion — resolving this ambiguity is essential for working with schema management.**

[`DFSchema`] and Arrow [`Schema`] are the two data-describing schemas used throughout this documentation. DataFusion also uses "schema" as a catalog namespace ([`SchemaProvider`]) and provides `Arc`-wrapped reference types (`SchemaRef`, `DFSchemaRef`) for efficient sharing. The table below separates all four:

| Term                                    | Abstraction Layer | Description                                                                                                                          | Accessed via                                             |
| :-------------------------------------- | :---------------- | :----------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------- |
| Catalog Schema ([`SchemaProvider`])       | DataFusion        | A namespace in the catalog hierarchy (like `"public"` in PostgreSQL). Contains registered tables — not a data description.           | `SessionState` → `CatalogProvider` → `SchemaProvider`    |
| **→ DataFrame Schema ([`DFSchema`])**     | **DataFusion**    | **Wraps an Arrow `Schema` and adds table qualifiers and functional dependencies for query planning. Embedded in [`LogicalPlan`].**   | **[`df.schema()`][`.schema()`] returns `&DFSchema`**     |
| **→ Arrow Schema ([`Schema`])**           | **Apache Arrow**  | **The columnar data description: field names, data types, nullability, and metadata. Knows nothing about table names or query context.** | **[`TableProvider::schema()`] returns `SchemaRef`**  |
| SchemaRef / DFSchemaRef                   | Both              | `Arc`-wrapped reference-counted pointers (`Arc<Schema>` and `Arc<DFSchema>`) for passing schemas cheaply without cloning.            | [`df.schema().inner()`][`.inner()`] returns `&SchemaRef` |

:::{admonition} For SQL engineers
:class: tip
In relational databases like PostgreSQL, Oracle, or SQL Server, "schema" primarily means a namespace (e.g., `public.users`). In the Arrow ecosystem, "schema" primarily means the column-level data contract — closer to what you see in `\d tablename` than in `\dn`. The concepts overlap but are handled differently. DataFusion's [`SchemaProvider`] fills the namespace role; [`DFSchema`] and Arrow [`Schema`] describe the data itself.
:::

:::{admonition} Scope for the rest of this section
:class: caution
The remaining pages in Schema Management focus on [`DFSchema`] and Arrow [`Schema`] — the data-describing contracts. When this documentation says "schema" without qualification, it means the column-level contract, not the catalog namespace.

For the detailed internal structure of `DFSchema` (fields, qualifiers, dependencies), see [Anatomy of a Schema](anatomy-schema.md).
:::

---

## DFSchema — The Query-Planning Layer

**[`DFSchema`] enables unambiguous column resolution and optimizer key relationships by wrapping Arrow's [`Schema`] with query-planning context — table qualifiers and functional dependencies — embedded in each [`LogicalPlan`] node.**

[`DFSchema`] wraps an Arrow [`Schema`] and connects the data description with the [`LogicalPlan`]. The Arrow [`Schema`] contains raw Fields — column names, data types, nullability, and metadata — with no relation to the query plan. [`DFSchema`] adds `field_qualifiers` (which table each field belongs to) and `functional_dependencies` (key relationships for the optimizer).

Without `field_qualifiers`, the planner cannot distinguish `users.id` from `orders.id` in joins. Without `functional_dependencies`, the optimizer cannot deduplicate or reduce GROUP BY clauses.

When you call DataFrame API methods like `.filter()`, `.select()`, or `.join()`, each method builds a new [`LogicalPlan`] node — and each node's [`DFSchema`] is what enables fail-fast validation and query optimization. Accessed via [`df.schema()`][`.schema()`], the underlying Arrow [`Schema`] via [`.inner()`] or [`.as_arrow()`].

| Component                 | Type                          | Purpose                                                                                         |
| :------------------------ | :---------------------------- | :---------------------------------------------------------------------------------------------- |
| `inner`                   | `SchemaRef` (`Arc<Schema>`)   | The Arrow Schema — field definitions (name, type, nullable, metadata)                           |
| `field_qualifiers`        | `Vec<Option<TableReference>>` | Tracks which table each field came from. `None` for computed expressions.                       |
| `functional_dependencies` | `FunctionalDependencies`      | Key relationships the optimizer uses (e.g., primary keys that uniquely determine other columns) |

### Accessing the Underlying Arrow Schema

When you need the Arrow Schema (e.g., for file writers, Arrow compute kernels, or interop with other Arrow-based tools), `DFSchema` provides two accessors:

- [`.inner()`] — returns `&SchemaRef` (`&Arc<Schema>`)
- [`.as_arrow()`] — returns `&Schema` (the dereferenced Arrow Schema)

`LogicalPlan::schema()` returns `&DFSchemaRef` (i.e., `&Arc<DFSchema>`), which derefs to `&DFSchema`. [`DataFrame::schema()`][`.schema()`] delegates to this and returns `&DFSchema` directly.

### Schema Immutability

[`DFSchema`] is immutable by design. Each [`LogicalPlan`] node carries its own [`DFSchema`] — transformations produce new plan nodes with new schemas, never mutating existing ones. This guarantees safe concurrent access, makes the plan tree a reliable audit trail, and ensures schema evolution is traceable throughout the query.

### Expression Validation

Schema errors in expressions surface at plan-build time — every `col()` reference and operator is checked against the current [`DFSchema`] before the plan node is constructed. When you write `col("amount").gt(lit(100))`, DataFusion verifies that `amount` exists in the schema and that the `>` operator is valid for its data type. Invalid references fail immediately, not during execution.

:::{admonition} Type coercion and DFSchema
:class: seealso
The optimizer's [`TypeCoercion`] rule reads the [`DFSchema`] to insert implicit widening casts. See [Type Coercion at a Glance](#type-coercion-at-a-glance) and [Type Coercion](type-coercion.md).
:::

For the detailed field-level anatomy (name, data_type, nullable, metadata), see [Anatomy of a Schema](anatomy-schema.md).

---

## Type Coercion at a Glance

**Output schemas may contain wider types than the input — DataFusion's type coercion automatically widens compatible types to prevent data loss, with different rules for expressions and set operations.**

When an `Int32` column is added to an `Int64` column, the result column is `Int64` — the schema of the resulting [`DataFrame`] reflects this widened type, even though no explicit cast was requested. Type coercion inserts these widening casts automatically, preventing data loss while keeping the API ergonomic. This is distinct from schema validation: validation catches structural errors at plan-build time, while coercion runs as a separate [`TypeCoercion`] analyzer rule during the optimization phase — after the plan is constructed.

DataFusion applies coercion in two modes:

1. **Auto-coercion in expressions** (`.select()`, `.filter()`, `.with_column()`): The optimizer widens types automatically. `Int32 + Int64` produces `Int64`. This is convenient and safe — it always widens, never narrows.

2. **Strict matching in set operations** (`.union()`, `.except()`, `.intersect()`): Columns in corresponding positions must have compatible types. If no safe coercion path exists, you must cast explicitly. This strictness is a deliberate safety measure to prevent silent data corruption when combining DataFrames.

:::{admonition} Join keys are auto-coerced
:class: note
Join keys are an exception to strict matching — DataFusion automatically coerces join keys to a common type (e.g., `Int32 = Int64` becomes `Int64 = Int64`). This happens transparently via the [`TypeCoercion`] analyzer rule.
:::

For the full coercion hierarchy, widening rules, and examples, see [Type Coercion](type-coercion.md).

---

## How the Initial Schema is Determined

**Every query begins with a schema from the data source — the accuracy and stability of this initial schema determines the reliability of the entire pipeline.**

The initial schema is the first [`DFSchema`] in the plan tree, set at the `TableScan` node by the [`TableProvider`]. Getting the initial schema right is critical: every subsequent transformation derives from it, and inference errors propagate through the entire plan. DataFusion determines the initial schema in one of three ways:

1. **Self-describing formats** ([Parquet], Avro, Arrow IPC): The schema is embedded in the file metadata. Types are known instantly at scan time — no inference needed.

2. **Text formats** (CSV, JSON): Types must be either **provided explicitly** (recommended) or **inferred** from a data sample. Inference samples a configurable number of rows (default: 1,000) and guesses types based on the values it finds.

3. **Custom sources** ([`TableProvider`]): The source of truth is the [`TableProvider::schema()`] method. This contract must return a stable schema to ensure predictable query behavior.

:::{admonition} Schema drift
:class: warning
Inferred schemas can drift as data evolves — a column inferred as `Int32` today may encounter values exceeding its range tomorrow. Explicit schemas eliminate drift entirely. For details and mitigation strategies, see [Schema Inference](schema-inference.md).
:::

For constructing schemas programmatically, see [Creating Schemas](creating-schemas.md). For applying schemas to specific formats (CSV, Parquet, partitioned data), see [Applying Schemas](applying-schemas-modeling-data.md).

---

## Schema Ownership — From Source to DataFrame

**The [`LogicalPlan`] is the schema owner; the [`DataFrame`] is the accessor — understanding this delegation is key to working with schemas in DataFusion.**

Every schema originates at a [`TableProvider`] — the schema creator. Each [`TableProvider`] implements a `.schema()` method that returns an Arrow [`SchemaRef`]. How that [`TableProvider`] is obtained depends on the access pattern — ephemeral reads create one internally, while registered tables store one in the catalog for later retrieval — but both paths converge at [`TableProvider::schema()`].

The two access patterns carry different implications for schema management:

1. **Ephemeral reads** (`ctx.read_csv()`, `ctx.read_batch()`): The [`SessionContext`] creates a [`TableProvider`] (e.g., `ListingTable`, `MemTable`) internally and uses it immediately. The schema may come from file inference or explicit options — inference-based schemas carry drift risk as data evolves.

2. **Registered tables** (`ctx.register_table()`, then `ctx.table("name")`): The [`TableProvider`] is stored in the catalog and retrieved later via the catalog chain (`SessionState` → `CatalogProvider` → `SchemaProvider` → `TableProvider`). The schema is stable and catalog-managed.

Both paths converge inside [`SessionContext`]: the Arrow [`Schema`] from the [`TableProvider`] is wrapped in a [`DFSchema`] (adding qualifiers and functional dependencies) by [`LogicalPlanBuilder`] and embedded in the [`LogicalPlan`] node. The [`DataFrame`] then delegates [`df.schema()`][`.schema()`] to `plan.schema()` — it borrows the schema from the plan, never copies it.

In Rust ownership terms: [`DFSchema`] is wrapped in `Arc<DFSchema>` (`DFSchemaRef`) for shared ownership via reference counting. The [`LogicalPlan`] _owns_ its `DFSchemaRef`. The [`DataFrame`] holds the [`LogicalPlan`] and borrows the schema through `plan.schema()`, which returns `&DFSchema`. No schema data is cloned during this delegation.

```text
SCHEMA OWNERSHIP FLOW
═══════════════════════════════════════════════════════════════════════

  Ephemeral Path                Named Path
  ──────────────                ──────────

[ 1. DATA SOURCE ]            [ 1. DATA SOURCE ]
┌─────────────────────┐       ┌──────────────────────┐
│  Text formats       │       │  Registered tables   │
│  (CSV, JSON)        │       │  Custom sources      │
│  In-memory data     │       │  (Postgres, Delta,   │
│  (RecordBatch)      │       │   Iceberg, ...)      │
│                     │       │                      │
│  Schema may need    │       │  Schema is stable,   │
│  inference          │       │  catalog-managed     │
└──────────┬──────────┘       └──────────┬───────────┘
           │                             │
           ▼                             ▼
[ 2. TABLE PROVIDER ]         [ 2. TABLE PROVIDER ]
     (SCHEMA CREATOR)              (SCHEMA CREATOR)
┌─────────────────────┐       ┌──────────────────────┐
│  ListingTable /     │       │  Custom Provider /   │
│  MemTable           │       │  ListingTable        │
│  .schema() → Arrow  │       │  .schema() → Arrow   │
└──────────┬──────────┘       └──────────┬───────────┘
           │                             │
           ▼                             ▼
[ 3. ACCESS METHOD ]          [ 3. ACCESS METHOD ]
┌─────────────────────┐       ┌──────────────────────┐
│  ctx.read_csv()     │       │  ctx.register_table()│
│  ctx.read_batch()   │       │  then ctx.table(..)  │
│  (immediate use)    │       │  (catalog lookup)    │
└──────────┬──────────┘       └──────────┬───────────┘
           │                             │
═══════════▼═════════════════════════════▼════════════
[ 4. THE HUB ]
┌──────────────────── SESSIONCONTEXT ─────────────────┐
│                                                     │
│TableProvider.schema() ──► Arrow Schema (SchemaRef)  │
│                                  │                  │
│          LogicalPlanBuilder::scan()                 │
│       wraps Arrow Schema as DFSchema                │
│   (adding qualifiers, functional dependencies)      │
│                                  │                  │
│                                  ▼                  │
│┌──────────────────┐  ┌────────────────────────────┐ │
││  SessionState    │  │  SCHEMA OWNER: LogicalPlan │ │
││  (Config,        │  │    projected_schema:       │ │
││   Catalog,       │  │    DFSchemaRef             │ │
││   Optimizer)     │  │    (Arc<DFSchema>)         │ │
│└──────────────────┘  └─────────────┬──────────────┘ │
│                                    │                │
│         DataFrame::new(state.clone(), plan)         │
└────────────────────────────────────┼────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────┐
│USER API: DataFrame                                  │
│  ├── plan: LogicalPlan  (owns the DFSchemaRef)      │
│  └── session_state: SessionState  (cloned)          │
│                                                     │
│df.schema() ── borrows ──► plan.schema() → &DFSchema │
└─────────────────────────────────────────────────────┘
```

:::{admonition} SessionState and DataFrame internals
:class: seealso
For a detailed explanation of the `SessionState` clone semantics and how each `DataFrame` captures its execution environment, see [Anatomy of a DataFrame](../Concepts/anatomy-dataframe.md).
:::

For what lives inside the [`DFSchema`] — fields, qualifiers, and functional dependencies — see [DFSchema — The Query-Planning Layer](#dfschema-the-query-planning-layer) above and [Anatomy of a Schema](anatomy-schema.md).

---

## Schema Propagation Through Transformations

**The schema evolves with every transformation — each `.filter()`, `.select()`, or `.aggregate()` produces a new [`DFSchema`], validated at plan-build time before the node is added to the plan tree.**

Schema propagation is how the [`DFSchema`] changes as transformations are chained on a [`DataFrame`]. Each call appends a new [`LogicalPlan`] node and derives its output schema from the input schema. Every derivation is validated immediately — structural errors are caught at the point of construction, not during `.collect()`. When `.collect()` (or another action method) triggers execution, the physical plan uses the already-validated schema; no further schema checks occur at runtime.

Each node derives its output schema from its input:

| Operation                    | Schema effect                                                            |
| :--------------------------- | :----------------------------------------------------------------------- |
| `.filter(expr)`              | Schema passes through unchanged — filtering rows does not change columns |
| `.select(exprs)`             | New schema with only the selected/computed columns                       |
| `.aggregate(group_by, aggs)` | New schema with group-by columns + aggregate result columns              |
| `.join(right, ...)`          | Combined schema from both inputs — qualifiers prevent column ambiguity. `LEFT`, `RIGHT`, and `FULL` joins force nullability on the null-extended side, even if the source fields were non-nullable. |
| `.with_column(name, expr)`   | Existing schema + one new or replaced column                             |
| `.drop_columns(names)`       | Existing schema minus the dropped columns                                |

The plan tree below illustrates how the schema narrows at each step:

```text
Aggregate(group=[region], agg=[sum(amount)])
│   DFSchema: {region: Utf8, sum(amount): Float64}           ← 2 columns
│
└─ Filter(amount > 100)
   │   DFSchema: {id: Int64, region: Utf8, amount: Float64}  ← unchanged
   │
   └─ TableScan("sales")
          DFSchema: {id: Int64, region: Utf8, amount: Float64}  ← 3 columns
```

Schema validation happens at plan-build time — `.filter(col("nonexistent").gt(lit(100)))` fails immediately with a "Column not found" error, before any data is scanned. For the full list of contract violations and error types, see [Contract Violations](#contract-violations--fail-fast-at-plan-build-time).

For detailed transformation patterns (qualifiers, combining schemas, nullability handling), see [Schema Transformation](schema-transformation.md). For the specific DataFrame methods that change schema, see [DataFrame Methods](dataframe-methods.md).

---

## Logical vs Physical Schema

**[`df.schema()`][`.schema()`] returns the _logical_ schema — what the plan expects to produce. The actual physical memory layout during execution may differ.**

The logical schema is the [`DFSchema`] attached to the [`LogicalPlan`] — column names, data types, and nullability as determined by the plan tree. The physical schema is the actual memory layout of `RecordBatch` results during execution. Understanding the distinction matters when implementing a custom [`TableProvider`], debugging unexpected types in output batches, or tuning physical execution performance. DataFusion enforces a core invariant: the physical schema (column names and types) must match the logical schema with qualifiers stripped — `RecordBatch` results always align with what [`df.schema()`][`.schema()`] promised.

The physical execution layer may diverge from the logical schema in three implementation details:

- **Dictionary encoding**: A `Utf8` column may be physically stored as dictionary-encoded integers for memory efficiency.
- **Nullability adjustments**: The optimizer may tighten or relax nullability based on analysis passes.
- **Implicit casts**: The [`TypeCoercion`] analyzer may insert cast operations that change the physical representation while preserving logical semantics.

These differences are handled transparently by the physical plan — the core invariant guarantees that user-facing results match the logical schema.

---

## Conclusion

**The schema is the contract that makes DataFusion's fail-fast behavior, type safety, and query optimization possible.**

Schema management in DataFusion follows one principle: define the data contract once, validate at plan-build time, propagate automatically through every transformation. The schema as a data contract (primary and secondary metadata), the four uses of the term "schema," [`DFSchema`]'s query-planning context, type coercion rules, initial schema determination, the ownership chain from data source through [`LogicalPlan`] to [`DataFrame`], schema propagation through transformations, and the logical-physical invariant — together form the conceptual foundation.

SQL queries in DataFusion follow the same schema lifecycle — parsing produces a [`LogicalPlan`] with identical [`DFSchema`] validation, coercion, and propagation rules.

The next step is to explore the internal structure of [`DFSchema`] in detail — see [Anatomy of a Schema](anatomy-schema.md) for the field-level deep dive into names, types, nullability, and metadata.

---

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
[`Schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`SchemaRef`]: https://docs.rs/arrow/latest/arrow/datatypes/type.SchemaRef.html
[`SchemaProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`TableProvider::schema()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html#tymethod.schema
[`TypeCoercion`]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/type_coercion/struct.TypeCoercion.html
[`LogicalPlanBuilder`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.inner()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.inner
[`.as_arrow()`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html#method.as_arrow
[Parquet]: https://parquet.apache.org/
