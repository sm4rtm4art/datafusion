<!--
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

<!-- TODO: Migration Checklist

- [ ] **Migrate Intro:** Move the "Introduction" and "The "birth" phase..." text here.
- [ ] **Migrate Philosophy:** Move "The Philosophy of Convergence" here.
- [ ] **Migrate Architecture:** Move "Architecture: From data source to a lazy plan" (including the ASCII diagram).
- [ ] **Create Navigation:** Add a list/table of links to the other files in this directory (`from-files.md`, `from-sql.md`, etc.).
- [ ] **Clean up:** Remove specific code examples that belong in the sub-pages (keep high-level concepts only).

-->

# Schema Management with DFSchema

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{toctree}
:maxdepth: 1
:numbered:
:titlesonly:
:caption: Schema Management with DFSchema
anatomy-schema
creating-schemas
dataframe-methods
inspection-and-validation
schema-transformation
```

## Introduction(Placeholde)

**The “health” phase of the DataFrame lifecycle: inspect, validate, and evolve schema.**

Schema management defines the structural contract of your data—names, types, and nullability—as it flows through DataFusion. Explicit schemas are critical for both correctness and performance, enabling the optimizer to push down predicates, select vectorized kernels, and prevent silent schema drift. You manage this contract via the [`DFSchema`] API, which wraps underlying Arrow types with the query-planning context needed for robust, predictable execution.

In this guide, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::{admonition} Style Note
:class: note

In this document, method notation follows a consistent pattern:

- **DataFrame methods** use `df.method()` (for example, `df.select(...)`)
- **DFSchema method**s use `df.schema().method()` (for example, `df.schema().fields()`)
- **Associated functions** use `DFSchema::method()` (for example, `DFSchema::try_from(...)`).
- **Standalone functions** use `function()` (for example, `col()`), and constructors use `Type::new()` (for example, `SessionContext::new()`).

:::

```{contents}
:local:
:depth: 2
```

## Introduction (placeholder)

**The “health” phase of the DataFrame lifecycle: inspect, validate, and evolve schema.**

Schema management defines the structural contract of your data—names, types, and nullability—as it flows through DataFusion. Explicit schemas are critical for both correctness and performance, enabling the optimizer to push down predicates, select vectorized kernels, and prevent silent schema drift. You manage this contract via the [`DFSchema`] API, which wraps underlying Arrow types with the query-planning context needed for robust, predictable execution.

In this guide, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::{admonition} Style Note
:class: note

In this document, method notation follows a consistent pattern:

- **DataFrame methods** use `df.method()` (for example, `df.select(...)`)
- **DFSchema method**s use `df.schema().method()` (for example, `df.schema().fields()`)
- **Associated functions** use `DFSchema::method()` (for example, `DFSchema::try_from(...)`).
- **Standalone functions** use `function()` (for example, `col()`), and constructors use `Type::new()` (for example, `SessionContext::new()`).

:::

</details>

```{contents}
:local:
:depth: 2
```

## Introduction

**Schema management connects data modeling, query planning, and execution correctness across the DataFusion ecosystem.**

In analytical systems, schema is the contract that binds source data, planner decisions, and runtime behavior. In DataFusion, that contract flows from data sources into the [`LogicalPlan`] and surfaces as [`DFSchema`] on each [`DataFrame`], where you inspect, validate, and evolve structure safely.

DataFusion uses the term "schema" for four distinct concepts. They fall into two layers:

### Where Schemas Come From

**A schema is the structural contract of your data—it defines column names, types, and constraints that enable the query engine to plan and execute efficiently.**

Without a schema, the query engine cannot validate your operations, optimize execution, or guarantee consistent results. Every DataFrame, every table, and every query plan carries a schema that describes "What shape and character are these data?"

### The Schema Ownership Flow

Understanding where schemas live—and how they flow through the system—is key to working with DataFusion. The diagram below illustrates the three stages:

1. **Origin (Catalog)**: <br>
   Registered tables store their Arrow [`Schema`] in the catalog via [`TableProvider`]. This is the source of truth for table definitions.

2. **Ownership (LogicalPlan)**: <br>
   When you build a query, the [`LogicalPlanBuilder`] takes the Arrow [`Schema`] from the [`TableProvider`], wraps it in a [`DFSchema`] (adding table qualifiers), and embeds it in the plan node (e.g., [`TableScan.projected_schema`]). Each transformation creates a new plan node with its own derived schema.

3. **Access (DataFrame)**: <br>
   The DataFrame wraps the [`LogicalPlan`] and delegates [`df.schema()`] to [`LogicalPlan.schema()`]. The DataFrame itself does not store the schema—it lives in the plan.

```text
DataFusion Schema Ownership Flow

┌───────────────────────────────────────────────────────┐
│ 1. SCHEMA ORIGIN (SessionState / Catalog)             │
│    Source of truth for *registered* tables.           │
│                                                       │
│   SessionState                                        │
│     └── CatalogProviderList                           │
│          └── CatalogProvider ("datafusion")           │
│               └── SchemaProvider ("public")           │
│                    └── TableProvider ("users")        │
│                         └── schema() -> Arrow Schema  │
└───────────────────────────────────────────────────────┘
                               │
                               ▼
                       (Plan Creation)
            Arrow Schema is wrapped in DFSchema
            (adding qualifiers) and embedded in the plan.
                               │
                               ▼
┌───────────────────────────────────────────────────────┐
│ 2. SCHEMA OWNER (LogicalPlan)                         │
│    Source of truth for the *current transformation*.  │
│                                                       │
│   LogicalPlan::TableScan                              │
│     ├── table_name: "users"                           │
│     └── projected_schema: DFSchema                    │
│              ├── inner: Arc<Schema>  (Arrow Schema)   │
│              ├── field_qualifiers    (TableReference) │
│              └── functional_dependencies              │
└───────────────────────────────────────────────────────┘
                               │
                               ▼
                       (API Wrapper)
            The DataFrame wraps the plan to provide
            a user-friendly API.
                               │
                               ▼
┌──────────────────────────────────────────────────────┐
│ 3. USER API (DataFrame)                              │
│                                                      │
│   DataFrame                                          │
│     ├── session_state: SessionState (Config Snapshot)│
│     └── plan: LogicalPlan (Holds the DFSchema)       │
│                                                      │
│   df.schema() ────delegates────► plan.schema()       │
└──────────────────────────────────────────────────────┘
```

---

### Types of Schemas: The Schema Dilemma

**In data systems, a schema is fundamentally a structural contract—a blueprint defining how data is organized. However, the scope of this contract changes drastically depending on the architectural layer.**

DataFusion uses the term "schema" for four distinct concepts. They fall into two layers:

**Layer 1 — DataFusion (Query Planning)**

| Type                                    | Purpose                                                                                                                                                                                    | Accessed via                                                       |
| :-------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------- |
| **Catalog Schema** ([`SchemaProvider`]) | A namespace in the catalog hierarchy (like `"public"` in Postgres). Contains registered tables.                                                                                            | `SessionState.catalog_list` → `CatalogProvider` → `SchemaProvider` |
| **DataFrame Schema** ([`DFSchema`])     | Wraps an Arrow `Schema` and adds **table qualifiers** so the planner can resolve ambiguous column references (e.g., `users.id` vs `orders.id` in a join). Embedded in the [`LogicalPlan`]. | [`df.schema()`] returns `&DFSchema`                                |

**Layer 2 — Apache Arrow (Data Description)**

| Type                                | Purpose                                                                                                                                            | Accessed via                                    |
| :---------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------- |
| **Arrow Schema** ([`Schema`])       | The generic columnar schema from Apache Arrow. Defines field names, data types, and nullability. Knows nothing about table names or query context. | [`TableProvider::schema()`] returns `SchemaRef` |
| **Arrow SchemaRef** ([`SchemaRef`]) | Simply `Arc<Schema>`—a reference-counted pointer for passing schemas cheaply between functions without cloning.                                    | `df.schema().inner()` returns `&SchemaRef`      |

#### Why `DFSchema` instead of Arrow's `Schema`?

Arrow's `Schema` describes _data_. `DFSchema` describes the _plan_—it adds table qualifiers for column resolution during query planning. When you need the underlying Arrow schema, use [`.inner()`] (returns `&SchemaRef`) or [`.as_arrow()`] (returns `&Schema`).

#### Logical vs Physical Schema:

[`df.schema()`][`.schema()`] returns the **logical** schema—what the plan _expects_ to produce. The actual physical memory layout during execution (e.g., dictionary encoding for strings, or nullable flags adjusted by optimizer passes) may differ. This is handled transparently by the physical plan; you rarely need to worry about it unless implementing a custom [`TableProvider`].

### How Schemas are Determined

DataFusion determines the initial schema in one of three ways, depending on your data source:

1.  **Self-Describing Formats ([Parquet], Avro, Arrow):** <br>
    The schema is embedded in the file metadata. Types are known instantly at scan time.
2.  **Text Formats (CSV, JSON):** <br>
    Types must be either **provided explicitly** (recommended) or **inferred** from a data sample (risk of **schema drift**—see below).
3.  **Custom Sources (TableProvider):**<br>
    The source of truth is the [`TableProvider::schema()`]-method implemented by the provider. This contract must remain stable to ensure predictable query behavior.

For a deep dive into the underlying [Apache Arrow] type system, see the [Arrow Schema Specification][`arrow schema`].

> **Schema Drift:** <br>
> Schema drift occurs when inferred types change silently across runs because the underlying data evolves. For example, a column inferred as `Int32` from the first 1000 rows may later contain values exceeding `Int32` range, or a previously all-numeric column may start containing strings. Because inference is sampling-based, these changes go undetected until they cause runtime errors or silent data corruption. Explicit schemas eliminate drift entirely—this is why they are recommended for production pipelines.

> **DataFrame vs SQL:** <br>
> Both APIs produce the same [`DataFrame`] containing the same [`LogicalPlan`] with identical schemas. The DataFrame API provides compile-time visibility into schema changes—each method returns a new [`DataFrame`] whose schema you can inspect programmatically before execution.

---

## Further Reading

Resources for understanding Arrow’s type system, schema metadata, and DataFusion’s coercion rules—useful when debugging schema mismatches, unexpected casts, or expensive conversions.

### Arrow & Memory Model (Essential)

| Resource                                                                                      | Description                                                                                                                           |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| [Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)            | Physical memory layout, validity bitmaps, and variable-size views (for example, `StringView`) — explains why some casts are expensive |
| [Arrow Schema IPC Message](https://arrow.apache.org/docs/format/Columnar.html#schema-message) | How fields, metadata, and nullability are serialized — helpful when diagnosing “schema mismatch” errors                               |

### Storage ↔ Memory Type Mapping

| Resource                                                                                                             | Description                                                                                                           |
| -------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| [Parquet Logical Types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)                        | How Parquet logical types (`DECIMAL`, timestamps, etc.) map into Arrow types                                          |
| [DataFusion Type Coercion Rules](https://docs.rs/datafusion/latest/datafusion/logical_expr/type_coercion/index.html) | The exact rules DataFusion uses to reconcile type differences (for example, joining or unioning `Int32` with `Int64`) |

### Execution & Optimization

| Resource                                                                                        | Description                                                                                                              |
| ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| [DataFusion Optimizer Rules](https://docs.rs/datafusion/latest/datafusion/optimizer/index.html) | How the optimizer rewrites plans (it may insert implicit `CAST`s); start with `type_coercion` and `simplify_expressions` |

### Books (Foundational)

| Resource                                                 | Description                                                                                                                                                                                                                                                              |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| The Data Model Resource Book (Vol 1–3) — Len Silverston  | Universal data models for common domains (Vol [1](https://www.oreilly.com/library/view/the-data-model/9780471380238/), [2](https://www.oreilly.com/library/view/the-data-model/9780471353485/), [3](https://www.oreilly.com/library/view/the-data-model/9780470178454/)) |
| Patterns of Data Modeling — David Hay                    | Conceptual modeling patterns that translate well to analytical schemas ([O’Reilly](https://www.oreilly.com/library/view/patterns-of-data/9781439819906/))                                                                                                                |
| The Data Warehouse Toolkit — Kimball & Ross              | Dimensional modeling (star schemas) for analytics ([O’Reilly](https://www.oreilly.com/library/view/the-data-warehouse/9781118530801/))                                                                                                                                   |
| Designing Data-Intensive Applications — Martin Kleppmann | Schema evolution and encoding trade-offs ([O’Reilly](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/))                                                                                                                         |
| How Query Engines Work — Andy Grove                      | Query engine internals (DataFusion’s creator) ([Leanpub](https://leanpub.com/how-query-engines-work))                                                                                                                                                                    |

---
