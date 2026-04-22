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



# Schema Management

**Every [`DataFrame`] carries a [`DFSchema`] — the structural contract
embedded in its [`LogicalPlan`] that defines column names, data types,
nullability, and table qualifiers for the query engine.**

Structured data processing requires a contract between your data and the
query engine — _what columns exist, what types they hold, and which values
may be null_. In DataFusion, this contract is captured by [`DFSchema`], which
wraps Apache Arrow's physical type system with the query-planning context
needed for column resolution, type coercion, and optimizer decisions. Every
[`LogicalPlan`] node carries its own `DFSchema`, and the [`DataFrame`]
exposes it via [`.schema()`] — making schema management a first-class,
programmatic concern rather than an implicit runtime detail.

:::{admonition} Style Note
:class: note
:collapsible: open

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


```{toctree}
:maxdepth: 1
:numbered:
:caption: Schema Management
schema-concepts
schema-anatomy
type-coercion
schema-inspection
schema-creation
schema-inference
schema-application
schema-transformation
schema-methods
```

## Schema Management Overview

**From conceptual foundations to hands-on operations: understand what schemas
are, then build, inspect, and transform them.**

The documents below progress from understanding (`DFSchema` architecture,
type system, coercion rules) to practical application (creating, applying,
inspecting, and transforming schemas). They are designed to be read
sequentially, but you can jump directly to the topic you need:

| Document                                                           |             Focus              | Description                                                                                    |
| :----------------------------------------------------------------- | :----------------------------: | :--------------------------------------------------------------------------------------------- |
| **[Schema Concepts](schema-concepts.md)**                          |      Structural contract       | Ownership flow, schema types, `DFSchema` vs Arrow `Schema` — what schemas are and where they live. |
| **[Anatomy of a Schema](schema-anatomy.md)**                       |      Field-level anatomy       | `DFSchema` internals: fields, data types, nullability, metadata, and qualifiers.               |
| **[Type Coercion](type-coercion.md)**                              |   Automatic type alignment     | Coercion hierarchy, implicit vs. explicit casting, and how the optimizer reconciles types.      |
| **[Inspecting & Validating](schema-inspection.md)**                | Display, access, and checks    | Human-readable display, programmatic field access, existence checks, and schema comparison.     |
| **[Creating Schemas](schema-creation.md)**                         |     Building in code           | Constructing `DFSchema` and Arrow `Schema` programmatically with field types and constraints.   |
| **[Schema Inference](schema-inference.md)**                        |    Inferred vs. explicit       | How DataFusion infers schemas from data, and when to provide them explicitly.                   |
| **[Applying Schemas](schema-application.md)**                      |   Format-specific strategies   | Applying schemas to CSV, Parquet, partitioned, and nested data sources.                        |
| **[Schema Transformation](schema-transformation.md)**              |   Evolving schema structure    | Qualifiers, combining schemas, nullability handling, and schema evolution patterns.             |
| **[Schema Methods](schema-methods.md)**                            | Methods that change the schema | DataFrame-specific methods (`.with_column()`, `.with_column_renamed()`, `.unnest_columns()`).  |

---

## Further Reading

Resources for understanding Arrow's type system, schema metadata, and DataFusion's coercion rules—useful when debugging schema mismatches, unexpected casts, or expensive conversions.

### Arrow & Memory Model (Essential)

| Resource                                                                                      | Description                                                                                                                           |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| [Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)            | Physical memory layout, validity bitmaps, and variable-size views (for example, `StringView`) — explains why some casts are expensive |
| [Arrow Schema IPC Message](https://arrow.apache.org/docs/format/Columnar.html#schema-message) | How fields, metadata, and nullability are serialized — helpful when diagnosing "schema mismatch" errors                               |

### Storage - Memory Type Mapping

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
| Patterns of Data Modeling — David Hay                    | Conceptual modeling patterns that translate well to analytical schemas ([O'Reilly](https://www.oreilly.com/library/view/patterns-of-data/9781439819906/))                                                                                                                |
| The Data Warehouse Toolkit — Kimball & Ross              | Dimensional modeling (star schemas) for analytics ([O'Reilly](https://www.oreilly.com/library/view/the-data-warehouse/9781118530801/))                                                                                                                                   |
| Designing Data-Intensive Applications — Martin Kleppmann | Schema evolution and encoding trade-offs ([O'Reilly](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/))                                                                                                                         |
| How Query Engines Work — Andy Grove                      | Query engine internals (DataFusion's creator) ([Leanpub](https://leanpub.com/how-query-engines-work))                                                                                                                                                                    |

---

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
