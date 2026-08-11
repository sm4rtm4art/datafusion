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

Structured data processing rests on a schema contract — _what columns
exist, what types they hold, and which values may be null_. This section
covers the **"Health" stage** of the DataFrame lifecycle, and it centers on
DataFusion's [`DFSchema`]: the query-planning schema that wraps Apache
Arrow's physical `Schema` with the context for column resolution, type
coercion, and optimizer decisions. Every [`LogicalPlan`] node carries its
own `DFSchema`, exposed on any [`DataFrame`] via [`.schema()`] — so the
pages here show how to create, apply, inspect, coerce, and evolve that
contract programmatically, rather than leaving it to implicit runtime
behavior.

```{toctree}
:maxdepth: 1
:numbered:
:caption: Schema Management
schema-concepts
schema-anatomy
type-coercion
schema-creation
schema-application
schema-inference
schema-inspection
schema-transformation
schema-dataframe-methods
```

## Schema Management Overview

**From conceptual foundations to hands-on operations: understand what schemas
are, then build, inspect, and transform them.**

The documents below progress from understanding (`DFSchema` architecture,
type system, coercion rules) to practical application (creating, applying,
inspecting, and transforming schemas). They are designed to be read
sequentially, but you can jump directly to the topic you need:

| Document                                           |             Focus              | Description                                                                                        |
| :------------------------------------------------- | :----------------------------: | :------------------------------------------------------------------------------------------------- |
| **[Schema Concepts][schema-concepts]**             |      Structural contract       | Ownership flow, schema types, `DFSchema` vs Arrow `Schema` — what schemas are and where they live. |
| **[Anatomy of a Schema][schema-anatomy]**          |      Field-level anatomy       | `DFSchema` internals: fields, data types, nullability, metadata, and qualifiers.                   |
| **[Type Coercion][type-coercion]**                 |    Automatic type alignment    | Coercion hierarchy, implicit vs. explicit casting, and how the optimizer reconciles types.         |
| **[Creating Schemas][schema-creation]**            |        Building in code        | Constructing `DFSchema` and Arrow `Schema` programmatically with field types and constraints.      |
| **[Applying Schemas][schema-application]**         |   Format-specific strategies   | Wiring schemas into CSV, NDJSON, Parquet readers, and partitioned datasets.                        |
| **[Schema Inference][schema-inference]**           |     Inferred vs. explicit      | How DataFusion infers schemas from data, and when to provide them explicitly.                      |
| **[Inspecting & Validating][schema-inspection]**   |  Display, access, and checks   | Human-readable display, programmatic field access, existence checks, and schema comparison.        |
| **[Schema Transformation][schema-transformation]** |   Evolving schema structure    | Qualifiers, combining schemas, nullability handling, and schema evolution patterns.                |
| **[Schema Methods][schema-dataframe-methods]**     | Methods that change the schema | DataFrame-specific methods (`.with_column()`, `.with_column_renamed()`, `.unnest_columns()`).      |

---

---

<!-- References -->

<!-- Internal documentation -->

[schema-anatomy]: schema-anatomy.md
[schema-application]: schema-application.md
[schema-concepts]: schema-concepts.md
[schema-creation]: schema-creation.md
[schema-dataframe-methods]: schema-dataframe-methods.md
[schema-inference]: schema-inference.md
[schema-inspection]: schema-inspection.md
[schema-transformation]: schema-transformation.md
[type-coercion]: type-coercion.md

<!-- Core types -->

[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`dfschema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`logicalplan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html

<!-- Methods and functions -->

[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
