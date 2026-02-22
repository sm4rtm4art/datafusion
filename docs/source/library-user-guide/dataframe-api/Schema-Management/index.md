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
-->s

<!-- TODO: Migration Checklist

- [ ] **Migrate Intro:** Move the "Introduction" and "The "birth" phase..." text here.
- [ ] **Migrate Philosophy:** Move "The Philosophy of Convergence" here.
- [ ] **Migrate Architecture:** Move "Architecture: From data source to a lazy plan" (including the ASCII diagram).
- [ ] **Create Navigation:** Add a list/table of links to the other files in this directory (`from-files.md`, `from-sql.md`, etc.).
- [ ] **Clean up:** Remove specific code examples that belong in the sub-pages (keep high-level concepts only).

-->

# Schema Management with DataFrameSchema

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

```{toctree}
:maxdepth: 2

anatomy
inspection-and-validation
io-and-modeling
dataframe-methods
type-coercion
```

## Introduction

## Where Schemas Come From

## The Schema Ownership Flow

## Types of Schemas: The Schema Dilemma

## How Schemas are Determined
