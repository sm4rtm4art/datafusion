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

<!--TODO (Stage 2 scaffold — landing skeleton; finalize after leaf pages
stabilize, markdown-landing.mdc §1.3; 2026-07-06)

1. MOVED OUT to transformation-concepts.md (move handshake): orientation
   paragraphs, SessionContext→LogicalPlan diagram, builder-vs-parser
   exposition, "Methodical Differences" + injection example, "Finding
   Balance" / TableProvider / use-case tables, SQL↔DataFrame method map.
   Duplicated intro blocks (copy-paste artifact) were deleted — one copy
   survives at the destination.
2. MOVED OUT to Concepts/expressions.md: TODO on function-library coverage
   (encoding, nested, datetime).
3. DELETED: "What's Ahead" (dead anchors into the old monolith; replaced by
   the curated routing table below). Style Note + contents block (prohibited
   on landing pages, markdown-landing.mdc §1.2).
4. PENDING: rendered starting-dataset table (text block, not code) once the
   running-example dataset is designed — narrates the data journey here,
   definition code lives in transformation-concepts.md.
5. PENDING: curated routing table (arc: concepts → single-frame →
   multi-frame → analytical → hybrid bridge → DataFrame-native → capstone).
-->

# Transformations with DataFrame API

**The "life" phase of the DataFrame lifecycle: build and refine a lazy query plan.**

Transformations are where you re-shape and analyze data: once you [create](../Creating-DataFrames/index.md) a DataFrame, you can filter, select, join, aggregate, sort, and enrich data by composing methods that build a [`LogicalPlan`]. In the [DataFrame lifecycle metaphor](../index.md#the-dataframe-lifecycle), this is the "life" phase—execution and persistence happen later (see [Writing & Executing](../Writing-DataFrames/index.md)). For the transformation mental models — laziness, expressions, joins, set operations — start with [Transformations Concepts](transformation-concepts.md).

```{toctree}
:maxdepth: 1
:caption: Transformations
:numbered:
:titlesonly:
transformation-concepts
selection
filtering
sorting-limiting
joins
set-operations
aggregations
window-functions
subqueries
hybrid-sql
reshaping
data-quality
dataframe-specifics
builder-patterns
```

<!-- PLACEHOLDER: curated routing table (see file-top TODO 5). -->

<!-- PLACEHOLDER: rendered starting-dataset table + one-paragraph journey
narration (see file-top TODO 4). -->

<!-- Link references -->

[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
