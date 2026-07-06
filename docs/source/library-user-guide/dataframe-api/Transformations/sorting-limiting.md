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

<!--TODO (content backlog, agreed 2026-07-06)

1. ABSTRACT
2. INTRODUCTION
3. EMPTY SHELL — no body content yet. Methods to cover: `.sort()`
   (SortExpr, asc/desc, nulls first/last), `.sort_by()` (default ascending
   convenience), `.limit(skip, fetch)` (OFFSET + LIMIT in one call).
4. POSITION — third page of the single-frame part (after selection and
   filtering); include SQL equivalents (ORDER BY / LIMIT / OFFSET) and the
   top-N pattern (sort + limit) with its optimization note.
-->

# Sorting and Limiting



:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::


```{contents} Table of Content
:local:
:depth: 2
```
