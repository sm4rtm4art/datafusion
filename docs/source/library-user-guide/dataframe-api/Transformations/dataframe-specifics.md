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

<!--TODO (restructuring map, agreed 2026-07-06)

3. DONE (2026-07-16) — keep as a slim classification-and-routing page titled
   "DataFrame-Native Capabilities".
4. DONE (2026-07-16) — usage → selection.md; effects →
   Schema-Management/schema-dataframe-methods.md.
5. DONE (2026-07-16) — hub-table anchors repaired to authoritative pages.
-->

<!--TODO (page-role reassessment, 2026-07-18)
1. Reassess whether this page should remain in the Transformations toctree. Its current classifier/router role does not fit cleanly with the surrounding action-oriented transformation pages.
2. Do not preserve the page merely because it already exists. Determine whether it owns a distinct reader problem that cannot be handled by the index, hybrid-sql.md, builder-patterns.md, or the individual method pages.
3. Current redistribution proposal:
  - SQL-equivalent versus DataFrame convenience comparisons: move to the relevant method pages.
  - SQL/DataFrame interface choice and bridge methods: move to hybrid-sql.md.
  - Rust control flow, branching, reuse, and pipeline functions: move to or remain in builder-patterns.md.
  - .unnest_columns() and related reshaping methods: move to or remain in reshaping.md.
  - .describe() and validation-oriented inspection: assess for data-quality.md.
  - general method routing: retain only where it improves the Transformations index.
4. Reassess the existing capability taxonomy. "API-level", "native convenience", "SQL-equivalent", and "bridge" may be useful locally, but may not justify a full page.
5. If the page is removed, preserve useful content through explicit move handshakes and repair all incoming links before deleting it. 6. Do not implement deletion or redistribution until the closing-section architecture has been approved. -->

# DataFrame-Native Capabilities

**Most DataFrame methods mirror SQL, while the smaller set that is dedicated to, native to, or more ergonomic in the DataFrame API is classified and routed here to its authoritative page.**

DataFusion provides one engine through both the SQL and DataFrame APIs. Most transformations are available through either API, but a smaller set is distinctive to the DataFrame API. This page answers what is meaningfully different about the DataFrame API, whether a capability is API-level, a native convenience, SQL-equivalent, or a bridge, and where its authoritative explanation lives. It classifies and routes capabilities; it does not teach method behavior.

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

## Four Ways a Capability Relates to SQL

The same engine can expose a capability differently through SQL and through a programmatic API. This classification distinguishes the relationship without treating either interface as universally better.

- **API-level capability** — programmatic plan construction, execution control, or API behavior rather than a SQL clause.
- **Native convenience** — SQL can produce the same result, but the DataFrame API offers a direct programmatic operation.
- **SQL-equivalent** — both APIs support it directly; the difference is ergonomics or composition.
- **Bridge capability** — moves expressions or plans between SQL and DataFrame workflows.

---

## Capability Map

| Capability                       | Classification       | Why it matters                                                                              | Authoritative page                                                        |
| -------------------------------- | -------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `.with_column()`                 | Native convenience   | Add or replace a column without re-listing the rest                                         | [Selection](selection.md#adding-renaming-and-dropping-columns)            |
| `.with_column_renamed()`         | Native convenience   | Rename one field in place                                                                   | [Selection](selection.md#adding-renaming-and-dropping-columns)            |
| `.drop_columns()`                | Native convenience   | Drop named columns, keeping the complement                                                  | [Selection](selection.md#adding-renaming-and-dropping-columns)            |
| `.union_by_name()`               | Native convenience   | Align set operations by column name, not position (a `_distinct` variant also deduplicates) | [Set Operations](set-operations.md#union-by-column-name)                  |
| `.fill_null()`                   | Native convenience   | Replace nulls across several columns in one call                                            | [Null Handling](../Concepts/null-handling.md#the-null-handling-toolkit)   |
| `.describe()`                    | Native convenience   | Summary statistics for every column in one action                                           | [Data Validation & Quality](data-quality.md#describing-data)              |
| `.unnest_columns()`              | Native convenience   | Explode array/list columns into rows                                                        | [Reshaping Data](reshaping.md#unnesting--exploding-arrays)                |
| `.unnest_columns_with_options()` | API-level capability | Control recursion depth and null handling when exploding                                    | [Reshaping Data](reshaping.md#controlling-unnest-behavior-with-options)   |
| `.into_view()`                   | Bridge capability    | Register a DataFrame as a SQL-queryable table                                               | [Mixing SQL and DataFrames](hybrid-sql.md#the-seamless-workflow)          |
| `.parse_sql_expr()`              | Bridge capability    | Parse a SQL expression string into an `Expr`                                                | [Mixing SQL and DataFrames](hybrid-sql.md#parsing-sql-expressions)        |
| `.select_exprs()`                | Bridge capability    | Project using SQL expression strings                                                        | [Mixing SQL and DataFrames](hybrid-sql.md#selecting-with-sql-expressions) |
| `.with_param_values()`           | Bridge capability    | Bind values to `$1`/`$name` placeholders                                                    | [Mixing SQL and DataFrames](hybrid-sql.md#parameter-binding)              |
| `.distinct_on()`                 | SQL-equivalent       | Keep the first row per key; DataFusion SQL also has `DISTINCT ON`                           | [Set Operations](set-operations.md#distinct-on-postgresql-style)          |
| `.alias()`                       | SQL-equivalent       | Qualify columns for self-joins, like SQL table aliasing                                     | [Join Patterns](joins.md#self-joins-and-qualified-columns)                |

The DataFrame API also owns capabilities outside transformation logic: programmatic construction such as `.from_columns()` ([Creating DataFrames](../Creating-DataFrames/index.md)) and execution control such as `.cache()`, `.collect_partitioned()`, and `.execute_stream()` ([Writing & Executing DataFrames](../Writing-DataFrames/index.md)). These are genuine API-level capabilities documented in their own lifecycle phases. Wide-to-long reshaping (melt/unpivot) has no built-in method—see [Melt and Unpivot](reshaping.md#melt-and-unpivot).

---

## Conclusion

These capabilities recur across the DataFrame-native leaves; follow the owner links for full behavior. The DataFrame API's edge here is ergonomics and composition, not doing what SQL cannot.
