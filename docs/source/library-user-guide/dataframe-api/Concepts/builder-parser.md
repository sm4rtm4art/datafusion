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

## Two Paths to the Same Plan: Parser vs Builder

```{contents} Table of Contents for Parser vs Builder
:local:
:depth: 2
```

**SQL and the DataFrame API are two front-ends to the same query engine**<br>

Both compile to identical [`LogicalPlan`] representations, receive the same optimizations, and execute with the same performance. This unified architecture means you can choose whichever API fits your workflow without sacrificing speed, and you can freely mix both in a single pipeline.

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                              SessionState                                  │
│           (Catalog, Function Registry, Config, Query Planner)              │
├──────────────────────────────────┬─────────────────────────────────────────┤
│         SQL (Parser)             │          DataFrame API (Builder)        │
├──────────────────────────────────┼─────────────────────────────────────────┤
│                                  │                                         │
│  "SELECT a, b FROM t             │  ctx.table("t")?                        │
│   WHERE a > 10"                  │     .filter(col("a").gt(lit(10)))?      │
│                                  │     .select(vec![col("a"), col("b")])?  │
│           │                      │              │                          │
│           │ parse                │              │ build                    │
│           ▼                      │              │                          │
│      ┌─────────┐                 │              │ (no AST step)            │
│      │   AST   │                 │              │                          │
│      └────┬────┘                 │              │                          │
│           │ plan                 │              │                          │
│           ▼                      │              ▼                          │
├───────────┴──────────────────────┴──────────────┴──────────────────────────┤
│                                                                            │
│                          LogicalPlan (Identical!)                          │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│                                      │                                     │
│                                      ▼                                     │
│                              Further Execution                             │
└────────────────────────────────────────────────────────────────────────────┘
```

**Reading the diagram:**

- **SessionState (top container)**:<br>
  Both paths operate within the same execution environment. The [`SessionState`] provides the catalog (table definitions), function registry (UDFs, aggregates), configuration, and query planner. This shared context is why both APIs have access to the same tables and functions.

- **SQL (Parser) path**:<br>
  A query string goes through `sqlparser`'s lexer and parser to produce an [Abstract Syntax Tree (AST)], then the logical planner converts the AST into a [`LogicalPlan`]. This extra step enables familiar SQL syntax but means errors surface at runtime.

- **DataFrame API (Builder) path**:<br>
  Method calls like [`.filter()`] and [`.select()`] construct [`LogicalPlan`] nodes directly—no parsing, no AST. This is why you get IDE autocomplete and why Rust can catch type errors at compile time (though schema errors remain runtime).

- **LogicalPlan (convergence point)**:<br>
  Both paths produce the _exact same_ [`LogicalPlan`] structure. There's no "SQL flavor" vs "DataFrame flavor"—just one unified representation. This is the key insight that makes mixing APIs free.

- **Further Execution**: <br>
  From here, the [`LogicalPlan`] flows through optimization, physical planning, and execution—identically regardless of which path created it.

> **Key takeaway**: <br>
> Parser vs Builder is purely a construction choice—once you have a `LogicalPlan`, DataFusion doesn't know or care how you built it.

---

### In Practice: Two Paths, One Result

The following example demonstrates the interchangeability of both APIs. We query the same table using SQL (parser path) and the DataFrame API (builder path), then verify that both produce identical results. This is the core promise of DataFusion's unified architecture—choose the API that fits your workflow, knowing the outcome is the same.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Set up a table with sample data
    ctx.sql("CREATE TABLE sales (region VARCHAR, amount INT) AS VALUES
        ('north', 1500), ('south', 800), ('east', 2000)").await?;

    // Parser path: SQL string → parse → AST → LogicalPlan → DataFrame
    let sql_df = ctx.sql(
        "SELECT region, amount FROM sales WHERE amount > 1000"
    ).await?;

    // Builder path: method calls → LogicalPlan nodes → DataFrame
    let builder_df = ctx.table("sales").await?
        .filter(col("amount").gt(lit(1000)))?
        .select(vec![col("region"), col("amount")])?;

    // Both paths produce this identical result
    let expected = [
        "+--------+--------+",
        "| region | amount |",
        "+--------+--------+",
        "| north  | 1500   |",
        "| east   | 2000   |",
        "+--------+--------+",
    ];

    let sql_result = sql_df.collect().await?;
    let builder_result = builder_df.collect().await?;

    assert_batches_eq!(expected, &sql_result);
    assert_batches_eq!(expected, &builder_result);

    Ok(())
}
```

---

### When to Choose Which?

Both APIs produce identical plans, so choose based on ergonomics:

| Choose **DataFrame API** when...                         | Choose **SQL** when...                              |
| -------------------------------------------------------- | --------------------------------------------------- |
| Query logic depends on runtime conditions                | Query is static and well-defined                    |
| You want reusable query fragments (extract to functions) | You need complex analytics (window functions, CTEs) |
| Security matters (no SQL injection by construction)      | Query comes from config files or user input         |
| IDE refactoring and autocomplete matter                  | Team is SQL-fluent, Rust is secondary               |

Neither is "better"—they're tools for different situations. Since both compile to the same [`LogicalPlan`], you can mix them freely: use [`.into_view()`] to register any DataFrame as a table for SQL queries.

> **Further reading**:<br>
> This dual-API architecture follows principles established in the broader data science ecosystem. For the theoretical foundation, see [Towards Scalable Dataframe Systems][dataframe-paper].

---
