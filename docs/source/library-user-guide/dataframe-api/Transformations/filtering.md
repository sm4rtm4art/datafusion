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

# Filtering Excellence

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents}
:local:
:depth: 2
:caption: Filtering Excellence
```

## Introduction (placeholder)

**Filtering controls which rows survive—applying predicates to discard irrelevant data early, before expensive joins or aggregations consume resources.**

<!--Check Reference

[projection](#selection-and-projection-mastery)
-->

Where projection shapes columns, filtering shapes rows. The [`.filter()`] method accepts any boolean expression built from these building blocks:

| Predicate Type   | Methods                           | Example                                   |
| :--------------- | :-------------------------------- | :---------------------------------------- |
| Comparisons      | [`.gt()`], [`.lt()`], [`.eq()`]   | `col("price").gt(lit(100))`               |
| Logical          | [`.and()`], [`.or()`], [`.not()`] | `condition_a.and(condition_b)`            |
| Set membership   | [`in_list()`]                     | `col("status").in_list(vec![...], false)` |
| Pattern matching | [`.like()`], [`.ilike()`]         | `col("name").like(lit("A%"))`             |
| Range            | [`.between()`]                    | `col("age").between(lit(18), lit(65))`    |

Predicate pushdown ensures filters reach the data source, letting formats like Parquet skip entire row groups.

**SQL equivalent:** [`WHERE condition`][`where`]

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Composable predicates built programmatically, Rust control flow for conditional logic, compile-time column checking, dynamic filters from runtime values
> - **SQL shines:** Familiar `WHERE` syntax, more readable for simple static conditions, clearer `AND`/`OR` precedence

**Performance note:** <br>
DataFusion excels at **predicate pushdown** — filters reach data sources so Parquet skips entire row groups and databases apply indexes. For highly selective point lookups (`WHERE id = 123`) on indexed row-based databases, the source DB may be faster. For complex multi-column predicates or full scans, DataFusion's vectorized evaluation wins.

### Basic Filtering

**Start simple:** most filters are single-column comparisons. <br>
Build the predicate with [`col()`] for the column, a comparison method like [`.gt()`], and [`lit()`] for the literal value. The pattern reads naturally: `col("price").gt(lit(100))` means "price greater than 100".

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // Filter: keep only rows where price > 100
    sample_df.clone()
        .filter(col("price").gt(lit(100)))?
        .show().await?;
    // Only Laptop (1200) survives — Mouse (25) and Keyboard (75) are filtered out
    // +---------+-------+----------+-------------+
    // | product | price | quantity | category    |
    // +---------+-------+----------+-------------+
    // | Laptop  | 1200  | 5        | Electronics |
    // +---------+-------+----------+-------------+

    Ok(())
}
```

### Intermediate: Complex Predicates

**Real-world filters combine multiple conditions.** <br>
Chain predicates with [`.and()`] and [`.or()`], check set membership with [`in_list()`], match patterns with [`.like()`] (case-sensitive) or [`.ilike()`] (case-insensitive), and validate ranges with [`.between()`].

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // AND/OR: (price > 50 AND quantity < 40) OR product = "Laptop"
    sample_df.clone()
        .filter(
            col("price").gt(lit(50))
                .and(col("quantity").lt(lit(40)))
                .or(col("product").eq(lit("Laptop")))
        )?
        .show().await?;
    // Keyboard matches (price=75 > 50, quantity=30 < 40)
    // Laptop matches via OR clause
    // +---------+-------+----------+-------------+
    // | product | price | quantity | category    |
    // +---------+-------+----------+-------------+
    // | Laptop  | 1200  | 5        | Electronics |
    // | Keyboard| 75    | 30       | Accessories |
    // +---------+-------+----------+-------------+

    Ok(())
}
```

**More examples for predicate patterns:**

```rust
use datafusion::prelude::*;
use datafusion::functions::string::expr_fn::lower;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // IN list: product IN ("Laptop", "Mouse")
    println!("IN list example:");
    sample_df.clone()
        .filter(in_list(col("product"), vec![lit("Laptop"), lit("Mouse")], false))?
        .show().await?;

    // Pattern matching: product LIKE '%board%' (contains "board")
    println!("LIKE example:");
    sample_df.clone()
        .filter(col("product").like(lit("%board%")))?
        .show().await?;

    // Case-insensitive matching: ILIKE or lower()
    // Method 1: .ilike() — SQL's ILIKE equivalent
    println!("ILIKE example:");
    sample_df.clone()
        .filter(col("product").ilike(lit("%BOARD%")))?  // Matches "Keyboard"
        .show().await?;

    // Method 2: Normalize both sides with lower()
    println!("lower() example:");
    sample_df.clone()
        .filter(lower(col("product")).eq(lit("keyboard")))?
        .show().await?;

    // Range: price BETWEEN 50 AND 500
    println!("BETWEEN example:");
    sample_df.clone()
        .filter(col("price").between(lit(50), lit(500)))?
        .show().await?;

    // Null safety: price IS NOT NULL (all rows pass — no nulls in sample_df)
    println!("IS NOT NULL example:");
    sample_df.clone()
        .filter(col("price").is_not_null())?
        .show().await?;

    Ok(())
}
```

> **Operator precedence:** [`.and()`] binds tighter than [`.or()`], just like SQL. Use parentheses (method chaining order) to make intent explicit: `a.and(b).or(c)` means `(a AND b) OR c`.

### Advanced: Dynamic Filter Building

**This is where DataFrames truly outshine SQL.** When filter criteria come from user input, configuration, or runtime logic, building queries dynamically showcases two critical safety advantages:

1. **Rust's type system catches errors at compile time.** <br> _Misspell a column name?_ <br>
   The compiler tells you. Pass a string where a number is expected? Caught before your code ever runs. With dynamic SQL, these errors surface at runtime—often in production.

2. **SQL injection becomes impossible by design.** <br> Values flow through [`lit()`] as typed data, not string fragments. There's no way for user input like [`"; DROP TABLE users;--"`](https://xkcd.com/327/) to escape into query structure. You don't need to remember to sanitize—the API makes unsafe patterns unrepresentable.

```rust
use datafusion::prelude::*;

/// Builds a filter expression from optional criteria.
/// Returns `lit(true)` if no criteria provided (matches all rows).
fn build_filter(min_price: Option<i32>, max_quantity: Option<i32>) -> Expr {
    let mut conditions: Vec<Expr> = Vec::new();

    if let Some(price) = min_price {
        conditions.push(col("price").gt_eq(lit(price)));
    }

    if let Some(qty) = max_quantity {
        conditions.push(col("quantity").lt_eq(lit(qty)));
    }

    // Fold conditions with AND; default to lit(true) if empty
    conditions
        .into_iter()
        .reduce(|acc, cond| acc.and(cond))
        .unwrap_or_else(|| lit(true))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // Example: min_price=50, no max_quantity constraint
    let filter_expr = build_filter(Some(50), None);
    sample_df.clone().filter(filter_expr)?.show().await?;
    // Only Laptop (1200) and Keyboard (75) have price >= 50
    // +---------+-------+----------+-------------+
    // | product | price | quantity | category    |
    // +---------+-------+----------+-------------+
    // | Laptop  | 1200  | 5        | Electronics |
    // | Keyboard| 75    | 30       | Accessories |
    // +---------+-------+----------+-------------+

    // Example: both constraints — price >= 50 AND quantity <= 10
    let filter_expr = build_filter(Some(50), Some(10));
    sample_df.clone().filter(filter_expr)?.show().await?;
    // Only Laptop matches (price=1200 >= 50, quantity=5 <= 10)
    // +---------+-------+----------+-------------+
    // | product | price | quantity | category    |
    // +---------+-------+----------+-------------+
    // | Laptop  | 1200  | 5        | Electronics |
    // +---------+-------+----------+-------------+

    Ok(())
}
```

> **Why [`unwrap_or_else`] instead of [`unwrap()`]?** <br>
> Calling [`unwrap()`] on `None` panics—crashing your program. Here, [`reduce()`] returns `None` when the conditions vector is empty (no filters provided). Instead of panicking, [`unwrap_or_else`] lets us provide a fallback: [`lit(true)`][`lit()`] matches all rows. This is a common Rust pattern for gracefully handling "no input" cases.

### Anti-Pattern: Multiple Sequential Filters

**Each [`.filter()`] call creates a separate node in the logical plan.** While DataFusion's optimizer _can_ merge adjacent filters, combining them yourself is clearer, guarantees a single predicate evaluation, and makes your intent explicit.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // ❌ DON'T: Chain multiple filter calls
    let _fragmented = sample_df.clone()
        .filter(col("price").gt(lit(50)))?
        .filter(col("quantity").lt(lit(100)))?
        .filter(col("product").is_not_null())?;

    // ✅ DO: Combine into single filter
    let combined = sample_df.clone()
        .filter(
            col("price").gt(lit(50))
                .and(col("quantity").lt(lit(100)))
                .and(col("product").is_not_null())
        )?;

    // Both return the same result — Keyboard (price=75, quantity=30)
    combined.show().await?;
    // +----------+-------+----------+-------------+
    // | product  | price | quantity | category    |
    // +----------+-------+----------+-------------+
    // | Keyboard | 75    | 30       | Accessories |
    // +----------+-------+----------+-------------+

    Ok(())
}
```

> **Why it matters:** The fragmented version creates 3 filter nodes; the combined version creates 1. In complex queries, this compounds—affecting plan readability and optimization opportunities.

### Filter Troubleshooting

| Symptom          | Cause                                                                  | Fix                                                 |
| :--------------- | :--------------------------------------------------------------------- | :-------------------------------------------------- |
| No rows returned | [three-valued logic]: `NULL > 5` → `NULL` (filtered out)               | Use [`.is_not_null()`] or [`coalesce()`]            |
| Nulls vanishing  | `col("x").eq(lit(false))` removes `NULL` too (`NULL = false` → `NULL`) | Add `.or(col("x").is_null())`                       |
| Slow filter      | Predicate not pushed to data source                                    | Check [`.explain()`]—filter should be _inside_ scan |

---
