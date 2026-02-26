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

# Testing sphinx

:::{note}
This method returns a new `DataFrame`; it does not mutate the original.
:::

:::{warning}
Calling `.collect()` triggers full execution — avoid in loops.
:::

:::{tip}
Chain `.limit()` before `.collect()` when exploring large datasets.
:::

:::{important}
Important notice to notice

:::

:::{caution}
CAUTION: This is a cautionary notice.
:::

:::{danger}
DANGER: This is a dangerous notice.
:::

:::{seealso}
SEE ALSO: This is a see also notice.
...

:::{warning} The #1 Schema Mismatch Cause
In the Rust DataFrame API, column names are **case-sensitive strings**. `col("Region")` and `col("region")` reference _different_ columns—this catches many users off guard.
:::

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)
- static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::

Consider building a search API where filters depend on user input:

```{code-block} rust
:caption: **SQL approach** — string concatenation:
:linenos:
:lineon-start: 3
:emphasize-lines: 7
use datafusion::prelude::*;


fn main() {
    let filter_department: Option<&str> = Some("Sales");

    let mut query = "SELECT * FROM employees WHERE 1=1".to_string();
    if let Some(dept) = filter_department {
        query.push_str(&format!(" AND department = '{}'", dept));
    }
}
```

This pattern has three problems:

1. **Injection vulnerability** — if `dept` contains [`'; DROP TABLE students; --`](https://xkcd.com/327/), you're in trouble.
2. **Runtime-only errors** — typos like `"deprtment"` won't surface until execution.
3. **The `WHERE 1=1` hack** — exists solely to simplify conditional string building.

**DataFrame approach** — type-safe, composable:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees_df = dataframe!("department" => ["Sales", "Engineering"])?;
    let filter_department: Option<&str> = Some("Sales");

    let mut result = employees_df;
    if let Some(dept) = filter_department {
        result = result.filter(col("department").eq(lit(dept)))?;
    }
    result.show().await?;
    Ok(())
}
```

<!--
:::{dropdown} 💅 Style Note: Method Notation
In this document, method notation follows a consistent pattern:

- **DataFrame methods** use `df.method()`
- **DFSchema methods** use `df.schema().method()`
- **Associated functions** use `DFSchema::try_from(...)`
  :::

::::{tab-set}
:::{tab-item} rust

```python
df.select(col("a"), col("b"))
```

:::
:::{tab-item} SQL equivalent

```sql
SELECT a, b FROM t
```

:::
::::

:::{dropdown} Style Note: Method Notation
:icon: info

**Style Note:** In this guide...
:::

::::{grid} 2
:::{grid-item-card} Transformation
`.select()`, `.filter()`, `.with_column()`
:::
:::{grid-item-card} Aggregation
`.aggregate()`, `.count()`
:::
::::

-->

``The example below creates a DataFrame using the `dataframe!`macro, casts a column, and inspects the resulting schema. To access the underlying Arrow Schema, use`.inner()` (returns [`&SchemaRef`) or `.as_arrow()`(returns`&Schema`).

```{code-block} rust
:caption: Creating a DataFrame and mutating the schema
:emphasize-lines: 14,15,16,17,20

use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "user_id"    => [1_i64],
        "email"      => [Some("alice@example.com")],
        "created_at" => [1735689600_i64],
        "active"     => [true]
    )?;

    // Cast created_at from Int64 to Timestamp
    let df = df.with_column(
        "created_at",
        cast(col("created_at"), DataType::Timestamp(TimeUnit::Second, None))
    )?;

    // Print the schema structure
    println!("{:#?}", df.schema().inner());

    Ok(())
}
```

Output — each field shows its four properties (name, data_type, nullable, metadata):

```{code-block} text
:caption: Standard Output (stdout)
:emphasize-lines: 5

Schema {
    fields: [
        Field { name: "user_id", data_type: Int64, nullable: true, metadata: {} },
        Field { name: "email", data_type: Utf8, nullable: true, metadata: {} },
        Field { name: "created_at", data_type: Timestamp(Second, None), nullable: true, metadata: {} },
        Field { name: "active", data_type: Boolean, nullable: true, metadata: {} },
    ],
    metadata: {},
}
```
