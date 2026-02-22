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

## Reshaping Data

**Reshaping transforms the structure of your data—changing rows to columns or columns to rows without altering the underlying values.** <br>

Two common reshaping patterns exist in data processing:

| Operation          | What it does                              | DataFrame support        |
| ------------------ | ----------------------------------------- | ------------------------ |
| **Explode/Unnest** | Expands array elements into separate rows | ✅ [`.unnest_columns()`] |
| **Melt/Unpivot**   | Converts columns into rows (wide → long)  | ❌ Not available         |

Unnesting is essential when working with nested JSON data, multi-valued fields, or array columns from Parquet files. For melt/unpivot operations, see the workaround in [DataFrame-Unique Methods](#dataframe-unique-methods).

> **See also:** [pandas.DataFrame.explode], [PySpark explode] — similar operations in other DataFrame libraries.

### Unnesting / Exploding Arrays

Unnesting expands each element of an array column into a **separate row**, duplicating the other columns. This is essential when working with nested JSON, multi-valued fields, or array columns from Parquet files.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Input: customers with array of order IDs
    // +----------+------------+
    // | customer | orders     |
    // +----------+------------+
    // | Alice    | [1, 2, 3]  |
    // | Bob      | [4, 5]     |
    // +----------+------------+
    let df = ctx.sql("
        SELECT * FROM (VALUES
            ('Alice', ARRAY[1, 2, 3]),
            ('Bob', ARRAY[4, 5])
        ) AS t(customer, orders)
    ").await?;

    // Unnest expands array elements into separate rows
    let expanded = df.unnest_columns(&["orders"])?;

    expanded.show().await?;
    // Output: one row per array element
    // +----------+--------+
    // | customer | orders |
    // +----------+--------+
    // | Alice    | 1      |
    // | Alice    | 2      |
    // | Alice    | 3      |
    // | Bob      | 4      |
    // | Bob      | 5      |
    // +----------+--------+

    Ok(())
}
```

> **See also:** [PySpark explode] — similar operation in Spark DataFrames.

---
