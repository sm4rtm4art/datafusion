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

# Schema inference

<!--TODO

1. ABSTRACT
2. Expand content (currently light at ~91 lines)
3. Fix typo in validation cross-ref ("interfered" should be "inferred")
4. Incorporate schema drift discussion more prominently (schema-concepts.md references this file for drift details)
5. Drift mitigation strategies: expand beyond "use explicit schemas" — cover ListingTable's ability to cache file statistics for consistent schema resolution, enforcing specific file sort orders to ensure consistent inference across runs, and schema evolution patterns for production pipelines.

-->

```{contents} Table of Contents for Schema inference
:local:
:depth: 2
```

## Introduction (placeholder)

**Schema inference derives column names and types from data samples—useful for exploration, but unreliable for production.**

When reading text formats (CSV, NDJSON) without an explicit schema, DataFusion samples the first N records to determine column structure. The sampling depth is controlled by [`schema_infer_max_records`] (default: 1,000). Fields not encountered within that window are excluded entirely—no new columns are added after inference completes.

### How inference Works

inference behavior varies by format. CSV uses **positional** alignment (column index determines mapping), while NDJSON uses **name-based** alignment (JSON keys map to fields by name).

| Aspect                  | CSV                                                                  | NDJSON                                            |
| :---------------------- | :------------------------------------------------------------------- | :------------------------------------------------ |
| **Field alignment**     | Positional (column index)                                            | Name-based (JSON key)                             |
| **Missing fields**      | Row-length mismatch errors by default                                | NULL if field exists in schema                    |
| **Short rows**          | Error; use [`.truncated_rows(true)`][`truncated_rows`] to fill NULLs | N/A (each line is a self-contained object)        |
| **Sampling window**     | First N records ([`schema_infer_max_records`])                       | First N records ([`schema_infer_max_records`])    |
| **Default sample size** | 1,000                                                                | 1,000                                             |
| **Type inference**      | Attempts numeric/boolean detection; falls back to `Utf8`             | Infers from JSON value types (`number`, `string`) |

> **Tip:** <br>
> For detailed format behavior with explicit schemas, see [Strategy 1: Text Formats](#strategy-text-formats).

If inference is necessary, increase the sample size to reduce the risk of missing columns or mistyped fields:

```rust
use datafusion::prelude::*;

fn main() {
    // CSV: increase from default 1,000 to 10,000 rows
    let csv_opts = CsvReadOptions::new()
        .schema_infer_max_records(10_000);
    # assert_eq!(csv_opts.schema_infer_max_records, 10_000);

    // NDJSON: same configuration pattern
    let json_opts = NdJsonReadOptions::default()
        .schema_infer_max_records(10_000);
    # assert_eq!(json_opts.schema_infer_max_records, 10_000);
}
```

### When to Use Explicit Schemas

| Scenario                                   | Recommendation                                        |
| :----------------------------------------- | :---------------------------------------------------- |
| Production pipelines                       | **Explicit** — prevents drift, ensures data quality   |
| Specific types needed (e.g., `Decimal128`) | **Explicit** — inference may choose `Float64`         |
| Multi-file reads with varying structure    | **Explicit** — guarantees consistency across files    |
| Interactive exploration / prototyping      | **Inference OK** — validate before relying on results |
| Single-file reads with uniform structure   | **Inference OK** — lower risk of missing fields       |

> **Warning:** <br>
> inference can drift as data evolves. A column that appears as `Int64` in the first 1,000 rows may contain floats later, causing runtime parse errors. Validate inferred schemas before deploying to production.

**See also:**<br>

- [Creating Schemas](#creating-schemas) for constructing explicit schemas.
- [Applying Schemas and Modeling Data](#applying-schemas-and-modeling-data) for format-specific configuration.
- [Validating Schemas](#validating-schemas) for checking interfered schemas before use.
