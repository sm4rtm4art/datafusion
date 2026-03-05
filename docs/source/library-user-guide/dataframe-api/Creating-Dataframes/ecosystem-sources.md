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

# extension-links

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents} Table of Contents for Extension Links
:maxdepth: 2
:caption: Creation Methods
```

# Ecosystem Data Sources

**DataFusion connects to lakehouse formats, cloud storage, and specialized
systems through community-maintained TableProvider implementations.**

[Brief: the TableProvider trait is the extension point. These crates
implement it for their format. Register their provider → use DataFrame
or SQL as usual.]

| Source         | Crate          | What it provides        |
| -------------- | -------------- | ----------------------- |
| Delta Lake     | `delta-rs`     | Read/write Delta tables |
| Apache Iceberg | `iceberg-rust` | Iceberg table format    |
| Lance          | `lance`        | ML-optimized columnar   |
| ...            | ...            | ...                     |

For implementing your own, see [Custom Table Providers](../custom-table-providers.md).

<!--
Real-world DataFusion users work with Delta Lake, Iceberg, Lance, object stores, etc. These are legitimate "creation pathways" that happen to live outside core DataFusion. Acknowledging them prevents users from thinking DataFusion only reads raw files.
-->
