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

<!--
TODO(Docs): streaming-execution.md - Unbounded Data & Memory Efficiency

1. CONSOLIDATE STREAMING CONCEPTS:
   - Combine the previously empty "Creating DataFrames from Streams" document with the execution side of streaming.
   - Explain the concept: DataFusion processes data natively as streams of `RecordBatch`es.

2. MOVE CONTENT HERE (from the old Executing draft):
   - "Stream merged results: `.execute_stream()`" (Explain how it avoids application-side buffering and supports backpressure).
   - "Stream partitions in parallel: `.execute_stream_partitioned()`" (Explain how this returns `Vec<SendableRecordBatchStream>` for parallel consumers).

3. KEY TAKEAWAYS TO HIGHLIGHT:
   - Streaming is the safest default for large datasets to avoid OOM.
   - Explain that even with streaming, operators like Sort, Join, and Aggregate still require memory for intermediate state (mention spilling to disk via `DiskManager`).
-->

## Streaming Execution

**Streaming execution processes data natively as streams of `RecordBatch`es, avoiding application-side buffering and supporting backpressure.**

```{contents} Table of Contents Streaming Execution
:local:
:depth: 2
:caption: Writing Concepts

```
