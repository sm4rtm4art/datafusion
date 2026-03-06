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
TODO(Docs): Fill out streaming.md (Creation Phase)

1. DEFINE UNBOUNDED DATAFRAMES:
   - Explain the difference between bounded data (Files, RecordBatches) and unbounded data (Kafka, Websockets, FIFO pipes).
   - Explain that a DataFrame created from an unbounded source is a "Streaming DataFrame".

2. HOW TO CREATE ONE:
   - Note that DataFusion doesn't have a built-in `.read_kafka()` method out of the box.
   - Explain that users must implement a custom `TableProvider` where `TableProvider::is_infinite()` returns `true`.
   - Briefly mention that operations on unbounded DataFrames (like `.join()` or `.aggregate()`) require special streaming operators (like Symmetric Hash Joins or Windowed Aggregations).

3. LINK TO EXECUTION:
   - Add a note: "Once you have created an unbounded DataFrame, you MUST use `.execute_stream()` to consume it. Calling `.collect()` on an infinite stream will cause an Out Of Memory (OOM) crash." (Link to the execution streaming doc).
-->

# Creating DataFrames with Streaming

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents} Table of Content
:local:
:depth: 2
```
