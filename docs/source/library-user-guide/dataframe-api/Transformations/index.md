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

# Transformations with DataFrame API

**The "life" phase of the DataFrame lifecycle: build and refine a lazy query plan.**

Transformations are where you re-shape and analyze data: once you [create](../Creating-DataFrames/index.md) a DataFrame, you can filter, select, join, aggregate, sort, and enrich data by composing methods that build a [`LogicalPlan`]. In the [DataFrame lifecycle metaphor](../index.md#the-dataframe-lifecycle), this is the "life" phase—execution and persistence happen later (see [Writing & Executing](../Writing-DataFrames/index.md)). For the transformation mental models — laziness, expressions, joins, set operations — start with [Transformations Concepts](transformation-concepts.md).

```{toctree}
:maxdepth: 1
:caption: Transformations
:numbered:
:titlesonly:
transformation-concepts
selection
filtering
sorting-limiting
joins
set-operations
aggregations
window-functions
subqueries
hybrid-sql
dataframe-specifics
reshaping
data-quality
builder-patterns
```

**Reading path.** The transformation methods group into families by what they change — the five dimensions from [Transformations Concepts](transformation-concepts.md#what-transformations-can-change). Use this to jump to the page that covers each family in full:

| What changes   | Method family                         | Page                                                                                                      |
| :------------- | :------------------------------------ | :-------------------------------------------------------------------------------------------------------- |
| Schema         | projection, column creation, renaming | [Selection](selection.md)                                                                                 |
| Cardinality    | filtering, limiting, deduplication    | [Filtering](filtering.md), [Sorting & Limiting](sorting-limiting.md), [Set Operations](set-operations.md) |
| Ordering       | sorting                               | [Sorting & Limiting](sorting-limiting.md)                                                                 |
| Grain          | aggregation, windows                  | [Aggregations](aggregations.md), [Window Functions](window-functions.md)                                  |
| Frame boundary | joins, set operations                 | [Joins](joins.md), [Set Operations](set-operations.md)                                                    |

**The running dataset.** Every page in this section works the same two frames — `customer_df` (one row per customer) and `orders_df` (one row per order, linked by `customer_id`). Two gaps are intentional: Carol has no orders, and order 104 points at a customer that does not exist, so joins and data-quality checks have something to reveal. The definition and a worked pipeline live in [Transformations Concepts](transformation-concepts.md#from-concepts-to-methods); here is the shape you will keep seeing:

`customer_df`

```text
+-------------+-------+--------+-------------+
| customer_id | name  | region | signup_date |
+-------------+-------+--------+-------------+
| 1           | Alice | West   | 2023-01-15  |
| 2           | Bob   | East   | 2023-03-22  |
| 3           | Carol | West   | 2023-06-10  |
| 4           | Dave  | East   | 2023-09-01  |
+-------------+-------+--------+-------------+
```

`orders_df`

```text
+----------+-------------+---------+--------+----------+------------+
| order_id | customer_id | product | amount | quantity | order_date |
+----------+-------------+---------+--------+----------+------------+
| 101      | 1           | Widget  | 100    | 2        | 2024-01-05 |
| 102      | 1           | Gadget  | 200    | 1        | 2024-02-11 |
| 103      | 2           | Widget  | 150    | 3        | 2024-01-20 |
| 104      | 99          | Gizmo   | 300    | 1        | 2024-03-02 |
+----------+-------------+---------+--------+----------+------------+
```

<!-- Link references -->

[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
