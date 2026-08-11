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

# Join Types

**Predict a joined `DataFrame` from `JoinType`: which matched or unmatched rows survive and which input fields the result carries.**

Choose among inner, outer, semi, anti, and mark joins from the result shape your pipeline needs. Mirrored Left/Right examples show how each family changes preservation and payload, while match multiplicity explains why pair-producing joins can multiply rows but one-sided joins do not. Use [Join Validation](join-validation.md) when key coverage, `NULL` behavior, or unexpected cardinality needs diagnosis.

:::{admonition} New to Joins?
:class: seealso

This page assumes basic join familiarity. See [Join Concepts](join-concepts.md) for DataFusion matching, preservation, payload, and cardinality, or the [PostgreSQL joins tutorial][postgresql-join-tutorial] for a general SQL introduction.
:::

**Key Methods**

| Method                                                        | Purpose                                                                              |
| :------------------------------------------------------------ | :----------------------------------------------------------------------------------- |
| [`.join()`](join-conditions.md#join-named-equality-keys)      | Build a named-key join; pass a [`JoinType`][jointype] to select preservation         |
| [`.join_on()`](join-conditions.md#join_on-boolean-conditions) | Build a Boolean-condition join; pass a [`JoinType`][jointype] to select preservation |

(jointype-catalogue)=

## `JoinType` catalogue

| `JoinType`    | Result rows                                                         | Result fields                                                                                   |
| :------------ | :------------------------------------------------------------------ | :---------------------------------------------------------------------------------------------- |
| [`Inner`]     | Matching pairs only; unmatched rows from either side are discarded. | Left and right fields.                                                                          |
| [`Left`]      | Matching pairs plus every unmatched left row.                       | Left and right fields; right fields are `NULL` for unmatched left rows and planned as nullable. |
| [`Right`]     | Matching pairs plus every unmatched right row.                      | Left and right fields; left fields are `NULL` for unmatched right rows and planned as nullable. |
| [`Full`]      | Matching pairs plus every unmatched row from both sides.            | Left and right fields; fields from the absent side are `NULL` and planned as nullable.          |
| [`LeftSemi`]  | Left rows that have a match.                                        | Left fields only.                                                                               |
| [`RightSemi`] | Right rows that have a match.                                       | Right fields only.                                                                              |
| [`LeftAnti`]  | Left rows that do not have a match.                                 | Left fields only.                                                                               |
| [`RightAnti`] | Right rows that do not have a match.                                | Right fields only.                                                                              |
| [`LeftMark`]  | Every left row.                                                     | Left fields plus a non-null Boolean `mark` field.                                               |
| [`RightMark`] | Every right row.                                                    | Right fields plus a non-null Boolean `mark` field.                                              |

Matching and condition construction live in [Join Conditions](join-conditions.md). The result model behind these variants lives in [Join Concepts](join-concepts.md).

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
- **Input roles:** Base DataFrame = method receiver/left input; Extension
  DataFrame = right argument/right input; `JoinType` controls preservation.

:::

```{contents} Table of Contents for Join Types
:local:
:depth: 2
```

## Choose `JoinType` by the Result You Need

**Choose `JoinType` from the rows and fields the result must retain: matching pairs, unmatched rows, one-sided existence results, or one side annotated with match status.**

A join condition decides which input-row pairs match. The [`JoinType`] argument then decides which matching and unmatched rows the result [`DataFrame`] emits and whether its planned schema carries fields from both inputs, one input, or one input plus a `mark` field.

Pair-producing types can emit several result rows for one input row when several matches exist. Semi, anti, and mark types emit at most one row per selected-side input-row occurrence. A missing match is not a failure: [`JoinType`] decides whether that row disappears, is null-extended, survives, or is annotated.

:::{admonition} Choose the API That Makes the Relationship Clear
:class: note

DataFrame join methods accept the public [`JoinType`] variants, including [`LeftMark`] and `RightMark`. SQL directly spells inner, outer, semi, and anti joins and also offers [`USING`], [`NATURAL JOIN`], [`CROSS JOIN`], and [`LATERAL`]; these are SQL forms or condition syntax, not additional [`JoinType`]‚ values. Use SQL when one of these forms states the relationship more clearly and DataFrame methods when programmatic Rust composition is clearer. Both APIs enter the same planning and execution pipeline; see [Join Conditions](join-conditions.md#when-the-dataframe-api-has-no-dedicated-join-form) for the construction boundary.
:::

---

## Keep Only Matching Pairs with `Inner`

**`Inner` emits one result row for every matching input-row pair, carries fields from both inputs, and omits rows with no match.**

Use `Inner` when matched payload from both inputs is required but unmatched rows have no place in the result.

The join condition identifies matching pairs, and `JoinType::Inner` excludes input rows that participate in no pair; the result schema carries fields from both inputs. Match multiplicity controls cardinality: one input row matching N rows contributes N result rows. In the fixture, Alice matches two orders, Bob one, Carol none, and order 104 none.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df [id, name] — Carol has no orders
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // orders_df [order_id, customer_id, amount] — order 104 has no customer
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // build lazy plan
    let matched_df = customers_df.join(
        orders_df,
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None,
    )?;

    // execute
    let batches = matched_df.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+----------+-------------+--------+",
            "| id | name  | order_id | customer_id | amount |",
            "+----+-------+----------+-------------+--------+",
            "| 1  | Alice | 101      | 1           | 100    |",
            "| 1  | Alice | 102      | 1           | 200    |",
            "| 2  | Bob   | 103      | 2           | 150    |",
            "+----+-------+----------+-------------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

Dropped rows leave no marker in an `Inner` result, so an apparently valid result can hide incomplete key coverage. Empty or unexpectedly large results usually indicate condition or data-cardinality issues rather than an `Inner` execution failure. Use [Join Validation](join-validation.md) for key coverage, `NULL` handling, and duplicate keys; choose semi joins when only one input's fields are needed, and outer joins when unmatched rows must remain visible.

---

## Preserve Unmatched Rows with Outer Joins

**Outer joins preserve unmatched rows from one or both inputs by emitting `NULL` for fields belonging to the missing match.**

Outer joins extend matching-pair behavior with unmatched-row preservation. Matching pairs still emit one row per pair and may multiply.

The selected variant determines which unmatched input rows survive and which opposite-side fields are null-extended. Use outer joins when input coverage matters: every customer, every order, or rows from both sources must remain visible without a match.

### Preserve One Input with `Left` and `Right`

**`Left` preserves every left row and `Right` preserves every right row; each null-extends fields from the side without a match.**

In method-call terms, the receiver/base `DataFrame` is left and the argument/extension `DataFrame` is right. `Left` preserves the receiver and `Right` preserves the argument; both carry fields from both inputs. Use either when one input must remain complete while matching payload is attached. Swapping inputs mirrors row-preservation logic but changes call-side roles and result-field order.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // build lazy plans
    let left_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Left,
        &["id"],
        &["customer_id"],
        None,
    )?;
    let right_result = customers_df.join(
        orders_df,
        JoinType::Right,
        &["id"],
        &["customer_id"],
        None,
    )?;

    // execute
    let left_batches = left_result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+----------+-------------+--------+",
            "| id | name  | order_id | customer_id | amount |",
            "+----+-------+----------+-------------+--------+",
            "| 1  | Alice | 101      | 1           | 100    |",
            "| 1  | Alice | 102      | 1           | 200    |",
            "| 2  | Bob   | 103      | 2           | 150    |",
            "| 3  | Carol |          |             |        |",
            "+----+-------+----------+-------------+--------+",
        ],
        &left_batches
    );
    let right_batches = right_result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+----------+-------------+--------+",
            "| id | name  | order_id | customer_id | amount |",
            "+----+-------+----------+-------------+--------+",
            "| 1  | Alice | 101      | 1           | 100    |",
            "| 1  | Alice | 102      | 1           | 200    |",
            "| 2  | Bob   | 103      | 2           | 150    |",
            "|    |       | 104      | 99          | 300    |",
            "+----+-------+----------+-------------+--------+",
        ],
        &right_batches
    );

    Ok(())
}
```

Duplicate matches can still multiply preserved rows, and a post-join filter on null-extended fields can remove them. See [Join Conditions](join-conditions.md) for predicate placement and [Join Validation](join-validation.md) for coverage and multiplication.

### Preserve Unmatched Rows from Both Inputs with `Full`

**`Full` preserves unmatched rows from both inputs while still emitting one result row for every matching pair.**

`Full` carries fields from both inputs; because either side may be absent, all result fields are planned as nullable. Use it when unmatched rows from both sources must remain visible, such as key-coverage or reconciliation workflows.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // build lazy plan
    let full_result = customers_df.join(
        orders_df,
        JoinType::Full,
        &["id"],
        &["customer_id"],
        None,
    )?;

    // execute
    let batches = full_result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+----------+-------------+--------+",
            "| id | name  | order_id | customer_id | amount |",
            "+----+-------+----------+-------------+--------+",
            "| 1  | Alice | 101      | 1           | 100    |",
            "| 1  | Alice | 102      | 1           | 200    |",
            "| 2  | Bob   | 103      | 2           | 150    |",
            "| 3  | Carol |          |             |        |",
            "|    |       | 104      | 99          | 300    |",
            "+----+-------+----------+-------------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

Matching duplicates can still multiply, and `Full` exposes unmatched rows but does not compare non-key values. Use [Join Validation](join-validation.md) to check unmatched keys and reconciliation results.

---

## Return or Annotate One Input by Match Existence

**Existence joins avoid pair multiplication: semi and anti joins select rows from one input, while mark joins retain every row from that input and append match status.**

Variants prefixed `Left` select the receiver/base input, and variants prefixed `Right` select the argument/extension input. Semi retains selected-side rows with at least one match, anti retains selected-side rows without a match, and mark retains all selected-side rows with match status. The tested input contributes no payload fields, multiple matches do not multiply outputs, and duplicate row occurrences on the selected side remain separate occurrences.

Use an inner or outer join when fields from both inputs are required. DataFusion SQL directly names `LEFT SEMI`, `RIGHT SEMI`, `LEFT ANTI`, and `RIGHT ANTI` joins; mark joins have no direct `JOIN` spelling, though SQL planning can introduce them for `EXISTS` decorrelation.

### Return Rows with Matches Using `LeftSemi` and `RightSemi`

**`LeftSemi` returns left rows with at least one match and `RightSemi` returns right rows with at least one match; each emits only the selected side's fields.**

Semi joins provide relationship-based filtering without tested-side payload. They emit at most one result per selected input-row occurrence regardless of match multiplicity, and omit a selected row when no match exists.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // build lazy plans
    let left_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::LeftSemi,
        &["id"],
        &["customer_id"],
        None,
    )?;
    let right_result = customers_df.join(
        orders_df,
        JoinType::RightSemi,
        &["id"],
        &["customer_id"],
        None,
    )?;

    // execute
    let left_batches = left_result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  | Bob   |",
            "+----+-------+",
        ],
        &left_batches
    );
    let right_batches = right_result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+-------------+--------+",
            "| order_id | customer_id | amount |",
            "+----------+-------------+--------+",
            "| 101      | 1           | 100    |",
            "| 102      | 1           | 200    |",
            "| 103      | 2           | 150    |",
            "+----------+-------------+--------+",
        ],
        &right_batches
    );

    Ok(())
}
```

Unexpected omissions route to [Join Validation](join-validation.md) for condition, key, and `NULL` checks. Use `Inner` if fields from both inputs are needed.

### Return Rows Without Matches Using `LeftAnti` and `RightAnti`

**`LeftAnti` returns unmatched left rows and `RightAnti` returns unmatched right rows; each emits only the selected side's fields.**

Anti joins find selected-side rows lacking a relationship. Matches are exclusion tests only and add no payload, while a no-match retains the selected row.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // build lazy plans
    let left_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::LeftAnti,
        &["id"],
        &["customer_id"],
        None,
    )?;
    let right_result = customers_df.join(
        orders_df,
        JoinType::RightAnti,
        &["id"],
        &["customer_id"],
        None,
    )?;

    // execute
    let left_batches = left_result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 3  | Carol |",
            "+----+-------+",
        ],
        &left_batches
    );
    let right_batches = right_result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+-------------+--------+",
            "| order_id | customer_id | amount |",
            "+----------+-------------+--------+",
            "| 104      | 99          | 300    |",
            "+----------+-------------+--------+",
        ],
        &right_batches
    );

    Ok(())
}
```

An outer-join-plus-`NULL`-filter pipeline is not universally interchangeable when the tested indicator may itself be nullable. Route predicate and `NULL` diagnosis to [Join Conditions](join-conditions.md) and [Join Validation](join-validation.md).

### Annotate Match Existence: `LeftMark` and `RightMark`

**`LeftMark` and `RightMark` retain every selected-side row and append `mark = true` when at least one match exists, otherwise `false`.**

`LeftMark` returns left fields plus `mark`, and `RightMark` returns right fields plus `mark`. The tested side contributes no fields, and match multiplicity does not multiply selected rows. The current DataFusion mark is non-null and two-valued (`true`/`false`, never `NULL`).

Mark joins are public specialist variants for retaining every selected-side row while carrying existence status; their source-documented role is `EXISTS` decorrelation in disjunctive predicates.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // build lazy plans
    let left_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::LeftMark,
        &["id"],
        &["customer_id"],
        None,
    )?;
    let right_result = customers_df.join(
        orders_df,
        JoinType::RightMark,
        &["id"],
        &["customer_id"],
        None,
    )?;

    // execute
    let left_batches = left_result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+-------+",
            "| id | name  | mark  |",
            "+----+-------+-------+",
            "| 1  | Alice | true  |",
            "| 2  | Bob   | true  |",
            "| 3  | Carol | false |",
            "+----+-------+-------+",
        ],
        &left_batches
    );
    let right_batches = right_result.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+-------------+--------+-------+",
            "| order_id | customer_id | amount | mark  |",
            "+----------+-------------+--------+-------+",
            "| 101      | 1           | 100    | true  |",
            "| 102      | 1           | 200    | true  |",
            "| 103      | 2           | 150    | true  |",
            "| 104      | 99          | 300    | false |",
            "+----------+-------------+--------+-------+",
        ],
        &right_batches
    );

    Ok(())
}
```

Use semi or anti joins when rows should be selected rather than annotated. Repeated mark joins can create repeated `mark` field names; route renaming and projection to [Join Workflows](join-workflows.md).

---

## Conclusion

After a join condition identifies matching pairs, `JoinType` determines which matches and nonmatches survive and whether the result carries fields from both inputs, one input, or one input plus `mark`. Use `Inner` for matched two-sided payload, outer variants for unmatched-row preservation, semi and anti for one-sided existence selection, and mark for annotation; hand off key coverage, `NULL` behavior, and unexpected multiplication to [Join Validation](join-validation.md) before treating the result as complete.

### Further Reading

- [Join Concepts](join-concepts.md) — Understand matching, preservation, payload, and cardinality.
- [Join Conditions](join-conditions.md) — Construct named-key and Boolean-condition joins.
- [Join Workflows](join-workflows.md) — Compose joins in larger `DataFrame` pipelines.
- [Join Validation](join-validation.md) — Diagnose key coverage, `NULL` behavior, and multiplication.
- [DataFusion SQL JOIN reference](../../../../user-guide/sql/select.md#join-clause) — Use SQL join syntax.
- [PostgreSQL joins tutorial][postgresql-join-tutorial] — Review a general SQL introduction.

[`full`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.Full
[`inner`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.Inner
[`left`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.Left
[`leftanti`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.LeftAnti
[`leftmark`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.LeftMark
[`leftsemi`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.LeftSemi
[`right`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.Right
[`rightanti`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.RightAnti
[`rightmark`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.RightMark
[`rightsemi`]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html#variant.RightSemi
[jointype]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html
[postgresql-join-tutorial]: https://www.postgresql.org/docs/current/tutorial-join.html
