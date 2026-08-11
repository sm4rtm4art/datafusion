# DataFrame API docs — preview branch (temporary)

> **Temporary file.** This `README.md` is for GitHub / fork preview only. It is
> not part of the Apache DataFusion documentation set and should be removed
> before any upstream PR that publishes these pages as final.

## What this branch is

`fork/docs/dataframe-api-finished` is an **active WIP / preview** snapshot of the
phased DataFrame API guide under
`docs/source/library-user-guide/dataframe-api/`.

It is **not** Apache `main`, and it is **not** a claim that every page here is
review-complete. Day-to-day editing happens on
`fork/docs/dataframe-api-ownership-rework`; this branch is refreshed from that
line when a readable portfolio snapshot is needed.

**Still actively worked** — expect TODOs, join leaves mid-finalize, and hub
pages that are not yet polished.

## Scope & timeline

| | |
| --- | --- |
| **Goal** | Finalize the phased DataFrame API documentation for opening stacked PRs |
| **Target** | August 2026 (rolling) |
| **Working branch** | `fork/docs/dataframe-api-ownership-rework` |



## What’s on this preview

Curated shipping-oriented leaves only (monoliths, revise plans, and unfinished
side pages are omitted from this snapshot).

| Area | Status on this branch |
| --- | --- |
| **Concepts/** | Present (shipping leaves; parked orphans omitted) |
| **Creating-DataFrames/** | Present (including `from-files/`) |
| **Schema-Management/** | Present |
| **Transformations/** | Core leaves + joins group (concepts → conditions → types → workflows → validation) |
| **Writing-DataFrames/** | Hub only on this preview — leaves still in progress on the working branch |

## How to read TODOs

Inline `TODO:` / `JOIN-TODO-*` markers mean work remains. Prefer the working
branch for editing; use this branch as a **readable progress snapshot**.
