<!--
WORKING OVERSIGHT — not a Sphinx page. Do not add to any {toctree}.
Author + coordinator status board for finishing the DataFrame API docs.
Updated: 2026-08-05
-->

# DataFrame API docs — finalize oversight

**Floor:** P0-narrow (mechanical finish) is an acceptable PR outcome.  
**Ceiling:** Full P0 → P1 (claim ledger) → P2 polish across shipping leaves.  
**Calendar:** Flexible; ramp and measure. Early rate ≠ 3–4 pages/day.

Governance Wave 1 + Wave 2 Minimal: **accepted** (2026-08-05). Live When-contract: `AGENTS.md` in this directory.

---

## Human workflow (Author cheat sheet)

Live names in `AGENTS.md`: **Plan → Draft → Polish → `page_final`**.  
Legacy Stages 1–6 map 1–2→Plan, 3–4→Draft, 5–6→Polish. Same rhythm you want:

| Your phase         | Live name                     | Ask (discuss)                                                     | Agent (implement)                                                                        | Stop                                 |
| ------------------ | ----------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------ |
| **1 Framing**      | **Plan**                      | Role, Diataxis, arc, ownership, heading tree, neighbor boundaries | Heading moves/merges/renames; handoffs; nav/TODO/move markers only — no paragraph polish | After each half                      |
| **2 Iteration**    | **Draft**                     | One H2 subtree: content, accuracy, overlap                        | Write that H2 only; openings/conclusion untouched                                        | Loop H2→H2                           |
| **3 Finalization** | **Polish** → **`page_final`** | Blind spots, consistency, abstract/conclusion plan                | Write openings/closings; run named gates                                                 | Independent judge (strong preferred) |

**Stagnation:** if an iteration adds no evidence or decision, return 2–3 smallest alternatives (including keep-as-is) to Author.  
**Judge:** after Polish gates — evidence, not a substitute for Plan. A premature “final” REVISE is an issue inventory for later Draft/Polish, not a license to skip Plan.

---

## Playbook (locked: P0-narrow)

| Phase              | In scope                                                                                                                                                                                               | Out of scope                              |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------- |
| **P0 mechanical**  | Trailing `---`; `## Conclusion` + optional `### Further Reading`; link defs; unclosed admonitions; `TODO:` / draft scaffolds; missing `{contents}` / forbidden `## Introduction`; `lib.rs` path casing | Gate A truth rewrites; storyline redesign |
| **P0.5 ownership** | Author-approved spine drops / one owner per topic — Concepts: drop `summary.md` + `bigger-picture.md` from spine (see board)                                                                           | Mixing into a sed pass                    |
| **Claim ledger**   | Short family rows (banned absolute → allowed framing → owner page) before first P1                                                                                                                     | A second documentation corpus             |
| **P1 truth**       | BLOCKER/MAJOR vs ledger + leaf verdicts; real oracles                                                                                                                                                  | Pages that still fail parse / markers     |
| **P2 polish**      | NITs, imports, admonition class, proofreading                                                                                                                                                          | Reopening ownership                       |

**Rule:** Do not truth-revise a leaf that still fails parse or still carries draft scaffolds.

---

## Parallel tracks

| Track         | Work                                                                                                      | Owner attention              |
| ------------- | --------------------------------------------------------------------------------------------------------- | ---------------------------- |
| **T-content** | Joins → remaining Transformations → Writing                                                               | Primary                      |
| **T-P0**      | Mechanical P0 by section + Concepts spine drops (`summary` / `bigger-picture`). `lib.rs` casing **done**. | Secondary instance OK        |
| **T-ledger**  | Fill claim ledger once before first P1                                                                    | Before Creating/Schema truth |

Do not pause joins for a full-corpus P0. Do not run P1 on Creating while joins are mid-flight unless the ledger exists and the leaf is parse-clean.

---

## Claim ledger (stub — fill before P1)

| Family              | Banned absolute (examples)                                             | Allowed framing                                                     | Owner page (TBD)            |
| ------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------- | --------------------------- |
| Performance         | fastest / identical perf / orders of magnitude / zero-copy (unsourced) | Mechanisms only; no ranking                                         | —                           |
| State freeze        | frozen catalog / immutable SessionState / deterministic                | Cloned maps + shared catalog/runtime + embedded providers           | `sessioncontext`            |
| Clone cost          | virtually free / Arc only                                              | Clones plan + boxed state; shares selected Arcs; no table-data copy | `sessioncontext` / Creating |
| Streaming / memory  | `.collect` streams; stream ⇒ bounded RAM                               | Result buffering vs operator state; spill conditional               | lifecycle / Writing         |
| Registration        | caches I/O / avoids rescans                                            | Naming + provider reuse; scans still happen                         | Creating                    |
| Plan identity       | same optimized plan / interchangeable                                  | Same IR + shared pipeline; not universal plan identity              | Concepts                    |
| Schema contracts    | lossless coerce; metadata always preserved; nullability = validation   | Operation-specific                                                  | Schema                      |
| Capability matrices | always / never / all providers                                         | Implementation- and format-dependent                                | From-files / Schema         |

---

## Section revise plans (detail backlog)

This file is the **cleaned status board**. Per-section judge packs and leaf rows live in:

| Section         | Plan file                                               |
| --------------- | ------------------------------------------------------- |
| Concepts        | `Concepts/ConceptsRevisePlan.md`                        |
| Creating        | `Creating-DataFrames/CreationRevisedplan.md`            |
| From-files      | `Creating-DataFrames/from-files/FromFilesRevisePlan.md` |
| Schema          | `Schema-Management/SchemaManagementRevisedplan.md`      |
| Transformations | `Transformations/TransfromationRevisedplan.md`          |

Do not duplicate full verdict text here — link and tick only. Writing has no revise plan yet.

---

## Section board

| Section         | Dominant debt                 | P0                              | P0.5                 | Ledger/P1         | Notes                        |
| --------------- | ----------------------------- | ------------------------------- | -------------------- | ----------------- | ---------------------------- |
| Concepts        | Structure + ownership + truth | open                            | **spine drops open** | open              | See revise plan; items below |
| Creating        | Registration myth + finish    | open (`lib.rs` casing **done**) | —                    | blocked on ledger | Uniform REVISE               |
| From-files      | Perf + ignore/oracle          | open (`lib.rs` casing **done**) | —                    | blocked on ledger | Same as Creating             |
| Schema          | Overstated contracts          | open (`lib.rs` casing **done**) | —                    | blocked on ledger | Uniform REVISE               |
| Transformations | Finish/markers; joins content | partial                         | joins Plan in flight | joins first       | Healthiest; several ACCEPT   |
| Writing         | Later                         | —                               | —                    | —                 | After Transformations        |

### Concepts P0.5 — spine drops (do not forget)

Author-locked; still in `Concepts/index.md` toctree + routing table:

| Page                         | Action              | Absorb / park                                                                                                                             |
| ---------------------------- | ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `Concepts/summary.md`        | **Drop from spine** | Glance + Birth/Life/Death exit → `Concepts/index.md`; no dedicated Summary leaf                                                           |
| `Concepts/bigger-picture.md` | **Drop from spine** | Optional LLVM blurb → `architectural-dataframe` or drop; roadmap → contributor guide only; do not keep Volcano/SIMD/perf as Concepts path |

Files may remain on disk parked; stop linking them as the section path. Full diagnosis: `Concepts/ConceptsRevisePlan.md` header §§1–4.

---

## Active focus

**Now:** Joins **Plan reopen** — section story arc + `join-concepts.md` purpose/fit (not Draft tighten).

Author intent (2026-08-05): concept page = DF-API / engine mental model for joins, not a general join tutorial; reduce AI-slop without hollow thinning; abstract stays deferred; first H2 = situation → DataFusion → DataFrame joins (name `.join()` / `.join_on()`), then columns, then rows.

**Paused:** Opus tighten packet (N1–N3 / B1) until Plan fit is re-approved.

**Next after Plan accept:** Draft one H2 subtree at a time on `join-concepts.md`.

**Parallel OK:** mechanical P0 on Creating closings (separate instance; no Gate A).

### Cross-cutting P0 tickets

| Ticket                                                                                                | Status                    |
| ----------------------------------------------------------------------------------------------------- | ------------------------- |
| `lib.rs` doctest path casing (`Creating-DataFrames/`, `Schema-Management/`)                           | **Done** 2026-08-05       |
| Concepts spine: drop `summary` + `bigger-picture` from toctree/routing (files may stay parked)        | Open — next easy          |
| Park/delete `Transformations/joins/WIP-join-concepts.md` + `Transformations/joins_old.md` before ship | Open — easy if not linked |
| Writing revise plan (create when Transformations settle)                                              | Deferred                  |

---

## Defect families (index)

1. Finish / parse / stencil (mechanical P0)
2. Doctest infrastructure (casing, ignore, oracle theater)
3. Overclaim / wrong mental model (P1 + ledger)
4. Ownership / altitude / story-arc (P0.5)
5. API / diagram falsehoods (Gate A when touching that leaf)

---

## Model routing (Author load)

| Seat        | Model               | Job                                   |
| ----------- | ------------------- | ------------------------------------- |
| Coordinator | Grok / thin replies | Status, one decision, handoffs        |
| Drafter     | Opus 5 (bounded)    | One packet / one H2 or named fix list |
| Researcher  | GPT terra           | One claim                             |
| Judge       | GPT 5.6 sol         | One review packet after gates         |

Opus tripwire: ignores response budget, invents claims, or Author fatigue → switch model for Draft.

---

## Done definitions

| Outcome          | Meaning                                                                                                                         |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **P0-narrow PR** | Shipping leaves parse-clean; closings/stencil OK; casing fixed; no draft scaffolds on “final” targets; known P1 listed honestly |
| **Full revise**  | Above + ledger applied + MAJOR truth fixed + Writing in shape                                                                   |

Never ship unresolved on-page `TODO:` / `JOIN-TODO` / DRAFT scaffolds on pages claimed `page_final`.
