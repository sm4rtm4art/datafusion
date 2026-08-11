<!--
WORKING OVERSIGHT — not a Sphinx page. Do not add to any {toctree}.
Author + coordinator status board for finishing the DataFrame API docs.
Updated: 2026-08-11
-->

# DataFrame API docs — finalize oversight

**Floor:** P0-narrow (mechanical finish) is an acceptable PR outcome.  
**Ceiling:** Full P0 → P1 (claim ledger) → P2 polish across shipping leaves.  
**Calendar:** Flexible; ramp and measure. Early rate ≠ 3–4 pages/day.

Governance Wave 1 + Wave 2 Minimal: **accepted** (2026-08-05). Live When-contract: `AGENTS.md` in this directory.

### Today (rewrite each morning — ≤3 bullets)

1. **Parallel / low load:** G0 **Schema-Management** (Creating closed 2026-08-11).  
2. **Main energy:** `join-validation.md` Plan → Draft (keep separate from workflows).  
3. **If capacity:** Polish/judge `join-workflows.md`; optional `join-concepts` B1; git hygiene before upstream/CV snapshot.

### Backlog map (do not invent a second board)

| Layer | Where | Job |
| ----- | ----- | --- |
| **Board** | this file | Active focus, G0/G1, git hygiene, spine drops, parallel seats |
| **Section backlog** | `*RevisedPlan.md` / `*RevisePlan.md` | Leaf verdicts, B/N findings |
| **Joins register** | `Transformations/joins/index.md` JOIN-TODO | Joins-only IDs |
| **Session tasks** | Cursor todos | Ephemeral — never replace this board |

**Rule:** cross-cutting work → one row here; leaf truth → section revise plan; do not duplicate full verdict text on the board.

---

## Mechanical finish gates — G0 Links → G1 Closing (locked 2026-08-09)

Corpus mechanical finish seat (not joins Draft, not G2 openings, not claim ledger / P1).  
**Sequence:** **G0 → G1, then stop** unless Author extends. Joins content track stays untouched from this seat.

| Gate | In scope | Out of scope |
| ---- | -------- | ------------ |
| **G0 Links** | Link warehouse unification (`markdown.mdc` §6.1–6.2); strip body/FR inline `](https://…)`; fix file remaps + fixable docs.rs 404s; lychee after refs stable; selective link-promotion on leaves | Storyline / Gate A truth; full Sphinx WARN sweep; inventing cites from monoliths |
| **G1 Closing stencil** | Shipping leaves: `## Conclusion` (+ optional `### Further Reading`); bottom warehouse; trailing `---` before defs; rival H2 cleanup for closings | Highlight / abstract / openings (G2); ownership essays |
| **Stop** | After G0→G1 | Claim ledger / P1; joins Draft; Writing revise plan |

**Parked files this pass:** **remap-only, leave on disk** (`summary.md`, `bigger-picture.md`, `joins_old.md`, monoliths). Delete later when that WIP arrives. `WIP-join-concepts.md` already deleted.

### G0 URL warehouse contract (Author-locked)

- Body / tables / Further Reading: **no** raw `[text](https://…)`.
- URLs live in EOF warehouse (not a heading):

```markdown
---
<!-- References -->
<!-- Internal documentation -->
<!-- Core types -->
<!-- Methods and functions -->
<!-- External resources -->
```

- Categories always present (omit empty); **A–Z within each**; **used-only**; no `## References` H2; no decorative banners; no shared Sphinx refs file.
- Stable labels: `` [`.join()`] ``, `` [`JoinType`] `` (PascalCase types); kebab for pages/essays.
- Mid-prose internal `.md` **may** stay inline; **Further Reading (all links) → warehouse**.
- EOF warehouse OK even if page lacks proper Conclusion (**G1 deferred**).
- Internal targets = **page files**; prefer `…/index.md` hubs; **no `#` section anchors** on internal docs. docs.rs `#method.…` anchors stay.
- `` [`batch_size`] `` / `` [`target_partitions`] `` → `configs.md` **page**, under **`<!-- Internal documentation -->`**.

### G0 link-promotion policy (Author agreed)

Plain `` `Expr` `` / `` `.filter()` `` after monolith→leaf split often never became citations. Fix from **leaf only**:

- Tables / first definitional mention → warehouse link.
- Dense paragraphs → **one link per type**, rest plain (avoid spam).
- Used-only defs; don’t invent from monolith.

### G0 findings (2026-08-09 → 2026-08-10)

**Lychee ops**

- Default `lychee.toml` excludes `https://` — CI file-link check never sees docs.rs. G0 needs a separate `--include '^https://'` pass.
- Glob `dataframe-api/*/*.md` misses nested dirs (`from-files/`, `joins/`); scope by section directory or recurse.

**Fixed (corpus, earlier G0 remaps)**

| Bucket | Examples |
| ------ | -------- |
| File remaps | `joins.md` → `joins/index.md`; Writing/Schema/Transform relative paths; from-files `format_options` / Schema paths |
| docs.rs 404 | `Expr` `is_null`/`is_not_null`; `DFSchema`; `JsonReadOptions`; `TableProvider` catalog path; `string_agg`; Csv/Avro/Arrow `*ReadOptions` |
| Other 404 | Term GitHub unlinked; LinkedIn “The Log” unlinked; DataFusion ACM → PDF mirror; Parquet encryption URL; Kleppmann → dataintensive.net |

**Author-manual 403s (shipping — park until Author resolves)**

| Page / area | Host |
| ----------- | ---- |
| `best-practices` | Stackademic |
| `hybrid-sql` | ACM Spark |
| Parked `Concepts/summary.md` | ACM DOI `10.1145/3626246.3653368` (ignore while parked) |

**Concepts section — G0 warehouse status (2026-08-10)**

| Leaf | Warehouse | Notes |
| ---- | --------- | ----- |
| `execution-lifecycle.md` | **done** | writing → `Writing-DataFrames/index.md`; `batch_size`/`target_partitions` under Internal |
| `null-handling.md` | **done** | page-level internals; no `#` anatomy anchors |
| `expressions.md` | **done** + promo | Core labels PascalCase; FR de-URLed |
| `builder-parser.md` | **done** | compound `.filter(col…)` citation fixed; `` [`DFSchema`] `` |
| `sessioncontext.md` | **done** | FR→warehouse; creating-concepts **page** (no `#`) |
| `anatomy-dataframe.md` | **done** + promo | broken/missing defs repaired; FR de-URLed |
| `architectural-dataframe.md` | **done** + promo | SIGMOD → warehouse; still on toctree (P0.5 dissolve separate) |
| `Concepts/index.md` | **done** | overview table + first-mention types → warehouse |
| `summary.md` / `bigger-picture.md` | **parked** | remap-only; skip warehouse polish |

**Concepts lychee (Author, 2026-08-10):** `Concepts/*.md` with `--include '^https://'` → **224 total, 223 OK, 1 Error** — only parked `summary.md` ACM 403. Shipping Concepts leaves clean for externals.

**Creating-DataFrames / from-files — G0 warehouse (2026-08-10)**

| Leaf | Warehouse | Notes |
| ---- | --------- | ----- |
| `from-files/index.md` | **done** | format table + S3 example → warehouse; no `#` |
| `from-files/parquet.md` | **done** | FR blogs/API de-URLed; cloud-storage → page-level index |
| `from-files/csv.md` | **done** | FR + Schema Management; format_options page-level |
| `from-files/json.md` | **done** | fixed broken `../../../../../user-guide/...` path; csv cross-ref page-level |
| `from-files/avro.md` | **done** | Avro spec + options API → warehouse |
| `from-files/arrow-ipc.md` | **done** | Arrow IPC/Flight + streaming neighbor |
| `creating-concepts.md` | **done** | fixed mid-line `` [`DataFrame`]: `` ref-def trap; wildcards unlinked |
| `index.md` | **done** | overview table → warehouse |
| `from-memory.md` / `from-sql.md` / `inline-data.md` | **done** | FR de-URLed; `#` stripped |
| `registered-tables.md` | **done** | TableProvider → catalog path; S3/externals warehoused |
| `streaming.md` | **done** | ecosystem + API FR → warehouse |
| `from-logical-plan.md` | **done** | + `` [`TreeNodeRewriter`] `` |
| `ecosystem-sources.md` | **done** | lakehouse/Ballista/Comet + contrib → warehouse |

**Creating residual G0 (2026-08-11)** — post-warehouse polish on top-level leaves (from-files already clean):

| Change | Leaves |
| ------ | ------ |
| Rival `## References` → `## Further Reading` (G1 still parks Conclusion stencil) | `ecosystem-sources`, `from-logical-plan`, `from-memory`, `from-sql`, `inline-data`, `streaming`, `registered-tables` |
| Collapse double/triple `---` before EOF warehouse | same seven + `creating-concepts` + `from-files/index` |
| Fix broken FR cite `[writing-dataframes][writing-dataframes]` | `ecosystem-sources.md` |
| Dedupe warehouse `` [`dataframe::new()`] `` | `from-logical-plan.md` |
| Promote `` [`MemTable::try_new()`] `` (+ avoid mid-line `` [`…`]: `` ref-def trap) | `from-memory.md` |
| Promote closing [`StreamTable`] / [`PartitionStream`] / [`StreamingTable`] / [`TableProvider`] | `streaming.md` |

**Creating https lychee (2026-08-11):** recurse `Creating-DataFrames/**/*.md` + top-level with `--include '^https://'` → **353 Total, 353 OK, 0 Errors**.

**Schema-Management — G0 warehouse (2026-08-11)**

| Leaf | Warehouse | Notes |
| ---- | --------- | ----- |
| `schema-anatomy.md` | **done** | was 0 defs / 35 bare cites; full warehouse; `#` stripped; neighbors warehoused; promo pass 2026-08-11 |
| `schema-inspection.md` | **done** | reformatted to G0 categories; deduped; nested-bracket `` Schema::new `` cite fixed |
| `schema-creation.md` | **done** | constructor cites → short labels; `#` stripped; issue #17715 warehoused |
| `schema-concepts.md` | **done** | FR de-URLed → warehouse; LogicalPlan → expr path; O’Reilly×5 → Wiley/Routledge (Blaha authorship fix) 2026-08-11 |
| `schema-application.md` | **done** | G0 warehouse; `#` stripped |
| `schema-transformation.md` | **done** | G0 warehouse; `#` stripped |
| `type-coercion.md` | **done** | G0 warehouse; `#` stripped |
| `schema-inference.md` | **done** | G0 warehouse |
| `schema-dataframe-methods.md` | **done** | G0 warehouse |
| `index.md` | **done** | G0 warehouse |

**Schema https lychee (2026-08-11):** `Schema-Management/*.md` → **370 Total, 365 OK, 5 Errors** — only `schema-concepts.md` O’Reilly ×5 (Author-manual). Prettier Schema shipping leaves — **PASS**.

**Schema FR books (2026-08-11):** `schema-concepts.md` O’Reilly×5 → Wiley (Silverston×3, Kimball) + Routledge (Blaha); authorship corrected Hay→Blaha. Leaf lychee https — **76 OK, 0 Errors**.


**Prettier vs label case (finding):** Prettier **2.7.1 lowercases** markdown link-reference definition labels (`` [`DataFrame`] `` → `` [`dataframe`] ``). Body cites may stay PascalCase; CommonMark matching is case-insensitive so links still resolve. Format gate requires the lowercased warehouse form. Documented for Author — optional later `prettier-ignore` if PascalCase defs are preferred on disk.

**Creating format gate (2026-08-11):** `prettier --write` then `--check` on residual Creating leaves — **PASS**.

**G1 status:** **parked** until Author reopens. Some Concepts pages still have `## Conclusions` / `### Futher Reading` typos + `<!-- TODO: Add conclusions -->` — leave for G1; warehouse already at true EOF. Creating `## Further Reading` H2s are interim — G1 nests as `### Further Reading` under `## Conclusion`.

**Gates after Concepts G0 (2026-08-10)**

| Gate | Command | Result |
| ---- | ------- | ------ |
| Format | `npx prettier@2.7.1 --check 'docs/.../Concepts/*.md'` | **PASS** — all matched files |
| Rust | `cargo test --doc -p datafusion dataframe_api_concepts` | **PASS** — 26 passed, 0 failed |
| Links | Author lychee `--include '^https://'` on `Concepts/*.md` | **PASS shipping** — 223 OK; 1 parked `summary.md` ACM 403 |

**Gates after Creating G0 residual (2026-08-11)**

| Gate | Command | Result |
| ---- | ------- | ------ |
| Format | `npx prettier@2.7.1 --check` on residual Creating leaves | **PASS** |
| Links | `lychee --include '^https://'` on `Creating-DataFrames/**/*.md` | **PASS** — 353 OK, 0 Errors |

---

## Git / fork hygiene (Author — before rebase or CV snapshot)

Live content branch: **`fork/docs/dataframe-api-ownership-rework`**.  
CV / portfolio preview: **`fork/docs/dataframe-api-finished`** (stale until refreshed — not the day-to-day line of work).

| Step | Action | Notes |
| ---- | ------ | ----- |
| 1 | WIP commit or stash on `ownership-rework` | **Required** before any upstream sync — tree is often dirty |
| 2 | `git fetch upstream` (+ `origin`) | Align with `apache/datafusion` |
| 3 | Rebase/merge **ownership-rework** onto `upstream/main` | Live docs branch |
| 4 | Refresh `dataframe-api-finished` from a clean ownership-rework snapshot + **temp README** (WIP / not upstream-ready) | Portfolio link only |
| 5 | Push both as needed | Prefer WIP commits over long uncommitted spans |

**Ban:** rebasing mid–joins Draft with a dirty tree; treating `dataframe-api-finished` as the content line of work.

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

| Track         | Work                                                                                                                                                                                                                      | Owner attention                                              |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| **T-content** | Deep work on `joins/join-concepts.md` (Plan/Draft); then rest of joins → Transforms → Writing                                                                                                                             | **Author + GPT Sol** — no Sphinx/P0 side quests; Opus parked |
| **T-Sphinx**  | **Section-scoped parse** — inventory whole tree once, but **fix blockers only for the active section(s)** (now: Transformations / `joins/`). Defer Creating/Schema/Writing WARN noise until those sections are in flight. | **Other Grok** — not Author/Opus                             |
| **T-P0**      | Mechanical fixes that fall out of the **active-section** Sphinx slice (+ Concepts orphan follow-ups if they block the build). `lib.rs` casing **done**.                                                                   | Same Sphinx seat                                             |
| **T-links**   | **G0 warehouse + https lychee** per section (see G0 plan above). CI file-link lychee still separate (`lychee.toml` excludes https).                                                                                        | Concepts + Creating **done**; next: Schema |
| **T-ledger**  | Fill claim ledger once before first P1                                                                                                                                                                                    | Before Creating/Schema truth                                 |

**Hygiene**

- Author + GPT: **documentation content only** (joins / active leaf). No Sphinx triage, no corpus link sweeps mid-Draft.
- Sphinx Grok: one full `dummy` inventory is fine for orientation; **resolve only blockers for the section under content work** (joins / Transformations now). Park the rest of the 17-file debt until that section’s turn. No storyline / Gate A rewrites.
- **lychee https / G0 warehouse** = section-scoped mechanical track (Concepts done); not on the critical path for join-concepts Draft.
- Do not dual-edit `join-concepts.md` from the Sphinx seat.
- Capture `/tmp/parse-before` before first edit of a Polish/`page_final` pass (`validation.md` §2.2).

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

This file is the **status board** (see **Backlog map** at top). Per-section judge packs and leaf rows live in:

| Section         | Plan file                                               |
| --------------- | ------------------------------------------------------- |
| Concepts        | `Concepts/ConceptsRevisePlan.md`                        |
| Creating        | `Creating-DataFrames/CreationRevisedplan.md`            |
| From-files      | `Creating-DataFrames/from-files/FromFilesRevisePlan.md` |
| Schema          | `Schema-Management/SchemaManagementRevisedplan.md`      |
| Transformations | `Transformations/TransfromationRevisedplan.md`          |
| Joins register  | `Transformations/joins/index.md` (JOIN-TODO)            |

Do not duplicate full verdict text here — link and tick only. Writing has no revise plan yet.

---

## Section board

| Section         | Dominant debt                 | P0                              | P0.5                          | Ledger/P1         | Notes                        |
| --------------- | ----------------------------- | ------------------------------- | ----------------------------- | ----------------- | ---------------------------- |
| Concepts        | Ownership + truth + G1 close  | **G0 warehouse done**           | **toctree done; absorb open** | open              | G1 parked; see G0 findings   |
| Creating        | Registration myth + finish    | **G0 warehouse + residual + lychee done** | —                             | blocked on ledger | Uniform REVISE               |
| From-files      | Perf + ignore/oracle          | **G0 done** (with Creating)     | —                             | blocked on ledger | Same as Creating             |
| Schema          | Overstated contracts          | **G0 warehouse done** (FR books → Wiley/Routledge) | —                             | blocked on ledger | Uniform REVISE               |
| Transformations | Finish/markers; joins content | partial                         | joins Plan in flight          | joins first       | Healthiest; several ACCEPT   |
| Writing         | Later                         | —                               | —                             | —                 | After Transformations        |

### Concepts P0.5 — spine drops (do not forget)

| Page                               | Action                         | Absorb / park                                                                                                                                                                                                 |
| ---------------------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Concepts/summary.md`              | **toctree+routing done**       | Glance + Birth/Life/Death exit → `Concepts/index.md`; no dedicated Summary leaf                                                                                                                             |
| `Concepts/bigger-picture.md`       | **toctree+routing done**       | Optional LLVM blurb → drop or short note on `sessioncontext` / contributor guide; roadmap → contributor guide only; do not keep Volcano/SIMD/perf as Concepts path                                            |
| `Concepts/architectural-dataframe.md` | **Author decision 2026-08-10** | **Do not merge into `anatomy-dataframe`** (fit ≠ internals; same rule as not stuffing product-fit into `schema-anatomy`). Fold engine fit / sources→context orientation into **`sessioncontext.md`** so SessionContext is the prominent early Concepts leaf; keep short OLAP/fit scent on `Concepts/index.md`. Then drop from toctree and park/delete like `summary` / `bigger-picture`. |

**Done (nav):** dropped `summary` + `bigger-picture` from `Concepts/index.md` `{toctree}` + overview table; removed shipping handoff from `execution-lifecycle.md` (was Bigger Picture sentence before `## References`). Those two files remain on disk parked.

**Open — architectural dissolve:** still on toctree today. Implement absorb into `sessioncontext.md` (+ index scent), then toctree drop + park/delete with the other Concepts orphans.

**Open follow-up — post-lifecycle context handoff:** `Concepts/execution-lifecycle.md` closing no longer points at a “broader context” leaf. Resolve when content lands (sessioncontext fit blurb, Birth/Life/Death exit on `Concepts/index.md`, or short handoff). Parked files `bigger-picture.md` / `summary.md` / (soon) `architectural-dataframe.md` still need Sphinx/orphan cleanup or delete/relocate.

Full diagnosis: `Concepts/ConceptsRevisePlan.md` header §§1–4.

---

## Active focus

**T-links / G0 (mechanical seat):** Concepts + Creating + Schema-Management G0 **complete** (Schema FR books fixed 2026-08-11; leaf lychee clean). **G1 parked.** Writing G0 deferred until content-ready. Do not touch leaves under active joins Draft.

**T-content (Author + GPT 5.6 Sol):** **`join-validation.md`** Plan → Draft (keep separate from workflows). Then Polish/judge **`join-workflows.md`**. Optional: `join-concepts` B1 (promote Logical Plan H2). Opus 5 parked for Ask — essay load.

**T-git (Author):** WIP commit → fetch upstream → rebase `ownership-rework` → refresh CV branch `dataframe-api-finished` + temp README. See **Git / fork hygiene**.

**T-Sphinx (other Grok):** section-scoped parse for active joins leaves only; no corpus WARN boil.

---

## Gates cheat sheet (corpus / Mac)

Canonical argv: coauthor `references/validation.md`. Order: **Format → Parse → Rust → Links (lychee) → Markers**.

### 1. Sphinx parse — section-scoped (T-Sphinx)

```bash
(cd docs && sphinx-build -b dummy source /tmp/df-sphinx-inventory) 2>&1 \
  | tee /tmp/df-sphinx-inventory.log \
  | rg -i 'warning|error' | sort -u > /tmp/df-sphinx-warn-error.txt

# Then scope to the active section (example: joins):
rg -i 'joins/|Transformations/joins' /tmp/df-sphinx-warn-error.txt
```

**Policy for the remaining ~17 files**

| Now                                                                                      | Later                                                                       |
| ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Blocking parse issues on **active** section (joins → then next section in content order) | Full-corpus WARN cleanup                                                    |
| Orphans that break the active toctree / build                                            | Parked Concepts files if they only warn and do not block joins              |
| Differential per page when polishing a leaf (`validation.md` §2.2)                       | Creating/Schema/Writing Sphinx debt until those sections are content-active |

Not every inventory line must be fixed this week — **unblock the section you are finalizing**.

### 2. Links — **lychee** (G0 + CI)

Tool: **lychee**. CI runs `bash ci/scripts/markdown_link_check.sh` in `dev.yml` (file links; `http(s)` excluded in `lychee.toml`).

**G0 https pass** (local, section-scoped) — separate from CI:

```bash
# Concepts:
lychee --no-progress --include '^https://' --exclude '^mailto:' --max-concurrency 4 \
  docs/source/library-user-guide/dataframe-api/Concepts/*.md

# Creating (recurse nested from-files/):
lychee --no-progress --include '^https://' --exclude '^mailto:' --max-concurrency 4 \
  docs/source/library-user-guide/dataframe-api/Creating-DataFrames/**/*.md \
  docs/source/library-user-guide/dataframe-api/Creating-DataFrames/*.md
```

After warehouse unification on a section, re-run that section before moving on. See **Mechanical finish gates — G0** above for contract + findings.

### Cross-cutting P0 tickets

| Ticket                                                                                                       | Status                                                                 |
| ------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| `lib.rs` doctest path casing (`Creating-DataFrames/`, `Schema-Management/`)                                  | **Done** 2026-08-05                                                    |
| Concepts spine: drop `summary` + `bigger-picture` from toctree/routing (files may stay parked)               | **toctree+handoff done**; index absorb still open                      |
| Parked `Concepts/bigger-picture.md` + `summary.md`: Sphinx `toc.not_included` (+ their xref/transition debt) | Open — **T-Sphinx**                                                    |
| Concepts spine: dissolve `architectural-dataframe.md` → fold into `sessioncontext.md` (not `anatomy-dataframe`); then park/delete with `summary` / `bigger-picture` | **Author decision 2026-08-10** — open; do after SessionContext content absorb |
| Sphinx: full inventory once, **fix blockers for active section only** (now joins)                            | **In flight** — other Grok; Author/GPT on content                      |
| Sphinx: rest of ~17-file / other-section WARN debt                                                           | **Parked** until that section is content-active                        |
| **lychee** https G0 (warehouse + externals)                                                                  | Concepts **done** 2026-08-10; Creating **done** 2026-08-11 (353 OK); Schema **done** (FR books Wiley/Routledge 2026-08-11); Writing deferred; CI file-lychee separate |
| Shipping 403s (Stackademic / ACM Spark)                                                                      | **Author manual** — parked during warehouse                            |
| `execution-lifecycle.md` post-lifecycle “broader context” handoff (removed Bigger Picture sentence)          | Open — restore via `sessioncontext` / index exit after architectural dissolve |
| Park/delete `Transformations/joins/WIP-join-concepts.md` + `Transformations/joins_old.md` before ship        | WIP **deleted**; `joins_old.md` still open — park/delete with joins finalize |
| Git hygiene: WIP commit → upstream sync on `ownership-rework` → refresh CV `dataframe-api-finished` + temp README | **Open** — before rebase or portfolio snapshot (see **Git / fork hygiene**) |
| Writing revise plan (create when Transformations settle)                                                     | Deferred                                                               |

---

## Defect families (index)

1. Finish / parse / stencil (mechanical P0)
2. Doctest infrastructure (casing, ignore, oracle theater)
3. Overclaim / wrong mental model (P1 + ledger)
4. Ownership / altitude / story-arc (P0.5)
5. API / diagram falsehoods (Gate A when touching that leaf)

---

## Model routing (Author load)

| Seat                     | Model                                   | Job                                        |
| ------------------------ | --------------------------------------- | ------------------------------------------ |
| Coordinator              | Grok (this)                             | Status, one decision, handoffs             |
| Content Ask + Draft      | **GPT 5.6 Sol (high)**                  | Preferred — grounded, less agreement-essay |
| Content Agent (optional) | GPT Sol or `datafusion-doc-implementer` | Approved bullet list only — AGENT skeleton |
| Sphinx / P0              | Other Grok                              | Parse inventory → mechanical render fixes  |
| Slop                     | Grok                                    | Tighten / cut                              |
| Researcher               | GPT terra                               | One claim                                  |
| Judge                    | GPT 5.6 sol                             | One review packet after gates              |

**Opus 5:** parked for this finish pass (2026-08-05). Strong on content ideas; weak as Ask partner (long evaluation loops). Re-open only for skeleton-only Agent packets if GPT stalls on prose craft.

**AGENT order:** after the bound is approved — implement first; open decisions last in the change report (`AGENTS.md`).
Session increments: Cursor **session task list** (`TodoWrite`) — never corpus `TODO:` markers.
Gates: full format/parse at **bound end** — not every increment (`validation.md` §1.1).
Tripwire: agreement essay without edits, invents claims, or Author fatigue → force implementer handoff or switch model (**triggered → GPT**).

---

## Done definitions

| Outcome          | Meaning                                                                                                                         |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **P0-narrow PR** | Shipping leaves parse-clean; closings/stencil OK; casing fixed; no draft scaffolds on “final” targets; known P1 listed honestly |
| **Full revise**  | Above + ledger applied + MAJOR truth fixed + Writing in shape                                                                   |

Never ship unresolved on-page `TODO:` / `JOIN-TODO` / DRAFT scaffolds on pages claimed `page_final`.
