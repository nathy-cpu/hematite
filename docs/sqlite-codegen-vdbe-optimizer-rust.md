# SQLite Query Optimization, Code Generation, and VDBE: Implementation Notes and Rust Reimplementation Guide

## Scope

This document focuses on how SQLite turns SQL into efficient execution:

- `src/parse.y`
- `src/build.c`
- `src/select.c`
- `src/where.c`
- `src/wherecode.c`
- `src/whereexpr.c`
- `src/whereInt.h`
- `src/expr.c`
- `src/window.c`
- `src/vdbe.c`
- `src/vdbeaux.c`
- `src/vdbesort.c`
- `src/in-operator.md`
- `doc/vdbesort-memory.md`
- focused optimizer and execution tests in `test/`

It intentionally does not re-cover the storage layer (`btree`, `pager`) except where planner and bytecode decisions depend on storage behavior.

The target reader is building a database engine in Rust and wants to reproduce SQLite-style performance properties:

- low prepare-time overhead for common queries,
- robust query rewrites that do not change SQL semantics,
- cheap index-driven execution,
- minimal unnecessary table lookups,
- good join ordering without a huge optimizer framework,
- and a VM/runtime that preserves the planner's wins instead of giving them back.

## The Main Lesson

SQLite does not get its performance from one big "optimizer". It gets it from four layers that cooperate:

1. AST normalization and rewrite
   Convert many SQL surface forms into a smaller set of planner-friendly forms.

2. Loop enumeration and bounded search
   Generate a large but manageable set of access strategies and run a dynamic-programming search over them.

3. Code generation shaped to the chosen plan
   Emit bytecode that preserves covering-index scans, deferred seeks, early exits, and one-pass update/delete plans.

4. A VM with specialized opcodes
   The VDBE has just enough execution-time machinery to make the generated program efficient without requiring a huge runtime.

That division is worth copying. In a Rust system, do not collapse parsing, optimization, and execution into one pass. SQLite is fast partly because each stage has a narrow responsibility.

## High-Level Pipeline

At a high level, a `SELECT` follows this path:

1. `parse.y`
   Lemon parser builds `Expr`, `Select`, `SrcList`, and related AST nodes.

2. `sqlite3SelectPrep()`
   Expands `*`, resolves names, assigns cursors, and normalizes joins and subqueries enough for later passes.

3. `sqlite3Select()`
   Runs rewrite passes such as flattening, constant propagation, push-down, min/max detection, and simple-count detection.

4. `sqlite3WhereBegin()`
   Breaks the WHERE clause into terms, manufactures virtual terms, enumerates `WhereLoop` candidates, solves join order, and opens the execution loops.

5. `expr.c`, `select.c`, `wherecode.c`
   Generate VDBE bytecode for expressions, subqueries, joins, aggregates, sorters, and DML.

6. `vdbe.c`
   Executes opcodes against btrees, sorters, and registers.

7. `sqlite3FinishCoding()`
   Finalizes the VM program with transaction checks, schema verification, `OP_Halt`, and statement metadata.

`src/select.c` itself documents the important milestones in `sqlite3Select()`. That outline is a good mental map of where optimization actually lives.

## Part I: Parser and AST Preparation

## What the parser does

The parser in `src/parse.y` is mostly conventional:

- parse SQL into AST nodes,
- attach expressions to `Select`, `Insert`, `Update`, and `Delete`,
- and hand off to semantic and codegen stages.

The parser is not where the interesting optimization happens.

That is important because many first implementations put too much policy into parsing. SQLite does not. The parser produces a faithful tree, then later passes reshape it.

## What `sqlite3SelectPrep()` adds

`sqlite3SelectPrep()` is the real beginning of optimization-friendly compilation. It performs:

- `sqlite3SelectExpand()`
- `sqlite3ResolveSelectNames()`
- `sqlite3SelectAddTypeInfo()`

This stage matters because it canonicalizes the tree before the optimizer sees it:

- `NATURAL` joins are converted into `USING`,
- `ON` and `USING` constraints are moved into the WHERE tree with flags like `EP_InnerON` and `EP_OuterON`,
- subqueries and views are expanded,
- `*` is expanded into explicit result expressions,
- cursor numbers are assigned,
- names are resolved to exact source columns.

This normalization is a big design lesson. Later passes can reason over one boolean-expression tree instead of re-implementing join semantics everywhere.

## Part II: Rewrite-Time Optimizations in `select.c` and `window.c`

These passes run before the WHERE planner proper. Their purpose is to turn a semantically rich SQL tree into something the planner can search effectively.

## 1. Subquery `ORDER BY` removal

Intent:

- remove ordering work that has no semantic effect,
- and unblock flattening or better index use.

Core idea:

- if a subquery has an `ORDER BY` that the outer query does not actually rely on, SQLite can drop it.

Why it matters:

- an unnecessary `ORDER BY` often forces a sorter or prevents flattening,
- which in turn hides indexes from the planner.

Safety strategy:

- SQLite only removes the `ORDER BY` under a long list of conditions,
- because inner ordering can affect semantics with `LIMIT`, window functions, aggregates, or compound queries.

Rust takeaway:

- treat "remove order" as a semantics-sensitive rewrite, not a cosmetic one,
- and gate it with explicit proof conditions.

## 2. Query flattening

Intent:

- avoid materializing subqueries into temp tables,
- expose base tables and indexes to the outer query,
- and reduce VM work.

Main algorithm:

1. Detect a FROM-clause subquery that is safe to merge.
2. Splice the subquery's FROM items into the parent query.
3. Rewrite result-column references so outer expressions point at the subquery's underlying expressions.
4. Merge WHERE/HAVING/ORDER/LIMIT details when legal.
5. Reject the transformation if any rule suggests the merge could change semantics.

What SQLite checks:

- `DISTINCT`,
- `LIMIT`,
- `ORDER BY`,
- compound query shape,
- aggregates and windows,
- recursive CTEs,
- outer-join position,
- RIGHT/FULL JOIN hazards,
- and many more.

The restrictions are long because flattening is one of the highest-value and highest-risk optimizations in the engine. Wrong flattening causes wrong answers, not just slow queries.

Why flattening helps so much:

- the planner can use indexes from the inner tables directly,
- join ordering now includes the formerly hidden subquery tables,
- and there is no temp btree/materialization pass in the common case.

Rust takeaway:

- implement flattening only after you can prove semantic guards cleanly,
- and encode those guards as named checks rather than ad hoc conditions spread through the optimizer.

## 3. `EXISTS` to join

Intent:

- turn some `EXISTS (SELECT ...)` predicates into normal joins,
- so the WHERE planner can optimize them with the rest of the join graph.

Algorithm in `existsToJoin()`:

1. Find `EXISTS (SELECT ...)` in the outer WHERE tree.
2. Require a simple subquery:
   - one FROM item,
   - no aggregate,
   - no `LIMIT`,
   - no compound,
   - no nested FROM-subquery.
3. Renumber the subquery cursor to avoid aliasing.
4. Replace the `EXISTS` expression with constant true.
5. Append the subquery's FROM source to the outer FROM list.
6. Append the subquery WHERE clause to the outer WHERE clause.
7. Mark the appended source with `fromExists` so codegen can stop after the first successful match.

That last step is crucial. The rewrite is only approximately "turn EXISTS into a join". The actual implementation preserves existential semantics by making the generated loop break as soon as one match is found.

Rust takeaway:

- existential rewrites need both logical rewriting and execution-shape support,
- not just AST surgery.

## 4. Constant propagation

Intent:

- use equalities like `a=5` or `5=a` to simplify other predicates that reference `a`.

Algorithm in `propagateConstants()`:

1. Scan top-level AND-connected WHERE terms.
2. Find safe column-equals-constant terms.
3. Walk the WHERE tree and replace eligible references to that column with a special fixed-column representation.
4. Repeat until no more changes happen.

Why SQLite does not literally replace expressions:

- affinity and collation matter.

SQLite's comments show tricky cases such as:

- numeric affinity versus text affinity,
- `LIKE` with text coercion,
- BLOB-affinity hazards.

So instead of rewriting `b=a` into `b=123` textually, SQLite tags the expression with `EP_FixedCol` and hangs the constant off the node so codegen can substitute the value while preserving comparison semantics.

This is one of the best examples of SQLite's style:

- exploit the optimization,
- but preserve SQL's coercion rules exactly.

Rust takeaway:

- your optimizer needs "semantic substitution" nodes, not just string-equivalent rewrites.

## 5. Predicate push-down

Intent:

- copy selective outer predicates into a subquery so fewer rows are produced before materialization or further processing.

Algorithm in `pushDownWhereTerms()`:

1. Walk outer WHERE terms.
2. For each candidate term, check whether it is safe to duplicate into the subquery.
3. If safe:
   - copy it into the subquery WHERE, or
   - into HAVING for aggregate cases.

Important restrictions:

- no recursive CTE term,
- no subquery `LIMIT`,
- careful handling of LEFT/RIGHT/FULL joins,
- compound-query collation restrictions,
- no VALUES subquery,
- special window-function restrictions.

Window-specific rule:

- a pushed-down predicate must consist only of constants and expressions from the window `PARTITION BY` keys.

Reason:

- it is safe to remove whole partitions,
- it is not safe to remove rows from within a partition and then recompute the window.

Rust takeaway:

- predicate push-down should be framed as "duplicate with proof", not "move aggressively".

## 6. Omit unused subquery result columns

Intent:

- avoid computing expressions a parent query never reads.

Algorithm in `disableUnusedSubqueryResultColumns()`:

1. Determine which subquery output columns are actually required by the parent, including columns needed by the subquery's `ORDER BY`.
2. For unused expressions, replace them with `TK_NULL`.

Restrictions:

- no correlated subquery,
- no CTE materialization dependency,
- no `DISTINCT`,
- no aggregate,
- no window functions,
- compound must be `UNION ALL` only.

Why this matters:

- it can eliminate expensive expressions, function calls, or large row construction.

`test/selectH.test` proves this with a side-effecting `counter()` function. If the column is truly unused, the function stops running.

Rust takeaway:

- dead-column elimination is very effective if your subquery representation keeps per-column liveness information.

## 7. `DISTINCT ORDER BY` to `GROUP BY`

Intent:

- let one ordered pass satisfy both duplicate elimination and requested ordering.

Transformation:

- `SELECT DISTINCT x ... ORDER BY x`
- can become the equivalent of
- `SELECT x ... GROUP BY x ORDER BY x`

Why:

- if the order keys and distinct keys line up, one grouping/order structure can do both jobs.

This is a classic example of SQLite preferring fewer temp structures over theoretically cleaner phases.

## 8. `HAVING` to `WHERE`

Intent:

- evaluate filters as early as possible.

Rule:

- if a `HAVING` term depends only on constants or GROUP BY expressions, it may be evaluated before aggregation in the WHERE phase.

Benefit:

- fewer rows enter the aggregate pipeline.

## 9. `count(*)` over a compound view

Intent:

- avoid materializing or scanning the whole result of a `UNION ALL` view just to count rows.

Algorithm in `countOfViewOptimization()`:

1. Detect `SELECT count(*) FROM (compound-subquery)`.
2. Require:
   - no outer WHERE/GROUP/HAVING/ORDER,
   - subquery is a compound,
   - every arm is `UNION ALL`,
   - no DISTINCT,
   - no LIMIT,
   - no aggregate arm.
3. Rewrite the compound so each arm computes its own `count(*)`.
4. Sum those counts.

This is a rewrite from row production to cardinality production.

Rust takeaway:

- a good optimizer recognizes when the consumer wants metadata about rows rather than the rows themselves.

## 10. Min/max optimization

Intent:

- answer `min(x)` or `max(x)` with ordered access instead of full aggregation.

Algorithm in `minMaxQuery()`:

1. Detect exactly one aggregate function.
2. Require it to be `min()` or `max()` with a single argument.
3. Synthesize an `ORDER BY` on that expression:
   - ascending for `min`,
   - descending for `max`,
   - plus special null-order handling for `min`.
4. Tell the WHERE planner to seek an ordered plan.
5. Use early-out when the first qualifying row is enough.

This is small but powerful. It converts an aggregate problem into an ordered lookup problem.

## 11. Simple count optimization

Intent:

- answer `SELECT count(*) FROM table` with the cheapest possible btree count.

Algorithm:

1. Detect the exact pattern:
   - single table,
   - no subquery source,
   - no WHERE,
   - no GROUP BY,
   - no HAVING,
   - one aggregate function,
   - `count(*)`.
2. Choose the smallest usable btree:
   - often an index, not the table,
   - but not an unordered or partial index.
3. Emit `OP_Count`.

Why the smallest index:

- fewer pages usually means less I/O and cache pressure.

This is a good example of SQLite's willingness to use physical layout facts to speed up logical SQL.

## 12. Window-function rewrite

Window functions are not optimized by a giant generic planner. Instead, `window.c` rewrites the query into a form that the rest of the engine can execute predictably.

Core strategy in `sqlite3WindowRewrite()`:

- wrap the original query in a parent/child arrangement,
- force the child query to deliver rows in partition/order sequence,
- then have the parent compute windows over that stream.

Important behavior:

- flattening is disabled on the rewritten form,
- compatible windows can share a scan,
- incompatible windows may trigger nested rewrites or extra processing,
- some functions require caching the whole partition, others can stream.

Rust takeaway:

- windows are often easier to implement as a rewrite into a constrained physical plan than as a special case sprinkled through the optimizer.

## Part III: WHERE-Clause Canonicalization in `whereexpr.c`

This is one of the most reusable ideas in SQLite.

The planner does not try to understand every SQL syntax form directly. Instead, `exprAnalyze()` manufactures extra `WhereTerm`s that represent planner-friendly implications of the original expression.

That drastically simplifies loop enumeration.

## Virtual terms SQLite creates

SQLite adds terms such as:

- commuted comparisons,
- `BETWEEN` into `>=` and `<=`,
- vector equality into scalar equalities,
- vector `IN` into per-component virtual terms,
- `NOT NULL` into a synthetic comparison form,
- LIKE/GLOB prefix ranges,
- OR-to-IN virtual terms,
- transitive equalities,
- virtual-table helper terms for `MATCH`, `LIKE`, `REGEXP`, and others.

The original term remains the semantic anchor. The virtual terms exist so the planner can search access paths more easily.

## LIKE/GLOB prefix optimization

Intent:

- convert prefix string search into an indexable range scan.

Example:

- `x LIKE 'abc%'`
- becomes effectively:
  - `x >= 'abc'`
  - `x < 'abd'`
  - plus the original LIKE check

Why keep the original LIKE:

- the range is only an approximation of the actual pattern semantics,
- especially with escaping, case folding, collations, and blob behavior.

SQLite also contains fixups so the generated string opcodes can be treated as blobs when needed on a second pass.

Rust takeaway:

- prefix-pattern optimization should be "approximate index range + exact residual predicate".

## OR optimization

`exprAnalyzeOrTerm()` handles several forms:

1. Homogeneous equality disjunction:
   - `x=a OR x=b OR x=c`
   - becomes synthetic `x IN (...)`

2. Strengthening of certain two-way disjunctions:
   - for example, deriving an extra lower bound such as `x>=A`

3. Multi-index OR:
   - if each OR arm is separately indexable for the same table, planner can evaluate branches independently and union rowids or primary keys.

This is a great example of staged optimization:

- first normalize OR into something simpler if possible,
- otherwise keep a more expensive but still optimized execution strategy.

## Transitive constraints

SQLite uses equalities like:

- `t1.a = t2.b`
- `t2.b = 123`

to infer:

- `t1.a = 123`

But `termIsEquivalence()` is conservative:

- it checks collation,
- affinity,
- join origin,
- RIGHT JOIN hazards,
- and whether transitive optimization is enabled.

This conservatism is deliberate. Wrong transitive inference is a classic source of wrong-answer bugs.

## Part IV: Planner Data Structures and Search in `where.c`

## The key structs

### `WhereTerm` (`whereInt.h:127`)

Represents one constraint, original or virtual:

- `Expr *pExpr` — the underlying expression
- `WhereClause *pWC` — the owning WHERE clause
- `u16 eOperator` — operator bitmask (`WO_EQ=0x0002`, `WO_LT=0x0010`, `WO_LE=0x0020`, `WO_GT=0x0040`, `WO_GE=0x0080`, `WO_IN=0x0001`, `WO_IS=0x0100`, `WO_ISNULL=0x0200`, etc.)
- `u16 wtFlags` — term metadata flags:
  - `TERM_DYNAMIC=0x0001` — dynamically allocated
  - `TERM_VIRTUAL=0x0002` — added by optimizer, not user SQL
  - `TERM_CODED=0x0004` — already code-generated
  - `TERM_COPIED=0x0008` — has a child term copy
  - `TERM_ORINFO=0x0010` — has `WhereTerm.u.pOrInfo`
  - `TERM_ANDINFO=0x0020` — has `WhereTerm.u.pAndInfo`
  - `TERM_OK=0x0040` — used during OR optimization
  - `TERM_VNULL=0x0080` — manufactured `x IS NOT NULL`
  - `TERM_LIKE=0x0200` — originates from LIKE optimization
  - `TERM_IS=0x0400` — `IS` operator (distinct from `=`)
  - `TERM_HIGHTRUTH=0x2000` — skip Bloom filter for this term
- `Bitmask prereqRight` — bitmask of tables referenced on RHS
- `Bitmask prereqAll` — bitmask of all tables referenced
- `LogEst truthProb` — estimated probability as LogEst (negative = selectivity)
- `int iParent` — index of parent term for subterms of OR/AND

### `WhereLoop` (`whereInt.h:294`)

Represents one candidate access method for one FROM item:

- `Bitmask prereq` — bitmask of tables that must be in outer loops
- `Bitmask maskSelf` — bitmask for this table
- `u8 iTab` — index into FROM clause
- `u8 nSkip` — columns skipped for skip-scan
- `u16 nLTerm` — number of entries in `aLTerm[]` (matched constraints)
- `LogEst rSetup` — one-time setup cost (e.g., auto-index creation)
- `LogEst rRun` — cost to run one iteration of this loop
- `LogEst nOut` — estimated rows output per iteration
- `u32 wsFlags` — strategy flags:
  - `WHERE_COLUMN_EQ=0x0001`, `WHERE_COLUMN_RANGE=0x0002`, `WHERE_COLUMN_IN=0x0004`
  - `WHERE_COLUMN_NULL=0x0008`, `WHERE_CONSTRAINT=0x000f`
  - `WHERE_TOP_LIMIT=0x0010`, `WHERE_BTM_LIMIT=0x0020`
  - `WHERE_IDX_ONLY=0x0040` (covering index — no table lookup needed)
  - `WHERE_IPK=0x0100`, `WHERE_INDEXED=0x0200`
  - `WHERE_ONEROW=0x1000`, `WHERE_MULTI_OR=0x2000`
  - `WHERE_AUTO_INDEX=0x4000`, `WHERE_SKIPSCAN=0x8000`
  - `WHERE_BLOOMFILTER=0x00400000`
  - `WHERE_VIRTUALTABLE=0x00800000`
- For btree access: `u.btree.nEq` (equality prefix length), `u.btree.pIndex`
- For virtual tables: `u.vtab.idxNum`, `u.vtab.idxStr`, `u.vtab.omitMask`

### `WherePath` (`whereInt.h:422`)

Represents a partial join order during the solver search:

- `Bitmask maskLoop` — tables already placed in this path
- `Bitmask revLoop` — reverse-order bitmask for ORDER BY satisfaction
- `LogEst nRow` — estimated total rows produced so far
- `LogEst rCost` — total cost including sort penalty
- `LogEst rUnsort` — total cost excluding sort penalty
- `i8 isOrdered` — `-1` = unknown, `0..N` = number of ORDER BY terms satisfied
- `WhereLoop **aLoop` — array of chosen loops (length = current depth)

### `WhereLevel` (`whereInt.h:457`)

The final executable loop after the solver has chosen a plan:

- `int iLeftJoin` — register for LEFT JOIN null-flag tracking
- `int iTabCur` — cursor number for the table
- `WhereLoop *pWLoop` — the chosen access method
- Jump/address labels for code generation: `addrBrk`, `addrNxt`, `addrSkip`, `addrCont`

This separation is extremely good design. It cleanly distinguishes constraints, candidate access methods, partial search states, and final executable loops.

## Logarithmic cost model

SQLite stores all cardinalities and costs as `LogEst` (`sqliteInt.h:874–897`), a `i16` approximating `10 * log2(x)`:

```
Value → LogEst    Value → LogEst     Value → LogEst
    1 →   0         100 →  66       1000000 → 199
    2 →  10        1000 →  99       1048576 → 200
    3 →  16        1024 → 100    4294967296 → 320
    4 →  20       10000 → 132
   10 →  33       25000 → 146
   20 →  43

Fractional: 0.5 → -10,  0.1 → -33,  0.0625 → -40
```

Key operations: `sqlite3LogEst(u64)` converts a count, `sqlite3LogEstAdd(a,b)` computes `log(2^a + 2^b)` (used for cost accumulation), `sqlite3LogEstToInt(x)` converts back. Addition of costs is `a + b` (multiply of quantities). This is the entire cost arithmetic for the planner.

## Fast-path planning: `whereShortCut()` (`where.c:6350–6440`)

For the very common single-table case, SQLite skips the full solver:

1. **Rowid equality** (`x = ?` on INTEGER PRIMARY KEY): `rRun = 33` (≈ cost 10). Sets `WHERE_COLUMN_EQ|WHERE_IPK|WHERE_ONEROW`.
2. **Unique index equality** (all key columns have `=`): `rRun = 39` (≈ cost 15). Sets `WHERE_COLUMN_EQ|WHERE_ONEROW|WHERE_INDEXED`. Also sets `WHERE_IDX_ONLY` if the index is covering.

Both bypass `whereLoopAddBtree()`, `wherePathSolver()`, and all sorting cost calculations. This matters because a huge fraction of embedded-database workload is tiny point queries — prepare-time overhead is visible at microsecond scale.

## Candidate generation

The planner generates many `WhereLoop`s for each table:

- full scan,
- rowid lookup,
- index lookup with equality prefix,
- range scan,
- skip-scan,
- IN-driven loops,
- LIKE-prefix range loops,
- OR-union strategies,
- automatic indexes,
- virtual-table plans.

The planner is effective because it does not search arbitrary plans. It searches a compact universe of loop templates.

## Selectivity estimation

SQLite combines:

- heuristics,
- `sqlite_stat1`,
- `sqlite_stat4`,
- explicit `likelihood()` hints,
- and residual-predicate adjustments.

Examples:

- a single range constraint heuristically cuts cardinality sharply,
- two-sided ranges reduce it more,
- extra residual predicates reduce output further,
- long LIKE/GLOB/MATCH patterns cut estimates more than short ones,
- STAT4 samples improve range estimates by comparing bound prefixes to real sampled keys.

This is not a mathematically perfect model. It is a robust engineering model.

## Index-prefix enumeration

In `whereLoopAddBtreeIndex()`, SQLite walks usable index columns left to right and tries combinations such as:

- equalities,
- `IS`,
- `IS NULL`,
- `IN`,
- one inequality range after equality prefix,
- LIKE-derived ranges,
- transitive equalities.

For each candidate, it computes:

- search work,
- scan work,
- table lookup cost if not covering,
- rowcount,
- and flags such as `WHERE_ONEROW` or `WHERE_IN_SEEKSCAN`.

That left-prefix discipline is critical. SQLite's planner is powerful because it deeply understands btree index structure, not because it uses a generic relational theorem prover.

## Skip-scan

Intent:

- use an index even when the left-most columns are unconstrained, if those columns have many duplicates.

Idea:

- iterate distinct values of the left-most skipped prefix,
- and for each distinct prefix value, run the constrained search on later columns.

SQLite only considers skip-scan when statistics suggest enough duplication to justify it. There is also a small fudge factor to make skip-scan slightly less attractive than a truly aligned index probe.

Rust takeaway:

- skip-scan is worth implementing only if you have stats and a plan representation that can model nested prefix iteration cheaply.

## Automatic indexes

Intent:

- build a transient index when a query would otherwise do an obviously bad nested-loop scan.

Algorithm in `constructAutomaticIndex()`:

1. Identify equality-driven terms useful for the inner table.
2. Build a transient covering index containing:
   - key columns needed for lookup,
   - extra columns needed by output or residual predicates.
3. Optionally make it partial if only a subset of rows matters.

SQLite also estimates whether building the index is worth it. It avoids auto-indexing when:

- the setup cost is unlikely to pay back,
- or a real index already covers the use case well enough.

Tests like `test/autoindex1.test` show both the speed win and the guardrails.

Rust takeaway:

- transient indexes are one of the simplest ways to rescue an otherwise poor MVP planner.

## Bloom filters

Intent:

- avoid expensive repeated probes into an inner table when many probes will miss.

Planner logic:

- only enable when statistics suggest many lookups and substantial filtering benefit,
- avoid cases where affinity, collation, expression indexes, or missing stats make correctness or usefulness uncertain.

Runtime shape:

- build a Bloom filter with `OP_FilterAdd`,
- test it with `OP_Filter`,
- skip the expensive search when the filter proves the key cannot match.

`test/bloom1.test` is valuable here because it focuses on the semantic edge cases:

- affinity coercion,
- collation behavior,
- expression indexes,
- missing `STAT1`.

Rust takeaway:

- Bloom filters are not just a data-structure feature. They require planner proof rules.

## ORDER BY, DISTINCT, and sorting-aware planning

SQLite does not treat ordering as an afterthought.

### `wherePathSatisfiesOrderBy()`

This routine determines how much of the required ordering is naturally satisfied by a candidate path.

Important ideas:

- `ORDER BY` requires strict left-to-right order compatibility,
- `GROUP BY` and `DISTINCT` only require equivalent rows to be adjacent,
- equality-constrained leading columns may be ignored for ordering purposes,
- subquery order and reverse scans are considered,
- unique-not-null columns can make a loop "order-distinct".

### Sorting cost: `whereSortingCost()` (`where.c:5527–5585`)

Models sort cost as `K * N * log(N)` with specific tuning:

```
rSortCost = nRow + nCol          // base: rows × column-width factor
nCol = sqlite3LogEst((nExpr+59)/30)  // column count scaled
```

Adjustments:
- **Partial sort**: if `nSorted` of `nOrderBy` terms already in order, scale by `(nOrderBy-nSorted)/nOrderBy`
- **LIMIT**: add `+10` (2x penalty), plus `+6` if also partial sort. Use LIMIT as `nRow` if smaller.
- **DISTINCT**: reduce `nRow` by factor of 2 (subtract 10 in LogEst)
- **Final multiply**: `rSortCost += estLog(nRow)` — the log(N) factor

A `+3` penalty is always added to sorting vs. no-sort plans to bias toward naturally-ordered output.

### `wherePathSolver()` — the join order solver (`where.c:5834–6257`)

This is a bounded dynamic programming search. It works by building increasingly longer paths one table at a time.

**Path budget (`mxChoice`)** — controls how many partial paths survive each round:

| `nLoop` | `mxChoice` | Notes |
|---|---|---|
| 1 | 1 | Single table — trivial |
| 2 | 5 | Two tables |
| 3+ | 12 or 18 | 18 for star queries (`computeMxChoice()`, `where.c:5651`) |

**Algorithm**:
1. Seed `aFrom[]` with one empty path (`nRow = min(nQueryLoop, 48)`)
2. For each round `iLoop = 0..nLoop-1`:
   - For each surviving path in `aFrom[]`:
     - For each `WhereLoop` not yet used:
       - Check prerequisites (`prereq & ~maskLoop == 0`)
       - Compute `rUnsort = rRun + nRow_from + rSetup`
       - Compute ordering satisfaction via `wherePathSatisfiesOrderBy()`
       - Add sort cost if ordering is incomplete: `rCost = rUnsort + aSortCost[isOrdered] + 3`
       - If no sort needed: `rCost = rUnsort; rUnsort -= 2` (slight bias for no-sort)
       - Replace worst path in `aTo[]` if this path is better
   - Swap `aFrom` and `aTo`
3. Final path is `aFrom[0]` — load into `WhereLevel[]` for code generation

**Domination check**: A candidate replaces an existing path at the same `maskLoop` using vector comparison: `(rCost, nRow, rUnsort)`. Ties are broken by `whereLoopIsNoBetter()` which prefers smaller index row sizes.

### Two-pass solve + interstage heuristic

When `ORDER BY` is present, the solver runs **twice** (`where.c:7105–7109`):

1. **Pass 1**: `wherePathSolver(pWInfo, 0)` — ignore sort cost, estimate total row count
2. **`whereInterstageHeuristic()`** (`where.c:6301–6337`): For each table in the Pass 1 plan that used an index equality lookup, **disable** all full-table-scan WhereLoops for that table (set `prereq = ALLBITS`). This prevents Pass 2 from swapping an index search for a full scan just to satisfy ORDER BY.
3. **Pass 2**: `wherePathSolver(pWInfo, nRowOut)` — include sort cost, using Pass 1's row estimate

The heuristic exists because the sorting cost model can overvalue "avoid sorting" and choose catastrophically bad full scans. `test/whereN.test` captures exactly this failure mode.

### Star-schema heuristic: `computeMxChoice()` (`where.c:5651–5798`)

For joins with 4+ tables, SQLite detects star schemas:

- A "fact table" has 3+ inner-joined "dimension tables" that depend on it
- Self-joins are excluded from being dimensions
- Outer/cross joins break the star pattern

**Effect**: SCAN costs for dimension tables are raised to be slightly above the maximum SCAN cost of the fact table. This keeps fact-first paths from being pruned by the `mxChoice` budget. Returns `mxChoice = 18` (vs. normal 12).

- bounded search for predictability,
- selective heuristics to prevent catastrophic pruning.

## Omit-noop joins

SQLite can sometimes remove the right-hand side of a LEFT JOIN when:

- the RHS contributes no needed columns,
- the join cannot affect row multiplicity in a semantically visible way,
- and removing it does not change null-extension behavior.

This is another example of SQLite preferring to delete work entirely once it can prove the work is irrelevant.

## Part V: Code Generation Tactics in `wherecode.c`, `expr.c`, and `select.c`

Once a plan is chosen, SQLite still has to generate bytecode that keeps the plan efficient.

## 1. Equality/range key setup

`codeAllEqualityTerms()` and `codeEqualityTerm()`:

- evaluate equality and `IN` constraints,
- store them in contiguous registers,
- adjust affinity bytes so comparisons use the correct coercion semantics,
- and set up skip-scan prefixes when needed.

This looks mundane, but it is a major source of correctness. A fast plan with wrong affinity handling is still wrong.

Notable detail:

- SQLite avoids disabling a transitive equality term unless it is sure doing so is safe. There is an explicit regression comment for this.

## 2. Skip-scan code shape

For skip-scan, SQLite emits a loop that:

- rewinds or seeks the index,
- reads the skipped left-prefix columns,
- then runs the constrained probe on the remaining key suffix.

This keeps skip-scan as a real executable strategy rather than a planner fiction.

## 3. `IN`-loop generation and early-out

`IN` is one of SQLite's most refined optimizations.

### High-level `IN` algorithm

`src/in-operator.md` describes the optimized algorithm:

1. Rewrite tiny constant lists to OR in membership contexts.
2. Handle NULL-sensitive cases carefully.
3. Binary-search or probe an indexed/materialized RHS if possible.
4. Fall back to NULL-aware scanning only when SQL three-valued logic requires it.

### RHS choices

`sqlite3FindInIndex()` and `sqlite3CodeRhsOfIN()` try, in order:

- existing suitable table/index,
- tiny-list no-op rewrite,
- ephemeral RHS materialization.

SQLite can also reuse a previously materialized RHS via `EP_Subrtn`, `OP_Gosub`, and `OP_OpenDup`.

### Early-out for multi-column `IN`

When an `IN` applies to a non-leftmost column of a multi-column index probe, SQLite uses:

- `OP_SeekHit`
- `OP_IfNoHope`

to detect when no remaining combination can possibly match and abandon the rest of the `IN` loop early.

This is a very SQLite-like optimization:

- tiny planner/runtime contract,
- big payoff on nested `IN` workloads.

## 4. Multi-index OR execution

When OR arms are independently indexable, SQLite generates separate branch plans and unions their row identities:

- `RowSet` for rowid tables,
- ephemeral primary-key table for `WITHOUT ROWID`.

Outer AND terms may be pushed into each OR branch when safe.

This is not a bitmap index framework. It is a compact, targeted union-of-branches strategy.

## 5. Deferred seeks and covering-index preservation

This is one of the most important runtime/codegen collaborations in SQLite.

### Deferred seek

`codeDeferredSeek()` emits `OP_DeferredSeek` instead of immediately moving from index entry to table row.

Meaning:

- use the index now,
- postpone the table btree lookup until some opcode truly needs table data.

If execution never asks for a table-only column, the table seek never happens.

### Alternate column map

`OP_DeferredSeek` may carry an integer array telling the VM that some requested table columns are actually available from the index cursor.

Later, `OP_Column` can redirect reads through the index cursor instead of finishing the table seek.

### Post-pass opcode rewrite

At the end of WHERE codegen, `sqlite3WhereEnd()` walks already-emitted opcodes and rewrites:

- `OP_Column` to read from the covering index,
- `OP_Rowid` to `OP_IdxRowid`,
- `OP_IfNullRow` to point at the index cursor.

This is brilliant because it lets earlier codegen stay mostly generic while still getting true covering-index execution in the final program.

Rust takeaway:

- give your IR or bytecode builder a late rewrite pass for physical-plan exploitation. Do not force every earlier compiler stage to know every coverage detail.

## 6. Subqueries as subroutines, coroutines, or materialized tables

SQLite chooses among several physical implementations:

- subroutine (`OP_BeginSubrtn`, `OP_Return`) for scalar subqueries/EXISTS,
- coroutine for streaming a subquery result,
- materialization into an ephemeral table when reuse or random access is needed,
- reuse of already computed CTE/view results when legal.

Uncorrelated subqueries get `OP_Once` so the work is done only once per statement execution.

This is one of the most important design lessons for a Rust engine:

- "subquery" is not an execution strategy,
- it is a logical construct that can map to multiple physical strategies.

## 7. One-pass `UPDATE` and `DELETE`

The same WHERE planner is reused by DML.

When safe, SQLite chooses one-pass modes such as:

- `ONEPASS_SINGLE`
- `ONEPASS_MULTI`

so `UPDATE` or `DELETE` can modify rows during the scan rather than first staging all rowids in temp storage.

This matters a lot for write-heavy workloads in embedded engines.

## 8. Ordered early exits

SQLite adds several small but important early-exit hooks once it knows the chosen loop order:

- `sqlite3WhereOrderByLimitOptLabel()` can stop scanning inner loops when an ordered `LIMIT` query has already produced enough rows,
- `sqlite3WhereMinMaxOptEarlyOut()` can stop after the first qualifying row for min/max ordered plans,
- `EXISTS`-to-join loops break as soon as one match is found.

These are not headline optimizations, but they are exactly the kind of details that make a mature engine feel fast on real workloads.

## Part VI: VDBE Runtime Optimizations

The VM is not a naive interpreter. It contains very targeted machinery to preserve the optimizer's intended fast paths.

## 1. `OP_Once`

Purpose:

- execute a block once per statement invocation.

Implementation:

- top-level programs use a self-altering-code trick against `OP_Init.p1`,
- subprograms use a per-frame bitmask because recursive triggers make the self-modifying trick unsafe there.

Why it matters:

- uncorrelated subqueries,
- hoisted constants,
- reusable RHS materialization,
- and some Bloom-filtered subroutines all depend on it.

## 2. `OP_DeferredSeek` plus `OP_Column`

At runtime, a deferred seek stores:

- target rowid,
- alternate index cursor,
- alternate column map.

Then `OP_Column` can often satisfy the request directly from the index cursor. This avoids both:

- the table seek,
- and the row payload decode.

`OP_Column` also has argument flags that let some callers avoid loading full payloads when they only need metadata such as length, type, or null-ness.

That is one of the core reasons SQLite's index plans stay fast in practice.

## 3. `OP_SeekHit` and `OP_IfNoHope`

These opcodes implement the `IN` early-out contract:

- `seekHit` tracks how much of an index prefix is known to have matched,
- `OP_IfNoHope` uses that to skip hopeless remaining combinations.

This is a good model for Rust too:

- do not be afraid to add a tiny VM field or opcode when it eliminates a common nested-loop waste pattern.

## 4. Bloom-filter opcodes

`OP_FilterAdd`:

- hash key registers into a Bloom filter blob.

`OP_Filter`:

- test whether a probe can be ruled out before a more expensive search.

These opcodes are intentionally simple. Most of the intelligence lives in the planner's decision about when they are safe and useful.

## 5. Specialized record comparison

`sqlite3VdbeFindCompare()` chooses faster comparator functions when the first key field has a common shape:

- integer-first records,
- plain-text-first records without collation,
- otherwise the generic record comparator.

This saves repeated decoding work in btree index comparisons.

Rust takeaway:

- hot path comparison is worth specializing aggressively for common key shapes.

## 6. Sorter internals

`src/vdbesort.c` implements an external merge sorter with these stages:

1. Accumulate rows in memory.
2. If memory threshold is exceeded, sort the batch and flush it as a PMA, a packed memory array.
3. At rewind time, flush remaining rows.
4. Merge PMAs with a tournament-tree style merge structure.
5. Optionally use worker threads for sort/flush/merge.

Important details:

- threshold is tied roughly to page-size times cache-size,
- if PMA count is large, SQLite uses a merge hierarchy rather than merging everything at once,
- sorter comparisons are also specialized for common key shapes,
- stability can be exploited in some index-build cases to reduce comparison work.

`test/sort.test`, `test/sort4.test`, and `doc/vdbesort-memory.md` are useful references here.

## Part VII: Tests That Reveal the Optimizer's Real Priorities

SQLite's tests are especially valuable because many are regression tests for very specific wrong-answer or pathological-plan bugs.

Useful files include:

- `test/where9.test`
  Multi-index OR behavior.

- `test/skipscan1.test`
  Skip-scan costing and correctness.

- `test/transitive1.test`
  Transitive equality safety across affinity/collation/join corner cases.

- `test/whereN.test`
  The interstage heuristic that prevents ORDER-BY-driven bad plans.

- `test/autoindex1.test`
  Automatic index usefulness and guardrails.

- `test/bloom1.test`
  Bloom-filter correctness around affinity, collation, missing stats, and expression indexes.

- `test/selectH.test`
  Omit-unused-subquery-column optimization, including side-effect checks.

- `test/windowpushd.test`
  Predicate push-down around window partitions.

- `test/existsexpr.test`
  EXISTS-to-join transformation boundaries.

- `test/subquery.test`
  Correlated subqueries, flattening behavior, index-only caveats.

- `test/select4.test`
  Push-down and constant-propagation interactions.

- `test/like.test` and `test/like3.test`
  LIKE prefix optimization and ordering behavior.

The quality lesson is clear:

- optimizer testing must focus on semantic invariants and historical bug patterns,
- not just "did the plan look better on one benchmark?"

## Part VIII: How to Recreate This in Rust

## 1. Keep the stages separate

Use explicit layers:

1. parser -> AST
2. AST normalization/rewrite
3. term analysis
4. candidate loop generation
5. bounded path search
6. bytecode or physical-plan generation
7. VM/runtime

Do not let the parser decide plans. Do not let the VM rediscover plan logic at runtime.

## 2. Represent semantic caveats directly

SQLite uses expression flags and origin tracking such as:

- outer-join provenance,
- fixed-column substitution,
- correlation markers,
- window-function markers.

A Rust optimizer should do the same with explicit enums/flags rather than implicit conventions.

## 3. Normalize aggressively before planning

Manufacture planner-friendly virtual constraints for:

- `BETWEEN`,
- OR-to-IN,
- LIKE prefix,
- vector comparisons,
- transitive equality.

That keeps the planner small and composable.

## 4. Use a `WhereLoop`-style search space

For each relation, enumerate access templates with:

- setup cost,
- per-run cost,
- output rows,
- ordering information,
- coverage information,
- and physical requirements.

Then do dynamic programming over join prefixes with a bounded frontier.

This is much easier to tune than a generic memo optimizer for an embedded engine.

## 5. Use integer log costs plus simple statistics

SQLite's `LogEst` approach is a very good fit for Rust too:

- stable integer comparisons,
- compact storage,
- simple heuristics,
- easy incorporation of sampled stats later.

You do not need a fancy cardinality estimator to get good plans. You need one that is:

- predictable,
- conservative around semantics,
- and easy to debug.

## 6. Plan for late physical rewrites

The covering-index rewrite in `sqlite3WhereEnd()` is a strong pattern:

- generate mostly generic code first,
- then rewrite instructions once the full physical picture is known.

That can be implemented in Rust with:

- bytecode patching,
- MIR-to-LIR lowering,
- or a post-selection plan rewrite pass.

## 7. Make subquery execution strategy explicit

Support several physical forms:

- once-computed subroutine,
- coroutine,
- materialized temp table,
- fully flattened query.

Treat the choice as an optimization problem, not a parser property.

## 8. Add execution features only when the planner can use them

The most valuable SQLite runtime features are planner-facing:

- deferred seek,
- covering-index redirection,
- `IN` early-out,
- Bloom filters,
- sorter specialization.

If you add these to a Rust VM, expose them as simple opcodes or plan nodes with precise semantics. Avoid a large generic execution framework that makes these optimizations awkward to express.

## 9. Optimize for wrong-answer resistance

Many SQLite optimizer restrictions exist because a more aggressive transformation once produced wrong answers.

That means your Rust implementation should:

- prefer conservative rewrites first,
- carry affinity/collation/null semantics through every optimization,
- test outer-join provenance carefully,
- and add a regression for every bug you fix.

## 10. Build the same style of test suite

You want:

- plan-shape tests,
- wrong-answer regression tests,
- stats-sensitive tests,
- side-effect tests for dead-expression elimination,
- collation and affinity tests,
- and tests that compare correlated versus uncorrelated behavior.

SQLite's optimizer is robust because its tests are adversarial.

## Suggested Rust Architecture

One practical translation is:

1. `sql_ast`
   Parser output plus semantic flags.

2. `logical_rewrite`
   Flattening, push-down, constant propagation, dead-column elimination, window rewrite.

3. `constraint_normalizer`
   Build `WhereTerm`-like objects and virtual terms.

4. `access_planner`
   Generate `WhereLoop`-like candidates for scans, indexes, skip-scan, auto-index, OR-union, and special aggregates.

5. `join_solver`
   Dynamic-programming search over loop prefixes with ordering metadata.

6. `lowering`
   Emit bytecode or a compact physical IR with deferred seeks, sorters, subroutines, and one-pass DML.

7. `vm`
   Small interpreter with specialized cursor and sorter support.

This is much closer to SQLite's success factors than trying to copy the C source line-for-line.

## Final Takeaways

The most important optimization ideas to copy are:

- normalize SQL into a planner-friendly core,
- represent many access strategies explicitly but keep the search bounded,
- preserve ordering and coverage information throughout planning,
- let codegen and runtime cooperate on deferred work,
- and treat every rewrite as a semantics proof obligation.

SQLite is fast here not because it has a giant abstract optimizer, but because it repeatedly turns expensive general problems into cheaper specialized ones:

- subquery into join,
- OR into IN,
- aggregate into ordered lookup,
- table probe into index-only read,
- repeated miss-prone probes into Bloom-filter tests,
- full-row production into dead-column elimination,
- and repeated subquery execution into once-per-statement subroutines.

That is the right mindset to bring into a Rust reimplementation.
