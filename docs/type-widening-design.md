# Type Widening Design

## Problem

`merge-schema` coupled column-addition with unconditional type widening, causing:
1. `ARRAY<INT>→ARRAY<BIGINT>` crash (`CastExecutors.resolve` returns null)
2. Inconsistent behavior across write paths (path-write widens, catalog INSERT doesn't)

## Solution

Add `write.merge-schema.type-widening` (default `false`, mirrors `delta.enableTypeWidening`).

## Config

| Option | Default | Semantics |
|--------|---------|-----------|
| `write.merge-schema` | `false` | Enable schema evolution (column additions) |
| `write.merge-schema.type-widening` | `false` | Also allow widening existing column types (only when merge-schema=true) |
| `write.merge-schema.explicit-cast` | `false` | Also allow lossy type changes (only when type-widening=true) |

## Core Logic (`SchemaMergingUtils.merge`)

```
typeWidening=false (default) → keep base type for existing columns, return base0
typeWidening=true            → widen: Decimal precision↑, length↑, supportsCast-safe types
typeWidening=true + explicit → also allow lossy casts (BIGINT→INT etc)
New columns                  → always added regardless of typeWidening
```

## Write Path Architecture

Three building blocks:
- `SchemaHelper.computeFinalSchema` — pure computation, no side-effects
- `SchemaHelper.commitSchemaEvolution` — persist schema to storage (idempotent)
- `SchemaHelper.alignColumns` — cast/reorder DataFrame columns to target schema

### Ordering by path

| Path | Step 1 (compute) | Step 3 (cast) | Step 2 (commit) |
|------|-------------------|---------------|-----------------|
| **V1 path-write** `save(location)` | inside `commitSchemaEvolution` | `alignColumns` (execution) | execution (`commitAndGetWriteSchema`) |
| **V1 catalog** `saveAsTable`/INSERT | `PaimonAnalysis.computeExpectedAttrs` (analysis) | `PaimonOutputResolver` (analysis) | `WriteIntoPaimonTable` → `commitAndGetWriteSchema` (execution) |
| **V2 catalog** (use-v2-write=true) | `PaimonAnalysis.computeExpectedAttrs` (analysis) | `PaimonOutputResolver` (analysis) | `PaimonV2Write` → `commitAndGetWriteSchema` (planning) |
| **MERGE INTO** | inside `commitSchemaEvolution` | `alignAllMergeActions` (analysis) | `evolveTargetIfNeeded` (analysis) |

Note: catalog paths compute→cast→commit; path-write/MERGE commit(includes compute)→cast.

## Known Limitations

1. **Complex element widening**: `ARRAY<INT>→ARRAY<BIGINT>` + `type-widening=true` throws
   (`SchemaManager.generateTableSchema` → `CastExecutors.resolve` = null). Follow-up.
2. **Position-based INSERT**: `INSERT INTO t VALUES(...)` — anonymous column names can't
   be name-matched, so `computeFinalSchema` is skipped (only `byName` writes compute it).
3. **MERGE INTO commit timing**: committed during analysis (not deferred to execution)
   because the target-read plan depends on the committed schema in the relation.

## Key Files

- `paimon-core/.../schema/SchemaMergingUtils.java` — `merge()` with typeWidening branch
- `paimon-spark/.../commands/SchemaHelper.scala` — three-step building blocks + trait entry points
- `paimon-spark/.../catalyst/analysis/PaimonAnalysis.scala` — `computeExpectedAttrs` for catalog write
- `paimon-spark/.../catalyst/analysis/MergeSchemaEvolutionHelper.scala` — MERGE INTO evolution
- `paimon-spark/.../SparkConnectorOptions.java` — `TYPE_WIDENING` option
