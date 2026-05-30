# Type Widening Design

## Problem

`merge-schema` coupled column-addition with unconditional type widening, causing:
1. `ARRAY<INT>→ARRAY<BIGINT>` crash (`CastExecutors.resolve` returns null)
2. Inconsistent type-widening behavior between catalog writes and `MERGE INTO`

## Scope

Schema evolution is supported for **catalog writes** (`saveAsTable`, SQL INSERT,
`writeTo(...)`) and `MERGE INTO`. **Path-based DataFrame write**
(`df.write.format("paimon").save("/path")`) and the **streaming sink**
(`writeStream...start(path)`) go through the V1 DataSource API
(`SparkSource.createRelation` / `createSink`), bypass `PaimonAnalysis`, and have no
analyzer hook to align the incoming data. They commit the evolved schema, but the
data is written as-is — so evolution works only when the incoming columns already
match the evolved layout (same names/order, new columns appended). For
reorder / null-fill / type-cast, write to a catalog table instead.

## Solution

Add `write.merge-schema.type-widening` (default `false`, mirrors Delta's
`enableTypeWidening`), decoupling type widening from column addition.

## Config

| Option | Default | Semantics |
|--------|---------|-----------|
| `write.merge-schema` | `false` | Enable schema evolution (column additions) |
| `write.merge-schema.type-widening` | `false` | Also widen existing column types (only when merge-schema=true) |
| `write.merge-schema.explicit-cast` | `false` | Also allow lossy type changes (only when type-widening=true) |

## Core Logic (`SchemaMergingUtils.merge`)

```
typeWidening=false (default) → keep base type for existing columns, return base0
typeWidening=true            → widen: Decimal precision↑, length↑, supportsCast-safe types
typeWidening=true + explicit → also allow lossy casts (BIGINT→INT etc)
New columns                  → always added regardless of typeWidening
```

## Write Path Architecture

`SchemaEvolutionHelper` (trait + companion object) holds the building blocks:
- `computeFinalSchema` — pure computation of the post-evolution schema (no side effects)
- `commitSchemaEvolution` — filter system columns, resolve flags, persist the evolved
  schema (idempotent; shared by catalog writes and `MERGE INTO`)
- `expectedAttrsForCatalogWrite` — the resolver's expected attrs for a catalog write:
  post-evolution attrs when type-widening + `byName`, else `table.output`
- `toAttributes` / `readFlags` — StructType → attrs; options/conf → `SchemaEvolutionFlags`

### Ordering by path

| Path | Compute | Cast | Commit |
|------|---------|------|--------|
| **V1 catalog** `saveAsTable`/INSERT | `PaimonAnalysis` → `expectedAttrsForCatalogWrite` (analysis) | `PaimonOutputResolver` (analysis) | trait `mergeSchema(DataFrame)` → `commitSchemaEvolution` (execution, `WriteIntoPaimonTable.run`) |
| **V2 catalog** (use-v2-write=true) | `PaimonAnalysis` → `expectedAttrsForCatalogWrite` (analysis) | `PaimonOutputResolver` (analysis) | trait `mergeSchema(StructType)` → `commitSchemaEvolution` (execution, `PaimonV2Write.toBatch`) |
| **MERGE INTO** | `evolveTargetIfNeeded` → `evolvedTableInMemory` (analysis, no persist) | `alignAllMergeActions` (analysis) | `commitEvolvedSchemaAtExecution` (execution, merge command `run`) |

Schema migration is always **computed during analysis but committed at execution**,
so analysis/planning stays side-effect-free (an `EXPLAIN` or re-planned write never
mutates the table schema). MERGE INTO presents the new columns to the plan via an
in-memory `FileStoreTable.copy(TableSchema)`; `mergeSchemas` assigns the next schema
id deterministically, so the execution-time commit reproduces the same schema. The
trait `mergeSchema(DataFrame)` overload returns the input unchanged (already cast by
the resolver); `mergeSchema(StructType)` returns the write schema.

## Known Limitations

1. **Path-based / streaming writes don't align data**: `.save(path)` and
   `writeStream...start(path)` go through `SparkSource.createRelation` / `createSink`,
   bypass `PaimonAnalysis`, and have no analyzer hook. They commit the evolved schema
   but write data as-is, so evolution works only when the incoming layout already
   matches. Use `saveAsTable` / SQL INSERT / `writeTo(...)` otherwise.
2. **Complex element widening**: `ARRAY<INT>→ARRAY<BIGINT>` + `type-widening=true` throws
   (`SchemaManager.generateTableSchema` → `CastExecutors.resolve` = null). Follow-up.
3. **Position-based INSERT**: `INSERT INTO t VALUES(...)` — anonymous column names can't
   be name-matched, so `expectedAttrsForCatalogWrite` returns `table.output` (only
   `byName` writes compute the evolved expected attrs).

## Key Files

- `paimon-core/.../schema/SchemaMergingUtils.java` — `merge()` with typeWidening branch
- `paimon-spark/.../commands/SchemaEvolutionHelper.scala` — building blocks + trait entry points
- `paimon-spark/.../catalyst/analysis/PaimonAnalysis.scala` — calls `expectedAttrsForCatalogWrite` for catalog write
- `paimon-spark/.../catalyst/analysis/MergeSchemaEvolutionHelper.scala` — `MERGE INTO` evolution
- `paimon-spark/.../catalyst/analysis/PaimonOutputResolver.scala` — column alignment / cast
- `paimon-spark/.../SparkConnectorOptions.java` — `TYPE_WIDENING` / `EXPLICIT_CAST` options
