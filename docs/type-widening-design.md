# Type Widening Design (Paimon Spark schema-evolution-on-write)

## Problem

`merge-schema` coupled column-addition with unconditional type widening in
`SchemaMergingUtils.merge`, causing:
1. ARRAY<INT> -> ARRAY<BIGINT> crash (CastExecutors.resolve returns null)
2. Inconsistent behavior: path-write/MERGE widen, catalog INSERT/saveAsTable don't
3. `explicit-cast` semantics differ per entry point

## Solution (Implemented)

Mirror Delta's `enableTypeWidening` with an explicit switch:
`write.merge-schema.type-widening` (default false).

### Core design (SchemaMergingUtils.merge)

When `typeWidening=false` (default): existing column types are KEPT (return base0).
Only new columns evolve the schema. Data is cast to the target type by the alignment
layer (Spark Cast in PaimonOutputResolver/alignColumns).

When `typeWidening=true`: existing columns widen to the incoming wider type (original
behavior). `explicit-cast` is honored only here as a lossy sub-modifier.

### Write path flow (target architecture)

All paths follow: **compute finalSchema -> cast data to finalSchema -> commit schema**

| Entry | finalSchema computation | Cast | Commit |
|-------|------------------------|------|--------|
| V1 path-write `save(location)` | SchemaHelper.mergeSchema (execution) | alignColumns | execution |
| V1 catalog `saveAsTable` / SQL INSERT | PaimonAnalysis (analysis, byName only) | PaimonOutputResolver | execution (WriteIntoPaimonTable) |
| V2 catalog | PaimonAnalysis (analysis, byName only) | PaimonOutputResolver | planning (PaimonV2Write) |
| MERGE INTO | evolveTargetIfNeeded (analysis) | alignAllMergeActions | analysis (commit in evolve) |

### Commit timing

- V1 path/catalog: execution (WriteIntoPaimonTable.run -> SchemaHelper.mergeSchema)
- V2 catalog: planning (PaimonV2Write constructor -> SchemaHelper.mergeSchema)
- MERGE INTO: analysis (evolveTargetIfNeeded -> SchemaHelper.mergeAndCommitSchema)

MERGE INTO commit cannot easily be deferred to execution because its target-read
plan depends on the committed schema in the DataSourceV2Relation. Deferring would
require reconstructing the target relation at execution time — left as follow-up.

### Known limitations

1. `typeWidening=true` + complex element widening (ARRAY<INT> -> ARRAY<BIGINT>)
   throws at SchemaManager.generateTableSchema (CastExecutors has no ARRAY cast rule).
   Asserted in test. Follow-up: fix SchemaManager complex-type cast validation.

2. Catalog INSERT typeWidening only works with BY NAME writes. Position-based
   INSERT INTO ... VALUES uses anonymous column names that cannot be name-matched.

3. V2 write commit remains at planning time (not deferred to execution).
   Follow-up: move commit to toBatch for full Delta-style deferral.

## Config

| Option | Default | Description |
|--------|---------|-------------|
| `write.merge-schema` | false | Enable schema evolution (add columns) |
| `write.merge-schema.type-widening` | false | Enable type widening for existing columns (only when merge-schema=true) |
| `write.merge-schema.explicit-cast` | false | Allow lossy type changes (only when type-widening=true) |

## Files changed

- `paimon-core/.../schema/SchemaMergingUtils.java` — typeWidening param, keep-existing branch
- `paimon-core/.../schema/SchemaManager.java` — thread typeWidening
- `paimon-core/.../FileStore.java`, `AbstractFileStore.java`, `PrivilegedFileStore.java` — thread
- `paimon-spark/.../SparkConnectorOptions.java` — TYPE_WIDENING option
- `paimon-spark/.../util/OptionUtils.scala` — accessor
- `paimon-spark/.../commands/SchemaHelper.scala` — thread + computeMergedSparkSchema + alignColumns cast
- `paimon-spark/.../catalyst/analysis/MergeSchemaEvolutionHelper.scala` — pass typeWidening, raw source types
- `paimon-spark/.../catalyst/analysis/PaimonAnalysis.scala` — finalSchema as resolver expected (byName)
