# Review Notes

## Branch: `feature/edl-compiler`

- **Parser enhancements**
  - Added `mergeTopLevelMetadata` so legacy top-level `urls`, `has`, `timeframes`, and `requiredCredentials` blocks are merged into the `exchange` object (`edl/compiler/src/parser/index.ts`).
  - Normalized parser definitions by honoring `iterator` aliases, array flags, and structured `postProcess` entries.

- **Code generation improvements**
  - Rebuilt the parser generator to:
    - Normalize array responses (`Array.isArray` guard) and capture `rawData`.
    - Emit computed fields using a new `raw` AST node and a lightweight placeholder expander.
    - Apply parser-level `postProcess` callbacks and honor literal/context mappings.
  - Added endpoint resolution so fetch helpers call the HTTP method declared in the DSL instead of hard-coded names.
  - Extended the AST/emitter to support raw expressions used by compute templates.

- **Tests**
  - Added `edl/compiler/src/__tests__/compiler.test.ts` covering metadata merging, array parser compute fields, and endpoint resolution.
  - Commands run:
    - `cd edl/compiler && npm install`
    - `cd edl/compiler && npm run build`
    - `cd edl/compiler && npm test`

## Branch: `feature/data-lake`

- **CLI updates**
  - Re-enabled backfill/advanced command registration.
  - Implemented `buildBackendConfig` with support for `--connection` (TimescaleDB) and `--s3-*` flags for Parquet deployments; applied the new options across `stats`, `gaps`, and `query`.

- **Parquet backend hardening**
  - Base64url-encoded partition path segments to prevent unsafe characters and decode them when rebuilding the index.
  - Added idle-time flushing (5s TTL) and ensured buffers flush immediately when `writeMany` is invoked with a custom `batchSize`.

- **Tests**
  - Updated Parquet tests to assert encoded paths, verify symbols containing `/`, and confirm batch-flush behavior.
  - Commands run from the worktree at `../ccxt-data-lake-worktree/lake`:
    - `npm install`
    - `npx vitest run`
    - `npx vitest run tests/benchmarks/throughput.test.ts`

