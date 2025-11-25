/**
 * CCXT Data Lake
 * Main entry point for the library
 */
export type { Candle, DataPoint, Gap, DataQuery, BackendConfig, LakeStats, } from './types.js';
export type { Backend, WriteOptions, ReadOptions, SearchOptions, SearchResult, GapDetectionOptions, ProgressCallback, BackfillOptions, ConnectionState, BackendFactory, } from './interface.js';
export { BaseBackend, createBackend } from './interface.js';
export * from './backends/index.js';
export { BackfillOrchestrator, parseTimestamp, } from './backfill.js';
export type { BackfillRequest, BackfillResult, ExchangeAdapter, } from './backfill.js';
//# sourceMappingURL=index.d.ts.map