/**
 * CCXT Data Lake
 * Main entry point for the library
 */
export type { Candle, DataPoint, Gap, DataQuery, BackendConfig, LakeStats, } from './types.js';
export type { Backend, WriteOptions, ReadOptions, SearchOptions, SearchResult, GapDetectionOptions, ProgressCallback, BackfillOptions, ConnectionState, BackendFactory, } from './interface.js';
export { BaseBackend, createBackend } from './interface.js';
export * from './backends/index.js';
//# sourceMappingURL=index.d.ts.map