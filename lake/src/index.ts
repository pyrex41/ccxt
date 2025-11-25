/**
 * CCXT Data Lake
 * Main entry point for the library
 */

// Export core types
export type {
  Candle,
  DataPoint,
  Gap,
  DataQuery,
  BackendConfig,
  LakeStats,
} from './types.js';

// Export backend interface
export type {
  Backend,
  WriteOptions,
  ReadOptions,
  SearchOptions,
  SearchResult,
  GapDetectionOptions,
  ProgressCallback,
  BackfillOptions,
  ConnectionState,
  BackendFactory,
} from './interface.js';

export { BaseBackend, createBackend } from './interface.js';

// Export backend implementations (to be added in subsequent tasks)
export * from './backends/index.js';
