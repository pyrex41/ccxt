/**
 * CCXT Data Lake
 * Main entry point for the library
 */
export { BaseBackend, createBackend } from './interface.js';
// Export backend implementations (to be added in subsequent tasks)
export * from './backends/index.js';
// Export backfill orchestrator
export { BackfillOrchestrator, parseTimestamp, } from './backfill.js';
//# sourceMappingURL=index.js.map