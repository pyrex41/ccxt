/**
 * Benchmark utilities and helpers for CCXT Data Lake
 */
import type { DataPoint } from '../../src/types.js';
/**
 * Generate test OHLCV data for benchmarking
 * @param count Number of data points to generate
 * @param options Options for data generation
 * @returns Array of test data points
 */
export declare function generateTestData(count: number, options?: {
    exchange?: string;
    symbol?: string;
    timeframe?: string;
    startTime?: number;
    interval?: number;
}): DataPoint[];
/**
 * Generate test data with intentional gaps
 * @param count Number of data points to generate
 * @param gapFrequency Insert gap every N candles
 * @param gapSize Size of gap in intervals
 * @param options Additional options
 * @returns Array of test data points with gaps
 */
export declare function generateTestDataWithGaps(count: number, gapFrequency: number, gapSize: number, options?: {
    exchange?: string;
    symbol?: string;
    timeframe?: string;
    startTime?: number;
    interval?: number;
}): DataPoint[];
/**
 * Measure execution time of an async function
 * @param fn Function to measure
 * @returns Execution time in milliseconds
 */
export declare function measureTime(fn: () => Promise<void>): Promise<number>;
/**
 * Calculate throughput in operations per second
 * @param count Number of operations
 * @param timeMs Time in milliseconds
 * @returns Throughput (ops/sec)
 */
export declare function calculateThroughput(count: number, timeMs: number): number;
/**
 * Format throughput for display
 * @param throughput Throughput in ops/sec
 * @returns Formatted string
 */
export declare function formatThroughput(throughput: number): string;
/**
 * Run a benchmark and print results
 * @param name Benchmark name
 * @param count Number of operations
 * @param fn Function to benchmark
 * @returns Throughput in ops/sec
 */
export declare function runBenchmark(name: string, count: number, fn: () => Promise<void>): Promise<number>;
/**
 * Benchmark configuration
 */
export interface BenchmarkConfig {
    name: string;
    description: string;
    count: number;
    warmup?: number;
    iterations?: number;
    timeout?: number;
}
/**
 * Benchmark result
 */
export interface BenchmarkResult {
    config: BenchmarkConfig;
    avgThroughput: number;
    minThroughput: number;
    maxThroughput: number;
    medianThroughput: number;
    iterations: number[];
}
/**
 * Run multiple iterations of a benchmark
 * @param config Benchmark configuration
 * @param fn Function to benchmark
 * @returns Benchmark results
 */
export declare function runBenchmarkSuite(config: BenchmarkConfig, fn: () => Promise<void>): Promise<BenchmarkResult>;
//# sourceMappingURL=index.d.ts.map