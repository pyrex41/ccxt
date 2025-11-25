/**
 * Benchmark utilities and helpers for CCXT Data Lake
 */

import type { DataPoint, Candle } from '../../src/types.js';

/**
 * Generate test OHLCV data for benchmarking
 * @param count Number of data points to generate
 * @param options Options for data generation
 * @returns Array of test data points
 */
export function generateTestData(
  count: number,
  options: {
    exchange?: string;
    symbol?: string;
    timeframe?: string;
    startTime?: number;
    interval?: number;
  } = {}
): DataPoint[] {
  const {
    exchange = 'binance',
    symbol = 'BTCUSDT',
    timeframe = '1m',
    startTime = Date.now(),
    interval = 60000, // 1 minute in ms
  } = options;

  const points: DataPoint[] = [];

  for (let i = 0; i < count; i++) {
    const timestamp = startTime + i * interval;
    points.push({
      timestamp,
      exchange,
      symbol,
      timeframe,
      data: {
        timestamp,
        open: 50000 + Math.random() * 1000,
        high: 51000 + Math.random() * 1000,
        low: 49000 + Math.random() * 1000,
        close: 50500 + Math.random() * 1000,
        volume: 100 + Math.random() * 50,
      } as Candle,
    });
  }

  return points;
}

/**
 * Generate test data with intentional gaps
 * @param count Number of data points to generate
 * @param gapFrequency Insert gap every N candles
 * @param gapSize Size of gap in intervals
 * @param options Additional options
 * @returns Array of test data points with gaps
 */
export function generateTestDataWithGaps(
  count: number,
  gapFrequency: number,
  gapSize: number,
  options: {
    exchange?: string;
    symbol?: string;
    timeframe?: string;
    startTime?: number;
    interval?: number;
  } = {}
): DataPoint[] {
  const {
    exchange = 'binance',
    symbol = 'BTCUSDT',
    timeframe = '1m',
    startTime = Date.now(),
    interval = 60000,
  } = options;

  const points: DataPoint[] = [];
  let currentTime = startTime;

  for (let i = 0; i < count; i++) {
    points.push({
      timestamp: currentTime,
      exchange,
      symbol,
      timeframe,
      data: {
        timestamp: currentTime,
        open: 50000 + Math.random() * 1000,
        high: 51000 + Math.random() * 1000,
        low: 49000 + Math.random() * 1000,
        close: 50500 + Math.random() * 1000,
        volume: 100 + Math.random() * 50,
      } as Candle,
    });

    // Add gap after every N candles
    if ((i + 1) % gapFrequency === 0) {
      currentTime += interval * gapSize;
    } else {
      currentTime += interval;
    }
  }

  return points;
}

/**
 * Measure execution time of an async function
 * @param fn Function to measure
 * @returns Execution time in milliseconds
 */
export async function measureTime(fn: () => Promise<void>): Promise<number> {
  const start = performance.now();
  await fn();
  const end = performance.now();
  return end - start;
}

/**
 * Calculate throughput in operations per second
 * @param count Number of operations
 * @param timeMs Time in milliseconds
 * @returns Throughput (ops/sec)
 */
export function calculateThroughput(count: number, timeMs: number): number {
  return (count / timeMs) * 1000;
}

/**
 * Format throughput for display
 * @param throughput Throughput in ops/sec
 * @returns Formatted string
 */
export function formatThroughput(throughput: number): string {
  if (throughput >= 1000000) {
    return `${(throughput / 1000000).toFixed(2)}M ops/sec`;
  } else if (throughput >= 1000) {
    return `${(throughput / 1000).toFixed(2)}K ops/sec`;
  } else {
    return `${throughput.toFixed(0)} ops/sec`;
  }
}

/**
 * Run a benchmark and print results
 * @param name Benchmark name
 * @param count Number of operations
 * @param fn Function to benchmark
 * @returns Throughput in ops/sec
 */
export async function runBenchmark(
  name: string,
  count: number,
  fn: () => Promise<void>
): Promise<number> {
  const timeMs = await measureTime(fn);
  const throughput = calculateThroughput(count, timeMs);

  console.log(
    `${name}: ${count} ops in ${timeMs.toFixed(2)}ms = ${formatThroughput(throughput)}`
  );

  return throughput;
}

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
export async function runBenchmarkSuite(
  config: BenchmarkConfig,
  fn: () => Promise<void>
): Promise<BenchmarkResult> {
  const iterations = config.iterations || 5;
  const warmup = config.warmup || 1;
  const results: number[] = [];

  // Warmup runs
  for (let i = 0; i < warmup; i++) {
    await fn();
  }

  // Actual benchmark runs
  for (let i = 0; i < iterations; i++) {
    const timeMs = await measureTime(fn);
    const throughput = calculateThroughput(config.count, timeMs);
    results.push(throughput);
  }

  // Calculate statistics
  results.sort((a, b) => a - b);
  const avgThroughput = results.reduce((a, b) => a + b, 0) / results.length;
  const minThroughput = results[0];
  const maxThroughput = results[results.length - 1];
  const medianThroughput = results[Math.floor(results.length / 2)];

  console.log(`\n${config.name}`);
  console.log(`Description: ${config.description}`);
  console.log(`Iterations: ${iterations}`);
  console.log(`Average: ${formatThroughput(avgThroughput)}`);
  console.log(`Min: ${formatThroughput(minThroughput)}`);
  console.log(`Max: ${formatThroughput(maxThroughput)}`);
  console.log(`Median: ${formatThroughput(medianThroughput)}`);

  return {
    config,
    avgThroughput,
    minThroughput,
    maxThroughput,
    medianThroughput,
    iterations: results,
  };
}
