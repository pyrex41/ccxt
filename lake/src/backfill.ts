/**
 * Backfill orchestrator for CCXT Data Lake
 * Fetches historical OHLCV data from exchanges
 */

import type { Backend, BackfillOptions } from './interface.js';
import type { DataPoint, DataQuery } from './types.js';

/**
 * Backfill request specification
 */
export interface BackfillRequest {
  exchange: string;
  symbol: string;
  timeframe: string;
  since?: number;
  until?: number;
}

/**
 * Result of a backfill operation
 */
export interface BackfillResult {
  exchange: string;
  symbol: string;
  timeframe: string;
  candlesWritten: number;
  startTimestamp?: number;
  endTimestamp?: number;
  gapsRemaining: number;
  errors: string[];
}

/**
 * Exchange adapter interface
 * Abstracts CCXT exchange for testing and flexibility
 */
export interface ExchangeAdapter {
  id: string;
  fetchOHLCV(symbol: string, timeframe: string, since?: number, limit?: number): Promise<number[][]>;
  rateLimit: number;
  has: { fetchOHLCV: boolean };
}

/**
 * Backfill orchestrator class
 *
 * Coordinates fetching historical data from exchanges and writing to the data lake.
 * Handles rate limiting, retries, gap filling, and progress reporting.
 */
export class BackfillOrchestrator {
  private backend: Backend;
  private options: BackfillOptions;

  constructor(backend: Backend, options: BackfillOptions = {}) {
    this.backend = backend;
    this.options = {
      rateLimit: options.rateLimit || 1, // requests per second
      retries: options.retries || 3,
      retryDelay: options.retryDelay || 1000,
      stopOnError: options.stopOnError || false,
      onProgress: options.onProgress,
    };
  }

  /**
   * Execute backfill for a single request
   *
   * Strategy:
   * 1. Check existing data range in backend
   * 2. Determine what needs to be fetched
   * 3. Fetch data in chunks (typically 1000 candles per request)
   * 4. Write to backend in batches
   * 5. Report progress throughout
   *
   * @param exchange Exchange adapter to fetch from
   * @param request Backfill request specification
   * @returns Result summary with statistics
   */
  async backfill(
    exchange: ExchangeAdapter,
    request: BackfillRequest
  ): Promise<BackfillResult> {
    const { exchange: exchangeName, symbol, timeframe, since, until } = request;
    const errors: string[] = [];
    let candlesWritten = 0;
    let startTimestamp: number | undefined;
    let endTimestamp: number | undefined;

    try {
      // Check if exchange supports OHLCV
      if (!exchange.has.fetchOHLCV) {
        throw new Error(`Exchange ${exchange.id} does not support fetchOHLCV`);
      }

      // Get existing time range
      const existingRange = await this.backend.getTimeRange({
        exchange: exchangeName,
        symbol,
        timeframe,
      });

      // Determine fetch range
      let fetchSince = since || (existingRange ? existingRange.latest : Date.now() - 365 * 24 * 60 * 60 * 1000);
      const fetchUntil = until || Date.now();

      // If we have existing data and no explicit since, start from latest
      if (existingRange && !since) {
        fetchSince = existingRange.latest;
      }

      // Fetch in chunks
      let currentSince = fetchSince;
      const timeframeMs = this.getTimeframeMs(timeframe);
      const limit = 1000; // Standard batch size for most exchanges
      let totalFetched = 0;
      const estimatedTotal = Math.ceil((fetchUntil - fetchSince) / (timeframeMs * limit));

      while (currentSince < fetchUntil) {
        try {
          // Fetch OHLCV data
          const candles = await this.fetchWithRetry(
            exchange,
            symbol,
            timeframe,
            currentSince,
            limit
          );

          if (candles.length === 0) {
            break; // No more data available
          }

          // Convert to DataPoint format
          const dataPoints: DataPoint[] = candles.map((candle) => ({
            timestamp: candle[0],
            exchange: exchangeName,
            symbol,
            timeframe,
            data: {
              timestamp: candle[0],
              open: candle[1],
              high: candle[2],
              low: candle[3],
              close: candle[4],
              volume: candle[5],
            },
          }));

          // Filter candles within range
          const filteredPoints = dataPoints.filter(
            (p) => p.timestamp >= fetchSince && p.timestamp <= fetchUntil
          );

          if (filteredPoints.length > 0) {
            // Write to backend
            const written = await this.backend.writeMany(filteredPoints, { upsert: true });
            candlesWritten += written;

            // Update timestamps
            if (!startTimestamp || filteredPoints[0].timestamp < startTimestamp) {
              startTimestamp = filteredPoints[0].timestamp;
            }
            if (!endTimestamp || filteredPoints[filteredPoints.length - 1].timestamp > endTimestamp) {
              endTimestamp = filteredPoints[filteredPoints.length - 1].timestamp;
            }

            // Move to next batch
            currentSince = candles[candles.length - 1][0] + timeframeMs;
          } else {
            break;
          }

          // Report progress
          totalFetched++;
          if (this.options.onProgress) {
            this.options.onProgress({
              current: totalFetched,
              total: estimatedTotal,
              exchange: exchangeName,
              symbol,
              timeframe,
              message: `Fetched ${candlesWritten} candles`,
            });
          }

          // Rate limiting
          await this.sleep(1000 / (this.options.rateLimit || 1));

        } catch (error) {
          const errorMsg = error instanceof Error ? error.message : String(error);
          errors.push(`Error at ${currentSince}: ${errorMsg}`);

          if (this.options.stopOnError) {
            throw error;
          }

          // Skip to next interval
          currentSince += timeframeMs * limit;
        }
      }

      // Check for remaining gaps
      const gaps = await this.backend.findGaps({
        exchange: exchangeName,
        symbol,
        timeframe,
        since: fetchSince,
        until: fetchUntil,
      });

      return {
        exchange: exchangeName,
        symbol,
        timeframe,
        candlesWritten,
        startTimestamp,
        endTimestamp,
        gapsRemaining: gaps.length,
        errors,
      };

    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      errors.push(`Fatal error: ${errorMsg}`);

      return {
        exchange: exchangeName,
        symbol,
        timeframe,
        candlesWritten,
        startTimestamp,
        endTimestamp,
        gapsRemaining: 0,
        errors,
      };
    }
  }

  /**
   * Execute backfill for multiple requests
   * Processes requests sequentially to respect rate limits
   *
   * @param exchanges Map of exchange adapters by ID
   * @param requests Array of backfill requests
   * @returns Array of results for each request
   */
  async backfillMany(
    exchanges: Map<string, ExchangeAdapter>,
    requests: BackfillRequest[]
  ): Promise<BackfillResult[]> {
    const results: BackfillResult[] = [];

    for (const request of requests) {
      const exchange = exchanges.get(request.exchange);
      if (!exchange) {
        results.push({
          exchange: request.exchange,
          symbol: request.symbol,
          timeframe: request.timeframe,
          candlesWritten: 0,
          gapsRemaining: 0,
          errors: [`Exchange ${request.exchange} not found`],
        });
        continue;
      }

      const result = await this.backfill(exchange, request);
      results.push(result);
    }

    return results;
  }

  /**
   * Fill gaps in existing data
   *
   * Identifies gaps using backend.findGaps() and fills them
   * by fetching the missing data from the exchange.
   *
   * @param exchange Exchange adapter to fetch from
   * @param query Query defining the range to check for gaps
   * @returns Result summary
   */
  async fillGaps(
    exchange: ExchangeAdapter,
    query: DataQuery
  ): Promise<BackfillResult> {
    const { exchange: exchangeName, symbol, timeframe } = query;

    // Find all gaps
    const gaps = await this.backend.findGaps(query);

    if (gaps.length === 0) {
      return {
        exchange: exchangeName,
        symbol,
        timeframe,
        candlesWritten: 0,
        gapsRemaining: 0,
        errors: [],
      };
    }

    // Report initial gap count
    if (this.options.onProgress) {
      this.options.onProgress({
        current: 0,
        total: gaps.length,
        exchange: exchangeName,
        symbol,
        timeframe,
        message: `Found ${gaps.length} gaps to fill`,
      });
    }

    let totalCandlesWritten = 0;
    const errors: string[] = [];
    let startTimestamp: number | undefined;
    let endTimestamp: number | undefined;

    // Fill each gap
    for (let i = 0; i < gaps.length; i++) {
      const gap = gaps[i];

      try {
        const result = await this.backfill(exchange, {
          exchange: exchangeName,
          symbol,
          timeframe,
          since: gap.start,
          until: gap.end,
        });

        totalCandlesWritten += result.candlesWritten;
        errors.push(...result.errors);

        if (result.startTimestamp) {
          if (!startTimestamp || result.startTimestamp < startTimestamp) {
            startTimestamp = result.startTimestamp;
          }
        }
        if (result.endTimestamp) {
          if (!endTimestamp || result.endTimestamp > endTimestamp) {
            endTimestamp = result.endTimestamp;
          }
        }

        // Report progress
        if (this.options.onProgress) {
          this.options.onProgress({
            current: i + 1,
            total: gaps.length,
            exchange: exchangeName,
            symbol,
            timeframe,
            message: `Filled gap ${i + 1}/${gaps.length}`,
          });
        }

      } catch (error) {
        const errorMsg = error instanceof Error ? error.message : String(error);
        errors.push(`Error filling gap ${gap.start}-${gap.end}: ${errorMsg}`);

        if (this.options.stopOnError) {
          break;
        }
      }
    }

    // Check remaining gaps
    const remainingGaps = await this.backend.findGaps(query);

    return {
      exchange: exchangeName,
      symbol,
      timeframe,
      candlesWritten: totalCandlesWritten,
      startTimestamp,
      endTimestamp,
      gapsRemaining: remainingGaps.length,
      errors,
    };
  }

  /**
   * Sleep helper for rate limiting
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Fetch with retry logic using exponential backoff
   *
   * @param exchange Exchange adapter
   * @param symbol Trading symbol
   * @param timeframe Candle timeframe
   * @param since Start timestamp
   * @param limit Number of candles to fetch
   * @returns Array of OHLCV arrays
   */
  private async fetchWithRetry(
    exchange: ExchangeAdapter,
    symbol: string,
    timeframe: string,
    since: number,
    limit: number
  ): Promise<number[][]> {
    let lastError: Error | null = null;
    const retries = this.options.retries || 3;
    const baseDelay = this.options.retryDelay || 1000;

    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        return await exchange.fetchOHLCV(symbol, timeframe, since, limit);
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));

        if (attempt < retries) {
          // Exponential backoff: delay * 2^attempt
          const delay = baseDelay * Math.pow(2, attempt);
          await this.sleep(delay);
        }
      }
    }

    throw lastError || new Error('Failed to fetch OHLCV data');
  }

  /**
   * Calculate timeframe in milliseconds
   * Supports: s (seconds), m (minutes), h (hours), d (days), w (weeks), M (months)
   */
  private getTimeframeMs(timeframe: string): number {
    const units: Record<string, number> = {
      's': 1000,
      'm': 60 * 1000,
      'h': 60 * 60 * 1000,
      'd': 24 * 60 * 60 * 1000,
      'w': 7 * 24 * 60 * 60 * 1000,
      'M': 30 * 24 * 60 * 60 * 1000, // Approximate month
    };

    const match = timeframe.match(/^(\d+)([smhdwM])$/);
    if (!match) {
      throw new Error(`Invalid timeframe format: ${timeframe}`);
    }

    const [, value, unit] = match;
    const multiplier = units[unit];
    if (!multiplier) {
      throw new Error(`Unknown timeframe unit: ${unit}`);
    }

    return parseInt(value, 10) * multiplier;
  }
}

/**
 * Helper function to parse timestamp from various formats
 * Supports:
 * - Unix timestamp in milliseconds (number)
 * - ISO date string (string)
 * - Relative time: "1d", "7d", "30d", "1w", "1M", "1y"
 *
 * @param input Timestamp input
 * @returns Unix timestamp in milliseconds
 */
export function parseTimestamp(input: string | number | undefined): number | undefined {
  if (input === undefined) {
    return undefined;
  }

  // If already a number, return as-is
  if (typeof input === 'number') {
    return input;
  }

  // Try parsing as ISO date
  const isoDate = Date.parse(input);
  if (!isNaN(isoDate)) {
    return isoDate;
  }

  // Try parsing as relative time
  const relativeMatch = input.match(/^(\d+)([dwMy])$/);
  if (relativeMatch) {
    const [, value, unit] = relativeMatch;
    const amount = parseInt(value, 10);
    const now = Date.now();

    switch (unit) {
      case 'd':
        return now - amount * 24 * 60 * 60 * 1000;
      case 'w':
        return now - amount * 7 * 24 * 60 * 60 * 1000;
      case 'M':
        return now - amount * 30 * 24 * 60 * 60 * 1000;
      case 'y':
        return now - amount * 365 * 24 * 60 * 60 * 1000;
    }
  }

  throw new Error(`Invalid timestamp format: ${input}`);
}
