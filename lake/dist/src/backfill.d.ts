/**
 * Backfill orchestrator for CCXT Data Lake
 * Fetches historical OHLCV data from exchanges
 */
import type { Backend, BackfillOptions } from './interface.js';
import type { DataQuery } from './types.js';
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
    has: {
        fetchOHLCV: boolean;
    };
}
/**
 * Backfill orchestrator class
 *
 * Coordinates fetching historical data from exchanges and writing to the data lake.
 * Handles rate limiting, retries, gap filling, and progress reporting.
 */
export declare class BackfillOrchestrator {
    private backend;
    private options;
    constructor(backend: Backend, options?: BackfillOptions);
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
    backfill(exchange: ExchangeAdapter, request: BackfillRequest): Promise<BackfillResult>;
    /**
     * Execute backfill for multiple requests
     * Processes requests sequentially to respect rate limits
     *
     * @param exchanges Map of exchange adapters by ID
     * @param requests Array of backfill requests
     * @returns Array of results for each request
     */
    backfillMany(exchanges: Map<string, ExchangeAdapter>, requests: BackfillRequest[]): Promise<BackfillResult[]>;
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
    fillGaps(exchange: ExchangeAdapter, query: DataQuery): Promise<BackfillResult>;
    /**
     * Sleep helper for rate limiting
     */
    private sleep;
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
    private fetchWithRetry;
    /**
     * Calculate timeframe in milliseconds
     * Supports: s (seconds), m (minutes), h (hours), d (days), w (weeks), M (months)
     */
    private getTimeframeMs;
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
export declare function parseTimestamp(input: string | number | undefined): number | undefined;
//# sourceMappingURL=backfill.d.ts.map