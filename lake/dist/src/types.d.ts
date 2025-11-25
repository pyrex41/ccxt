/**
 * Core type definitions for CCXT Data Lake
 */
/**
 * Candle OHLCV data
 */
export interface Candle {
    timestamp: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
}
/**
 * Generic data point
 */
export interface DataPoint {
    timestamp: number;
    exchange: string;
    symbol: string;
    timeframe: string;
    data: Candle | Record<string, unknown>;
}
/**
 * Gap in data
 */
export interface Gap {
    start: number;
    end: number;
    exchange: string;
    symbol: string;
    timeframe: string;
}
/**
 * Query parameters for retrieving data
 */
export interface DataQuery {
    exchange: string;
    symbol: string;
    timeframe: string;
    since?: number;
    until?: number;
    limit?: number;
}
/**
 * Backend configuration
 */
export interface BackendConfig {
    type: 'sqlite' | 'parquet' | 'timescaledb';
    path?: string;
    connectionString?: string;
    s3?: {
        bucket: string;
        region: string;
        accessKeyId?: string;
        secretAccessKey?: string;
    };
}
/**
 * Statistics about stored data
 */
export interface LakeStats {
    totalCandles: number;
    exchanges: string[];
    symbols: string[];
    timeframes: string[];
    oldestTimestamp?: number;
    newestTimestamp?: number;
}
//# sourceMappingURL=types.d.ts.map