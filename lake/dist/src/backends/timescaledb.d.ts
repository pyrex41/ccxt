/**
 * TimescaleDB backend for CCXT Data Lake
 * Uses pg with hypertables for time-series data
 */
import type { BackendConfig } from '../types.js';
import { BaseBackend } from '../interface.js';
import type { WriteOptions, ReadOptions, GapDetectionOptions } from '../interface.js';
import type { Candle, DataPoint, DataQuery, Gap, LakeStats } from '../types.js';
/**
 * TimescaleDB backend implementation
 *
 * Features:
 * - Connection pooling with pg.Pool
 * - Hypertable partitioning for time-series optimization
 * - Compression policies for older data
 * - Window functions for efficient gap detection
 * - Batch upserts with ON CONFLICT
 */
export declare class TimescaleDBBackend extends BaseBackend {
    readonly type: "timescaledb";
    private pool;
    private config;
    constructor(config: BackendConfig);
    /**
     * Connect to TimescaleDB and setup schema
     */
    connect(): Promise<void>;
    /**
     * Disconnect from TimescaleDB
     */
    disconnect(): Promise<void>;
    /**
     * Setup database schema with hypertable and policies
     */
    private setupSchema;
    /**
     * Write a single data point
     */
    write(point: DataPoint, options?: WriteOptions): Promise<void>;
    /**
     * Write multiple data points in a batch
     */
    writeMany(points: DataPoint[], options?: WriteOptions): Promise<number>;
    /**
     * Read candles matching the query
     */
    read(query: DataQuery, options?: ReadOptions): Promise<Candle[]>;
    /**
     * Find gaps in the data using window functions
     */
    findGaps(query: DataQuery, options?: GapDetectionOptions): Promise<Gap[]>;
    /**
     * Get statistics about stored data
     */
    getStats(exchange?: string, symbol?: string): Promise<LakeStats>;
    /**
     * Check if data exists for the given query
     */
    exists(query: DataQuery): Promise<boolean>;
    /**
     * Delete data matching the query
     */
    delete(query: DataQuery): Promise<number>;
    /**
     * Get the earliest and latest timestamps for a query
     */
    getTimeRange(query: Omit<DataQuery, 'since' | 'until' | 'limit'>): Promise<{
        earliest: number;
        latest: number;
    } | null>;
    /**
     * Count the number of candles matching the query
     */
    count(query: DataQuery): Promise<number>;
    /**
     * Optimize the database
     * Runs VACUUM ANALYZE and manual compression
     */
    optimize(): Promise<void>;
}
//# sourceMappingURL=timescaledb.d.ts.map