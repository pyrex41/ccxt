/**
 * SQLite backend for CCXT Data Lake
 * Uses better-sqlite3 with FTS5 for full-text search
 */
import type { BackendConfig } from '../types.js';
import { BaseBackend } from '../interface.js';
import type { WriteOptions, ReadOptions, GapDetectionOptions, SearchOptions, SearchResult } from '../interface.js';
import type { Candle, DataPoint, DataQuery, Gap, LakeStats } from '../types.js';
/**
 * SQLite backend implementation with FTS5 full-text search
 * Optimized for high-throughput writes (>50,000 candles/sec)
 */
export declare class SQLiteBackend extends BaseBackend {
    readonly type: "sqlite";
    private db;
    private readonly dbPath;
    private stmtInsert;
    private stmtTimeRange;
    constructor(config: BackendConfig);
    /**
     * Connect to the SQLite database and create schema
     */
    connect(): Promise<void>;
    /**
     * Disconnect from the SQLite database
     */
    disconnect(): Promise<void>;
    /**
     * Write a single data point
     */
    write(point: DataPoint, _options?: WriteOptions): Promise<void>;
    /**
     * Write multiple data points in a batch (optimized for high throughput)
     */
    writeMany(points: DataPoint[], options?: WriteOptions): Promise<number>;
    /**
     * Read data points matching the query
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
     * Search for symbols/exchanges using FTS5
     */
    search(searchTerm: string, options?: SearchOptions): Promise<SearchResult[]>;
    /**
     * Optimize the database (VACUUM and ANALYZE)
     */
    optimize(): Promise<void>;
    /**
     * Create the database schema with FTS5 support
     */
    private createSchema;
    /**
     * Prepare frequently used SQL statements
     */
    private prepareStatements;
}
//# sourceMappingURL=sqlite.d.ts.map