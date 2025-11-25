/**
 * Parquet backend for CCXT Data Lake
 * Uses @dsnp/parquetjs with S3 support
 */
import type { BackendConfig } from '../types.js';
import { BaseBackend } from '../interface.js';
import type { WriteOptions, ReadOptions, GapDetectionOptions } from '../interface.js';
import type { Candle, DataPoint, DataQuery, Gap, LakeStats } from '../types.js';
/**
 * Parquet backend implementation with S3 support
 */
export declare class ParquetBackend extends BaseBackend {
    readonly type: "parquet";
    private basePath;
    private s3Client?;
    private s3Bucket?;
    private partitionIndex;
    private writeBuffers;
    private readonly BUFFER_SIZE;
    private readonly schema;
    constructor(config: BackendConfig);
    /**
     * Connect and initialize the backend
     */
    connect(): Promise<void>;
    /**
     * Disconnect and flush pending writes
     */
    disconnect(): Promise<void>;
    /**
     * Write a single data point
     */
    write(point: DataPoint, _options?: WriteOptions): Promise<void>;
    /**
     * Write multiple data points in batch
     */
    writeMany(points: DataPoint[], options?: WriteOptions): Promise<number>;
    /**
     * Read data points matching the query
     */
    read(query: DataQuery, options?: ReadOptions): Promise<Candle[]>;
    /**
     * Find gaps in the data
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
     * Get the earliest and latest timestamps
     */
    getTimeRange(query: Omit<DataQuery, 'since' | 'until' | 'limit'>): Promise<{
        earliest: number;
        latest: number;
    } | null>;
    /**
     * Count records matching the query
     */
    count(query: DataQuery): Promise<number>;
    /**
     * Optimize storage by merging small partitions
     */
    optimize(): Promise<void>;
    /**
     * Scan existing partitions and build index
     */
    private scanPartitions;
    /**
     * Scan local filesystem for partitions
     */
    private scanLocalPartitions;
    /**
     * Scan S3 bucket for partitions
     */
    private scanS3Partitions;
    /**
     * Index a partition file
     */
    private indexPartition;
    /**
     * Parse partition path to extract metadata
     */
    private parsePartitionPath;
    /**
     * Get partition key from data point
     */
    private getPartitionKey;
    /**
     * Get buffer key for a data point
     */
    private getBufferKey;
    /**
     * Make partition key string
     */
    private makePartitionKey;
    /**
     * Parse partition key string
     */
    private parsePartitionKey;
    /**
     * Get partition path
     */
    private getPartitionPath;
    /**
     * Get relevant partitions for a query
     */
    private getRelevantPartitions;
    /**
     * Flush a specific buffer
     */
    private flushBuffer;
    /**
     * Flush all buffers
     */
    private flushAll;
    /**
     * Flush buffers relevant to a query
     */
    private flushQueryBuffers;
    /**
     * Read partition data
     */
    private readPartition;
    /**
     * Read partition file (from S3 or local)
     */
    private readPartitionFile;
    /**
     * Write partition file
     */
    private writePartition;
    /**
     * Upload file to S3
     */
    private uploadToS3;
    /**
     * Download file from S3
     */
    private downloadFromS3;
}
//# sourceMappingURL=parquet.d.ts.map