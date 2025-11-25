/**
 * Parquet backend for CCXT Data Lake
 * Uses @dsnp/parquetjs with S3 support
 */
import * as fs from 'fs';
import * as path from 'path';
import * as parquet from '@dsnp/parquetjs';
import { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command, DeleteObjectCommand, } from '@aws-sdk/client-s3';
import { BaseBackend } from '../interface.js';
/**
 * Parquet backend implementation with S3 support
 */
export class ParquetBackend extends BaseBackend {
    type = 'parquet';
    basePath;
    s3Client;
    s3Bucket;
    // In-memory partition index for fast lookups
    partitionIndex = new Map();
    // Write buffer for batch operations
    writeBuffers = new Map();
    BUFFER_SIZE = 1000; // Flush after 1000 points
    // Parquet schema for OHLCV data
    schema = new parquet.ParquetSchema({
        timestamp: { type: 'INT64' },
        open: { type: 'DOUBLE' },
        high: { type: 'DOUBLE' },
        low: { type: 'DOUBLE' },
        close: { type: 'DOUBLE' },
        volume: { type: 'DOUBLE' },
    });
    constructor(config) {
        super();
        this.basePath = config.path || './data';
        // Initialize S3 client if S3 config is provided
        if (config.s3) {
            this.s3Client = new S3Client({
                region: config.s3.region,
                credentials: config.s3.accessKeyId && config.s3.secretAccessKey
                    ? {
                        accessKeyId: config.s3.accessKeyId,
                        secretAccessKey: config.s3.secretAccessKey,
                    }
                    : undefined,
            });
            this.s3Bucket = config.s3.bucket;
        }
    }
    /**
     * Connect and initialize the backend
     */
    async connect() {
        if (this._state === 'connected') {
            return;
        }
        this._state = 'connecting';
        try {
            // Create base directory if using local filesystem
            if (!this.s3Client) {
                await fs.promises.mkdir(this.basePath, { recursive: true });
            }
            // Scan and index existing partitions
            await this.scanPartitions();
            this._state = 'connected';
        }
        catch (error) {
            this._state = 'error';
            throw new Error(`Failed to connect to Parquet backend: ${error}`);
        }
    }
    /**
     * Disconnect and flush pending writes
     */
    async disconnect() {
        if (this._state === 'disconnected') {
            return;
        }
        try {
            // Flush all pending writes
            await this.flushAll();
            this._state = 'disconnected';
        }
        catch (error) {
            this._state = 'error';
            throw new Error(`Failed to disconnect from Parquet backend: ${error}`);
        }
    }
    /**
     * Write a single data point
     */
    async write(point, _options) {
        this.ensureConnected();
        const bufferKey = this.getBufferKey(point);
        let buffer = this.writeBuffers.get(bufferKey);
        if (!buffer) {
            buffer = { points: [], lastWrite: Date.now() };
            this.writeBuffers.set(bufferKey, buffer);
        }
        buffer.points.push(point);
        buffer.lastWrite = Date.now();
        // Flush if buffer size threshold exceeded
        if (buffer.points.length >= this.BUFFER_SIZE) {
            await this.flushBuffer(bufferKey);
        }
    }
    /**
     * Write multiple data points in batch
     */
    async writeMany(points, options) {
        this.ensureConnected();
        if (points.length === 0) {
            return 0;
        }
        // Group points by partition
        const partitionGroups = new Map();
        for (const point of points) {
            const bufferKey = this.getBufferKey(point);
            const group = partitionGroups.get(bufferKey) || [];
            group.push(point);
            partitionGroups.set(bufferKey, group);
        }
        // Write each partition group
        for (const [bufferKey, groupPoints] of partitionGroups) {
            const buffer = this.writeBuffers.get(bufferKey) || { points: [], lastWrite: Date.now() };
            buffer.points.push(...groupPoints);
            buffer.lastWrite = Date.now();
            this.writeBuffers.set(bufferKey, buffer);
        }
        // Flush all buffers if requested or if any buffer exceeds threshold
        const shouldFlush = options?.batchSize !== undefined
            ? false
            : Array.from(this.writeBuffers.values()).some(b => b.points.length >= this.BUFFER_SIZE);
        if (shouldFlush) {
            await this.flushAll();
        }
        return points.length;
    }
    /**
     * Read data points matching the query
     */
    async read(query, options) {
        this.ensureConnected();
        // Flush pending writes for this query before reading
        await this.flushQueryBuffers(query);
        const partitions = this.getRelevantPartitions(query);
        const candles = [];
        for (const partition of partitions) {
            try {
                const partitionData = await this.readPartition(partition);
                // Filter by time range if specified
                let filtered = partitionData;
                if (query.since !== undefined) {
                    filtered = filtered.filter(c => c.timestamp >= query.since);
                }
                if (query.until !== undefined) {
                    filtered = filtered.filter(c => c.timestamp <= query.until);
                }
                candles.push(...filtered);
            }
            catch (error) {
                // Partition may not exist or be readable, skip it
                continue;
            }
        }
        // Sort by timestamp
        candles.sort((a, b) => a.timestamp - b.timestamp);
        // Apply order
        if (options?.order === 'desc') {
            candles.reverse();
        }
        // Apply limit
        if (query.limit !== undefined && query.limit > 0) {
            return candles.slice(0, query.limit);
        }
        return candles;
    }
    /**
     * Find gaps in the data
     */
    async findGaps(query, options) {
        // Use default implementation from BaseBackend
        return super.findGaps(query, options);
    }
    /**
     * Get statistics about stored data
     */
    async getStats(exchange, symbol) {
        this.ensureConnected();
        const exchanges = new Set();
        const symbols = new Set();
        const timeframes = new Set();
        let totalCandles = 0;
        let oldestTimestamp;
        let newestTimestamp;
        for (const [key, metadata] of this.partitionIndex) {
            const parts = this.parsePartitionKey(key);
            // Apply filters
            if (exchange && parts.exchange !== exchange)
                continue;
            if (symbol && parts.symbol !== symbol)
                continue;
            exchanges.add(parts.exchange);
            symbols.add(parts.symbol);
            timeframes.add(parts.timeframe);
            totalCandles += metadata.count;
            if (oldestTimestamp === undefined || metadata.minTimestamp < oldestTimestamp) {
                oldestTimestamp = metadata.minTimestamp;
            }
            if (newestTimestamp === undefined || metadata.maxTimestamp > newestTimestamp) {
                newestTimestamp = metadata.maxTimestamp;
            }
        }
        return {
            totalCandles,
            exchanges: Array.from(exchanges),
            symbols: Array.from(symbols),
            timeframes: Array.from(timeframes),
            oldestTimestamp,
            newestTimestamp,
        };
    }
    /**
     * Check if data exists for the given query
     */
    async exists(query) {
        this.ensureConnected();
        const partitions = this.getRelevantPartitions(query);
        return partitions.length > 0;
    }
    /**
     * Delete data matching the query
     */
    async delete(query) {
        this.ensureConnected();
        const partitions = this.getRelevantPartitions(query);
        let deletedCount = 0;
        for (const partition of partitions) {
            const metadata = this.partitionIndex.get(partition);
            if (!metadata)
                continue;
            try {
                if (this.s3Client && this.s3Bucket) {
                    // Delete from S3
                    await this.s3Client.send(new DeleteObjectCommand({
                        Bucket: this.s3Bucket,
                        Key: metadata.key,
                    }));
                }
                else {
                    // Delete from local filesystem
                    await fs.promises.unlink(metadata.path);
                }
                deletedCount += metadata.count;
                this.partitionIndex.delete(partition);
            }
            catch (error) {
                // Partition may already be deleted, continue
                continue;
            }
        }
        return deletedCount;
    }
    /**
     * Get the earliest and latest timestamps
     */
    async getTimeRange(query) {
        this.ensureConnected();
        const fullQuery = {
            ...query,
            since: undefined,
            until: undefined,
        };
        const partitions = this.getRelevantPartitions(fullQuery);
        if (partitions.length === 0) {
            return null;
        }
        let earliest;
        let latest;
        for (const partitionKey of partitions) {
            const metadata = this.partitionIndex.get(partitionKey);
            if (!metadata)
                continue;
            if (earliest === undefined || metadata.minTimestamp < earliest) {
                earliest = metadata.minTimestamp;
            }
            if (latest === undefined || metadata.maxTimestamp > latest) {
                latest = metadata.maxTimestamp;
            }
        }
        if (earliest === undefined || latest === undefined) {
            return null;
        }
        return { earliest, latest };
    }
    /**
     * Count records matching the query
     */
    async count(query) {
        this.ensureConnected();
        const partitions = this.getRelevantPartitions(query);
        let count = 0;
        for (const partitionKey of partitions) {
            const metadata = this.partitionIndex.get(partitionKey);
            if (!metadata)
                continue;
            // If no time range specified, use partition metadata
            if (query.since === undefined && query.until === undefined) {
                count += metadata.count;
            }
            else {
                // Need to read and filter
                try {
                    const candles = await this.readPartition(partitionKey);
                    let filtered = candles;
                    if (query.since !== undefined) {
                        filtered = filtered.filter(c => c.timestamp >= query.since);
                    }
                    if (query.until !== undefined) {
                        filtered = filtered.filter(c => c.timestamp <= query.until);
                    }
                    count += filtered.length;
                }
                catch (error) {
                    continue;
                }
            }
        }
        return count;
    }
    /**
     * Optimize storage by merging small partitions
     */
    async optimize() {
        this.ensureConnected();
        // Flush all pending writes first
        await this.flushAll();
        // Group partitions by exchange/symbol/timeframe
        const groups = new Map();
        for (const [key] of this.partitionIndex) {
            const parts = this.parsePartitionKey(key);
            const groupKey = `${parts.exchange}:${parts.symbol}:${parts.timeframe}`;
            const group = groups.get(groupKey) || [];
            group.push(key);
            groups.set(groupKey, group);
        }
        // Process each group (re-compression, deduplication)
        for (const [_groupKey, partitionKeys] of groups) {
            for (const partitionKey of partitionKeys) {
                try {
                    // Read partition data
                    const candles = await this.readPartition(partitionKey);
                    // Deduplicate by timestamp
                    const uniqueCandles = new Map();
                    for (const candle of candles) {
                        uniqueCandles.set(candle.timestamp, candle);
                    }
                    const sortedCandles = Array.from(uniqueCandles.values())
                        .sort((a, b) => a.timestamp - b.timestamp);
                    // Rewrite partition with optimized data
                    const parts = this.parsePartitionKey(partitionKey);
                    const metadata = this.partitionIndex.get(partitionKey);
                    if (!metadata)
                        continue;
                    await this.writePartition(parts, sortedCandles, metadata.path, metadata.key);
                }
                catch (error) {
                    // Skip partitions that can't be optimized
                    continue;
                }
            }
        }
    }
    /**
     * Scan existing partitions and build index
     */
    async scanPartitions() {
        this.partitionIndex.clear();
        if (this.s3Client && this.s3Bucket) {
            // Scan S3 bucket
            await this.scanS3Partitions();
        }
        else {
            // Scan local filesystem
            await this.scanLocalPartitions();
        }
    }
    /**
     * Scan local filesystem for partitions
     */
    async scanLocalPartitions(dir = this.basePath) {
        try {
            const entries = await fs.promises.readdir(dir, { withFileTypes: true });
            for (const entry of entries) {
                const fullPath = path.join(dir, entry.name);
                if (entry.isDirectory()) {
                    await this.scanLocalPartitions(fullPath);
                }
                else if (entry.isFile() && entry.name.endsWith('.parquet')) {
                    await this.indexPartition(fullPath, fullPath);
                }
            }
        }
        catch (error) {
            // Directory may not exist yet
        }
    }
    /**
     * Scan S3 bucket for partitions
     */
    async scanS3Partitions() {
        if (!this.s3Client || !this.s3Bucket)
            return;
        try {
            let continuationToken;
            do {
                const response = await this.s3Client.send(new ListObjectsV2Command({
                    Bucket: this.s3Bucket,
                    Prefix: '',
                    ContinuationToken: continuationToken,
                }));
                if (response.Contents) {
                    for (const object of response.Contents) {
                        if (object.Key && object.Key.endsWith('.parquet')) {
                            const localPath = path.join(this.basePath, object.Key);
                            await this.indexPartition(localPath, object.Key);
                        }
                    }
                }
                continuationToken = response.NextContinuationToken;
            } while (continuationToken);
        }
        catch (error) {
            throw new Error(`Failed to scan S3 partitions: ${error}`);
        }
    }
    /**
     * Index a partition file
     */
    async indexPartition(filePath, s3Key) {
        try {
            // Parse partition information from path
            const parts = this.parsePartitionPath(filePath);
            if (!parts)
                return;
            const partitionKey = this.makePartitionKey(parts);
            // Read partition metadata (first and last timestamp, count)
            const candles = await this.readPartitionFile(filePath, s3Key);
            if (candles.length === 0)
                return;
            const timestamps = candles.map(c => c.timestamp).sort((a, b) => a - b);
            this.partitionIndex.set(partitionKey, {
                path: filePath,
                key: s3Key,
                count: candles.length,
                minTimestamp: timestamps[0],
                maxTimestamp: timestamps[timestamps.length - 1],
            });
        }
        catch (error) {
            // Failed to index partition, skip it
        }
    }
    /**
     * Parse partition path to extract metadata
     */
    parsePartitionPath(filePath) {
        // Expected format: .../exchange=X/symbol=Y/timeframe=Z/YYYY/MM/DD.parquet
        const match = filePath.match(/exchange=([^/]+)\/symbol=([^/]+)\/timeframe=([^/]+)\/(\d{4})\/(\d{2})\/(\d{2})\.parquet$/);
        if (!match)
            return null;
        return {
            exchange: match[1],
            symbol: match[2],
            timeframe: match[3],
            year: parseInt(match[4], 10),
            month: parseInt(match[5], 10),
            day: parseInt(match[6], 10),
        };
    }
    /**
     * Get partition key from data point
     */
    getPartitionKey(point) {
        const date = new Date(point.timestamp);
        return {
            exchange: point.exchange,
            symbol: point.symbol,
            timeframe: point.timeframe,
            year: date.getUTCFullYear(),
            month: date.getUTCMonth() + 1,
            day: date.getUTCDate(),
        };
    }
    /**
     * Get buffer key for a data point
     */
    getBufferKey(point) {
        const key = this.getPartitionKey(point);
        return this.makePartitionKey(key);
    }
    /**
     * Make partition key string
     */
    makePartitionKey(key) {
        return `${key.exchange}:${key.symbol}:${key.timeframe}:${key.year}:${String(key.month).padStart(2, '0')}:${String(key.day).padStart(2, '0')}`;
    }
    /**
     * Parse partition key string
     */
    parsePartitionKey(key) {
        const [exchange, symbol, timeframe, year, month, day] = key.split(':');
        return {
            exchange,
            symbol,
            timeframe,
            year: parseInt(year, 10),
            month: parseInt(month, 10),
            day: parseInt(day, 10),
        };
    }
    /**
     * Get partition path
     */
    getPartitionPath(key) {
        const yearStr = String(key.year);
        const monthStr = String(key.month).padStart(2, '0');
        const dayStr = String(key.day).padStart(2, '0');
        const relativePath = `exchange=${key.exchange}/symbol=${key.symbol}/timeframe=${key.timeframe}/${yearStr}/${monthStr}/${dayStr}.parquet`;
        return {
            localPath: path.join(this.basePath, relativePath),
            s3Key: relativePath,
        };
    }
    /**
     * Get relevant partitions for a query
     */
    getRelevantPartitions(query) {
        const partitions = [];
        for (const [key, metadata] of this.partitionIndex) {
            const parts = this.parsePartitionKey(key);
            // Filter by exchange, symbol, timeframe
            if (parts.exchange !== query.exchange)
                continue;
            if (parts.symbol !== query.symbol)
                continue;
            if (parts.timeframe !== query.timeframe)
                continue;
            // Filter by time range if specified
            if (query.since !== undefined && metadata.maxTimestamp < query.since)
                continue;
            if (query.until !== undefined && metadata.minTimestamp > query.until)
                continue;
            partitions.push(key);
        }
        // Sort by timestamp
        partitions.sort((a, b) => {
            const metaA = this.partitionIndex.get(a);
            const metaB = this.partitionIndex.get(b);
            return metaA.minTimestamp - metaB.minTimestamp;
        });
        return partitions;
    }
    /**
     * Flush a specific buffer
     */
    async flushBuffer(bufferKey) {
        const buffer = this.writeBuffers.get(bufferKey);
        if (!buffer || buffer.points.length === 0)
            return;
        const points = buffer.points;
        this.writeBuffers.delete(bufferKey);
        // Group by candle data
        const candles = points.map(p => p.data);
        // Sort by timestamp and deduplicate
        const uniqueCandles = new Map();
        for (const candle of candles) {
            uniqueCandles.set(candle.timestamp, candle);
        }
        const sortedCandles = Array.from(uniqueCandles.values())
            .sort((a, b) => a.timestamp - b.timestamp);
        // Parse buffer key to get partition info
        const parts = this.parsePartitionKey(bufferKey);
        const { localPath, s3Key } = this.getPartitionPath(parts);
        // Read existing partition if it exists
        let existingCandles = [];
        try {
            existingCandles = await this.readPartitionFile(localPath, s3Key);
        }
        catch (error) {
            // Partition doesn't exist yet, that's ok
        }
        // Merge with existing data
        const mergedMap = new Map();
        for (const candle of existingCandles) {
            mergedMap.set(candle.timestamp, candle);
        }
        for (const candle of sortedCandles) {
            mergedMap.set(candle.timestamp, candle);
        }
        const mergedCandles = Array.from(mergedMap.values())
            .sort((a, b) => a.timestamp - b.timestamp);
        // Write partition
        await this.writePartition(parts, mergedCandles, localPath, s3Key);
        // Update partition index
        const timestamps = mergedCandles.map(c => c.timestamp);
        this.partitionIndex.set(bufferKey, {
            path: localPath,
            key: s3Key,
            count: mergedCandles.length,
            minTimestamp: Math.min(...timestamps),
            maxTimestamp: Math.max(...timestamps),
        });
    }
    /**
     * Flush all buffers
     */
    async flushAll() {
        const bufferKeys = Array.from(this.writeBuffers.keys());
        for (const bufferKey of bufferKeys) {
            await this.flushBuffer(bufferKey);
        }
    }
    /**
     * Flush buffers relevant to a query
     */
    async flushQueryBuffers(query) {
        const relevantBuffers = [];
        for (const [bufferKey] of this.writeBuffers) {
            const parts = this.parsePartitionKey(bufferKey);
            if (parts.exchange === query.exchange &&
                parts.symbol === query.symbol &&
                parts.timeframe === query.timeframe) {
                relevantBuffers.push(bufferKey);
            }
        }
        for (const bufferKey of relevantBuffers) {
            await this.flushBuffer(bufferKey);
        }
    }
    /**
     * Read partition data
     */
    async readPartition(partitionKey) {
        const metadata = this.partitionIndex.get(partitionKey);
        if (!metadata) {
            throw new Error(`Partition not found: ${partitionKey}`);
        }
        return this.readPartitionFile(metadata.path, metadata.key);
    }
    /**
     * Read partition file (from S3 or local)
     */
    async readPartitionFile(localPath, s3Key) {
        let buffer;
        if (this.s3Client && this.s3Bucket) {
            // Read from S3
            buffer = await this.downloadFromS3(s3Key);
        }
        else {
            // Read from local filesystem
            buffer = await fs.promises.readFile(localPath);
        }
        // Parse parquet file
        const reader = await parquet.ParquetReader.openBuffer(buffer);
        const cursor = reader.getCursor();
        const candles = [];
        let record = await cursor.next();
        while (record) {
            candles.push({
                timestamp: Number(record.timestamp),
                open: record.open,
                high: record.high,
                low: record.low,
                close: record.close,
                volume: record.volume,
            });
            record = await cursor.next();
        }
        await reader.close();
        return candles;
    }
    /**
     * Write partition file
     */
    async writePartition(_key, candles, localPath, s3Key) {
        // Ensure directory exists for local filesystem
        if (!this.s3Client) {
            await fs.promises.mkdir(path.dirname(localPath), { recursive: true });
        }
        // Create temporary file for writing
        const tempPath = `${localPath}.tmp`;
        const writer = await parquet.ParquetWriter.openFile(this.schema, tempPath);
        // Write all candles
        for (const candle of candles) {
            await writer.appendRow({
                timestamp: BigInt(candle.timestamp),
                open: candle.open,
                high: candle.high,
                low: candle.low,
                close: candle.close,
                volume: candle.volume,
            });
        }
        await writer.close();
        // Read the file into buffer
        const buffer = await fs.promises.readFile(tempPath);
        if (this.s3Client && this.s3Bucket) {
            // Upload to S3
            await this.uploadToS3(s3Key, buffer);
            // Delete temporary file
            await fs.promises.unlink(tempPath);
        }
        else {
            // Move temporary file to final location
            await fs.promises.rename(tempPath, localPath);
        }
    }
    /**
     * Upload file to S3
     */
    async uploadToS3(key, buffer) {
        if (!this.s3Client || !this.s3Bucket) {
            throw new Error('S3 client not initialized');
        }
        await this.s3Client.send(new PutObjectCommand({
            Bucket: this.s3Bucket,
            Key: key,
            Body: buffer,
            ContentType: 'application/octet-stream',
        }));
    }
    /**
     * Download file from S3
     */
    async downloadFromS3(key) {
        if (!this.s3Client || !this.s3Bucket) {
            throw new Error('S3 client not initialized');
        }
        const response = await this.s3Client.send(new GetObjectCommand({
            Bucket: this.s3Bucket,
            Key: key,
        }));
        if (!response.Body) {
            throw new Error(`Failed to download from S3: ${key}`);
        }
        // Convert stream to buffer
        const chunks = [];
        for await (const chunk of response.Body) {
            chunks.push(chunk);
        }
        return Buffer.concat(chunks);
    }
}
//# sourceMappingURL=parquet.js.map