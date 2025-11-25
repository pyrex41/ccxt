/**
 * Unit tests for Parquet backend
 * Tests partitioning, buffering, read/write operations, and optimization
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { ParquetBackend } from '../../src/backends/parquet.js';
// Helper functions
function createTestCandle(timestamp, value = 100) {
    return {
        timestamp,
        open: value,
        high: value + 10,
        low: value - 10,
        close: value + 5,
        volume: 1000,
    };
}
function createTestDataPoint(exchange, symbol, timeframe, timestamp, value = 100) {
    return {
        timestamp,
        exchange,
        symbol,
        timeframe,
        data: createTestCandle(timestamp, value),
    };
}
// Helper to clean up test directory
async function cleanupTestDir(dir) {
    if (fs.existsSync(dir)) {
        await fs.promises.rm(dir, { recursive: true, force: true });
    }
}
// Helper to count files in directory recursively
function countFiles(dir, extension = '.parquet') {
    let count = 0;
    if (!fs.existsSync(dir))
        return 0;
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
            count += countFiles(fullPath, extension);
        }
        else if (entry.isFile() && entry.name.endsWith(extension)) {
            count++;
        }
    }
    return count;
}
// Helper to check if directory structure exists
function checkPartitionStructure(basePath, exchange, symbol, timeframe, year, month, day) {
    const monthStr = String(month).padStart(2, '0');
    const dayStr = String(day).padStart(2, '0');
    const expectedPath = path.join(basePath, `exchange=${exchange}`, `symbol=${symbol}`, `timeframe=${timeframe}`, String(year), monthStr, `${dayStr}.parquet`);
    return fs.existsSync(expectedPath);
}
describe('ParquetBackend', () => {
    let backend;
    const testDataPath = './test-parquet-data';
    beforeEach(async () => {
        await cleanupTestDir(testDataPath);
        const config = {
            type: 'parquet',
            path: testDataPath,
        };
        backend = new ParquetBackend(config);
        await backend.connect();
    });
    afterEach(async () => {
        await backend.disconnect();
        await cleanupTestDir(testDataPath);
    });
    describe('Connection', () => {
        it('should create data directory on connect', async () => {
            expect(fs.existsSync(testDataPath)).toBe(true);
        });
        it('should have connected state after connect', () => {
            expect(backend.state).toBe('connected');
        });
        it('should scan existing partitions on connect', async () => {
            // Write some data and disconnect
            const point = createTestDataPoint('binance', 'BTCUSDT', '1m', Date.now());
            await backend.write(point);
            await backend.disconnect();
            // Reconnect and check that partition is indexed
            await backend.connect();
            const stats = await backend.getStats();
            expect(stats.totalCandles).toBeGreaterThan(0);
        });
        it('should not connect if already connected', async () => {
            // Already connected in beforeEach
            const stateBefore = backend.state;
            await backend.connect();
            expect(backend.state).toBe(stateBefore);
            expect(backend.state).toBe('connected');
        });
        it('should flush buffers on disconnect', async () => {
            // Write data without triggering buffer flush
            const point = createTestDataPoint('binance', 'BTCUSDT', '1m', Date.now());
            await backend.write(point);
            // Disconnect should flush
            await backend.disconnect();
            // Reconnect and verify data was written
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(data.length).toBe(1);
        });
    });
    describe('Write Operations', () => {
        it('should buffer writes until threshold', async () => {
            // Use a fixed date to ensure all data goes into same partition
            const baseTime = new Date('2024-01-15T00:00:00Z').getTime();
            // Write 999 points (below threshold of 1000)
            for (let i = 0; i < 999; i++) {
                const point = createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + i * 60000);
                await backend.write(point);
            }
            // No partition files should exist yet (still buffered)
            const fileCount = countFiles(testDataPath);
            expect(fileCount).toBe(0);
            // Write one more to exceed threshold (1000th point)
            const point = createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + 999 * 60000);
            await backend.write(point);
            // Now partition file should exist (buffer flushed automatically)
            const fileCountAfter = countFiles(testDataPath);
            expect(fileCountAfter).toBeGreaterThan(0);
        });
        it('should create partitioned files by date', async () => {
            const date1 = new Date('2024-01-15T12:00:00Z').getTime();
            const date2 = new Date('2024-01-16T12:00:00Z').getTime();
            const point1 = createTestDataPoint('binance', 'BTCUSDT', '1m', date1);
            const point2 = createTestDataPoint('binance', 'BTCUSDT', '1m', date2);
            await backend.write(point1);
            await backend.write(point2);
            await backend.disconnect(); // Flush buffers
            // Check partition structure
            const hasPartition1 = checkPartitionStructure(testDataPath, 'binance', 'BTCUSDT', '1m', 2024, 1, 15);
            const hasPartition2 = checkPartitionStructure(testDataPath, 'binance', 'BTCUSDT', '1m', 2024, 1, 16);
            expect(hasPartition1).toBe(true);
            expect(hasPartition2).toBe(true);
        });
        it('should write multiple data points in batch with writeMany', async () => {
            const timestamp = Date.now();
            const points = [];
            for (let i = 0; i < 100; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp + i * 60000));
            }
            const written = await backend.writeMany(points);
            expect(written).toBe(100);
            await backend.disconnect(); // Flush
            await backend.connect(); // Reconnect
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(data.length).toBe(100);
        });
        it('should handle writeMany with empty array', async () => {
            const written = await backend.writeMany([]);
            expect(written).toBe(0);
        });
        it('should partition data across different exchanges', async () => {
            const timestamp = Date.now();
            const point1 = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp);
            const point2 = createTestDataPoint('coinbase', 'BTCUSDT', '1m', timestamp);
            await backend.write(point1);
            await backend.write(point2);
            await backend.disconnect();
            await backend.connect(); // Reconnect
            const stats = await backend.getStats();
            expect(stats.exchanges).toContain('binance');
            expect(stats.exchanges).toContain('coinbase');
        });
        it('should partition data across different symbols', async () => {
            const timestamp = Date.now();
            const point1 = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp);
            const point2 = createTestDataPoint('binance', 'ETHUSDT', '1m', timestamp);
            await backend.write(point1);
            await backend.write(point2);
            await backend.disconnect();
            await backend.connect(); // Reconnect
            const stats = await backend.getStats();
            expect(stats.symbols).toContain('BTCUSDT');
            expect(stats.symbols).toContain('ETHUSDT');
        });
        it('should partition data across different timeframes', async () => {
            const timestamp = Date.now();
            const point1 = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp);
            const point2 = createTestDataPoint('binance', 'BTCUSDT', '5m', timestamp);
            await backend.write(point1);
            await backend.write(point2);
            await backend.disconnect();
            await backend.connect(); // Reconnect
            const stats = await backend.getStats();
            expect(stats.timeframes).toContain('1m');
            expect(stats.timeframes).toContain('5m');
        });
    });
    describe('Read Operations', () => {
        it('should read data from partitions', async () => {
            const timestamp = Date.now();
            const points = [];
            for (let i = 0; i < 10; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp + i * 60000, 100 + i));
            }
            await backend.writeMany(points);
            await backend.disconnect(); // Flush
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(data.length).toBe(10);
            expect(data[0].timestamp).toBe(timestamp);
            expect(data[9].timestamp).toBe(timestamp + 9 * 60000);
        });
        it('should filter data by time range (since)', async () => {
            const baseTime = new Date('2024-01-15T12:00:00Z').getTime();
            const points = [];
            for (let i = 0; i < 10; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + i * 60000));
            }
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
                since: baseTime + 5 * 60000,
            });
            expect(data.length).toBe(5); // Points 5-9
            expect(data[0].timestamp).toBe(baseTime + 5 * 60000);
        });
        it('should filter data by time range (until)', async () => {
            const baseTime = new Date('2024-01-15T12:00:00Z').getTime();
            const points = [];
            for (let i = 0; i < 10; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + i * 60000));
            }
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
                until: baseTime + 4 * 60000,
            });
            expect(data.length).toBe(5); // Points 0-4
            expect(data[data.length - 1].timestamp).toBe(baseTime + 4 * 60000);
        });
        it('should filter data by time range (since and until)', async () => {
            const baseTime = new Date('2024-01-15T12:00:00Z').getTime();
            const points = [];
            for (let i = 0; i < 10; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + i * 60000));
            }
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
                since: baseTime + 2 * 60000,
                until: baseTime + 7 * 60000,
            });
            expect(data.length).toBe(6); // Points 2-7
            expect(data[0].timestamp).toBe(baseTime + 2 * 60000);
            expect(data[data.length - 1].timestamp).toBe(baseTime + 7 * 60000);
        });
        it('should read across multiple partitions (different days)', async () => {
            const day1 = new Date('2024-01-15T12:00:00Z').getTime();
            const day2 = new Date('2024-01-16T12:00:00Z').getTime();
            const day3 = new Date('2024-01-17T12:00:00Z').getTime();
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', day1),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day1 + 60000),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day2),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day2 + 60000),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day3),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day3 + 60000),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(data.length).toBe(6);
            // Data should be sorted by timestamp
            expect(data[0].timestamp).toBe(day1);
            expect(data[data.length - 1].timestamp).toBe(day3 + 60000);
        });
        it('should return data in ascending order by default', async () => {
            const timestamp = Date.now();
            const points = [];
            for (let i = 0; i < 5; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp + i * 60000));
            }
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            for (let i = 1; i < data.length; i++) {
                expect(data[i].timestamp).toBeGreaterThan(data[i - 1].timestamp);
            }
        });
        it('should return data in descending order when specified', async () => {
            const timestamp = Date.now();
            const points = [];
            for (let i = 0; i < 5; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp + i * 60000));
            }
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            }, { order: 'desc' });
            for (let i = 1; i < data.length; i++) {
                expect(data[i].timestamp).toBeLessThan(data[i - 1].timestamp);
            }
        });
        it('should apply limit to results', async () => {
            const timestamp = Date.now();
            const points = [];
            for (let i = 0; i < 100; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp + i * 60000));
            }
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
                limit: 10,
            });
            expect(data.length).toBe(10);
        });
        it('should return empty array when no data matches query', async () => {
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(data).toEqual([]);
        });
        it('should flush pending writes before reading', async () => {
            const timestamp = Date.now();
            const point = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp);
            // Write without exceeding buffer threshold
            await backend.write(point);
            // Read should flush the buffer for this query
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(data.length).toBe(1);
            expect(data[0].timestamp).toBe(timestamp);
        });
    });
    describe('Partitioning', () => {
        it('should partition data by date correctly', async () => {
            const dates = [
                new Date('2024-01-15T12:00:00Z').getTime(),
                new Date('2024-02-20T12:00:00Z').getTime(),
                new Date('2024-03-25T12:00:00Z').getTime(),
            ];
            for (const date of dates) {
                const point = createTestDataPoint('binance', 'BTCUSDT', '1m', date);
                await backend.write(point);
            }
            await backend.disconnect();
            // Verify partition structure for each date
            expect(checkPartitionStructure(testDataPath, 'binance', 'BTCUSDT', '1m', 2024, 1, 15)).toBe(true);
            expect(checkPartitionStructure(testDataPath, 'binance', 'BTCUSDT', '1m', 2024, 2, 20)).toBe(true);
            expect(checkPartitionStructure(testDataPath, 'binance', 'BTCUSDT', '1m', 2024, 3, 25)).toBe(true);
        });
        it('should use correct partition path format', async () => {
            const timestamp = new Date('2024-01-15T12:00:00Z').getTime();
            const point = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp);
            await backend.write(point);
            await backend.disconnect();
            const expectedPath = path.join(testDataPath, 'exchange=binance', 'symbol=BTCUSDT', 'timeframe=1m', '2024', '01', '15.parquet');
            expect(fs.existsSync(expectedPath)).toBe(true);
        });
        it('should maintain partition index', async () => {
            const timestamp = new Date('2024-01-15T12:00:00Z').getTime();
            const points = [];
            for (let i = 0; i < 10; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp + i * 60000));
            }
            await backend.writeMany(points);
            await backend.disconnect();
            // Reconnect to rebuild index
            await backend.connect();
            const stats = await backend.getStats();
            expect(stats.totalCandles).toBe(10);
            expect(stats.exchanges).toContain('binance');
            expect(stats.symbols).toContain('BTCUSDT');
            expect(stats.timeframes).toContain('1m');
        });
        it('should handle partition boundaries correctly', async () => {
            // Write data at day boundary
            const endOfDay = new Date('2024-01-15T23:59:00Z').getTime();
            const startOfDay = new Date('2024-01-16T00:01:00Z').getTime();
            const point1 = createTestDataPoint('binance', 'BTCUSDT', '1m', endOfDay);
            const point2 = createTestDataPoint('binance', 'BTCUSDT', '1m', startOfDay);
            await backend.writeMany([point1, point2]);
            await backend.disconnect();
            // Should create two separate partitions
            const hasDay1 = checkPartitionStructure(testDataPath, 'binance', 'BTCUSDT', '1m', 2024, 1, 15);
            const hasDay2 = checkPartitionStructure(testDataPath, 'binance', 'BTCUSDT', '1m', 2024, 1, 16);
            expect(hasDay1).toBe(true);
            expect(hasDay2).toBe(true);
        });
    });
    describe('Statistics and Queries', () => {
        it('should aggregate stats across partitions', async () => {
            const day1 = new Date('2024-01-15T12:00:00Z').getTime();
            const day2 = new Date('2024-01-16T12:00:00Z').getTime();
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', day1),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day1 + 60000),
                createTestDataPoint('binance', 'ETHUSDT', '1m', day1),
                createTestDataPoint('coinbase', 'BTCUSDT', '5m', day2),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const stats = await backend.getStats();
            expect(stats.totalCandles).toBe(4);
            expect(stats.exchanges).toContain('binance');
            expect(stats.exchanges).toContain('coinbase');
            expect(stats.symbols).toContain('BTCUSDT');
            expect(stats.symbols).toContain('ETHUSDT');
            expect(stats.timeframes).toContain('1m');
            expect(stats.timeframes).toContain('5m');
            expect(stats.oldestTimestamp).toBe(day1);
            expect(stats.newestTimestamp).toBe(day2);
        });
        it('should filter stats by exchange', async () => {
            const timestamp = Date.now();
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp),
                createTestDataPoint('binance', 'ETHUSDT', '1m', timestamp),
                createTestDataPoint('coinbase', 'BTCUSDT', '1m', timestamp),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const stats = await backend.getStats('binance');
            expect(stats.totalCandles).toBe(2);
            expect(stats.exchanges).toEqual(['binance']);
        });
        it('should filter stats by symbol', async () => {
            const timestamp = Date.now();
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp),
                createTestDataPoint('binance', 'ETHUSDT', '1m', timestamp),
                createTestDataPoint('coinbase', 'BTCUSDT', '1m', timestamp),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const stats = await backend.getStats(undefined, 'BTCUSDT');
            expect(stats.totalCandles).toBe(2);
            expect(stats.symbols).toEqual(['BTCUSDT']);
        });
        it('should check if data exists', async () => {
            const timestamp = Date.now();
            const point = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp);
            await backend.write(point);
            await backend.disconnect();
            await backend.connect();
            const exists = await backend.exists({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(exists).toBe(true);
        });
        it('should return false when data does not exist', async () => {
            const exists = await backend.exists({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(exists).toBe(false);
        });
        it('should count records across partitions', async () => {
            const day1 = new Date('2024-01-15T12:00:00Z').getTime();
            const day2 = new Date('2024-01-16T12:00:00Z').getTime();
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', day1),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day1 + 60000),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day2),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day2 + 60000),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const count = await backend.count({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(count).toBe(4);
        });
        it('should count records with time range filter', async () => {
            const baseTime = new Date('2024-01-15T12:00:00Z').getTime();
            const points = [];
            for (let i = 0; i < 10; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + i * 60000));
            }
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const count = await backend.count({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
                since: baseTime + 3 * 60000,
                until: baseTime + 7 * 60000,
            });
            expect(count).toBe(5); // Points 3-7
        });
        it('should get time range from partitions', async () => {
            const day1 = new Date('2024-01-15T12:00:00Z').getTime();
            const day2 = new Date('2024-01-16T12:00:00Z').getTime();
            const day3 = new Date('2024-01-17T12:00:00Z').getTime();
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', day1),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day2),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day3),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const range = await backend.getTimeRange({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(range).not.toBeNull();
            expect(range?.earliest).toBe(day1);
            expect(range?.latest).toBe(day3);
        });
        it('should return null for time range when no data exists', async () => {
            const range = await backend.getTimeRange({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(range).toBeNull();
        });
    });
    describe('Delete Operations', () => {
        it('should delete partition files', async () => {
            const timestamp = new Date('2024-01-15T12:00:00Z').getTime();
            const point = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp);
            await backend.write(point);
            await backend.disconnect();
            await backend.connect();
            // Verify data exists
            const existsBefore = await backend.exists({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(existsBefore).toBe(true);
            // Delete
            const deleted = await backend.delete({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(deleted).toBe(1);
            // Verify data is gone
            const existsAfter = await backend.exists({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(existsAfter).toBe(false);
        });
        it('should return count of deleted records', async () => {
            const day1 = new Date('2024-01-15T12:00:00Z').getTime();
            const day2 = new Date('2024-01-16T12:00:00Z').getTime();
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', day1),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day1 + 60000),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day2),
                createTestDataPoint('binance', 'BTCUSDT', '1m', day2 + 60000),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const deleted = await backend.delete({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(deleted).toBe(4);
        });
        it('should not delete data from other partitions', async () => {
            const timestamp = Date.now();
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp),
                createTestDataPoint('binance', 'ETHUSDT', '1m', timestamp),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            // Delete only BTC/USDT
            await backend.delete({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            // ETH/USDT should still exist
            const exists = await backend.exists({
                exchange: 'binance',
                symbol: 'ETHUSDT',
                timeframe: '1m',
            });
            expect(exists).toBe(true);
        });
    });
    describe('Optimize Operations', () => {
        it('should deduplicate data in partitions', async () => {
            const timestamp = new Date('2024-01-15T12:00:00Z').getTime();
            // Write duplicate timestamps with different values
            const point1 = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp, 100);
            const point2 = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp, 200); // Same timestamp, different value
            // Note: The backend already deduplicates on write when merging partitions,
            // so writing duplicates in separate sessions will result in only one record
            await backend.write(point1);
            await backend.disconnect();
            await backend.connect();
            await backend.write(point2);
            await backend.disconnect();
            await backend.connect();
            // Check count - backend already deduplicated during writes
            const count = await backend.count({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(count).toBe(1);
            // Optimize should maintain the deduplicated state
            await backend.optimize();
            const afterCount = await backend.count({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(afterCount).toBe(1);
            // Verify the latest value is kept (point2 overwrote point1)
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(data[0].open).toBe(200); // Latest value
        });
        it('should sort data after optimization', async () => {
            const baseTime = new Date('2024-01-15T12:00:00Z').getTime();
            // Write data out of order
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + 2 * 60000),
                createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime),
                createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + 1 * 60000),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            await backend.optimize();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            // Should be sorted
            expect(data[0].timestamp).toBe(baseTime);
            expect(data[1].timestamp).toBe(baseTime + 1 * 60000);
            expect(data[2].timestamp).toBe(baseTime + 2 * 60000);
        });
        it('should flush buffers before optimizing', async () => {
            const timestamp = new Date('2024-01-15T12:00:00Z').getTime();
            // Write data without flushing
            const point = createTestDataPoint('binance', 'BTCUSDT', '1m', timestamp);
            await backend.write(point);
            // Optimize should flush first
            await backend.optimize();
            // Data should be written
            const count = await backend.count({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(count).toBe(1);
        });
    });
    describe('Gap Detection', () => {
        it('should find gaps in data', async () => {
            const baseTime = new Date('2024-01-15T12:00:00Z').getTime();
            const oneMinute = 60000;
            // Create data with a gap
            const points = [
                createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime),
                createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + oneMinute),
                // Gap here (missing 2 candles)
                createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + 4 * oneMinute),
                createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + 5 * oneMinute),
            ];
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const gaps = await backend.findGaps({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(gaps.length).toBe(1);
            expect(gaps[0].start).toBe(baseTime + 2 * oneMinute);
            expect(gaps[0].end).toBe(baseTime + 3 * oneMinute);
            expect(gaps[0].exchange).toBe('binance');
            expect(gaps[0].symbol).toBe('BTCUSDT');
            expect(gaps[0].timeframe).toBe('1m');
        });
        it('should not find gaps in continuous data', async () => {
            const baseTime = new Date('2024-01-15T12:00:00Z').getTime();
            const oneMinute = 60000;
            const points = [];
            for (let i = 0; i < 10; i++) {
                points.push(createTestDataPoint('binance', 'BTCUSDT', '1m', baseTime + i * oneMinute));
            }
            await backend.writeMany(points);
            await backend.disconnect();
            await backend.connect();
            const gaps = await backend.findGaps({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(gaps.length).toBe(0);
        });
    });
    describe('Error Handling', () => {
        it('should throw error when operating on disconnected backend', async () => {
            await backend.disconnect();
            const point = createTestDataPoint('binance', 'BTCUSDT', '1m', Date.now());
            await expect(backend.write(point)).rejects.toThrow('not connected');
        });
        it('should handle reading non-existent partitions', async () => {
            const data = await backend.read({
                exchange: 'nonexistent',
                symbol: 'FAKEUSD',
                timeframe: '1m',
            });
            expect(data).toEqual([]);
        });
        it('should handle deleting non-existent data', async () => {
            const deleted = await backend.delete({
                exchange: 'nonexistent',
                symbol: 'FAKEUSD',
                timeframe: '1m',
            });
            expect(deleted).toBe(0);
        });
    });
    describe('Data Integrity', () => {
        it('should preserve all candle fields', async () => {
            const timestamp = new Date('2024-01-15T12:00:00Z').getTime();
            const candle = {
                timestamp,
                open: 42000.5,
                high: 42500.75,
                low: 41800.25,
                close: 42100.0,
                volume: 1234.5678,
            };
            const point = {
                timestamp,
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
                data: candle,
            };
            await backend.write(point);
            await backend.disconnect();
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(data.length).toBe(1);
            expect(data[0].timestamp).toBe(candle.timestamp);
            expect(data[0].open).toBe(candle.open);
            expect(data[0].high).toBe(candle.high);
            expect(data[0].low).toBe(candle.low);
            expect(data[0].close).toBe(candle.close);
            expect(data[0].volume).toBe(candle.volume);
        });
        it('should handle large numbers correctly', async () => {
            const timestamp = new Date('2024-01-15T12:00:00Z').getTime();
            const largeValue = 999999999.123456;
            const candle = {
                timestamp,
                open: largeValue,
                high: largeValue,
                low: largeValue,
                close: largeValue,
                volume: largeValue,
            };
            const point = {
                timestamp,
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
                data: candle,
            };
            await backend.write(point);
            await backend.disconnect();
            await backend.connect();
            const data = await backend.read({
                exchange: 'binance',
                symbol: 'BTCUSDT',
                timeframe: '1m',
            });
            expect(data[0].open).toBeCloseTo(largeValue, 5);
            expect(data[0].volume).toBeCloseTo(largeValue, 5);
        });
    });
});
//# sourceMappingURL=parquet.test.js.map