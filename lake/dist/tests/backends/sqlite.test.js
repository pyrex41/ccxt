/**
 * Unit tests for SQLite backend
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import { SQLiteBackend } from '../../src/backends/sqlite.js';
// Helper to create test candle data
function createTestCandle(timestamp, basePrice = 100) {
    return {
        timestamp,
        open: basePrice,
        high: basePrice * 1.1,
        low: basePrice * 0.9,
        close: basePrice * 1.05,
        volume: 1000,
    };
}
// Helper to create test data point
function createTestDataPoint(exchange, symbol, timeframe, timestamp, basePrice = 100) {
    return {
        timestamp,
        exchange,
        symbol,
        timeframe,
        data: createTestCandle(timestamp, basePrice),
    };
}
// Helper to create a series of test data points
function createTestSeries(exchange, symbol, timeframe, count, startTimestamp, interval) {
    const points = [];
    for (let i = 0; i < count; i++) {
        points.push(createTestDataPoint(exchange, symbol, timeframe, startTimestamp + i * interval, 100 + i));
    }
    return points;
}
describe('SQLiteBackend', () => {
    let backend;
    const testDbPath = './test-lake.db';
    beforeEach(async () => {
        // Clean up before each test
        if (fs.existsSync(testDbPath)) {
            fs.unlinkSync(testDbPath);
        }
        backend = new SQLiteBackend({ type: 'sqlite', path: testDbPath });
        await backend.connect();
    });
    afterEach(async () => {
        await backend.disconnect();
        // Clean up test database
        if (fs.existsSync(testDbPath)) {
            fs.unlinkSync(testDbPath);
        }
    });
    describe('Connection Tests', () => {
        it('should create database file on connect', () => {
            expect(fs.existsSync(testDbPath)).toBe(true);
        });
        it('should set state to connected after connect', () => {
            expect(backend.state).toBe('connected');
        });
        it('should properly close connection on disconnect', async () => {
            await backend.disconnect();
            expect(backend.state).toBe('disconnected');
        });
        it('should allow reconnection after disconnect', async () => {
            await backend.disconnect();
            expect(backend.state).toBe('disconnected');
            await backend.connect();
            expect(backend.state).toBe('connected');
        });
        it('should throw error when path is not provided', () => {
            expect(() => {
                new SQLiteBackend({ type: 'sqlite' });
            }).toThrow('SQLite backend requires a path');
        });
        it('should not connect twice if already connected', async () => {
            // Already connected from beforeEach
            await backend.connect();
            expect(backend.state).toBe('connected');
        });
        it('should not disconnect twice if already disconnected', async () => {
            await backend.disconnect();
            await backend.disconnect();
            expect(backend.state).toBe('disconnected');
        });
    });
    describe('Write Operations', () => {
        it('should write a single data point', async () => {
            const point = createTestDataPoint('binance', 'BTC/USDT', '1m', Date.now());
            await backend.write(point);
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(results).toHaveLength(1);
            expect(results[0].timestamp).toBe(point.timestamp);
            expect(results[0].open).toBe(point.data.open);
        });
        it('should write multiple data points with writeMany', async () => {
            const points = createTestSeries('binance', 'BTC/USDT', '1m', 10, Date.now(), 60000);
            const written = await backend.writeMany(points);
            expect(written).toBe(10);
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(results).toHaveLength(10);
        });
        it('should handle upsert behavior (INSERT OR REPLACE)', async () => {
            const timestamp = Date.now();
            const point1 = createTestDataPoint('binance', 'BTC/USDT', '1m', timestamp, 100);
            await backend.write(point1);
            // Write again with different price
            const point2 = createTestDataPoint('binance', 'BTC/USDT', '1m', timestamp, 200);
            await backend.write(point2);
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(results).toHaveLength(1);
            expect(results[0].open).toBe(200); // Should have new value
        });
        it('should respect batch size configuration', async () => {
            const points = createTestSeries('binance', 'BTC/USDT', '1m', 2500, Date.now(), 60000);
            const written = await backend.writeMany(points, { batchSize: 500 });
            expect(written).toBe(2500);
            const count = await backend.count({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(count).toBe(2500);
        });
        it('should handle empty writeMany', async () => {
            const written = await backend.writeMany([]);
            expect(written).toBe(0);
        });
        it('should verify data integrity after writes', async () => {
            const timestamp = Date.now();
            const candle = {
                timestamp,
                open: 42000.5,
                high: 42500.75,
                low: 41800.25,
                close: 42300.0,
                volume: 12345.678,
            };
            const point = {
                timestamp,
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1h',
                data: candle,
            };
            await backend.write(point);
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1h',
            });
            expect(results).toHaveLength(1);
            expect(results[0]).toEqual(candle);
        });
        it('should throw error when writing to disconnected backend', async () => {
            await backend.disconnect();
            const point = createTestDataPoint('binance', 'BTC/USDT', '1m', Date.now());
            await expect(backend.write(point)).rejects.toThrow('not connected');
        });
    });
    describe('Read Operations', () => {
        beforeEach(async () => {
            // Populate with test data
            const points = createTestSeries('binance', 'BTC/USDT', '1m', 100, 1000000, 60000);
            await backend.writeMany(points);
        });
        it('should read data with basic query', async () => {
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(results).toHaveLength(100);
        });
        it('should filter by since parameter', async () => {
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
                since: 1000000 + 50 * 60000, // Start from 50th candle
            });
            expect(results).toHaveLength(50);
            expect(results[0].timestamp).toBeGreaterThanOrEqual(1000000 + 50 * 60000);
        });
        it('should filter by until parameter', async () => {
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
                until: 1000000 + 49 * 60000, // Up to 50th candle
            });
            expect(results).toHaveLength(50);
            expect(results[results.length - 1].timestamp).toBeLessThanOrEqual(1000000 + 49 * 60000);
        });
        it('should filter by both since and until', async () => {
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
                since: 1000000 + 25 * 60000,
                until: 1000000 + 74 * 60000,
            });
            expect(results).toHaveLength(50);
        });
        it('should respect limit parameter', async () => {
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
                limit: 10,
            });
            expect(results).toHaveLength(10);
        });
        it('should return results in ascending order by default', async () => {
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
                limit: 10,
            });
            for (let i = 1; i < results.length; i++) {
                expect(results[i].timestamp).toBeGreaterThan(results[i - 1].timestamp);
            }
        });
        it('should return results in descending order when specified', async () => {
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
                limit: 10,
            }, { order: 'desc' });
            for (let i = 1; i < results.length; i++) {
                expect(results[i].timestamp).toBeLessThan(results[i - 1].timestamp);
            }
        });
        it('should return empty array for non-existent data', async () => {
            const results = await backend.read({
                exchange: 'kraken',
                symbol: 'ETH/USDT',
                timeframe: '1h',
            });
            expect(results).toHaveLength(0);
        });
        it('should handle queries with no matches in time range', async () => {
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
                since: 9999999999999, // Far future
            });
            expect(results).toHaveLength(0);
        });
    });
    describe('Gap Detection Tests', () => {
        it('should find gaps in data', async () => {
            const baseTime = 1000000;
            const interval = 60000; // 1 minute
            // Create data with a gap
            const points = [
                ...createTestSeries('binance', 'BTC/USDT', '1m', 10, baseTime, interval),
                // Gap here (10 minutes missing)
                ...createTestSeries('binance', 'BTC/USDT', '1m', 10, baseTime + 20 * interval, interval),
            ];
            await backend.writeMany(points);
            const gaps = await backend.findGaps({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(gaps).toHaveLength(1);
            expect(gaps[0].start).toBe(baseTime + 10 * interval);
            expect(gaps[0].end).toBe(baseTime + 19 * interval);
        });
        it('should detect multiple gaps', async () => {
            const baseTime = 1000000;
            const interval = 60000;
            const points = [
                ...createTestSeries('binance', 'BTC/USDT', '1m', 5, baseTime, interval),
                // Gap 1
                ...createTestSeries('binance', 'BTC/USDT', '1m', 5, baseTime + 10 * interval, interval),
                // Gap 2
                ...createTestSeries('binance', 'BTC/USDT', '1m', 5, baseTime + 20 * interval, interval),
            ];
            await backend.writeMany(points);
            const gaps = await backend.findGaps({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(gaps).toHaveLength(2);
        });
        it('should respect minGapSize configuration', async () => {
            const baseTime = 1000000;
            const interval = 60000;
            // Create data with small gaps
            const points = [
                ...createTestSeries('binance', 'BTC/USDT', '1m', 5, baseTime, interval),
                // Small gap (2 minutes)
                ...createTestSeries('binance', 'BTC/USDT', '1m', 5, baseTime + 7 * interval, interval),
                // Large gap (10 minutes)
                ...createTestSeries('binance', 'BTC/USDT', '1m', 5, baseTime + 22 * interval, interval),
            ];
            await backend.writeMany(points);
            // Set minimum gap size to 5 minutes
            const gaps = await backend.findGaps({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            }, {
                minGapSize: 5 * 60000,
            });
            // Should only find the large gap
            expect(gaps).toHaveLength(1);
            expect(gaps[0].end - gaps[0].start).toBeGreaterThan(5 * 60000);
        });
        it('should return empty array for contiguous data', async () => {
            const points = createTestSeries('binance', 'BTC/USDT', '1m', 100, 1000000, 60000);
            await backend.writeMany(points);
            const gaps = await backend.findGaps({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(gaps).toHaveLength(0);
        });
        it('should work with different timeframes', async () => {
            const baseTime = 1000000;
            const interval = 3600000; // 1 hour
            const points = [
                ...createTestSeries('binance', 'BTC/USDT', '1h', 5, baseTime, interval),
                // Gap
                ...createTestSeries('binance', 'BTC/USDT', '1h', 5, baseTime + 10 * interval, interval),
            ];
            await backend.writeMany(points);
            const gaps = await backend.findGaps({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1h',
            });
            expect(gaps).toHaveLength(1);
        });
        it('should return empty array for single data point', async () => {
            const point = createTestDataPoint('binance', 'BTC/USDT', '1m', 1000000);
            await backend.write(point);
            const gaps = await backend.findGaps({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(gaps).toHaveLength(0);
        });
    });
    describe('Statistics Tests', () => {
        beforeEach(async () => {
            // Populate with diverse test data
            const points = [
                ...createTestSeries('binance', 'BTC/USDT', '1m', 50, 1000000, 60000),
                ...createTestSeries('binance', 'ETH/USDT', '1m', 30, 1000000, 60000),
                ...createTestSeries('kraken', 'BTC/USDT', '1h', 20, 1000000, 3600000),
                ...createTestSeries('coinbase', 'BTC/USD', '5m', 10, 1000000, 300000),
            ];
            await backend.writeMany(points);
        });
        it('should return correct total count', async () => {
            const stats = await backend.getStats();
            expect(stats.totalCandles).toBe(110);
        });
        it('should return all exchanges', async () => {
            const stats = await backend.getStats();
            expect(stats.exchanges).toEqual(['binance', 'coinbase', 'kraken']);
        });
        it('should return all symbols', async () => {
            const stats = await backend.getStats();
            expect(stats.symbols).toContain('BTC/USDT');
            expect(stats.symbols).toContain('ETH/USDT');
            expect(stats.symbols).toContain('BTC/USD');
        });
        it('should return all timeframes', async () => {
            const stats = await backend.getStats();
            expect(stats.timeframes).toContain('1m');
            expect(stats.timeframes).toContain('1h');
            expect(stats.timeframes).toContain('5m');
        });
        it('should return correct time range', async () => {
            const stats = await backend.getStats();
            expect(stats.oldestTimestamp).toBe(1000000);
            expect(stats.newestTimestamp).toBeDefined();
            expect(stats.newestTimestamp).toBeGreaterThan(1000000);
        });
        it('should filter by exchange', async () => {
            const stats = await backend.getStats('binance');
            expect(stats.totalCandles).toBe(80); // 50 + 30
            expect(stats.exchanges).toEqual(['binance']);
        });
        it('should filter by exchange and symbol', async () => {
            const stats = await backend.getStats('binance', 'BTC/USDT');
            expect(stats.totalCandles).toBe(50);
            expect(stats.exchanges).toEqual(['binance']);
            expect(stats.symbols).toEqual(['BTC/USDT']);
        });
        it('should return empty stats for non-existent exchange', async () => {
            const stats = await backend.getStats('nonexistent');
            expect(stats.totalCandles).toBe(0);
            expect(stats.exchanges).toHaveLength(0);
            expect(stats.symbols).toHaveLength(0);
            expect(stats.oldestTimestamp).toBeUndefined();
            expect(stats.newestTimestamp).toBeUndefined();
        });
    });
    describe('Search Tests (FTS5)', () => {
        beforeEach(async () => {
            // Populate with searchable data
            const points = [
                ...createTestSeries('binance', 'BTC/USDT', '1m', 10, 1000000, 60000),
                ...createTestSeries('binance', 'ETH/USDT', '1m', 10, 1000000, 60000),
                ...createTestSeries('kraken', 'BTC/EUR', '1h', 10, 1000000, 3600000),
                ...createTestSeries('coinbase', 'BTC/USD', '5m', 10, 1000000, 300000),
            ];
            await backend.writeMany(points);
        });
        it('should search with prefix mode', async () => {
            const results = await backend.search('BTC', { matchMode: 'prefix', limit: 10 });
            expect(results.length).toBeGreaterThan(0);
            results.forEach(result => {
                expect(result.symbol.includes('BTC') ||
                    result.exchange.includes('BTC') ||
                    result.timeframe.includes('BTC')).toBe(true);
            });
        });
        it('should search with phrase mode', async () => {
            const results = await backend.search('binance', { matchMode: 'phrase', limit: 10 });
            expect(results.length).toBeGreaterThan(0);
            results.forEach(result => {
                expect(result.exchange).toBe('binance');
            });
        });
        it('should return results with relevance scores', async () => {
            const results = await backend.search('BTC');
            expect(results.length).toBeGreaterThan(0);
            results.forEach(result => {
                expect(result.score).toBeDefined();
                expect(typeof result.score).toBe('number');
            });
        });
        it('should return results ordered by rank', async () => {
            const results = await backend.search('BTC', { limit: 5 });
            // Scores should be in descending order (higher is better)
            for (let i = 1; i < results.length; i++) {
                expect(results[i].score).toBeLessThanOrEqual(results[i - 1].score);
            }
        });
        it('should respect limit parameter', async () => {
            const results = await backend.search('BTC', { limit: 2 });
            expect(results.length).toBeLessThanOrEqual(2);
        });
        it('should include snippet in results', async () => {
            const results = await backend.search('binance');
            expect(results.length).toBeGreaterThan(0);
            results.forEach(result => {
                expect(result.snippet).toBeDefined();
                expect(result.snippet).toContain(result.exchange);
                expect(result.snippet).toContain(result.symbol);
                expect(result.snippet).toContain(result.timeframe);
            });
        });
        it('should return empty array for no matches', async () => {
            const results = await backend.search('nonexistent_xyz_123');
            expect(results).toHaveLength(0);
        });
        it('should use default limit of 10', async () => {
            const results = await backend.search('BTC');
            expect(results.length).toBeLessThanOrEqual(10);
        });
    });
    describe('Other Operations', () => {
        describe('exists()', () => {
            beforeEach(async () => {
                const points = createTestSeries('binance', 'BTC/USDT', '1m', 50, 1000000, 60000);
                await backend.writeMany(points);
            });
            it('should return true for existing data', async () => {
                const exists = await backend.exists({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                });
                expect(exists).toBe(true);
            });
            it('should return false for non-existing data', async () => {
                const exists = await backend.exists({
                    exchange: 'kraken',
                    symbol: 'ETH/USD',
                    timeframe: '1h',
                });
                expect(exists).toBe(false);
            });
            it('should check existence with time range', async () => {
                const exists = await backend.exists({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                    since: 1000000,
                    until: 1000000 + 10 * 60000,
                });
                expect(exists).toBe(true);
            });
            it('should return false for non-matching time range', async () => {
                const exists = await backend.exists({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                    since: 9999999999999,
                });
                expect(exists).toBe(false);
            });
        });
        describe('delete()', () => {
            beforeEach(async () => {
                const points = createTestSeries('binance', 'BTC/USDT', '1m', 100, 1000000, 60000);
                await backend.writeMany(points);
            });
            it('should delete all matching records', async () => {
                const deleted = await backend.delete({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                });
                expect(deleted).toBe(100);
                const exists = await backend.exists({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                });
                expect(exists).toBe(false);
            });
            it('should delete records in time range', async () => {
                const deleted = await backend.delete({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                    since: 1000000,
                    until: 1000000 + 49 * 60000,
                });
                expect(deleted).toBe(50);
                const remaining = await backend.count({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                });
                expect(remaining).toBe(50);
            });
            it('should return 0 for non-existing records', async () => {
                const deleted = await backend.delete({
                    exchange: 'kraken',
                    symbol: 'ETH/USD',
                    timeframe: '1h',
                });
                expect(deleted).toBe(0);
            });
            it('should update FTS5 index after delete', async () => {
                // Delete all BTC/USDT data
                await backend.delete({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                });
                // Search should not find deleted records (search by exchange)
                const results = await backend.search('binance');
                // Since we only had binance BTC/USDT data, search should return empty
                expect(results).toHaveLength(0);
            });
        });
        describe('count()', () => {
            beforeEach(async () => {
                const points = createTestSeries('binance', 'BTC/USDT', '1m', 100, 1000000, 60000);
                await backend.writeMany(points);
            });
            it('should count all matching records', async () => {
                const count = await backend.count({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                });
                expect(count).toBe(100);
            });
            it('should count records in time range', async () => {
                const count = await backend.count({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                    since: 1000000 + 50 * 60000,
                });
                expect(count).toBe(50);
            });
            it('should return 0 for non-existing records', async () => {
                const count = await backend.count({
                    exchange: 'kraken',
                    symbol: 'ETH/USD',
                    timeframe: '1h',
                });
                expect(count).toBe(0);
            });
        });
        describe('getTimeRange()', () => {
            beforeEach(async () => {
                const points = createTestSeries('binance', 'BTC/USDT', '1m', 50, 1000000, 60000);
                await backend.writeMany(points);
            });
            it('should return earliest and latest timestamps', async () => {
                const range = await backend.getTimeRange({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                });
                expect(range).not.toBeNull();
                expect(range.earliest).toBe(1000000);
                expect(range.latest).toBe(1000000 + 49 * 60000);
            });
            it('should return null for non-existing data', async () => {
                const range = await backend.getTimeRange({
                    exchange: 'kraken',
                    symbol: 'ETH/USD',
                    timeframe: '1h',
                });
                expect(range).toBeNull();
            });
        });
        describe('optimize()', () => {
            it('should run VACUUM and ANALYZE', async () => {
                // Populate with data
                const points = createTestSeries('binance', 'BTC/USDT', '1m', 100, 1000000, 60000);
                await backend.writeMany(points);
                // Should not throw
                await expect(backend.optimize()).resolves.not.toThrow();
            });
            it('should optimize FTS5 index', async () => {
                // Populate with data
                const points = createTestSeries('binance', 'BTC/USDT', '1m', 100, 1000000, 60000);
                await backend.writeMany(points);
                await backend.optimize();
                // Search should still work after optimization
                const results = await backend.search('BTC');
                expect(results.length).toBeGreaterThan(0);
            });
        });
    });
    describe('Edge Cases and Error Handling', () => {
        it('should handle very large batch writes', async () => {
            const points = createTestSeries('binance', 'BTC/USDT', '1m', 10000, 1000000, 60000);
            const written = await backend.writeMany(points, { batchSize: 1000 });
            expect(written).toBe(10000);
            const count = await backend.count({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(count).toBe(10000);
        });
        it('should handle special characters in symbol names', async () => {
            const point = createTestDataPoint('binance', 'BTC/USDT:PERP', '1m', 1000000);
            await backend.write(point);
            const exists = await backend.exists({
                exchange: 'binance',
                symbol: 'BTC/USDT:PERP',
                timeframe: '1m',
            });
            expect(exists).toBe(true);
        });
        it('should handle queries with all optional parameters', async () => {
            const points = createTestSeries('binance', 'BTC/USDT', '1m', 100, 1000000, 60000);
            await backend.writeMany(points);
            const results = await backend.read({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
                since: 1000000 + 25 * 60000,
                until: 1000000 + 74 * 60000,
                limit: 10,
            }, { order: 'desc' });
            expect(results).toHaveLength(10);
            expect(results[0].timestamp).toBeGreaterThan(results[1].timestamp);
        });
        it('should handle invalid timeframe in getTimeframeMs', async () => {
            const backend2 = new SQLiteBackend({ type: 'sqlite', path: './test-lake-2.db' });
            await backend2.connect();
            try {
                await backend2.findGaps({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: 'invalid',
                }, { expectedInterval: undefined });
                expect.fail('Should have thrown error');
            }
            catch (error) {
                expect(error.message).toContain('Invalid timeframe format');
            }
            finally {
                await backend2.disconnect();
                if (fs.existsSync('./test-lake-2.db')) {
                    fs.unlinkSync('./test-lake-2.db');
                }
            }
        });
        it('should handle concurrent writes', async () => {
            const promises = [];
            for (let i = 0; i < 10; i++) {
                const point = createTestDataPoint('binance', 'BTC/USDT', '1m', 1000000 + i * 60000);
                promises.push(backend.write(point));
            }
            await Promise.all(promises);
            const count = await backend.count({
                exchange: 'binance',
                symbol: 'BTC/USDT',
                timeframe: '1m',
            });
            expect(count).toBe(10);
        });
        it('should handle search with special characters', async () => {
            const points = createTestSeries('binance', 'BTC/USDT', '1m', 10, 1000000, 60000);
            await backend.writeMany(points);
            // Search should work with alphanumeric terms
            const results = await backend.search('binance');
            expect(Array.isArray(results)).toBe(true);
            expect(results.length).toBeGreaterThan(0);
        });
        it('should verify WAL mode is enabled', async () => {
            // WAL mode is set during connect, we can verify by checking the database works
            // and doesn't lock on reads
            const points = createTestSeries('binance', 'BTC/USDT', '1m', 10, 1000000, 60000);
            await backend.writeMany(points);
            // Simultaneous read and write should work with WAL mode
            const [readResult, _writeResult] = await Promise.all([
                backend.read({
                    exchange: 'binance',
                    symbol: 'BTC/USDT',
                    timeframe: '1m',
                }),
                backend.write(createTestDataPoint('binance', 'ETH/USDT', '1m', 1000000)),
            ]);
            expect(readResult).toHaveLength(10);
            const exists = await backend.exists({
                exchange: 'binance',
                symbol: 'ETH/USDT',
                timeframe: '1m',
            });
            expect(exists).toBe(true);
        });
    });
});
//# sourceMappingURL=sqlite.test.js.map