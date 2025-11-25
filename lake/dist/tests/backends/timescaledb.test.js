/**
 * Unit and integration tests for TimescaleDB backend
 *
 * Integration tests are skipped by default unless TIMESCALEDB_CONNECTION_STRING is set
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { TimescaleDBBackend } from '../../src/backends/timescaledb.js';
// Skip integration tests if no connection string is provided
const connectionString = process.env.TIMESCALEDB_CONNECTION_STRING;
const shouldSkip = !connectionString;
// Helper functions
function createTestCandle(timestamp, overrides) {
    return {
        timestamp,
        open: 100,
        high: 110,
        low: 90,
        close: 105,
        volume: 1000,
        ...overrides,
    };
}
function createTestDataPoint(exchange, symbol, timeframe, timestamp, candleOverrides) {
    return {
        timestamp,
        exchange,
        symbol,
        timeframe,
        data: createTestCandle(timestamp, candleOverrides),
    };
}
// ============================================================================
// Unit Tests (No Database Required)
// ============================================================================
describe('TimescaleDBBackend (Unit)', () => {
    it('should require connectionString in config', () => {
        expect(() => {
            new TimescaleDBBackend({ type: 'timescaledb' });
        }).toThrow('connectionString');
    });
    it('should have correct type', () => {
        const backend = new TimescaleDBBackend({
            type: 'timescaledb',
            connectionString: 'postgres://localhost/test',
        });
        expect(backend.type).toBe('timescaledb');
    });
    it('should start in disconnected state', () => {
        const backend = new TimescaleDBBackend({
            type: 'timescaledb',
            connectionString: 'postgres://localhost/test',
        });
        expect(backend.state).toBe('disconnected');
    });
});
// ============================================================================
// Integration Tests (Require Database)
// ============================================================================
describe.skipIf(shouldSkip)('TimescaleDBBackend (Integration)', () => {
    let backend;
    const testExchange = 'test_exchange';
    const testSymbol = 'TEST/USDT';
    const testTimeframe = '1m';
    beforeAll(async () => {
        const config = {
            type: 'timescaledb',
            connectionString: connectionString,
        };
        backend = new TimescaleDBBackend(config);
        await backend.connect();
    });
    afterAll(async () => {
        // Clean up all test data
        if (backend.state === 'connected') {
            await backend.delete({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            await backend.disconnect();
        }
    });
    beforeEach(async () => {
        // Clean up before each test
        await backend.delete({
            exchange: testExchange,
            symbol: testSymbol,
            timeframe: testTimeframe,
        });
    });
    // ==========================================================================
    // Connection Tests
    // ==========================================================================
    describe('Connection', () => {
        it('should connect successfully', () => {
            expect(backend.state).toBe('connected');
        });
        it('should handle multiple connect calls', async () => {
            await backend.connect(); // Should not throw
            expect(backend.state).toBe('connected');
        });
        it('should fail with invalid connection string', async () => {
            const badBackend = new TimescaleDBBackend({
                type: 'timescaledb',
                connectionString: 'postgres://invalid:5432/nonexistent',
            });
            await expect(badBackend.connect()).rejects.toThrow();
            expect(badBackend.state).toBe('error');
        });
    });
    // ==========================================================================
    // Write Operations
    // ==========================================================================
    describe('Write Operations', () => {
        it('should write a single data point', async () => {
            const point = createTestDataPoint(testExchange, testSymbol, testTimeframe, Date.now());
            await backend.write(point);
            const exists = await backend.exists({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(exists).toBe(true);
        });
        it('should write multiple data points in batch', async () => {
            const now = Date.now();
            const points = Array.from({ length: 100 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            const written = await backend.writeMany(points);
            expect(written).toBe(100);
            const count = await backend.count({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(count).toBe(100);
        });
        it('should write large batches with custom batch size', async () => {
            const now = Date.now();
            const points = Array.from({ length: 2500 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            const written = await backend.writeMany(points, { batchSize: 500 });
            expect(written).toBe(2500);
            const count = await backend.count({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(count).toBe(2500);
        });
        it('should handle empty batch', async () => {
            const written = await backend.writeMany([]);
            expect(written).toBe(0);
        });
        it('should upsert on conflict', async () => {
            const timestamp = Date.now();
            const point1 = createTestDataPoint(testExchange, testSymbol, testTimeframe, timestamp);
            const point2 = createTestDataPoint(testExchange, testSymbol, testTimeframe, timestamp, { close: 200 });
            await backend.write(point1, { upsert: true });
            await backend.write(point2, { upsert: true });
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(candles.length).toBe(1);
            expect(candles[0].close).toBe(200);
        });
        it('should batch upsert on conflict', async () => {
            const now = Date.now();
            // First batch
            const batch1 = Array.from({ length: 50 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            await backend.writeMany(batch1, { upsert: true });
            // Second batch with overlapping timestamps and different values
            const batch2 = Array.from({ length: 50 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + (i + 25) * 60000, { close: 200 }));
            await backend.writeMany(batch2, { upsert: true });
            const count = await backend.count({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            // Should have 75 total (50 + 25 new)
            expect(count).toBe(75);
            // Check that overlapping records were updated
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
                since: now + 25 * 60000,
                until: now + 49 * 60000,
            });
            candles.forEach(candle => {
                expect(candle.close).toBe(200);
            });
        });
        it('should throw on duplicate without upsert', async () => {
            const timestamp = Date.now();
            const point = createTestDataPoint(testExchange, testSymbol, testTimeframe, timestamp);
            await backend.write(point);
            await expect(backend.write(point)).rejects.toThrow();
        });
    });
    // ==========================================================================
    // Read Operations
    // ==========================================================================
    describe('Read Operations', () => {
        beforeEach(async () => {
            // Setup test data
            const now = Date.now();
            const points = Array.from({ length: 100 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            await backend.writeMany(points);
        });
        it('should read all data without filters', async () => {
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(candles.length).toBe(100);
        });
        it('should read with time range filter (since)', async () => {
            const now = Date.now();
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
                since: now + 50 * 60000,
            });
            expect(candles.length).toBe(50);
            expect(candles[0].timestamp).toBeGreaterThanOrEqual(now + 50 * 60000);
        });
        it('should read with time range filter (until)', async () => {
            const now = Date.now();
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
                until: now + 49 * 60000,
            });
            expect(candles.length).toBe(50);
            expect(candles[candles.length - 1].timestamp).toBeLessThanOrEqual(now + 49 * 60000);
        });
        it('should read with time range filter (since and until)', async () => {
            const now = Date.now();
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
                since: now + 25 * 60000,
                until: now + 74 * 60000,
            });
            expect(candles.length).toBe(50);
            expect(candles[0].timestamp).toBeGreaterThanOrEqual(now + 25 * 60000);
            expect(candles[candles.length - 1].timestamp).toBeLessThanOrEqual(now + 74 * 60000);
        });
        it('should respect limit', async () => {
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
                limit: 10,
            });
            expect(candles.length).toBe(10);
        });
        it('should order by timestamp ascending (default)', async () => {
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            for (let i = 1; i < candles.length; i++) {
                expect(candles[i].timestamp).toBeGreaterThan(candles[i - 1].timestamp);
            }
        });
        it('should order by timestamp descending', async () => {
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            }, { order: 'desc' });
            for (let i = 1; i < candles.length; i++) {
                expect(candles[i].timestamp).toBeLessThan(candles[i - 1].timestamp);
            }
        });
        it('should return correct candle structure', async () => {
            const candles = await backend.read({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
                limit: 1,
            });
            expect(candles.length).toBe(1);
            const candle = candles[0];
            expect(candle).toHaveProperty('timestamp');
            expect(candle).toHaveProperty('open');
            expect(candle).toHaveProperty('high');
            expect(candle).toHaveProperty('low');
            expect(candle).toHaveProperty('close');
            expect(candle).toHaveProperty('volume');
            expect(typeof candle.timestamp).toBe('number');
            expect(typeof candle.open).toBe('number');
            expect(typeof candle.high).toBe('number');
            expect(typeof candle.low).toBe('number');
            expect(typeof candle.close).toBe('number');
            expect(typeof candle.volume).toBe('number');
        });
        it('should return empty array when no data matches', async () => {
            const candles = await backend.read({
                exchange: 'nonexistent',
                symbol: 'NONE/USDT',
                timeframe: '1m',
            });
            expect(candles).toEqual([]);
        });
    });
    // ==========================================================================
    // Gap Detection
    // ==========================================================================
    describe('Gap Detection', () => {
        it('should find gaps using window functions', async () => {
            const now = Date.now();
            // Create data with a gap
            const points = [
                ...Array.from({ length: 10 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000)),
                // Gap here (10 minutes missing)
                ...Array.from({ length: 10 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + (i + 20) * 60000)),
            ];
            await backend.writeMany(points);
            const gaps = await backend.findGaps({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(gaps.length).toBe(1);
            expect(gaps[0].start).toBe(now + 10 * 60000);
            expect(gaps[0].end).toBe(now + 19 * 60000);
            expect(gaps[0].exchange).toBe(testExchange);
            expect(gaps[0].symbol).toBe(testSymbol);
            expect(gaps[0].timeframe).toBe(testTimeframe);
        });
        it('should find multiple gaps', async () => {
            const now = Date.now();
            const points = [
                ...Array.from({ length: 5 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000)),
                // Gap 1 (5 minutes)
                ...Array.from({ length: 5 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + (i + 10) * 60000)),
                // Gap 2 (5 minutes)
                ...Array.from({ length: 5 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + (i + 20) * 60000)),
            ];
            await backend.writeMany(points);
            const gaps = await backend.findGaps({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(gaps.length).toBe(2);
        });
        it('should respect minGapSize option', async () => {
            const now = Date.now();
            const points = [
                ...Array.from({ length: 10 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000)),
                // Small gap (2 minutes)
                ...Array.from({ length: 10 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + (i + 12) * 60000)),
            ];
            await backend.writeMany(points);
            // With large minGapSize, should not detect the small gap
            const gaps = await backend.findGaps({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            }, { minGapSize: 5 * 60000 }); // 5 minutes
            expect(gaps.length).toBe(0);
        });
        it('should return empty array when no gaps exist', async () => {
            const now = Date.now();
            const points = Array.from({ length: 100 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            await backend.writeMany(points);
            const gaps = await backend.findGaps({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(gaps).toEqual([]);
        });
        it('should work with time range filters', async () => {
            const now = Date.now();
            const points = [
                ...Array.from({ length: 10 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000)),
                // Gap 1
                ...Array.from({ length: 10 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + (i + 20) * 60000)),
                // Gap 2
                ...Array.from({ length: 10 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + (i + 40) * 60000)),
            ];
            await backend.writeMany(points);
            // Only search in first half
            const gaps = await backend.findGaps({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
                since: now,
                until: now + 30 * 60000,
            });
            // Should only find the first gap
            expect(gaps.length).toBe(1);
        });
    });
    // ==========================================================================
    // Statistics
    // ==========================================================================
    describe('Statistics', () => {
        beforeEach(async () => {
            // Setup test data with multiple exchanges and symbols
            const now = Date.now();
            const points = [
                ...Array.from({ length: 100 }, (_, i) => createTestDataPoint('exchange1', 'BTC/USDT', '1m', now + i * 60000)),
                ...Array.from({ length: 50 }, (_, i) => createTestDataPoint('exchange1', 'ETH/USDT', '1m', now + i * 60000)),
                ...Array.from({ length: 75 }, (_, i) => createTestDataPoint('exchange2', 'BTC/USDT', '1h', now + i * 3600000)),
            ];
            await backend.writeMany(points);
        });
        afterEach(async () => {
            // Clean up test data
            await backend.delete({ exchange: 'exchange1', symbol: 'BTC/USDT', timeframe: '1m' });
            await backend.delete({ exchange: 'exchange1', symbol: 'ETH/USDT', timeframe: '1m' });
            await backend.delete({ exchange: 'exchange2', symbol: 'BTC/USDT', timeframe: '1h' });
        });
        it('should return correct overall stats', async () => {
            const stats = await backend.getStats();
            expect(stats.totalCandles).toBe(225);
            expect(stats.exchanges).toContain('exchange1');
            expect(stats.exchanges).toContain('exchange2');
            expect(stats.symbols).toContain('BTC/USDT');
            expect(stats.symbols).toContain('ETH/USDT');
            expect(stats.timeframes).toContain('1m');
            expect(stats.timeframes).toContain('1h');
            expect(stats.oldestTimestamp).toBeDefined();
            expect(stats.newestTimestamp).toBeDefined();
            expect(stats.oldestTimestamp).toBeLessThan(stats.newestTimestamp);
        });
        it('should filter stats by exchange', async () => {
            const stats = await backend.getStats('exchange1');
            expect(stats.totalCandles).toBe(150); // 100 + 50
            expect(stats.exchanges).toEqual(['exchange1']);
            expect(stats.symbols).toContain('BTC/USDT');
            expect(stats.symbols).toContain('ETH/USDT');
        });
        it('should filter stats by exchange and symbol', async () => {
            const stats = await backend.getStats('exchange1', 'BTC/USDT');
            expect(stats.totalCandles).toBe(100);
            expect(stats.exchanges).toEqual(['exchange1']);
            expect(stats.symbols).toEqual(['BTC/USDT']);
        });
        it('should return zero stats for nonexistent data', async () => {
            const stats = await backend.getStats('nonexistent');
            expect(stats.totalCandles).toBe(0);
            expect(stats.exchanges).toEqual([]);
            expect(stats.symbols).toEqual([]);
            expect(stats.timeframes).toEqual([]);
            expect(stats.oldestTimestamp).toBeUndefined();
            expect(stats.newestTimestamp).toBeUndefined();
        });
    });
    // ==========================================================================
    // Exists, Count, TimeRange
    // ==========================================================================
    describe('Data Operations', () => {
        it('should check if data exists', async () => {
            const existsBefore = await backend.exists({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(existsBefore).toBe(false);
            const point = createTestDataPoint(testExchange, testSymbol, testTimeframe, Date.now());
            await backend.write(point);
            const existsAfter = await backend.exists({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(existsAfter).toBe(true);
        });
        it('should count candles', async () => {
            const now = Date.now();
            const points = Array.from({ length: 42 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            await backend.writeMany(points);
            const count = await backend.count({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(count).toBe(42);
        });
        it('should count with time range filters', async () => {
            const now = Date.now();
            const points = Array.from({ length: 100 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            await backend.writeMany(points);
            const count = await backend.count({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
                since: now + 50 * 60000,
                until: now + 74 * 60000,
            });
            expect(count).toBe(25);
        });
        it('should get time range', async () => {
            const now = Date.now();
            const points = Array.from({ length: 100 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            await backend.writeMany(points);
            const range = await backend.getTimeRange({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(range).not.toBeNull();
            expect(range.earliest).toBe(now);
            expect(range.latest).toBe(now + 99 * 60000);
        });
        it('should return null time range for nonexistent data', async () => {
            const range = await backend.getTimeRange({
                exchange: 'nonexistent',
                symbol: 'NONE/USDT',
                timeframe: '1m',
            });
            expect(range).toBeNull();
        });
    });
    // ==========================================================================
    // Delete Operations
    // ==========================================================================
    describe('Delete Operations', () => {
        it('should delete all data for query', async () => {
            const now = Date.now();
            const points = Array.from({ length: 100 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            await backend.writeMany(points);
            const deleted = await backend.delete({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(deleted).toBe(100);
            const exists = await backend.exists({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(exists).toBe(false);
        });
        it('should delete with time range filter', async () => {
            const now = Date.now();
            const points = Array.from({ length: 100 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            await backend.writeMany(points);
            const deleted = await backend.delete({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
                since: now + 50 * 60000,
                until: now + 74 * 60000,
            });
            expect(deleted).toBe(25);
            const remaining = await backend.count({
                exchange: testExchange,
                symbol: testSymbol,
                timeframe: testTimeframe,
            });
            expect(remaining).toBe(75);
        });
        it('should return 0 when deleting nonexistent data', async () => {
            const deleted = await backend.delete({
                exchange: 'nonexistent',
                symbol: 'NONE/USDT',
                timeframe: '1m',
            });
            expect(deleted).toBe(0);
        });
    });
    // ==========================================================================
    // Optimize
    // ==========================================================================
    describe('Optimize', () => {
        it('should run optimize without errors', async () => {
            const now = Date.now();
            const points = Array.from({ length: 100 }, (_, i) => createTestDataPoint(testExchange, testSymbol, testTimeframe, now + i * 60000));
            await backend.writeMany(points);
            // Should not throw
            await expect(backend.optimize()).resolves.not.toThrow();
        });
    });
    // ==========================================================================
    // Error Handling
    // ==========================================================================
    describe('Error Handling', () => {
        it('should throw when operating on disconnected backend', async () => {
            const tempBackend = new TimescaleDBBackend({
                type: 'timescaledb',
                connectionString: connectionString,
            });
            const point = createTestDataPoint(testExchange, testSymbol, testTimeframe, Date.now());
            await expect(tempBackend.write(point)).rejects.toThrow('not connected');
        });
        it('should handle disconnect gracefully', async () => {
            const tempBackend = new TimescaleDBBackend({
                type: 'timescaledb',
                connectionString: connectionString,
            });
            await tempBackend.connect();
            expect(tempBackend.state).toBe('connected');
            await tempBackend.disconnect();
            expect(tempBackend.state).toBe('disconnected');
            // Multiple disconnects should not throw
            await tempBackend.disconnect();
            expect(tempBackend.state).toBe('disconnected');
        });
    });
});
//# sourceMappingURL=timescaledb.test.js.map