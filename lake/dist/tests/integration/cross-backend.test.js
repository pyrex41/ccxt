import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import * as fs from 'fs';
import { SQLiteBackend } from '../../src/backends/sqlite.js';
import { ParquetBackend } from '../../src/backends/parquet.js';
function createTestData(count) {
    const baseTime = Date.now() - count * 60000;
    return Array.from({ length: count }, (_, i) => ({
        timestamp: baseTime + i * 60000,
        exchange: 'binance',
        symbol: 'BTCUSDT', // Use symbol without slash for file path compatibility
        timeframe: '1m',
        data: {
            timestamp: baseTime + i * 60000,
            open: 50000 + Math.random() * 1000,
            high: 51000 + Math.random() * 1000,
            low: 49000 + Math.random() * 1000,
            close: 50500 + Math.random() * 1000,
            volume: 100 + Math.random() * 50,
        },
    }));
}
describe('Cross-Backend Consistency', () => {
    const backends = [];
    const testData = createTestData(100);
    beforeAll(async () => {
        // Setup SQLite
        const sqlitePath = './test-cross-sqlite.db';
        const sqliteBackend = new SQLiteBackend({ type: 'sqlite', path: sqlitePath });
        await sqliteBackend.connect();
        backends.push({
            name: 'SQLite',
            backend: sqliteBackend,
            cleanup: async () => {
                await sqliteBackend.disconnect();
                if (fs.existsSync(sqlitePath))
                    fs.unlinkSync(sqlitePath);
            },
        });
        // Setup Parquet
        const parquetPath = './test-cross-parquet';
        const parquetBackend = new ParquetBackend({ type: 'parquet', path: parquetPath });
        await parquetBackend.connect();
        backends.push({
            name: 'Parquet',
            backend: parquetBackend,
            cleanup: async () => {
                await parquetBackend.disconnect();
                await fs.promises.rm(parquetPath, { recursive: true, force: true });
            },
        });
        // Write same data to all backends
        for (const { backend } of backends) {
            await backend.writeMany(testData);
        }
        // Flush all backends to ensure data is persisted
        // (Parquet backend uses write buffers that need explicit flushing)
        for (const { backend } of backends) {
            await backend.disconnect();
            await backend.connect();
        }
    });
    afterAll(async () => {
        for (const { cleanup } of backends) {
            await cleanup();
        }
    });
    it('should return same count across backends', async () => {
        const counts = await Promise.all(backends.map(async ({ backend }) => backend.count({
            exchange: 'binance',
            symbol: 'BTCUSDT',
            timeframe: '1m',
        })));
        // All backends should have same count
        expect(new Set(counts).size).toBe(1);
        expect(counts[0]).toBe(testData.length);
    });
    it('should return same time range across backends', async () => {
        const ranges = await Promise.all(backends.map(async ({ backend }) => backend.getTimeRange({
            exchange: 'binance',
            symbol: 'BTCUSDT',
            timeframe: '1m',
        })));
        // All backends should have same time range
        for (const range of ranges) {
            expect(range).not.toBeNull();
            expect(range.earliest).toBe(ranges[0].earliest);
            expect(range.latest).toBe(ranges[0].latest);
        }
    });
    it('should read same data across backends', async () => {
        const results = await Promise.all(backends.map(async ({ backend }) => backend.read({
            exchange: 'binance',
            symbol: 'BTCUSDT',
            timeframe: '1m',
            limit: 10,
        }, { order: 'asc' })));
        // Verify same timestamps
        for (const result of results) {
            expect(result.length).toBe(10);
            expect(result.map(c => c.timestamp)).toEqual(results[0].map(c => c.timestamp));
        }
    });
});
//# sourceMappingURL=cross-backend.test.js.map