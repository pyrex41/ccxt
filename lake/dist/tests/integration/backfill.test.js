import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import { BackfillOrchestrator, parseTimestamp } from '../../src/backfill.js';
import { SQLiteBackend } from '../../src/backends/sqlite.js';
// Mock exchange adapter
function createMockExchange(data) {
    return {
        id: 'mock',
        rateLimit: 100,
        has: { fetchOHLCV: true },
        fetchOHLCV: async (_symbol, _timeframe, since, limit) => {
            // Return mock data filtered by since and limited
            return data
                .filter(c => c[0] >= (since || 0))
                .slice(0, limit || 1000);
        },
    };
}
describe('Backfill Orchestrator', () => {
    let backend;
    const testDbPath = './test-backfill.db';
    beforeEach(async () => {
        if (fs.existsSync(testDbPath))
            fs.unlinkSync(testDbPath);
        backend = new SQLiteBackend({ type: 'sqlite', path: testDbPath });
        await backend.connect();
    });
    afterEach(async () => {
        await backend.disconnect();
        if (fs.existsSync(testDbPath))
            fs.unlinkSync(testDbPath);
    });
    it('should backfill data from mock exchange', async () => {
        // Create mock OHLCV data
        const mockData = Array.from({ length: 100 }, (_, i) => [
            Date.now() - (100 - i) * 60000, // timestamp
            100, // open
            110, // high
            90, // low
            105, // close
            1000, // volume
        ]);
        const exchange = createMockExchange(mockData);
        const orchestrator = new BackfillOrchestrator(backend, {
            rateLimit: 10,
        });
        const result = await orchestrator.backfill(exchange, {
            exchange: 'mock',
            symbol: 'BTC/USDT',
            timeframe: '1m',
        });
        expect(result.candlesWritten).toBeGreaterThan(0);
        expect(result.errors.length).toBe(0);
    });
    it('should track progress', async () => {
        const mockData = Array.from({ length: 50 }, (_, i) => [
            Date.now() - (50 - i) * 60000,
            100, 110, 90, 105, 1000,
        ]);
        const exchange = createMockExchange(mockData);
        const progressCalls = [];
        const orchestrator = new BackfillOrchestrator(backend, {
            rateLimit: 100,
            onProgress: (progress) => {
                progressCalls.push(progress.current);
            },
        });
        await orchestrator.backfill(exchange, {
            exchange: 'mock',
            symbol: 'BTC/USDT',
            timeframe: '1m',
        });
        expect(progressCalls.length).toBeGreaterThan(0);
    });
});
describe('parseTimestamp', () => {
    it('should parse numeric timestamps', () => {
        expect(parseTimestamp(1234567890000)).toBe(1234567890000);
    });
    it('should parse ISO dates', () => {
        const result = parseTimestamp('2024-01-01T00:00:00Z');
        expect(result).toBe(Date.parse('2024-01-01T00:00:00Z'));
    });
    it('should parse relative times', () => {
        const now = Date.now();
        const result = parseTimestamp('7d');
        expect(result).toBeLessThan(now);
        expect(result).toBeGreaterThan(now - 8 * 24 * 60 * 60 * 1000);
    });
    it('should return undefined for undefined input', () => {
        expect(parseTimestamp(undefined)).toBeUndefined();
    });
});
//# sourceMappingURL=backfill.test.js.map