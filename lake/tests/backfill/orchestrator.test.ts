import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import { BackfillOrchestrator, parseTimestamp } from '../../src/backfill.js';
import { SQLiteBackend } from '../../src/backends/sqlite.js';
import type { ExchangeAdapter, BackfillRequest } from '../../src/backfill.js';

// Mock exchange factory
function createMockExchange(options: {
  data?: number[][];
  shouldFail?: boolean;
  delay?: number;
  rateLimit?: number;
}): ExchangeAdapter {
  const { data = [], shouldFail = false, delay = 0, rateLimit = 100 } = options;

  return {
    id: 'mock',
    rateLimit,
    has: { fetchOHLCV: true },
    fetchOHLCV: async (_symbol, _timeframe, since, limit) => {
      if (delay > 0) {
        await new Promise(r => setTimeout(r, delay));
      }

      if (shouldFail) {
        throw new Error('Mock exchange error');
      }

      return data
        .filter(c => c[0] >= (since || 0))
        .slice(0, limit || 1000);
    },
  };
}

// Generate mock OHLCV data
function generateMockOHLCV(count: number, startTime?: number): number[][] {
  const start = startTime || Date.now() - count * 60000;
  return Array.from({ length: count }, (_, i) => [
    start + i * 60000, // timestamp
    100 + Math.random() * 10, // open
    110 + Math.random() * 10, // high
    90 + Math.random() * 10,  // low
    105 + Math.random() * 10, // close
    1000 + Math.random() * 100, // volume
  ]);
}

describe('BackfillOrchestrator', () => {
  let backend: SQLiteBackend;
  const dbPath = './test-backfill-orch.db';

  beforeEach(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
    backend = new SQLiteBackend({ type: 'sqlite', path: dbPath });
    await backend.connect();
  });

  afterEach(async () => {
    await backend.disconnect();
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  });

  describe('Rate Limiting', () => {
    it('should respect rate limit configuration', async () => {
      const data = generateMockOHLCV(2000); // Enough for 2 batches
      const exchange = createMockExchange({ data, rateLimit: 100 });

      const orchestrator = new BackfillOrchestrator(backend, {
        rateLimit: 2, // 2 requests per second (500ms between)
      });

      const startTime = Date.now();
      await orchestrator.backfill(exchange, {
        exchange: 'mock',
        symbol: 'BTC/USDT',
        timeframe: '1m',
      });
      const elapsed = Date.now() - startTime;

      // Should have taken at least 500ms for rate limiting
      expect(elapsed).toBeGreaterThan(400);
    });
  });

  describe('Retry Logic', () => {
    it('should retry on failure with exponential backoff', async () => {
      let attempts = 0;
      const exchange: ExchangeAdapter = {
        id: 'retry-test',
        rateLimit: 100,
        has: { fetchOHLCV: true },
        fetchOHLCV: async () => {
          attempts++;
          if (attempts < 3) {
            throw new Error('Temporary failure');
          }
          return generateMockOHLCV(10);
        },
      };

      const orchestrator = new BackfillOrchestrator(backend, {
        retries: 3,
        retryDelay: 100,
      });

      const result = await orchestrator.backfill(exchange, {
        exchange: 'retry-test',
        symbol: 'BTC/USDT',
        timeframe: '1m',
      });

      expect(attempts).toBe(3);
      expect(result.candlesWritten).toBeGreaterThan(0);
    });

    it('should fail after max retries', async () => {
      const exchange = createMockExchange({ shouldFail: true });

      const orchestrator = new BackfillOrchestrator(backend, {
        retries: 2,
        retryDelay: 10,
        stopOnError: false,
      });

      const now = Date.now();
      const result = await orchestrator.backfill(exchange, {
        exchange: 'mock',
        symbol: 'BTC/USDT',
        timeframe: '1m',
        since: now - 60000, // Just 1 minute of data
        until: now,
      });

      expect(result.errors.length).toBeGreaterThan(0);
    }, 10000); // 10 second timeout
  });

  describe('Progress Callbacks', () => {
    it('should call progress callback with correct data', async () => {
      const data = generateMockOHLCV(100);
      const exchange = createMockExchange({ data });

      const progressUpdates: any[] = [];
      const orchestrator = new BackfillOrchestrator(backend, {
        rateLimit: 100,
        onProgress: (progress) => {
          progressUpdates.push({ ...progress });
        },
      });

      await orchestrator.backfill(exchange, {
        exchange: 'mock',
        symbol: 'BTC/USDT',
        timeframe: '1m',
      });

      expect(progressUpdates.length).toBeGreaterThan(0);
      expect(progressUpdates[0]).toHaveProperty('current');
      expect(progressUpdates[0]).toHaveProperty('total');
      expect(progressUpdates[0]).toHaveProperty('exchange');
      expect(progressUpdates[0]).toHaveProperty('symbol');
      expect(progressUpdates[0]).toHaveProperty('timeframe');
    });
  });

  describe('Gap Filling', () => {
    it('should fill gaps in existing data', async () => {
      // First, write some data with a gap
      const baseTime = Date.now() - 10 * 60000;
      const points = [
        // First 3 candles
        ...Array.from({ length: 3 }, (_, i) => ({
          timestamp: baseTime + i * 60000,
          exchange: 'mock',
          symbol: 'BTC/USDT',
          timeframe: '1m',
          data: { timestamp: baseTime + i * 60000, open: 100, high: 110, low: 90, close: 105, volume: 1000 },
        })),
        // Gap here (candles 3, 4, 5 missing)
        // Last 3 candles
        ...Array.from({ length: 3 }, (_, i) => ({
          timestamp: baseTime + (i + 6) * 60000,
          exchange: 'mock',
          symbol: 'BTC/USDT',
          timeframe: '1m',
          data: { timestamp: baseTime + (i + 6) * 60000, open: 100, high: 110, low: 90, close: 105, volume: 1000 },
        })),
      ];

      await backend.writeMany(points);

      // Verify gap exists
      const gapsBefore = await backend.findGaps({
        exchange: 'mock',
        symbol: 'BTC/USDT',
        timeframe: '1m',
      });
      expect(gapsBefore.length).toBe(1);

      // Create exchange with data to fill gap
      const gapData = generateMockOHLCV(10, baseTime);
      const exchange = createMockExchange({ data: gapData });

      const orchestrator = new BackfillOrchestrator(backend, { rateLimit: 100 });
      const result = await orchestrator.fillGaps(exchange, {
        exchange: 'mock',
        symbol: 'BTC/USDT',
        timeframe: '1m',
      });

      expect(result.candlesWritten).toBeGreaterThan(0);
    });
  });

  describe('Multiple Requests', () => {
    it('should handle multiple backfill requests', async () => {
      const data = generateMockOHLCV(50);
      const exchanges = new Map<string, ExchangeAdapter>([
        ['exchange1', createMockExchange({ data })],
        ['exchange2', createMockExchange({ data })],
      ]);

      const requests: BackfillRequest[] = [
        { exchange: 'exchange1', symbol: 'BTC/USDT', timeframe: '1m' },
        { exchange: 'exchange2', symbol: 'ETH/USDT', timeframe: '1m' },
      ];

      const orchestrator = new BackfillOrchestrator(backend, { rateLimit: 100 });
      const results = await orchestrator.backfillMany(exchanges, requests);

      expect(results.length).toBe(2);
      expect(results[0].exchange).toBe('exchange1');
      expect(results[1].exchange).toBe('exchange2');
    });

    it('should handle missing exchange gracefully', async () => {
      const exchanges = new Map<string, ExchangeAdapter>();

      const requests: BackfillRequest[] = [
        { exchange: 'nonexistent', symbol: 'BTC/USDT', timeframe: '1m' },
      ];

      const orchestrator = new BackfillOrchestrator(backend, { rateLimit: 100 });
      const results = await orchestrator.backfillMany(exchanges, requests);

      expect(results.length).toBe(1);
      expect(results[0].errors.length).toBeGreaterThan(0);
      expect(results[0].errors[0]).toContain('not found');
    });
  });
});

describe('parseTimestamp', () => {
  it('should handle all supported formats', () => {
    // Numeric
    expect(parseTimestamp(1700000000000)).toBe(1700000000000);

    // ISO string
    const isoResult = parseTimestamp('2024-01-15T12:00:00Z');
    expect(isoResult).toBe(Date.parse('2024-01-15T12:00:00Z'));

    // Relative: days
    const daysResult = parseTimestamp('7d')!;
    const expectedDays = Date.now() - 7 * 24 * 60 * 60 * 1000;
    expect(Math.abs(daysResult - expectedDays)).toBeLessThan(1000);

    // Relative: weeks
    const weeksResult = parseTimestamp('2w')!;
    const expectedWeeks = Date.now() - 14 * 24 * 60 * 60 * 1000;
    expect(Math.abs(weeksResult - expectedWeeks)).toBeLessThan(1000);

    // Relative: months
    const monthsResult = parseTimestamp('1M')!;
    const expectedMonths = Date.now() - 30 * 24 * 60 * 60 * 1000;
    expect(Math.abs(monthsResult - expectedMonths)).toBeLessThan(1000);

    // Undefined
    expect(parseTimestamp(undefined)).toBeUndefined();
  });

  it('should throw for invalid format', () => {
    expect(() => parseTimestamp('invalid')).toThrow('Invalid timestamp');
  });
});
