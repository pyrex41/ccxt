/**
 * TimescaleDB backend for CCXT Data Lake
 * Uses pg with hypertables for time-series data
 */

import { Pool } from 'pg';
import type { BackendConfig } from '../types.js';
import { BaseBackend } from '../interface.js';
import type {
  WriteOptions,
  ReadOptions,
  GapDetectionOptions,
} from '../interface.js';
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
export class TimescaleDBBackend extends BaseBackend {
  readonly type = 'timescaledb' as const;
  private pool: Pool | null = null;
  private config: BackendConfig;

  constructor(config: BackendConfig) {
    super();
    this.config = config;

    if (!config.connectionString) {
      throw new Error('TimescaleDB backend requires connectionString in config');
    }
  }

  /**
   * Connect to TimescaleDB and setup schema
   */
  async connect(): Promise<void> {
    if (this._state === 'connected') {
      return;
    }

    this._state = 'connecting';

    try {
      // Create connection pool
      this.pool = new Pool({
        connectionString: this.config.connectionString,
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000,
      });

      // Test connection
      const client = await this.pool.connect();
      try {
        await client.query('SELECT 1');
      } finally {
        client.release();
      }

      // Setup schema (idempotent)
      await this.setupSchema();

      this._state = 'connected';
    } catch (error) {
      this._state = 'error';
      this.pool = null;
      throw new Error(`Failed to connect to TimescaleDB: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from TimescaleDB
   */
  async disconnect(): Promise<void> {
    if (this._state === 'disconnected') {
      return;
    }

    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }

    this._state = 'disconnected';
  }

  /**
   * Setup database schema with hypertable and policies
   */
  private async setupSchema(): Promise<void> {
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Enable TimescaleDB extension
      await client.query('CREATE EXTENSION IF NOT EXISTS timescaledb');

      // Create main candles table
      await client.query(`
        CREATE TABLE IF NOT EXISTS candles (
          timestamp TIMESTAMPTZ NOT NULL,
          exchange TEXT NOT NULL,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          open DOUBLE PRECISION NOT NULL,
          high DOUBLE PRECISION NOT NULL,
          low DOUBLE PRECISION NOT NULL,
          close DOUBLE PRECISION NOT NULL,
          volume DOUBLE PRECISION NOT NULL,
          PRIMARY KEY (exchange, symbol, timeframe, timestamp)
        )
      `);

      // Convert to hypertable (idempotent)
      await client.query(`
        SELECT create_hypertable('candles', 'timestamp',
          chunk_time_interval => INTERVAL '1 day',
          if_not_exists => TRUE
        )
      `);

      // Create index for efficient queries
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_candles_lookup
        ON candles (exchange, symbol, timeframe, timestamp DESC)
      `);

      // Add compression policy (compress chunks older than 7 days)
      // Note: compression must be enabled first
      await client.query(`
        DO $$
        BEGIN
          BEGIN
            ALTER TABLE candles SET (
              timescaledb.compress,
              timescaledb.compress_segmentby = 'exchange, symbol, timeframe'
            );
          EXCEPTION
            WHEN duplicate_object THEN NULL;
          END;
        END $$;
      `);

      await client.query(`
        SELECT add_compression_policy('candles', INTERVAL '7 days', if_not_exists => TRUE)
      `);

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Write a single data point
   */
  async write(point: DataPoint, options?: WriteOptions): Promise<void> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const candle = point.data as Candle;

    const query = options?.upsert
      ? `
        INSERT INTO candles (timestamp, exchange, symbol, timeframe, open, high, low, close, volume)
        VALUES (to_timestamp($1 / 1000.0), $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (exchange, symbol, timeframe, timestamp)
        DO UPDATE SET
          open = EXCLUDED.open,
          high = EXCLUDED.high,
          low = EXCLUDED.low,
          close = EXCLUDED.close,
          volume = EXCLUDED.volume
      `
      : `
        INSERT INTO candles (timestamp, exchange, symbol, timeframe, open, high, low, close, volume)
        VALUES (to_timestamp($1 / 1000.0), $2, $3, $4, $5, $6, $7, $8, $9)
      `;

    await this.pool.query(query, [
      point.timestamp,
      point.exchange,
      point.symbol,
      point.timeframe,
      candle.open,
      candle.high,
      candle.low,
      candle.close,
      candle.volume,
    ]);
  }

  /**
   * Write multiple data points in a batch
   */
  async writeMany(points: DataPoint[], options?: WriteOptions): Promise<number> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    if (points.length === 0) {
      return 0;
    }

    const batchSize = options?.batchSize || 1000;
    let totalWritten = 0;

    // Process in batches
    for (let i = 0; i < points.length; i += batchSize) {
      const batch = points.slice(i, i + batchSize);

      const client = await this.pool.connect();
      try {
        await client.query('BEGIN');

        // Build values array for batch insert
        const values: any[] = [];
        const valueStrings: string[] = [];
        let paramIndex = 1;

        for (const point of batch) {
          const candle = point.data as Candle;

          valueStrings.push(
            `(to_timestamp($${paramIndex} / 1000.0), $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, $${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6}, $${paramIndex + 7}, $${paramIndex + 8})`
          );

          values.push(
            point.timestamp,
            point.exchange,
            point.symbol,
            point.timeframe,
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume
          );

          paramIndex += 9;
        }

        const query = options?.upsert
          ? `
            INSERT INTO candles (timestamp, exchange, symbol, timeframe, open, high, low, close, volume)
            VALUES ${valueStrings.join(', ')}
            ON CONFLICT (exchange, symbol, timeframe, timestamp)
            DO UPDATE SET
              open = EXCLUDED.open,
              high = EXCLUDED.high,
              low = EXCLUDED.low,
              close = EXCLUDED.close,
              volume = EXCLUDED.volume
          `
          : `
            INSERT INTO candles (timestamp, exchange, symbol, timeframe, open, high, low, close, volume)
            VALUES ${valueStrings.join(', ')}
          `;

        await client.query(query, values);
        await client.query('COMMIT');

        totalWritten += batch.length;
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    }

    return totalWritten;
  }

  /**
   * Read candles matching the query
   */
  async read(query: DataQuery, options?: ReadOptions): Promise<Candle[]> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const order = options?.order === 'desc' ? 'DESC' : 'ASC';
    const params: any[] = [query.exchange, query.symbol, query.timeframe];
    let paramIndex = 4;

    let sql = `
      SELECT
        EXTRACT(EPOCH FROM timestamp) * 1000 as timestamp,
        open, high, low, close, volume
      FROM candles
      WHERE exchange = $1 AND symbol = $2 AND timeframe = $3
    `;

    if (query.since !== undefined) {
      sql += ` AND timestamp >= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.since);
      paramIndex++;
    }

    if (query.until !== undefined) {
      sql += ` AND timestamp <= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.until);
      paramIndex++;
    }

    sql += ` ORDER BY timestamp ${order}`;

    if (query.limit !== undefined) {
      sql += ` LIMIT $${paramIndex}`;
      params.push(query.limit);
    }

    const result = await this.pool.query(sql, params);

    return result.rows.map(row => ({
      timestamp: Math.round(parseFloat(row.timestamp)),
      open: parseFloat(row.open),
      high: parseFloat(row.high),
      low: parseFloat(row.low),
      close: parseFloat(row.close),
      volume: parseFloat(row.volume),
    }));
  }

  /**
   * Find gaps in the data using window functions
   */
  async findGaps(query: DataQuery, options?: GapDetectionOptions): Promise<Gap[]> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const expectedInterval = options?.expectedInterval || this.getTimeframeMs(query.timeframe);
    const minGapSize = options?.minGapSize || expectedInterval * 1.5;

    const params: any[] = [query.exchange, query.symbol, query.timeframe];
    let paramIndex = 4;

    let whereClause = 'WHERE exchange = $1 AND symbol = $2 AND timeframe = $3';

    if (query.since !== undefined) {
      whereClause += ` AND timestamp >= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.since);
      paramIndex++;
    }

    if (query.until !== undefined) {
      whereClause += ` AND timestamp <= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.until);
      paramIndex++;
    }

    // Add the gap threshold as interval
    const gapIntervalMs = minGapSize;
    params.push(`${gapIntervalMs} milliseconds`);
    const gapParamIndex = paramIndex;

    const sql = `
      WITH ordered_candles AS (
        SELECT
          timestamp,
          LEAD(timestamp) OVER (ORDER BY timestamp) as next_timestamp
        FROM candles
        ${whereClause}
        ORDER BY timestamp
      )
      SELECT
        EXTRACT(EPOCH FROM timestamp) * 1000 as gap_start,
        EXTRACT(EPOCH FROM next_timestamp) * 1000 as gap_end,
        EXTRACT(EPOCH FROM (next_timestamp - timestamp)) * 1000 as gap_ms
      FROM ordered_candles
      WHERE next_timestamp - timestamp > $${gapParamIndex}::interval
    `;

    const result = await this.pool.query(sql, params);

    return result.rows.map(row => ({
      start: Math.round(parseFloat(row.gap_start)) + expectedInterval,
      end: Math.round(parseFloat(row.gap_end)) - expectedInterval,
      exchange: query.exchange,
      symbol: query.symbol,
      timeframe: query.timeframe,
    }));
  }

  /**
   * Get statistics about stored data
   */
  async getStats(exchange?: string, symbol?: string): Promise<LakeStats> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const params: any[] = [];
    let whereClause = '';
    let paramIndex = 1;

    if (exchange) {
      whereClause = `WHERE exchange = $${paramIndex}`;
      params.push(exchange);
      paramIndex++;

      if (symbol) {
        whereClause += ` AND symbol = $${paramIndex}`;
        params.push(symbol);
        paramIndex++;
      }
    } else if (symbol) {
      whereClause = `WHERE symbol = $${paramIndex}`;
      params.push(symbol);
      paramIndex++;
    }

    const countQuery = `SELECT COUNT(*) as total FROM candles ${whereClause}`;
    const countResult = await this.pool.query(countQuery, params);

    const exchangesQuery = `SELECT DISTINCT exchange FROM candles ${whereClause} ORDER BY exchange`;
    const exchangesResult = await this.pool.query(exchangesQuery, params);

    const symbolsQuery = `SELECT DISTINCT symbol FROM candles ${whereClause} ORDER BY symbol`;
    const symbolsResult = await this.pool.query(symbolsQuery, params);

    const timeframesQuery = `SELECT DISTINCT timeframe FROM candles ${whereClause} ORDER BY timeframe`;
    const timeframesResult = await this.pool.query(timeframesQuery, params);

    const timeRangeQuery = `
      SELECT
        EXTRACT(EPOCH FROM MIN(timestamp)) * 1000 as oldest,
        EXTRACT(EPOCH FROM MAX(timestamp)) * 1000 as newest
      FROM candles ${whereClause}
    `;
    const timeRangeResult = await this.pool.query(timeRangeQuery, params);

    const timeRange = timeRangeResult.rows[0];

    return {
      totalCandles: parseInt(countResult.rows[0].total, 10),
      exchanges: exchangesResult.rows.map(r => r.exchange),
      symbols: symbolsResult.rows.map(r => r.symbol),
      timeframes: timeframesResult.rows.map(r => r.timeframe),
      oldestTimestamp: timeRange.oldest ? Math.round(parseFloat(timeRange.oldest)) : undefined,
      newestTimestamp: timeRange.newest ? Math.round(parseFloat(timeRange.newest)) : undefined,
    };
  }

  /**
   * Check if data exists for the given query
   */
  async exists(query: DataQuery): Promise<boolean> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const params: any[] = [query.exchange, query.symbol, query.timeframe];
    let paramIndex = 4;

    let sql = `
      SELECT EXISTS(
        SELECT 1 FROM candles
        WHERE exchange = $1 AND symbol = $2 AND timeframe = $3
    `;

    if (query.since !== undefined) {
      sql += ` AND timestamp >= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.since);
      paramIndex++;
    }

    if (query.until !== undefined) {
      sql += ` AND timestamp <= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.until);
      paramIndex++;
    }

    sql += ' LIMIT 1) as exists';

    const result = await this.pool.query(sql, params);
    return result.rows[0].exists;
  }

  /**
   * Delete data matching the query
   */
  async delete(query: DataQuery): Promise<number> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const params: any[] = [query.exchange, query.symbol, query.timeframe];
    let paramIndex = 4;

    let sql = `
      DELETE FROM candles
      WHERE exchange = $1 AND symbol = $2 AND timeframe = $3
    `;

    if (query.since !== undefined) {
      sql += ` AND timestamp >= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.since);
      paramIndex++;
    }

    if (query.until !== undefined) {
      sql += ` AND timestamp <= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.until);
      paramIndex++;
    }

    const result = await this.pool.query(sql, params);
    return result.rowCount || 0;
  }

  /**
   * Get the earliest and latest timestamps for a query
   */
  async getTimeRange(query: Omit<DataQuery, 'since' | 'until' | 'limit'>): Promise<{
    earliest: number;
    latest: number;
  } | null> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const sql = `
      SELECT
        EXTRACT(EPOCH FROM MIN(timestamp)) * 1000 as earliest,
        EXTRACT(EPOCH FROM MAX(timestamp)) * 1000 as latest
      FROM candles
      WHERE exchange = $1 AND symbol = $2 AND timeframe = $3
    `;

    const result = await this.pool.query(sql, [query.exchange, query.symbol, query.timeframe]);
    const row = result.rows[0];

    if (!row.earliest || !row.latest) {
      return null;
    }

    return {
      earliest: Math.round(parseFloat(row.earliest)),
      latest: Math.round(parseFloat(row.latest)),
    };
  }

  /**
   * Count the number of candles matching the query
   */
  async count(query: DataQuery): Promise<number> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const params: any[] = [query.exchange, query.symbol, query.timeframe];
    let paramIndex = 4;

    let sql = `
      SELECT COUNT(*) as count
      FROM candles
      WHERE exchange = $1 AND symbol = $2 AND timeframe = $3
    `;

    if (query.since !== undefined) {
      sql += ` AND timestamp >= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.since);
      paramIndex++;
    }

    if (query.until !== undefined) {
      sql += ` AND timestamp <= to_timestamp($${paramIndex} / 1000.0)`;
      params.push(query.until);
      paramIndex++;
    }

    const result = await this.pool.query(sql, params);
    return parseInt(result.rows[0].count, 10);
  }

  /**
   * Optimize the database
   * Runs VACUUM ANALYZE and manual compression
   */
  async optimize(): Promise<void> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    const client = await this.pool.connect();
    try {
      // VACUUM cannot run inside a transaction block
      // Run it separately
      await client.query('VACUUM ANALYZE candles');

      // Manually compress chunks if needed
      // This forces compression on chunks that match the policy
      await client.query(`
        SELECT compress_chunk(i, if_not_compressed => true)
        FROM show_chunks('candles') i
      `);
    } finally {
      client.release();
    }
  }
}
