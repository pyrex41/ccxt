/**
 * Backend interface for CCXT Data Lake
 *
 * This interface defines the contract that all storage backends
 * (SQLite, Parquet, TimescaleDB) must implement.
 */

import type {
  Candle,
  DataPoint,
  DataQuery,
  Gap,
  LakeStats,
  BackendConfig,
} from './types.js';

/**
 * Write options for batch operations
 */
export interface WriteOptions {
  /** Whether to replace existing data (upsert) */
  upsert?: boolean;
  /** Batch size for bulk operations */
  batchSize?: number;
}

/**
 * Read options for query operations
 */
export interface ReadOptions {
  /** Order of results: 'asc' or 'desc' by timestamp */
  order?: 'asc' | 'desc';
  /** Include metadata in results */
  includeMetadata?: boolean;
}

/**
 * Search options for full-text search (FTS5 in SQLite)
 */
export interface SearchOptions {
  /** Fields to search in */
  fields?: string[];
  /** Maximum number of results */
  limit?: number;
  /** Match mode: prefix, phrase, or token */
  matchMode?: 'prefix' | 'phrase' | 'token';
}

/**
 * Search result with relevance score
 */
export interface SearchResult {
  exchange: string;
  symbol: string;
  timeframe: string;
  /** Relevance score (higher is better) */
  score: number;
  /** Matched content snippet */
  snippet?: string;
}

/**
 * Gap detection options
 */
export interface GapDetectionOptions {
  /** Minimum gap size in milliseconds to report */
  minGapSize?: number;
  /** Expected interval between candles in milliseconds */
  expectedInterval?: number;
}

/**
 * Backfill progress callback
 */
export type ProgressCallback = (progress: {
  current: number;
  total: number;
  exchange: string;
  symbol: string;
  timeframe: string;
  message?: string;
}) => void;

/**
 * Backfill options
 */
export interface BackfillOptions {
  /** Progress callback */
  onProgress?: ProgressCallback;
  /** Rate limit in requests per second */
  rateLimit?: number;
  /** Retry count for failed requests */
  retries?: number;
  /** Delay between retries in milliseconds */
  retryDelay?: number;
  /** Stop on error or continue */
  stopOnError?: boolean;
}

/**
 * Connection state
 */
export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error';

/**
 * Backend interface that all storage implementations must implement
 */
export interface Backend {
  /**
   * Backend type identifier
   */
  readonly type: BackendConfig['type'];

  /**
   * Current connection state
   */
  readonly state: ConnectionState;

  /**
   * Connect to the backend storage
   * @returns Promise that resolves when connected
   */
  connect(): Promise<void>;

  /**
   * Disconnect from the backend storage
   * @returns Promise that resolves when disconnected
   */
  disconnect(): Promise<void>;

  /**
   * Write a single data point
   * @param point Data point to write
   * @param options Write options
   */
  write(point: DataPoint, options?: WriteOptions): Promise<void>;

  /**
   * Write multiple data points in a batch
   * @param points Array of data points to write
   * @param options Write options including batch size
   * @returns Number of points written
   */
  writeMany(points: DataPoint[], options?: WriteOptions): Promise<number>;

  /**
   * Read data points matching the query
   * @param query Query parameters
   * @param options Read options
   * @returns Array of candles
   */
  read(query: DataQuery, options?: ReadOptions): Promise<Candle[]>;

  /**
   * Find gaps in the data
   * @param query Query parameters specifying the range to check
   * @param options Gap detection options
   * @returns Array of gaps found
   */
  findGaps(query: DataQuery, options?: GapDetectionOptions): Promise<Gap[]>;

  /**
   * Get statistics about stored data
   * @param exchange Optional exchange filter
   * @param symbol Optional symbol filter
   * @returns Lake statistics
   */
  getStats(exchange?: string, symbol?: string): Promise<LakeStats>;

  /**
   * Check if data exists for the given query
   * @param query Query parameters
   * @returns True if data exists
   */
  exists(query: DataQuery): Promise<boolean>;

  /**
   * Delete data matching the query
   * @param query Query parameters
   * @returns Number of records deleted
   */
  delete(query: DataQuery): Promise<number>;

  /**
   * Search for symbols/exchanges (optional - mainly for SQLite FTS5)
   * @param searchTerm Search term
   * @param options Search options
   * @returns Search results with relevance scores
   */
  search?(searchTerm: string, options?: SearchOptions): Promise<SearchResult[]>;

  /**
   * Vacuum/optimize the storage (optional)
   * @returns Promise that resolves when optimization is complete
   */
  optimize?(): Promise<void>;

  /**
   * Get the earliest and latest timestamps for a query
   * @param query Query parameters (exchange, symbol, timeframe)
   * @returns Object with earliest and latest timestamps, or null if no data
   */
  getTimeRange(query: Omit<DataQuery, 'since' | 'until' | 'limit'>): Promise<{
    earliest: number;
    latest: number;
  } | null>;

  /**
   * Count the number of candles matching the query
   * @param query Query parameters
   * @returns Number of candles
   */
  count(query: DataQuery): Promise<number>;
}

/**
 * Abstract base class for backend implementations
 * Provides common functionality and default implementations
 */
export abstract class BaseBackend implements Backend {
  abstract readonly type: BackendConfig['type'];
  protected _state: ConnectionState = 'disconnected';

  get state(): ConnectionState {
    return this._state;
  }

  abstract connect(): Promise<void>;
  abstract disconnect(): Promise<void>;
  abstract write(point: DataPoint, options?: WriteOptions): Promise<void>;
  abstract writeMany(points: DataPoint[], options?: WriteOptions): Promise<number>;
  abstract read(query: DataQuery, options?: ReadOptions): Promise<Candle[]>;
  abstract getStats(exchange?: string, symbol?: string): Promise<LakeStats>;
  abstract exists(query: DataQuery): Promise<boolean>;
  abstract delete(query: DataQuery): Promise<number>;
  abstract getTimeRange(query: Omit<DataQuery, 'since' | 'until' | 'limit'>): Promise<{
    earliest: number;
    latest: number;
  } | null>;
  abstract count(query: DataQuery): Promise<number>;

  /**
   * Ensure backend is connected before operations
   */
  protected ensureConnected(): void {
    if (this._state !== 'connected') {
      throw new Error(`Backend is not connected (state: ${this._state})`);
    }
  }

  /**
   * Calculate expected interval based on timeframe
   */
  protected getTimeframeMs(timeframe: string): number {
    const units: Record<string, number> = {
      's': 1000,
      'm': 60 * 1000,
      'h': 60 * 60 * 1000,
      'd': 24 * 60 * 60 * 1000,
      'w': 7 * 24 * 60 * 60 * 1000,
      'M': 30 * 24 * 60 * 60 * 1000, // Approximate month
    };

    const match = timeframe.match(/^(\d+)([smhdwM])$/);
    if (!match) {
      throw new Error(`Invalid timeframe format: ${timeframe}`);
    }

    const [, value, unit] = match;
    const multiplier = units[unit];
    if (!multiplier) {
      throw new Error(`Unknown timeframe unit: ${unit}`);
    }

    return parseInt(value, 10) * multiplier;
  }

  /**
   * Default gap detection implementation
   * Backends can override for optimized implementations
   */
  async findGaps(query: DataQuery, options?: GapDetectionOptions): Promise<Gap[]> {
    this.ensureConnected();

    const candles = await this.read(query, { order: 'asc' });
    if (candles.length < 2) {
      return [];
    }

    const expectedInterval = options?.expectedInterval || this.getTimeframeMs(query.timeframe);
    const minGapSize = options?.minGapSize || expectedInterval * 1.5;
    const gaps: Gap[] = [];

    for (let i = 1; i < candles.length; i++) {
      const gap = candles[i].timestamp - candles[i - 1].timestamp;
      if (gap > minGapSize) {
        gaps.push({
          start: candles[i - 1].timestamp + expectedInterval,
          end: candles[i].timestamp - expectedInterval,
          exchange: query.exchange,
          symbol: query.symbol,
          timeframe: query.timeframe,
        });
      }
    }

    return gaps;
  }
}

/**
 * Factory function type for creating backends
 */
export type BackendFactory = (config: BackendConfig) => Backend;

/**
 * Create a backend instance based on configuration
 * This will be implemented once all backends are available
 */
export async function createBackend(config: BackendConfig): Promise<Backend> {
  switch (config.type) {
    case 'sqlite':
      // Dynamic import to avoid loading all backends
      const { SQLiteBackend } = await import('./backends/sqlite.js');
      return new SQLiteBackend(config);
    case 'parquet':
      const { ParquetBackend } = await import('./backends/parquet.js');
      return new ParquetBackend(config);
    case 'timescaledb':
      const { TimescaleDBBackend } = await import('./backends/timescaledb.js');
      return new TimescaleDBBackend(config);
    default:
      throw new Error(`Unknown backend type: ${(config as BackendConfig).type}`);
  }
}
