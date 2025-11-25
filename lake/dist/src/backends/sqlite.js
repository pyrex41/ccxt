/**
 * SQLite backend for CCXT Data Lake
 * Uses better-sqlite3 with FTS5 for full-text search
 */
import Database from 'better-sqlite3';
import { BaseBackend } from '../interface.js';
/**
 * SQLite backend implementation with FTS5 full-text search
 * Optimized for high-throughput writes (>50,000 candles/sec)
 */
export class SQLiteBackend extends BaseBackend {
    type = 'sqlite';
    db = null;
    dbPath;
    // Prepared statements for performance
    stmtInsert = null;
    stmtTimeRange = null;
    constructor(config) {
        super();
        if (!config.path) {
            throw new Error('SQLite backend requires a path in the configuration');
        }
        this.dbPath = config.path;
    }
    /**
     * Connect to the SQLite database and create schema
     */
    async connect() {
        if (this._state === 'connected') {
            return;
        }
        try {
            this._state = 'connecting';
            // Open database with optimal settings
            this.db = new Database(this.dbPath);
            // Enable WAL mode for better concurrency and performance
            this.db.pragma('journal_mode = WAL');
            // Synchronous mode for better performance
            this.db.pragma('synchronous = NORMAL');
            // Increase cache size (10MB)
            this.db.pragma('cache_size = -10000');
            // Memory mapped I/O (1GB)
            this.db.pragma('mmap_size = 1073741824');
            // Optimize for speed
            this.db.pragma('temp_store = MEMORY');
            // Create schema
            this.createSchema();
            // Prepare frequently used statements
            this.prepareStatements();
            this._state = 'connected';
        }
        catch (error) {
            this._state = 'error';
            throw new Error(`Failed to connect to SQLite database: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Disconnect from the SQLite database
     */
    async disconnect() {
        if (this._state === 'disconnected') {
            return;
        }
        try {
            // Clear prepared statement references
            // better-sqlite3 automatically cleans up statements when database closes
            this.stmtInsert = null;
            this.stmtTimeRange = null;
            // Close database
            if (this.db) {
                this.db.close();
                this.db = null;
            }
            this._state = 'disconnected';
        }
        catch (error) {
            this._state = 'error';
            throw new Error(`Failed to disconnect from SQLite database: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Write a single data point
     */
    async write(point, _options) {
        this.ensureConnected();
        if (!this.db || !this.stmtInsert) {
            throw new Error('Database not properly initialized');
        }
        const candle = point.data;
        try {
            this.stmtInsert.run(point.exchange, point.symbol, point.timeframe, point.timestamp, candle.open, candle.high, candle.low, candle.close, candle.volume);
        }
        catch (error) {
            throw new Error(`Failed to write data point: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Write multiple data points in a batch (optimized for high throughput)
     */
    async writeMany(points, options) {
        this.ensureConnected();
        if (!this.db || !this.stmtInsert) {
            throw new Error('Database not properly initialized');
        }
        if (points.length === 0) {
            return 0;
        }
        const batchSize = options?.batchSize || 1000;
        let written = 0;
        try {
            // Process in batches within transactions for optimal performance
            for (let i = 0; i < points.length; i += batchSize) {
                const batch = points.slice(i, i + batchSize);
                const transaction = this.db.transaction((items) => {
                    for (const point of items) {
                        const candle = point.data;
                        this.stmtInsert.run(point.exchange, point.symbol, point.timeframe, point.timestamp, candle.open, candle.high, candle.low, candle.close, candle.volume);
                    }
                });
                transaction(batch);
                written += batch.length;
            }
            return written;
        }
        catch (error) {
            throw new Error(`Failed to write batch: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Read data points matching the query
     */
    async read(query, options) {
        this.ensureConnected();
        if (!this.db) {
            throw new Error('Database not properly initialized');
        }
        try {
            const order = options?.order === 'desc' ? 'DESC' : 'ASC';
            const params = [query.exchange, query.symbol, query.timeframe];
            let sql = `
        SELECT timestamp, open, high, low, close, volume
        FROM candles
        WHERE exchange = ? AND symbol = ? AND timeframe = ?
      `;
            if (query.since !== undefined) {
                sql += ' AND timestamp >= ?';
                params.push(query.since);
            }
            if (query.until !== undefined) {
                sql += ' AND timestamp <= ?';
                params.push(query.until);
            }
            sql += ` ORDER BY timestamp ${order}`;
            if (query.limit !== undefined) {
                sql += ' LIMIT ?';
                params.push(query.limit);
            }
            const stmt = this.db.prepare(sql);
            const rows = stmt.all(...params);
            return rows;
        }
        catch (error) {
            throw new Error(`Failed to read data: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Find gaps in the data using window functions
     */
    async findGaps(query, options) {
        this.ensureConnected();
        if (!this.db) {
            throw new Error('Database not properly initialized');
        }
        try {
            const expectedInterval = options?.expectedInterval || this.getTimeframeMs(query.timeframe);
            const minGapSize = options?.minGapSize || expectedInterval * 1.5;
            const params = [query.exchange, query.symbol, query.timeframe];
            let sql = `
        SELECT
          timestamp as current_ts,
          LEAD(timestamp) OVER (ORDER BY timestamp) as next_ts
        FROM candles
        WHERE exchange = ? AND symbol = ? AND timeframe = ?
      `;
            if (query.since !== undefined) {
                sql += ' AND timestamp >= ?';
                params.push(query.since);
            }
            if (query.until !== undefined) {
                sql += ' AND timestamp <= ?';
                params.push(query.until);
            }
            sql += ' ORDER BY timestamp';
            const stmt = this.db.prepare(sql);
            const rows = stmt.all(...params);
            const gaps = [];
            for (const row of rows) {
                if (row.next_ts !== null) {
                    const gapSize = row.next_ts - row.current_ts;
                    if (gapSize > minGapSize) {
                        gaps.push({
                            start: row.current_ts + expectedInterval,
                            end: row.next_ts - expectedInterval,
                            exchange: query.exchange,
                            symbol: query.symbol,
                            timeframe: query.timeframe,
                        });
                    }
                }
            }
            return gaps;
        }
        catch (error) {
            throw new Error(`Failed to find gaps: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Get statistics about stored data
     */
    async getStats(exchange, symbol) {
        this.ensureConnected();
        if (!this.db) {
            throw new Error('Database not properly initialized');
        }
        try {
            const params = [];
            let whereClause = '';
            if (exchange) {
                whereClause = 'WHERE exchange = ?';
                params.push(exchange);
                if (symbol) {
                    whereClause += ' AND symbol = ?';
                    params.push(symbol);
                }
            }
            // Get total count
            const countSql = `SELECT COUNT(*) as count FROM candles ${whereClause}`;
            const countStmt = this.db.prepare(countSql);
            const countRow = countStmt.get(...params);
            // Get distinct exchanges
            const exchangesSql = `SELECT DISTINCT exchange FROM candles ${whereClause} ORDER BY exchange`;
            const exchangesStmt = this.db.prepare(exchangesSql);
            const exchangeRows = exchangesStmt.all(...params);
            // Get distinct symbols
            const symbolsSql = `SELECT DISTINCT symbol FROM candles ${whereClause} ORDER BY symbol`;
            const symbolsStmt = this.db.prepare(symbolsSql);
            const symbolRows = symbolsStmt.all(...params);
            // Get distinct timeframes
            const timeframesSql = `SELECT DISTINCT timeframe FROM candles ${whereClause} ORDER BY timeframe`;
            const timeframesStmt = this.db.prepare(timeframesSql);
            const timeframeRows = timeframesStmt.all(...params);
            // Get time range
            const timeSql = `SELECT MIN(timestamp) as oldest, MAX(timestamp) as newest FROM candles ${whereClause}`;
            const timeStmt = this.db.prepare(timeSql);
            const timeRow = timeStmt.get(...params);
            return {
                totalCandles: countRow.count,
                exchanges: exchangeRows.map(r => r.exchange),
                symbols: symbolRows.map(r => r.symbol),
                timeframes: timeframeRows.map(r => r.timeframe),
                oldestTimestamp: timeRow.oldest || undefined,
                newestTimestamp: timeRow.newest || undefined,
            };
        }
        catch (error) {
            throw new Error(`Failed to get stats: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Check if data exists for the given query
     */
    async exists(query) {
        this.ensureConnected();
        if (!this.db) {
            throw new Error('Database not properly initialized');
        }
        try {
            const params = [query.exchange, query.symbol, query.timeframe];
            let sql = `
        SELECT 1 FROM candles
        WHERE exchange = ? AND symbol = ? AND timeframe = ?
      `;
            if (query.since !== undefined) {
                sql += ' AND timestamp >= ?';
                params.push(query.since);
            }
            if (query.until !== undefined) {
                sql += ' AND timestamp <= ?';
                params.push(query.until);
            }
            sql += ' LIMIT 1';
            const stmt = this.db.prepare(sql);
            const row = stmt.get(...params);
            return row !== undefined;
        }
        catch (error) {
            throw new Error(`Failed to check existence: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Delete data matching the query
     */
    async delete(query) {
        this.ensureConnected();
        if (!this.db) {
            throw new Error('Database not properly initialized');
        }
        try {
            const params = [query.exchange, query.symbol, query.timeframe];
            let sql = `
        DELETE FROM candles
        WHERE exchange = ? AND symbol = ? AND timeframe = ?
      `;
            if (query.since !== undefined) {
                sql += ' AND timestamp >= ?';
                params.push(query.since);
            }
            if (query.until !== undefined) {
                sql += ' AND timestamp <= ?';
                params.push(query.until);
            }
            const stmt = this.db.prepare(sql);
            const result = stmt.run(...params);
            return result.changes;
        }
        catch (error) {
            throw new Error(`Failed to delete data: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Get the earliest and latest timestamps for a query
     */
    async getTimeRange(query) {
        this.ensureConnected();
        if (!this.db || !this.stmtTimeRange) {
            throw new Error('Database not properly initialized');
        }
        try {
            const result = this.stmtTimeRange.get(query.exchange, query.symbol, query.timeframe);
            if (result.earliest === null || result.latest === null) {
                return null;
            }
            return {
                earliest: result.earliest,
                latest: result.latest,
            };
        }
        catch (error) {
            throw new Error(`Failed to get time range: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Count the number of candles matching the query
     */
    async count(query) {
        this.ensureConnected();
        if (!this.db) {
            throw new Error('Database not properly initialized');
        }
        try {
            const params = [query.exchange, query.symbol, query.timeframe];
            let sql = `
        SELECT COUNT(*) as count FROM candles
        WHERE exchange = ? AND symbol = ? AND timeframe = ?
      `;
            if (query.since !== undefined) {
                sql += ' AND timestamp >= ?';
                params.push(query.since);
            }
            if (query.until !== undefined) {
                sql += ' AND timestamp <= ?';
                params.push(query.until);
            }
            const stmt = this.db.prepare(sql);
            const result = stmt.get(...params);
            return result.count;
        }
        catch (error) {
            throw new Error(`Failed to count data: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Search for symbols/exchanges using FTS5
     */
    async search(searchTerm, options) {
        this.ensureConnected();
        if (!this.db) {
            throw new Error('Database not properly initialized');
        }
        try {
            const limit = options?.limit || 10;
            const matchMode = options?.matchMode || 'prefix';
            // Build FTS5 query based on match mode
            let ftsQuery = searchTerm;
            if (matchMode === 'prefix') {
                ftsQuery = `${searchTerm}*`;
            }
            else if (matchMode === 'phrase') {
                ftsQuery = `"${searchTerm}"`;
            }
            const sql = `
        SELECT
          exchange,
          symbol,
          timeframe,
          rank
        FROM candles_fts
        WHERE candles_fts MATCH ?
        ORDER BY rank
        LIMIT ?
      `;
            const stmt = this.db.prepare(sql);
            const rows = stmt.all(ftsQuery, limit);
            return rows.map(row => ({
                exchange: row.exchange,
                symbol: row.symbol,
                timeframe: row.timeframe,
                score: -row.rank, // Convert rank to positive score (lower rank = better match)
                snippet: `${row.exchange}:${row.symbol}@${row.timeframe}`,
            }));
        }
        catch (error) {
            throw new Error(`Failed to search: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Optimize the database (VACUUM and ANALYZE)
     */
    async optimize() {
        this.ensureConnected();
        if (!this.db) {
            throw new Error('Database not properly initialized');
        }
        try {
            // Run VACUUM to reclaim space and defragment
            this.db.exec('VACUUM');
            // Run ANALYZE to update query optimizer statistics
            this.db.exec('ANALYZE');
            // Optimize FTS5 index
            this.db.exec("INSERT INTO candles_fts(candles_fts) VALUES('optimize')");
        }
        catch (error) {
            throw new Error(`Failed to optimize database: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
    /**
     * Create the database schema with FTS5 support
     */
    createSchema() {
        if (!this.db) {
            throw new Error('Database not initialized');
        }
        // Create main candles table
        this.db.exec(`
      CREATE TABLE IF NOT EXISTS candles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        UNIQUE(exchange, symbol, timeframe, timestamp)
      )
    `);
        // Create index for fast lookups
        this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_candles_lookup
      ON candles(exchange, symbol, timeframe, timestamp)
    `);
        // Create FTS5 virtual table for full-text search
        this.db.exec(`
      CREATE VIRTUAL TABLE IF NOT EXISTS candles_fts USING fts5(
        exchange,
        symbol,
        timeframe,
        content='candles',
        content_rowid='id'
      )
    `);
        // Create trigger to keep FTS in sync on INSERT
        this.db.exec(`
      CREATE TRIGGER IF NOT EXISTS candles_ai AFTER INSERT ON candles BEGIN
        INSERT INTO candles_fts(rowid, exchange, symbol, timeframe)
        VALUES (new.id, new.exchange, new.symbol, new.timeframe);
      END
    `);
        // Create trigger to keep FTS in sync on DELETE
        this.db.exec(`
      CREATE TRIGGER IF NOT EXISTS candles_ad AFTER DELETE ON candles BEGIN
        INSERT INTO candles_fts(candles_fts, rowid, exchange, symbol, timeframe)
        VALUES('delete', old.id, old.exchange, old.symbol, old.timeframe);
      END
    `);
        // Create trigger to keep FTS in sync on UPDATE
        this.db.exec(`
      CREATE TRIGGER IF NOT EXISTS candles_au AFTER UPDATE ON candles BEGIN
        INSERT INTO candles_fts(candles_fts, rowid, exchange, symbol, timeframe)
        VALUES('delete', old.id, old.exchange, old.symbol, old.timeframe);
        INSERT INTO candles_fts(rowid, exchange, symbol, timeframe)
        VALUES (new.id, new.exchange, new.symbol, new.timeframe);
      END
    `);
    }
    /**
     * Prepare frequently used SQL statements
     */
    prepareStatements() {
        if (!this.db) {
            throw new Error('Database not initialized');
        }
        // Insert statement with UPSERT (INSERT OR REPLACE)
        this.stmtInsert = this.db.prepare(`
      INSERT OR REPLACE INTO candles
      (exchange, symbol, timeframe, timestamp, open, high, low, close, volume)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
        // Time range statement
        this.stmtTimeRange = this.db.prepare(`
      SELECT
        MIN(timestamp) as earliest,
        MAX(timestamp) as latest
      FROM candles
      WHERE exchange = ? AND symbol = ? AND timeframe = ?
    `);
    }
}
//# sourceMappingURL=sqlite.js.map