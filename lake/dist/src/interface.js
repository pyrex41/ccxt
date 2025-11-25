/**
 * Backend interface for CCXT Data Lake
 *
 * This interface defines the contract that all storage backends
 * (SQLite, Parquet, TimescaleDB) must implement.
 */
/**
 * Abstract base class for backend implementations
 * Provides common functionality and default implementations
 */
export class BaseBackend {
    _state = 'disconnected';
    get state() {
        return this._state;
    }
    /**
     * Ensure backend is connected before operations
     */
    ensureConnected() {
        if (this._state !== 'connected') {
            throw new Error(`Backend is not connected (state: ${this._state})`);
        }
    }
    /**
     * Calculate expected interval based on timeframe
     */
    getTimeframeMs(timeframe) {
        const units = {
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
    async findGaps(query, options) {
        this.ensureConnected();
        const candles = await this.read(query, { order: 'asc' });
        if (candles.length < 2) {
            return [];
        }
        const expectedInterval = options?.expectedInterval || this.getTimeframeMs(query.timeframe);
        const minGapSize = options?.minGapSize || expectedInterval * 1.5;
        const gaps = [];
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
 * Create a backend instance based on configuration
 * This will be implemented once all backends are available
 */
export async function createBackend(config) {
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
            throw new Error(`Unknown backend type: ${config.type}`);
    }
}
//# sourceMappingURL=interface.js.map