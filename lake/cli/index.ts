#!/usr/bin/env node

/**
 * CCXT Data Lake CLI Tool
 *
 * Command-line interface for managing and querying the data lake.
 */

import { Command } from 'commander';
import { createBackend } from '../src/index.js';
import type { BackendConfig, DataQuery, Candle, Gap, LakeStats } from '../src/types.js';
import type { Backend } from '../src/interface.js';
// TODO: Re-enable after fixing conflicts
// import { registerBackfillCommands } from './commands/backfill.js';
// import { registerAdvancedCommands } from './commands/advanced.js';

const program = new Command();

// ===========================
// Helper Functions
// ===========================

/**
 * Format timestamp to readable date string
 */
function formatDate(timestamp: number): string {
  const date = new Date(timestamp);
  return date.toISOString().replace('T', ' ').substring(0, 19);
}

/**
 * Format number with thousand separators
 */
function formatNumber(num: number): string {
  return num.toLocaleString();
}

/**
 * Create backend from CLI options
 */
async function getBackend(options: any): Promise<Backend> {
  const config: BackendConfig = {
    type: options.backend || 'sqlite',
    path: options.path || './lake.db',
  };

  try {
    const backend = await createBackend(config);
    await backend.connect();
    return backend;
  } catch (error) {
    console.error(`Error connecting to backend: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  }
}

/**
 * Simple table formatter
 */
function formatTable(headers: string[], rows: string[][]): string {
  // Calculate column widths
  const widths = headers.map((header, i) => {
    const maxRowWidth = Math.max(...rows.map(row => (row[i] || '').length));
    return Math.max(header.length, maxRowWidth);
  });

  // Create separator
  const separator = '+' + widths.map(w => '-'.repeat(w + 2)).join('+') + '+';

  // Format header
  const headerRow = '|' + headers.map((h, i) => ' ' + h.padEnd(widths[i]) + ' ').join('|') + '|';

  // Format rows
  const dataRows = rows.map(row =>
    '|' + row.map((cell, i) => ' ' + (cell || '').padEnd(widths[i]) + ' ').join('|') + '|'
  );

  return [separator, headerRow, separator, ...dataRows, separator].join('\n');
}

/**
 * Format candles as table
 */
function formatCandlesTable(candles: Candle[]): string {
  const headers = ['Timestamp', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'];
  const rows = candles.map(c => [
    c.timestamp.toString(),
    formatDate(c.timestamp),
    c.open.toFixed(8),
    c.high.toFixed(8),
    c.low.toFixed(8),
    c.close.toFixed(8),
    c.volume.toFixed(8),
  ]);

  return formatTable(headers, rows);
}

/**
 * Format candles as CSV
 */
function formatCandlesCSV(candles: Candle[]): string {
  const headers = 'timestamp,date,open,high,low,close,volume';
  const rows = candles.map(c => {
    return `${c.timestamp},${formatDate(c.timestamp)},${c.open},${c.high},${c.low},${c.close},${c.volume}`;
  });

  return [headers, ...rows].join('\n');
}

/**
 * Format stats as table
 */
function formatStatsTable(stats: LakeStats): string {
  const rows = [
    ['Total Candles', formatNumber(stats.totalCandles)],
    ['Exchanges', stats.exchanges.length.toString()],
    ['Symbols', stats.symbols.length.toString()],
    ['Timeframes', stats.timeframes.length.toString()],
  ];

  if (stats.oldestTimestamp) {
    rows.push(['Oldest Data', `${formatDate(stats.oldestTimestamp)} (${stats.oldestTimestamp})`]);
  }

  if (stats.newestTimestamp) {
    rows.push(['Newest Data', `${formatDate(stats.newestTimestamp)} (${stats.newestTimestamp})`]);
  }

  const result = [formatTable(['Metric', 'Value'], rows)];

  if (stats.exchanges.length > 0) {
    result.push('\nExchanges:');
    result.push('  ' + stats.exchanges.join(', '));
  }

  if (stats.symbols.length > 0 && stats.symbols.length <= 20) {
    result.push('\nSymbols:');
    result.push('  ' + stats.symbols.join(', '));
  } else if (stats.symbols.length > 20) {
    result.push(`\nSymbols: ${stats.symbols.length} total (showing first 20)`);
    result.push('  ' + stats.symbols.slice(0, 20).join(', ') + ', ...');
  }

  if (stats.timeframes.length > 0) {
    result.push('\nTimeframes:');
    result.push('  ' + stats.timeframes.join(', '));
  }

  return result.join('\n');
}

/**
 * Format gaps as table
 */
function formatGapsTable(gaps: Gap[]): string {
  if (gaps.length === 0) {
    return 'No gaps found!';
  }

  const headers = ['#', 'Start', 'End', 'Duration (ms)', 'Duration (human)'];
  const rows = gaps.map((gap, i) => {
    const duration = gap.end - gap.start;
    const durationHuman = formatDuration(duration);
    return [
      (i + 1).toString(),
      formatDate(gap.start),
      formatDate(gap.end),
      formatNumber(duration),
      durationHuman,
    ];
  });

  return formatTable(headers, rows);
}

/**
 * Format duration in human-readable format
 */
function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);

  if (days > 0) return `${days}d ${hours % 24}h`;
  if (hours > 0) return `${hours}h ${minutes % 60}m`;
  if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

// ===========================
// CLI Commands
// ===========================

program
  .name('lake')
  .description('CCXT Data Lake CLI - Manage and query market data')
  .version('0.1.0');

/**
 * Stats command - Show lake statistics
 */
program
  .command('stats')
  .description('Show lake statistics')
  .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
  .option('-p, --path <path>', 'Database path', './lake.db')
  .option('-e, --exchange <exchange>', 'Filter by exchange')
  .option('-s, --symbol <symbol>', 'Filter by symbol')
  .action(async (options) => {
    const backend = await getBackend(options);

    try {
      const stats = await backend.getStats(options.exchange, options.symbol);

      console.log('\n' + formatStatsTable(stats) + '\n');
    } catch (error) {
      console.error(`Error getting stats: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(1);
    } finally {
      await backend.disconnect();
    }
  });

/**
 * Gaps command - Find gaps in data
 */
program
  .command('gaps <exchange> <symbol> <timeframe>')
  .description('Find gaps in data for a specific symbol')
  .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
  .option('-p, --path <path>', 'Database path', './lake.db')
  .option('--since <timestamp>', 'Start timestamp (ms)', (val) => parseInt(val, 10))
  .option('--until <timestamp>', 'End timestamp (ms)', (val) => parseInt(val, 10))
  .option('--min-gap <ms>', 'Minimum gap size in milliseconds', (val) => parseInt(val, 10))
  .action(async (exchange, symbol, timeframe, options) => {
    const backend = await getBackend(options);

    try {
      const query: DataQuery = {
        exchange,
        symbol,
        timeframe,
        since: options.since,
        until: options.until,
      };

      const gapOptions = options.minGap ? { minGapSize: options.minGap } : undefined;

      console.log(`\nSearching for gaps in ${exchange}:${symbol}@${timeframe}...\n`);

      const gaps = await backend.findGaps(query, gapOptions);

      console.log(formatGapsTable(gaps));

      if (gaps.length > 0) {
        console.log(`\nFound ${formatNumber(gaps.length)} gap(s)\n`);
      } else {
        console.log('');
      }
    } catch (error) {
      console.error(`Error finding gaps: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(1);
    } finally {
      await backend.disconnect();
    }
  });

/**
 * Query command - Query candles from the lake
 */
program
  .command('query <exchange> <symbol> <timeframe>')
  .description('Query candles from the lake')
  .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
  .option('-p, --path <path>', 'Database path', './lake.db')
  .option('--since <timestamp>', 'Start timestamp (ms)', (val) => parseInt(val, 10))
  .option('--until <timestamp>', 'End timestamp (ms)', (val) => parseInt(val, 10))
  .option('-l, --limit <n>', 'Limit results', (val) => parseInt(val, 10), 100)
  .option('-o, --order <order>', 'Order (asc|desc)', 'asc')
  .option('-f, --format <format>', 'Output format (json|table|csv)', 'table')
  .action(async (exchange, symbol, timeframe, options) => {
    const backend = await getBackend(options);

    try {
      const query: DataQuery = {
        exchange,
        symbol,
        timeframe,
        since: options.since,
        until: options.until,
        limit: options.limit,
      };

      const readOptions = {
        order: options.order as 'asc' | 'desc',
      };

      console.log(`\nQuerying ${exchange}:${symbol}@${timeframe}...\n`);

      const candles = await backend.read(query, readOptions);

      if (candles.length === 0) {
        console.log('No data found.\n');
      } else {
        switch (options.format) {
          case 'json':
            console.log(JSON.stringify(candles, null, 2));
            break;
          case 'csv':
            console.log(formatCandlesCSV(candles));
            break;
          case 'table':
          default:
            console.log(formatCandlesTable(candles));
            console.log(`\n${formatNumber(candles.length)} candle(s) found\n`);
            break;
        }
      }
    } catch (error) {
      console.error(`Error querying data: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(1);
    } finally {
      await backend.disconnect();
    }
  });

/**
 * Info command - Show lake information
 */
program
  .command('info')
  .description('Show lake configuration and available backends')
  .action(() => {
    console.log(`
╔════════════════════════════════════════════════════════════╗
║              CCXT Data Lake CLI v0.1.0                     ║
╚════════════════════════════════════════════════════════════╝

Description:
  High-performance market data storage and retrieval system
  for cryptocurrency exchanges.

Available Backends:
  • sqlite       - SQLite with FTS5 full-text search
  • parquet      - Apache Parquet columnar storage
  • timescaledb  - TimescaleDB time-series database

Commands:
  lake stats                   Show statistics about stored data
  lake gaps <params>           Find gaps in data
  lake query <params>          Query candles from the lake
  lake info                    Show this information

Examples:
  # Show statistics for all data
  lake stats

  # Show statistics for specific exchange
  lake stats --exchange binance

  # Find gaps in Binance BTC/USDT 1-minute data
  lake gaps binance BTC/USDT 1m

  # Query last 100 candles
  lake query binance BTC/USDT 1m --limit 100

  # Query with time range (JSON output)
  lake query binance BTC/USDT 1h \\
    --since 1609459200000 \\
    --until 1609545600000 \\
    --format json

  # Query and export to CSV
  lake query binance BTC/USDT 1d \\
    --limit 365 \\
    --format csv > btc_daily.csv

For more information, use --help with any command.
`);
  });

// Register additional command modules
// TODO: Re-enable after fixing conflicts
// registerBackfillCommands(program);
// registerAdvancedCommands(program);

// Parse command line arguments
program.parse();
