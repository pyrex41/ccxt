/**
 * Advanced CLI commands
 *
 * Commands for data management, search, export/import, and optimization
 */
import { createBackend } from '../../src/interface.js';
import { parseTimestamp } from '../../src/backfill.js';
import * as fs from 'fs/promises';
import * as path from 'path';
/**
 * Register advanced commands
 */
export function registerAdvancedCommands(program) {
    // Search command - uses FTS5 for SQLite
    program
        .command('search <term>')
        .description('Search for symbols and exchanges (full-text search)')
        .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
        .option('-p, --path <path>', 'Database path (for SQLite/Parquet)', './lake.db')
        .option('-c, --connection <string>', 'Connection string (for TimescaleDB)')
        .option('-l, --limit <n>', 'Maximum number of results', '10')
        .action(async (term, options) => {
        try {
            console.log(`\nSearching for: "${term}"\n`);
            // Create backend config
            const backendConfig = {
                type: options.backend,
                path: options.path,
                connectionString: options.connection,
            };
            // Create backend
            const backend = await createBackend(backendConfig);
            await backend.connect();
            try {
                // Check if backend supports search
                if (!backend.search) {
                    console.log('Search is not supported by this backend');
                    console.log('Note: Full-text search is available with SQLite backend');
                    return;
                }
                // Perform search
                const results = await backend.search(term, {
                    limit: parseInt(options.limit, 10),
                });
                if (results.length === 0) {
                    console.log('No results found');
                    return;
                }
                console.log(`Found ${results.length} result(s):\n`);
                for (const result of results) {
                    console.log(`  ${result.exchange}/${result.symbol} (${result.timeframe})`);
                    console.log(`    Score: ${result.score.toFixed(2)}`);
                    if (result.snippet) {
                        console.log(`    Match: ${result.snippet}`);
                    }
                    console.log();
                }
            }
            finally {
                await backend.disconnect();
            }
        }
        catch (error) {
            console.error('\nError:', error instanceof Error ? error.message : String(error));
            process.exit(1);
        }
    });
    // Export command - export data to file
    program
        .command('export <exchange> <symbol> <timeframe>')
        .description('Export data to file (CSV, JSON, or Parquet)')
        .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
        .option('-p, --path <path>', 'Database path (for SQLite/Parquet)', './lake.db')
        .option('-c, --connection <string>', 'Connection string (for TimescaleDB)')
        .option('-o, --output <file>', 'Output file path (auto-generates if not specified)')
        .option('-f, --format <format>', 'Output format (csv|json|parquet)', 'csv')
        .option('--since <timestamp>', 'Start timestamp')
        .option('--until <timestamp>', 'End timestamp')
        .option('--limit <n>', 'Maximum number of candles to export')
        .action(async (exchange, symbol, timeframe, options) => {
        try {
            console.log(`\nExporting ${exchange} ${symbol} ${timeframe}...\n`);
            // Create backend config
            const backendConfig = {
                type: options.backend,
                path: options.path,
                connectionString: options.connection,
            };
            // Create backend
            const backend = await createBackend(backendConfig);
            await backend.connect();
            try {
                // Parse timestamps
                const since = parseTimestamp(options.since);
                const until = parseTimestamp(options.until);
                // Read data
                const candles = await backend.read({
                    exchange,
                    symbol,
                    timeframe,
                    since,
                    until,
                    limit: options.limit ? parseInt(options.limit, 10) : undefined,
                }, { order: 'asc' });
                if (candles.length === 0) {
                    console.log('No data found to export');
                    return;
                }
                console.log(`Found ${candles.length} candles`);
                // Generate output filename if not specified
                const outputFile = options.output ||
                    `${exchange}_${symbol.replace('/', '_')}_${timeframe}_${Date.now()}.${options.format}`;
                // Export based on format
                switch (options.format) {
                    case 'csv':
                        await exportToCSV(candles, outputFile);
                        break;
                    case 'json':
                        await exportToJSON(candles, outputFile);
                        break;
                    case 'parquet':
                        console.log('Parquet export requires additional dependencies');
                        console.log('Install: npm install parquetjs');
                        console.log('For now, exporting as JSON instead...');
                        await exportToJSON(candles, outputFile.replace('.parquet', '.json'));
                        break;
                    default:
                        throw new Error(`Unsupported format: ${options.format}`);
                }
                console.log(`\nExported to: ${outputFile}`);
            }
            finally {
                await backend.disconnect();
            }
        }
        catch (error) {
            console.error('\nError:', error instanceof Error ? error.message : String(error));
            process.exit(1);
        }
    });
    // Import command - import data from file
    program
        .command('import <file>')
        .description('Import data from file (CSV or JSON)')
        .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
        .option('-p, --path <path>', 'Database path (for SQLite/Parquet)', './lake.db')
        .option('-c, --connection <string>', 'Connection string (for TimescaleDB)')
        .option('-e, --exchange <exchange>', 'Exchange name (required if not in file)')
        .option('-s, --symbol <symbol>', 'Symbol (required if not in file)')
        .option('-t, --timeframe <timeframe>', 'Timeframe (required if not in file)')
        .option('--upsert', 'Upsert data (update if exists)', false)
        .option('--dry-run', 'Parse file without importing', false)
        .action(async (file, options) => {
        try {
            console.log(`\nImporting from: ${file}\n`);
            // Check file exists
            try {
                await fs.access(file);
            }
            catch {
                throw new Error(`File not found: ${file}`);
            }
            // Detect format from extension
            const ext = path.extname(file).toLowerCase();
            let candles;
            if (ext === '.json') {
                const content = await fs.readFile(file, 'utf-8');
                candles = JSON.parse(content);
            }
            else if (ext === '.csv') {
                candles = await importFromCSV(file);
            }
            else {
                throw new Error(`Unsupported file format: ${ext}`);
            }
            if (!Array.isArray(candles) || candles.length === 0) {
                throw new Error('No valid candle data found in file');
            }
            console.log(`Parsed ${candles.length} candles`);
            // Validate required fields
            const exchange = options.exchange || candles[0].exchange;
            const symbol = options.symbol || candles[0].symbol;
            const timeframe = options.timeframe || candles[0].timeframe;
            if (!exchange || !symbol || !timeframe) {
                throw new Error('Exchange, symbol, and timeframe must be specified or present in data');
            }
            if (options.dryRun) {
                console.log('\nDRY RUN MODE - Data not imported\n');
                console.log('Sample data:');
                console.log(JSON.stringify(candles.slice(0, 3), null, 2));
                return;
            }
            // Create backend config
            const backendConfig = {
                type: options.backend,
                path: options.path,
                connectionString: options.connection,
            };
            // Create backend
            const backend = await createBackend(backendConfig);
            await backend.connect();
            try {
                // Convert to DataPoint format
                const dataPoints = candles.map((candle) => ({
                    timestamp: candle.timestamp,
                    exchange: candle.exchange || exchange,
                    symbol: candle.symbol || symbol,
                    timeframe: candle.timeframe || timeframe,
                    data: {
                        timestamp: candle.timestamp,
                        open: candle.open,
                        high: candle.high,
                        low: candle.low,
                        close: candle.close,
                        volume: candle.volume,
                    },
                }));
                // Write to backend
                const written = await backend.writeMany(dataPoints, {
                    upsert: options.upsert,
                });
                console.log(`\nImported ${written} candles successfully`);
            }
            finally {
                await backend.disconnect();
            }
        }
        catch (error) {
            console.error('\nError:', error instanceof Error ? error.message : String(error));
            process.exit(1);
        }
    });
    // Optimize command - vacuum and optimize database
    program
        .command('optimize')
        .description('Optimize the database (VACUUM, reindex, etc.)')
        .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
        .option('-p, --path <path>', 'Database path (for SQLite/Parquet)', './lake.db')
        .option('-c, --connection <string>', 'Connection string (for TimescaleDB)')
        .action(async (options) => {
        try {
            console.log('\nOptimizing database...\n');
            // Create backend config
            const backendConfig = {
                type: options.backend,
                path: options.path,
                connectionString: options.connection,
            };
            // Create backend
            const backend = await createBackend(backendConfig);
            await backend.connect();
            try {
                // Check if backend supports optimization
                if (!backend.optimize) {
                    console.log('Optimization is not supported by this backend');
                    return;
                }
                const startTime = Date.now();
                await backend.optimize();
                const duration = Date.now() - startTime;
                console.log(`Optimization completed in ${(duration / 1000).toFixed(2)}s`);
            }
            finally {
                await backend.disconnect();
            }
        }
        catch (error) {
            console.error('\nError:', error instanceof Error ? error.message : String(error));
            process.exit(1);
        }
    });
    // Delete command - remove data from lake
    program
        .command('delete <exchange> <symbol> <timeframe>')
        .description('Delete data from the lake')
        .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
        .option('-p, --path <path>', 'Database path (for SQLite/Parquet)', './lake.db')
        .option('-c, --connection <string>', 'Connection string (for TimescaleDB)')
        .option('--since <timestamp>', 'Start timestamp')
        .option('--until <timestamp>', 'End timestamp')
        .option('-y, --yes', 'Skip confirmation prompt', false)
        .action(async (exchange, symbol, timeframe, options) => {
        try {
            // Create backend config
            const backendConfig = {
                type: options.backend,
                path: options.path,
                connectionString: options.connection,
            };
            // Create backend
            const backend = await createBackend(backendConfig);
            await backend.connect();
            try {
                // Parse timestamps
                const since = parseTimestamp(options.since);
                const until = parseTimestamp(options.until);
                // Count records to be deleted
                const count = await backend.count({
                    exchange,
                    symbol,
                    timeframe,
                    since,
                    until,
                });
                if (count === 0) {
                    console.log('\nNo data found to delete');
                    return;
                }
                console.log(`\nWARNING: About to delete ${count} candle(s)`);
                console.log(`  Exchange:   ${exchange}`);
                console.log(`  Symbol:     ${symbol}`);
                console.log(`  Timeframe:  ${timeframe}`);
                if (since)
                    console.log(`  Since:      ${new Date(since).toISOString()}`);
                if (until)
                    console.log(`  Until:      ${new Date(until).toISOString()}`);
                if (!options.yes) {
                    console.log('\nThis action cannot be undone!');
                    console.log('Run with --yes flag to confirm deletion');
                    return;
                }
                // Delete data
                const deleted = await backend.delete({
                    exchange,
                    symbol,
                    timeframe,
                    since,
                    until,
                });
                console.log(`\nDeleted ${deleted} candle(s) successfully`);
            }
            finally {
                await backend.disconnect();
            }
        }
        catch (error) {
            console.error('\nError:', error instanceof Error ? error.message : String(error));
            process.exit(1);
        }
    });
}
/**
 * Export candles to CSV format
 */
async function exportToCSV(candles, outputFile) {
    const lines = [
        'timestamp,datetime,open,high,low,close,volume',
        ...candles.map(c => `${c.timestamp},${new Date(c.timestamp).toISOString()},${c.open},${c.high},${c.low},${c.close},${c.volume}`)
    ];
    await fs.writeFile(outputFile, lines.join('\n'));
}
/**
 * Export candles to JSON format
 */
async function exportToJSON(candles, outputFile) {
    await fs.writeFile(outputFile, JSON.stringify(candles, null, 2));
}
/**
 * Import candles from CSV format
 */
async function importFromCSV(file) {
    const content = await fs.readFile(file, 'utf-8');
    const lines = content.split('\n').filter(line => line.trim());
    // Skip header
    const dataLines = lines.slice(1);
    return dataLines.map(line => {
        const [timestamp, , open, high, low, close, volume] = line.split(',');
        return {
            timestamp: parseInt(timestamp, 10),
            open: parseFloat(open),
            high: parseFloat(high),
            low: parseFloat(low),
            close: parseFloat(close),
            volume: parseFloat(volume),
        };
    });
}
//# sourceMappingURL=advanced.js.map