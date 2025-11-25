/**
 * Backfill CLI commands
 *
 * Commands for fetching historical data from exchanges
 */
import { createBackend } from '../../src/interface.js';
import { /* BackfillOrchestrator, */ parseTimestamp } from '../../src/backfill.js';
/**
 * Register backfill-related commands
 */
export function registerBackfillCommands(program) {
    // Backfill command - fetch historical data
    program
        .command('backfill <exchange> <symbol> <timeframe>')
        .description('Backfill historical OHLCV data from an exchange')
        .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
        .option('-p, --path <path>', 'Database path (for SQLite/Parquet)', './lake.db')
        .option('-c, --connection <string>', 'Connection string (for TimescaleDB)')
        .option('--since <timestamp>', 'Start timestamp (ms, ISO date, or relative like "30d")')
        .option('--until <timestamp>', 'End timestamp (ms, ISO date, or relative like "1d")')
        .option('--rate-limit <rps>', 'Rate limit in requests per second', '1')
        .option('--retries <n>', 'Number of retries for failed requests', '3')
        .option('--stop-on-error', 'Stop backfill on first error', false)
        .option('--dry-run', 'Show what would be fetched without writing', false)
        .action(async (exchange, symbol, timeframe, options) => {
        try {
            console.log(`\nBackfilling ${exchange} ${symbol} ${timeframe}...\n`);
            if (options.dryRun) {
                console.log('DRY RUN MODE - No data will be written\n');
                console.log('Configuration:');
                console.log(`  Exchange:   ${exchange}`);
                console.log(`  Symbol:     ${symbol}`);
                console.log(`  Timeframe:  ${timeframe}`);
                console.log(`  Backend:    ${options.backend}`);
                console.log(`  Path:       ${options.path || options.connection || 'default'}`);
                console.log(`  Since:      ${options.since || 'latest or 365 days ago'}`);
                console.log(`  Until:      ${options.until || 'now'}`);
                console.log(`  Rate Limit: ${options.rateLimit} req/s`);
                console.log(`  Retries:    ${options.retries}`);
                console.log('\nNote: Actual CCXT integration required for live backfill');
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
                // Parse timestamps
                const since = parseTimestamp(options.since);
                const until = parseTimestamp(options.until);
                // Create orchestrator (commented out - placeholder implementation)
                // const orchestrator = new BackfillOrchestrator(backend, {
                //   rateLimit: parseFloat(options.rateLimit),
                //   retries: parseInt(options.retries, 10),
                //   stopOnError: options.stopOnError,
                //   onProgress: (progress) => {
                //     const percent = Math.round((progress.current / progress.total) * 100);
                //     process.stdout.write(`\r[${percent}%] ${progress.message || `${progress.current}/${progress.total}`}`);
                //   },
                // });
                console.log('Note: CCXT integration required. This is a placeholder implementation.\n');
                console.log('To use this command, you need to:');
                console.log('1. Install CCXT: npm install ccxt');
                console.log('2. Create exchange adapter in your code');
                console.log('3. Pass the adapter to orchestrator.backfill()');
                console.log('\nExample code:');
                console.log('```typescript');
                console.log('import ccxt from "ccxt";');
                console.log(`const exchange = new ccxt.${exchange}();`);
                console.log('const result = await orchestrator.backfill(exchange, {');
                console.log(`  exchange: "${exchange}",`);
                console.log(`  symbol: "${symbol}",`);
                console.log(`  timeframe: "${timeframe}",`);
                console.log(`  since: ${since || 'undefined'},`);
                console.log(`  until: ${until || 'undefined'},`);
                console.log('});');
                console.log('```\n');
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
    // Fill gaps command - fill missing data in existing time series
    program
        .command('fill-gaps <exchange> <symbol> <timeframe>')
        .description('Fill gaps in existing data')
        .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
        .option('-p, --path <path>', 'Database path (for SQLite/Parquet)', './lake.db')
        .option('-c, --connection <string>', 'Connection string (for TimescaleDB)')
        .option('--since <timestamp>', 'Start timestamp for gap detection')
        .option('--until <timestamp>', 'End timestamp for gap detection')
        .option('--min-gap <ms>', 'Minimum gap size in milliseconds to fill')
        .option('--rate-limit <rps>', 'Rate limit in requests per second', '1')
        .option('--dry-run', 'Show gaps without filling them', false)
        .action(async (exchange, symbol, timeframe, options) => {
        try {
            console.log(`\nFinding gaps in ${exchange} ${symbol} ${timeframe}...\n`);
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
                // Find gaps
                const gaps = await backend.findGaps({
                    exchange,
                    symbol,
                    timeframe,
                    since,
                    until,
                }, {
                    minGapSize: options.minGap ? parseInt(options.minGap, 10) : undefined,
                });
                if (gaps.length === 0) {
                    console.log('No gaps found!');
                    return;
                }
                console.log(`Found ${gaps.length} gap(s):\n`);
                for (const gap of gaps) {
                    const startDate = new Date(gap.start).toISOString();
                    const endDate = new Date(gap.end).toISOString();
                    const durationMs = gap.end - gap.start;
                    const durationHours = (durationMs / (1000 * 60 * 60)).toFixed(2);
                    console.log(`  ${startDate} to ${endDate} (${durationHours} hours)`);
                }
                if (options.dryRun) {
                    console.log('\nDRY RUN MODE - Gaps not filled');
                    console.log('Run without --dry-run to fill these gaps');
                    return;
                }
                console.log('\nNote: CCXT integration required to fill gaps.');
                console.log('See "backfill" command documentation for setup instructions.\n');
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
    // List backfill status - show data coverage
    program
        .command('coverage <exchange> <symbol> <timeframe>')
        .description('Show data coverage and gaps')
        .option('-b, --backend <type>', 'Backend type (sqlite|parquet|timescaledb)', 'sqlite')
        .option('-p, --path <path>', 'Database path (for SQLite/Parquet)', './lake.db')
        .option('-c, --connection <string>', 'Connection string (for TimescaleDB)')
        .option('--since <timestamp>', 'Start timestamp for analysis')
        .option('--until <timestamp>', 'End timestamp for analysis')
        .action(async (exchange, symbol, timeframe, options) => {
        try {
            console.log(`\nAnalyzing coverage for ${exchange} ${symbol} ${timeframe}...\n`);
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
                // Get time range
                const timeRange = await backend.getTimeRange({
                    exchange,
                    symbol,
                    timeframe,
                });
                if (!timeRange) {
                    console.log('No data found for this query');
                    return;
                }
                console.log('Data Coverage:');
                console.log(`  Earliest: ${new Date(timeRange.earliest).toISOString()}`);
                console.log(`  Latest:   ${new Date(timeRange.latest).toISOString()}`);
                const rangeMs = timeRange.latest - timeRange.earliest;
                const rangeDays = (rangeMs / (1000 * 60 * 60 * 24)).toFixed(2);
                console.log(`  Duration: ${rangeDays} days\n`);
                // Count candles
                const count = await backend.count({
                    exchange,
                    symbol,
                    timeframe,
                    since: parseTimestamp(options.since) || timeRange.earliest,
                    until: parseTimestamp(options.until) || timeRange.latest,
                });
                console.log(`Total Candles: ${count}`);
                // Find gaps
                const gaps = await backend.findGaps({
                    exchange,
                    symbol,
                    timeframe,
                    since: parseTimestamp(options.since) || timeRange.earliest,
                    until: parseTimestamp(options.until) || timeRange.latest,
                });
                if (gaps.length > 0) {
                    console.log(`\nGaps Found: ${gaps.length}`);
                    const totalGapMs = gaps.reduce((sum, gap) => sum + (gap.end - gap.start), 0);
                    const totalGapHours = (totalGapMs / (1000 * 60 * 60)).toFixed(2);
                    console.log(`Total Gap Duration: ${totalGapHours} hours`);
                }
                else {
                    console.log('\nNo gaps found - complete coverage!');
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
}
//# sourceMappingURL=backfill.js.map