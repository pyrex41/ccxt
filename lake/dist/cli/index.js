#!/usr/bin/env node
/**
 * CCXT Data Lake CLI Tool
 *
 * Command-line interface for managing and querying the data lake.
 * Will support operations like:
 * - Importing data from exchanges
 * - Querying stored data
 * - Managing backends
 * - Viewing statistics
 */
import { Command } from 'commander';
const program = new Command();
program
    .name('ccxt-lake')
    .description('CCXT Data Lake CLI - Manage and query market data')
    .version('0.1.0');
// TODO: Add commands in subsequent tasks
program
    .command('info')
    .description('Show data lake information')
    .action(() => {
    console.log('CCXT Data Lake CLI - Coming soon');
});
program.parse();
//# sourceMappingURL=index.js.map