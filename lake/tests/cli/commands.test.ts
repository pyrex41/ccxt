import { describe, it, expect } from 'vitest';
import { Command } from 'commander';
import { registerBackfillCommands } from '../../cli/commands/backfill.js';
import { registerAdvancedCommands } from '../../cli/commands/advanced.js';

describe('CLI Commands', () => {
  describe('Backfill Commands', () => {
    it('should register backfill command', () => {
      const program = new Command();
      registerBackfillCommands(program);

      const commands = program.commands.map(c => c.name());
      expect(commands).toContain('backfill');
      expect(commands).toContain('fill-gaps');
      expect(commands).toContain('coverage');
    });

    it('should have correct backfill options', () => {
      const program = new Command();
      registerBackfillCommands(program);

      const backfill = program.commands.find(c => c.name() === 'backfill');
      expect(backfill).toBeDefined();

      const optionNames = backfill!.options.map(o => o.long);
      expect(optionNames).toContain('--backend');
      expect(optionNames).toContain('--path');
      expect(optionNames).toContain('--since');
      expect(optionNames).toContain('--until');
      expect(optionNames).toContain('--rate-limit');
    });
  });

  describe('Advanced Commands', () => {
    it('should register all advanced commands', () => {
      const program = new Command();
      registerAdvancedCommands(program);

      const commands = program.commands.map(c => c.name());
      expect(commands).toContain('search');
      expect(commands).toContain('export');
      expect(commands).toContain('import');
      expect(commands).toContain('optimize');
      expect(commands).toContain('delete');
    });

    it('should have correct delete options', () => {
      const program = new Command();
      registerAdvancedCommands(program);

      const deleteCmd = program.commands.find(c => c.name() === 'delete');
      expect(deleteCmd).toBeDefined();

      const optionNames = deleteCmd!.options.map(o => o.long);
      expect(optionNames).toContain('--backend');
      expect(optionNames).toContain('--yes');
      expect(optionNames).toContain('--since');
      expect(optionNames).toContain('--until');
    });
  });
});
