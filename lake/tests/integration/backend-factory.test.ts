import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'fs';
import { createBackend } from '../../src/index.js';
import type { BackendConfig } from '../../src/types.js';

describe('Backend Factory', () => {
  const testDbPath = './test-factory.db';

  afterEach(async () => {
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
  });

  it('should create SQLite backend', async () => {
    const config: BackendConfig = { type: 'sqlite', path: testDbPath };
    const backend = await createBackend(config);

    expect(backend.type).toBe('sqlite');
    await backend.connect();
    expect(backend.state).toBe('connected');
    await backend.disconnect();
  });

  it('should create Parquet backend', async () => {
    const config: BackendConfig = { type: 'parquet', path: './test-parquet' };
    const backend = await createBackend(config);

    expect(backend.type).toBe('parquet');
    await backend.connect();
    expect(backend.state).toBe('connected');
    await backend.disconnect();

    // Cleanup
    await fs.promises.rm('./test-parquet', { recursive: true, force: true });
  });

  it('should throw for unknown backend type', async () => {
    const config = { type: 'unknown' } as any;
    await expect(createBackend(config)).rejects.toThrow('Unknown backend');
  });
});
