#!/usr/bin/env node
import { existsSync } from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = path.join(rootDir, 'emulator-data');
const args = ['emulators:start', '--project', 'local', `--export-on-exit=${dataDir}`];
if (existsSync(path.join(dataDir, 'firebase-export-metadata.json'))) {
  args.push(`--import=${dataDir}`);
  console.log(`Loading saved dev data from ${dataDir}`);
} else {
  console.log('No saved dev snapshot yet. Data will be saved on graceful exit.');
}
const child = spawn('firebase', args, { cwd: rootDir, stdio: 'inherit' });
// The CLI receives terminal signals too. Let it finish exporting before exiting.
process.on('SIGINT', () => {});
process.on('SIGTERM', () => child.kill('SIGTERM'));
child.on('error', (error) => {
  console.error(error.message);
  process.exitCode = 1;
});
child.on('exit', (code) => {
  process.exitCode = code ?? 1;
});
