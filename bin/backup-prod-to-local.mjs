#!/usr/bin/env node
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '..');
const BACKUP_PATH = path.resolve(ROOT_DIR, 'backups', 'prod-latest.json');

function run(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: 'inherit',
      cwd: ROOT_DIR,
      ...options
    });
    child.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Command "${command} ${args.join(' ')}" exited with code ${code}`));
      }
    });
  });
}

async function main() {
  console.log('Switching to prod profile...');
  await run('node', ['bin/use-profile.mjs', 'prod']);

  console.log('Backing up prod Firestore...');
  await run('node', ['functions/scripts/backup-firestore.mjs', '--out', BACKUP_PATH]);

  console.log('Switching to local profile...');
  await run('node', ['bin/use-profile.mjs', 'local']);

  console.log('Restoring backup into local profile...');
  await run('node', ['functions/scripts/restore-firestore.mjs', '--in', BACKUP_PATH, '--drop-existing']);

  console.log('\nDone! Local Firestore emulates the latest prod backup.');
  console.log(`Backup file: ${BACKUP_PATH}`);
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
