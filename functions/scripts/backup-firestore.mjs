import fs from 'node:fs/promises';
import path from 'node:path';
import admin from 'firebase-admin';
import { fileURLToPath } from 'node:url';
import { loadActiveProfile } from '../../lib/profile-config.js';
import { serializeData } from './utils/firestore-serialization.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '../..');
const DEFAULT_BACKUP_DIR = path.resolve(ROOT_DIR, 'backups');

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

function getArgValue(flag) {
  const index = process.argv.indexOf(flag);
  if (index === -1) return null;
  return process.argv[index + 1] || null;
}

function resolveOutputPath() {
  const explicitPath = getArgValue('--out');
  if (explicitPath) {
    return path.resolve(explicitPath);
  }
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  return path.resolve(DEFAULT_BACKUP_DIR, `firestore-backup-${timestamp}.json`);
}

async function exportCollection(collectionRef, documents) {
  const snapshot = await collectionRef.get();
  for (const docSnap of snapshot.docs) {
    const data = serializeData(docSnap.data());
    documents.push({
      path: docSnap.ref.path,
      data
    });
    const subcollections = await docSnap.ref.listCollections();
    for (const sub of subcollections) {
      await exportCollection(sub, documents);
    }
  }
}

async function collectDocuments() {
  const documents = [];
  const collections = await db.listCollections();
  collections.sort((a, b) => a.id.localeCompare(b.id));
  for (const collection of collections) {
    await exportCollection(collection, documents);
  }
  documents.sort((a, b) => a.path.localeCompare(b.path));
  return documents;
}

async function ensureDirectory(filePath) {
  const dir = path.dirname(filePath);
  await fs.mkdir(dir, { recursive: true });
}

async function writeBackup(filePath, payload) {
  const json = JSON.stringify(payload, null, 2);
  await ensureDirectory(filePath);
  await fs.writeFile(filePath, `${json}\n`, 'utf8');
}

async function main() {
  const outputPath = resolveOutputPath();
  console.log(`Creating Firestore backup for project ${projectId}...`);
  const documents = await collectDocuments();
  const payload = {
    version: 1,
    projectId,
    createdAt: new Date().toISOString(),
    documentCount: documents.length,
    documents
  };
  await writeBackup(outputPath, payload);
  console.log(`Backup complete. Saved ${documents.length} documents to ${outputPath}`);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Backup failed', err);
    process.exit(1);
  });
