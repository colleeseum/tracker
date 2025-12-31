import fs from 'node:fs/promises';
import path from 'node:path';
import admin from 'firebase-admin';
import { fileURLToPath } from 'node:url';
import { loadActiveProfile } from '../../lib/profile-config.js';
import { deserializeData } from './utils/firestore-serialization.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

function getArgValue(flag) {
  const index = process.argv.indexOf(flag);
  if (index === -1) return null;
  return process.argv[index + 1] || null;
}

function requireInputPath() {
  const inputFlag = getArgValue('--in');
  if (!inputFlag) {
    throw new Error('Missing --in <path> argument for restore script.');
  }
  return path.resolve(inputFlag);
}

function shouldDropExisting() {
  return process.argv.includes('--drop-existing');
}

async function loadBackup(filePath) {
  const raw = await fs.readFile(filePath, 'utf8');
  const payload = JSON.parse(raw);
  if (!Array.isArray(payload?.documents)) {
    throw new Error('Invalid backup file: missing "documents" array.');
  }
  return payload;
}

async function deleteCollection(collectionRef) {
  const snapshot = await collectionRef.get();
  for (const docSnap of snapshot.docs) {
    const subcollections = await docSnap.ref.listCollections();
    for (const sub of subcollections) {
      await deleteCollection(sub);
    }
    await docSnap.ref.delete();
  }
}

async function purgeDatabase() {
  const collections = await db.listCollections();
  for (const collection of collections) {
    await deleteCollection(collection);
  }
}

async function writeDocuments(documents) {
  const chunkSize = 400;
  for (let i = 0; i < documents.length; i += chunkSize) {
    const batch = db.batch();
    const slice = documents.slice(i, i + chunkSize);
    slice.forEach((entry) => {
      const docRef = db.doc(entry.path);
      const payload = deserializeData(entry.data, db);
      batch.set(docRef, payload, { merge: false });
    });
    await batch.commit();
    console.log(`Restored ${Math.min(i + chunkSize, documents.length)} / ${documents.length} documents...`);
  }
}

async function main() {
  const inputPath = requireInputPath();
  const dropExisting = shouldDropExisting();
  console.log(`Restoring Firestore data from ${inputPath} into project ${projectId}...`);
  const backup = await loadBackup(inputPath);
  if (backup.projectId && backup.projectId !== projectId) {
    console.warn(
      `Warning: backup was created for project ${backup.projectId}, but active profile is ${projectId}.`
    );
  }
  if (dropExisting) {
    console.log('Dropping all existing documents before restore (--drop-existing enabled).');
    await purgeDatabase();
  }
  const documents = Array.from(backup.documents).sort((a, b) => a.path.localeCompare(b.path));
  await writeDocuments(documents);
  console.log(`Restore complete. Loaded ${documents.length} documents.`);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Restore failed', err);
    process.exit(1);
  });
